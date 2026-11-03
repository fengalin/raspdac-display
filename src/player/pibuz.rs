//! SSE-based pibuz status listener
//!
//! pibuz status is tracked on its SSE socket.
//! `GET /api/now-playing` supplies the initial snapshot on connect.
use std::borrow::Cow;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use super::{NamedPlayerNotification, PlaybackState, PlayerData, PlayerNotification};

/// The pibuz daemon listens on this address (same machine).
const PIBUZ_ADDR: &str = "127.0.0.1:8182";
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// A pibuz `PlaybackState` value as serialized on the SSE stream.
///
/// `Loading` is a pibuz-specific state (buffering); for the display it behaves
/// like a playing track, so it maps to [`PlaybackState::Playing`].
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
enum PibuzState {
    #[default]
    Stopped,
    Playing,
    Paused,
    Loading,
}

impl From<PibuzState> for PlaybackState {
    fn from(s: PibuzState) -> Self {
        match s {
            PibuzState::Stopped => PlaybackState::Stopped,
            PibuzState::Playing | PibuzState::Loading => PlaybackState::Playing,
            PibuzState::Paused => PlaybackState::Paused,
        }
    }
}

/// One SSE frame, parsed out of the `event:` / `data:` lines.
#[derive(Debug, PartialEq)]
struct SseEvent {
    event: String,
    data: String,
}

/// Payload of `TrackStarted` — we only read what the display needs.
#[derive(Debug, Deserialize)]
struct TrackStartedData {
    #[serde(default)]
    track: Track,
    #[serde(default)]
    position_secs: u64,
}

#[derive(Debug, Default, Deserialize)]
struct Track {
    #[serde(default)]
    title: String,
    #[serde(default)]
    duration_secs: u64,
}

/// Payload of `PositionUpdated`.
#[derive(Debug, Deserialize)]
struct PositionUpdatedData {
    #[serde(default)]
    position_secs: u64,
    #[serde(default)]
    duration_secs: u64,
}

/// Payload of `PlaybackStateChanged`.
#[derive(Debug, Deserialize)]
struct StateChangedData {
    state: PibuzState,
}

#[derive(Debug, thiserror::Error)]
pub enum PibuzError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("channel error: {0}")]
    Channel(#[from] tokio::sync::mpsc::error::SendError<NamedPlayerNotification>),
}

/// Per-player state update sent to the aggregator.
#[derive(Debug)]
pub struct PibuzPlayer {
    name: &'static str,
    player_notif_tx: mpsc::Sender<NamedPlayerNotification>,
    state: PlaybackState,
    data: PlayerData,
    notify_listener_termination: bool,
}

impl PibuzPlayer {
    pub fn new(name: &'static str, player_notif_tx: mpsc::Sender<NamedPlayerNotification>) -> Self {
        PibuzPlayer {
            name,
            player_notif_tx,
            state: PlaybackState::Stopped,
            data: Default::default(),
            notify_listener_termination: false,
        }
    }

    fn clear(&mut self) {
        self.state = PlaybackState::Stopped;
        self.notify_listener_termination = false;
        self.data.clear();
    }

    /// Run the listener loop for one player, reconnecting when it ends.
    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                biased;
                _  = stop_rx.recv() => {
                    info!(player = %self.name, "shutting down due to stop request");
                    break;
                }
                _ = self.connect() => (),
            }
        }
    }

    async fn connect(&mut self) -> Result<(), PibuzError> {
        loop {
            let res = self.listen().await;

            if res.is_ok() {
                info!(player = %self.name, "stream closed");
            }

            if self.notify_listener_termination {
                if let Err(err) = res {
                    debug!(player = %self.name, %err, "listener")
                }

                self.clear();
                self.player_notif_tx
                    .send(NamedPlayerNotification {
                        player_name: self.name,
                        notif: PlayerNotification::Update {
                            state: self.state,
                            data: self.data.clone(),
                        },
                    })
                    .await
                    .inspect_err(|err| error!(%err, "Player notif channel closed"))?;
            }

            tokio::time::sleep(RECONNECT_TIMEOUT).await;
        }
    }

    async fn listen(&mut self) -> Result<(), PibuzError> {
        let mut resp = reqwest::Client::new()
            .get(format!("http://{PIBUZ_ADDR}/api/events"))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await?;

        info!(player = %self.name, "connected SSE");
        self.notify_listener_termination = true;

        let mut sse = Sse::default();
        let mut events = vec![];

        while let Some(chunk) = resp.chunk().await? {
            sse.ingest(&chunk, &mut events);
            for ev in events.drain(..) {
                self.on_event(ev).await?;
            }
        }

        info!(player = %self.name, "event stream terminated");
        Ok(())
    }

    async fn on_event(&mut self, ev: SseEvent) -> Result<(), PibuzError> {
        let data: Value = match serde_json::from_str(&ev.data) {
            Ok(v) => v,
            Err(err) => {
                warn!(player = %self.name, event = %ev.event, %err, "malformed event data");
                return Ok(());
            }
        };

        let Some(inner) = data.get("data").cloned() else {
            warn!(player = %self.name, event = %ev.event, "event without a data field");
            return Ok(());
        };

        let must_notify = match ev.event.as_str() {
            "PlaybackStateChanged" => {
                let Ok(StateChangedData { state }) = serde_json::from_value(inner) else {
                    warn!(player = %self.name, "PlaybackStateChanged with bad data");
                    return Ok(());
                };
                let new_state = state.into();
                if self.state != new_state {
                    debug!(
                        player = %self.name,
                        old = ?self.state, new = ?new_state,
                        "new status",
                    );
                    self.state = new_state;
                    if new_state == PlaybackState::Playing {
                        self.data.base_position_instant = Instant::now();
                    }
                    true
                } else {
                    false
                }
            }
            "TrackStarted" => {
                let Ok(TrackStartedData {
                    track,
                    position_secs,
                }) = serde_json::from_value(inner)
                else {
                    warn!(player = %self.name, "TrackStarted with bad data");
                    return Ok(());
                };

                let mut notify = false;
                if !track.title.is_empty() && self.data.title.as_ref() != track.title {
                    debug!(player = %self.name, old = %self.data.title, new = %track.title, "new title");
                    self.data.title = track.title.into();
                    notify = true;
                }

                let duration = track.duration_secs as u16;
                if self.data.duration_s != duration {
                    debug!(player = %self.name, old = %self.data.duration_s, new = %duration, "new duration");
                    self.data.duration_s = duration;
                    notify = true;
                }

                self.data.base_position_s = position_secs as u16;
                self.data.base_position_instant = Instant::now();

                notify
            }
            "PositionUpdated" => {
                let Ok(PositionUpdatedData {
                    position_secs,
                    duration_secs,
                }) = serde_json::from_value(inner)
                else {
                    warn!(player = %self.name, "PositionUpdated with bad data");
                    return Ok(());
                };

                let now = Instant::now();
                let position_s = position_secs as u16;
                self.data.base_position_s = position_s;
                self.data.base_position_instant = now;

                let duration_s = duration_secs as u16;
                if self.data.duration_s != duration_s {
                    self.data.duration_s = duration_s;
                }

                debug!(player = %self.name, position = %self.data.base_position_s, "position");
                self.player_notif_tx
                    .send(NamedPlayerNotification {
                        player_name: self.name,
                        notif: PlayerNotification::BasePosition {
                            position_s,
                            instant: now,
                        },
                    })
                    .await?;
                false
            }
            _ => false,
        };

        if must_notify {
            self.send_update().await?;
        }

        Ok(())
    }

    async fn send_update(&mut self) -> Result<(), PibuzError> {
        self.player_notif_tx
            .send(NamedPlayerNotification {
                player_name: self.name,
                notif: PlayerNotification::Update {
                    state: self.state,
                    data: self.data.clone(),
                },
            })
            .await?;

        Ok(())
    }
}

/// Minimal SSE parser: buffers incoming bytes, splits them into complete
/// frames on the blank-line terminator, and dispatches each frame.
///
/// A frame is a run of `field: value` lines terminated by an empty line. The
/// pibuz daemon uses single-line `data:` fields, but multi-line `data:` is
/// handled per the SSE spec (join with `\n`). Comments (`: …`) carry the
/// `: pibuz event stream` prime and `: lagged N event(s)` notices.
///
/// The buffer is a `String` holding only the *incomplete trailing line* (a
/// frame is at most a few hundred bytes, so this stays tiny). Each chunk is
/// borrowed, not copied: `chunk()` hands back a `Bytes` view into the
/// transport's buffer, which `ingest` scans in one O(n) pass — validating
/// UTF-8 (via [`<&[u8]>::utf8_chunks`]) and splitting lines in the same loop.
#[derive(Default)]
struct Sse {
    /// The incomplete trailing line left over from the previous chunk (no
    /// terminating `\n`). A frame is at most a few hundred bytes, so this
    /// stays tiny — it is the only line state that persists between chunks.
    buf: String,
    event: String,
    data: Vec<String>,
    /// A multi-byte UTF-8 sequence split across a chunk boundary. At most 3
    /// bytes: a sequence is ≤ 4 bytes, and the next chunk's first byte decides
    /// its fate. Persists between chunks, like `buf`.
    seq: [u8; 3],
    seq_len: usize,
}

impl Sse {
    /// Feed a chunk of the body. Returns every `(event, data)` pair whose
    /// terminating blank line has now been seen, in order. An empty `Vec`
    /// means the chunk completed no frame (the open line is kept in
    /// `self.buf`).
    ///
    /// One O(n) pass: the stream comes from an external source, so the bytes
    /// are NOT assumed to be valid UTF-8. They are validated by the stdlib's
    /// [`<&[u8]>::utf8_chunks`], which hands back the maximal valid
    /// subsequences alongside the invalid bytes; the line state machine runs
    /// over the decoded characters in the same loop. Genuinely broken bytes
    /// become U+FFFD. A sequence merely *split across a chunk boundary* is
    /// held in `self.seq` (at most 3 bytes) and reassembled with the next
    /// chunk. Line endings are `\n`, `\r\n`, or a lone `\r`, per the SSE
    /// spec; the incomplete trailing line persists in `self.buf`.
    fn ingest(&mut self, bytes: &[u8], out: &mut Vec<SseEvent>) {
        out.clear();

        let mut item = String::new(); // this chunk's part of the line in progress
        let mut pending_cr = false; // a `\r` that may still be a CRLF
        let mut bad = 0; // broken (non-encodable) bytes seen in this chunk
        // Signal streams.
        let mut seq: [u8; 3] = self.seq;
        let mut seq_len = self.seq_len;

        // One decoded character: terminators per the SSE spec, everything else
        // accumulates in the open line. `\r` is always a terminator (flush the
        // line) and arms `pending_cr` so a following `\n` is swallowed (CRLF is
        // ONE terminator, not two); a lone `\n` also flushes. A terminator
        // processes the full line — the part carried over in `self.buf` plus
        // this chunk's part — and resets both.
        let mut handle = |ch: char| {
            match ch {
                '\r' => {
                    self.buf.push_str(&item);
                    let line = std::mem::take(&mut self.buf);
                    if let Some(ev) = self.line(&line) {
                        out.push(ev);
                    }
                    item.clear();
                    pending_cr = true; // a following `\n` is part of the CRLF
                }
                '\n' if pending_cr => {
                    // The `\n` of a CRLF: the `\r` already flushed the line.
                    pending_cr = false;
                }
                '\n' => {
                    self.buf.push_str(&item);
                    let line = std::mem::take(&mut self.buf);
                    if let Some(ev) = self.line(&line) {
                        out.push(ev);
                    }
                    item.clear();
                }
                _ => item.push(ch),
            }
        };

        // Total length of the sequence a lead byte announces (0 = not a lead).
        fn seq_len_of(lead: u8) -> usize {
            match lead {
                0b110_00000..=0b110_11111 => 2,
                0b1110_0000..=0b1110_1111 => 3,
                0b1111_0000..=0b1111_0100 => 4,
                _ => 0,
            }
        }

        // An invalid run is a *sequence split across a chunk boundary* (to be
        // held) iff it is a proper prefix of an encodable sequence: a lead byte
        // that announces more bytes than the run holds, followed only by
        // continuation bytes.
        fn is_holdable_prefix(run: &[u8]) -> bool {
            seq_len_of(run[0]) > run.len() && run[1..].iter().all(|&b| (0x80..=0xbf).contains(&b))
        }

        // A multi-byte sequence split across the previous chunk boundary (≤ 3
        // bytes) is held in `seq`. Its continuation bytes are in `bytes`, but
        // they are not addressable as raw bytes: a valid run is a string of
        // complete characters, and its first bytes are not necessarily
        // continuation bytes. So we prepend the held bytes to `bytes` and
        // re-validate the combined buffer: `utf8_chunks` puts the lead byte and
        // its continuations in one context, and the completed character comes
        // back in `valid()`. A held run that is broken (lead byte cannot
        // encode, or the continuations do not follow) comes back in `invalid()`
        // as a run starting with the held bytes.
        //
        // `to_scan` is the buffer the single pass below runs over: `bytes`
        // as-is when nothing is held, or the held bytes prepended to `bytes`
        // when a sequence is open. `owned` holds that combined buffer only in
        // the held-bytes case (rare — once per split sequence); it is a `Cow`
        // so the no-hold path borrows `bytes` without a copy.
        let owned = if seq_len > 0 && seq_len_of(seq[0]) > 0 {
            // An encodable held sequence: combine and re-validate below.
            let mut combined = vec![0u8; seq_len + bytes.len()];
            combined[..seq_len].copy_from_slice(&seq[..seq_len]);
            combined[seq_len..].copy_from_slice(bytes);
            seq_len = 0; // settled by the scan below
            Cow::Owned(combined)
        } else if seq_len > 0 {
            // The held lead can never encode a sequence: replace it (and its
            // continuation bytes, which are all broken) before scanning.
            bad += seq_len;
            seq_len = 0;
            handle('\u{FFFD}');
            Cow::Borrowed(bytes)
        } else {
            Cow::Borrowed(bytes)
        };
        let to_scan: &[u8] = &owned;

        // `utf8_chunks` never yields a partial sequence in `valid()` — a run
        // left open at the end of a slice is reported in `invalid()`, exactly
        // like a broken one. Only the run the scan ENDS on can be a boundary
        // split; every earlier one is definitively broken. `last_invalid`
        // tracks that end-run (a valid run resets it) and is settled after the
        // loop: held in `seq` if it can still complete, else FFFD'd.
        let mut last_invalid: Option<&[u8]> = None;

        for chunk in to_scan.utf8_chunks() {
            // The valid run (every character complete) and the invalid run (an
            // earlier one is broken now; a run the scan ends on may still
            // complete in the next chunk).
            if chunk.invalid().is_empty() {
                if let Some(prev) = last_invalid {
                    bad += prev.len();
                    handle('\u{FFFD}');
                }
                last_invalid = None;
                for ch in chunk.valid().chars() {
                    handle(ch);
                }
            } else {
                if let Some(prev) = last_invalid {
                    bad += prev.len();
                    handle('\u{FFFD}');
                }
                last_invalid = Some(chunk.invalid());
            }
        }
        // Settle the run the scan ended on: hold it if it can still complete in
        // the next chunk (a proper prefix of an encodable sequence), otherwise
        // it is broken — one U+FFFD per run.
        if let Some(invalid) = last_invalid {
            if is_holdable_prefix(invalid) {
                seq[..invalid.len()].copy_from_slice(invalid);
                seq_len = invalid.len();
            } else {
                bad += invalid.len();
                handle('\u{FFFD}');
            }
        }

        if bad != 0 {
            warn!("SSE chunk contained malformed UTF-8; replaced with U+FFFD");
        }

        // Persist the cross-chunk state.
        self.seq = seq;
        self.seq_len = seq_len;
        if !item.is_empty() {
            self.buf.push_str(&item);
            item.clear();
        }
    }

    /// Handle one decoded line. Returns the event when this line is the blank
    /// line that terminates a non-empty event block.
    fn line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            // Blank line = end of the event block. Per the SSE spec, the event
            // is dispatched only if `data` was set (a block with no `data`
            // produces nothing); the `event` field merely names it.
            let data = self.data.join("\n");
            let event = std::mem::take(&mut self.event);
            self.data.clear();
            return if data.is_empty() {
                None
            } else {
                Some(SseEvent { event, data })
            };
        }
        if let Some(rest) = line.strip_prefix(':') {
            // Comment: the stream prime or a lagged-events notice.
            debug!("sse comment: {rest}");
            return None;
        }
        match line.split_once(':') {
            Some((field, value)) => match field {
                "event" => {
                    self.event = value.trim().to_string();
                }
                "data" => {
                    // Per spec: strip exactly one leading space.
                    let value = value.strip_prefix(' ').unwrap_or(value);
                    self.data.push(value.to_string());
                }
                // not playing => don't observe ticks
                // resubmission).
                _ => {}
            },
            None => {
                // A bare field name is `field: ""` per the SSE spec.
                if line == "event" {
                    self.event.clear();
                } else if line == "data" {
                    self.data.push(String::new());
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{Sse, SseEvent};

    fn run_case(bytes: &[&[u8]]) -> Vec<(String, String)> {
        let mut sse = Sse::default();
        let mut events = vec![];
        let mut got = Vec::new();
        for b in bytes {
            sse.ingest(b, &mut events);
            for ev in events.drain(..) {
                got.push((ev.event, ev.data));
            }
        }
        got
    }

    #[test]
    fn frames_split_across_chunks_are_reassembled() {
        let got = run_case(&[
            b": pibuz event stream\n\neven",
            b"t: PositionUpdated\ndata: {\"t",
            b"ype\":\"PositionUpdated\",\"data\":{\"position_secs\":42}}\n\nevent: Pla",
            b"ybackStateChanged\ndata: {\"type\":\"PlaybackStateChanged\",\"data\":{\"state\":\"Paused\"}}\n",
            b"\n",
        ]);
        assert_eq!(
            got,
            vec![
                (
                    "PositionUpdated".into(),
                    r#"{"type":"PositionUpdated","data":{"position_secs":42}}"#.into(),
                ),
                (
                    "PlaybackStateChanged".into(),
                    r#"{"type":"PlaybackStateChanged","data":{"state":"Paused"}}"#.into(),
                ),
            ]
        );
    }

    #[test]
    fn crlf_and_bare_fields_are_handled() {
        let got = run_case(&[b"event: VolumeChanged\r\ndata\r\ndata: 0.5\r\n\r\n"]);
        assert_eq!(got, vec![("VolumeChanged".into(), "\n0.5".into())]);
    }

    #[test]
    fn incomplete_frame_is_not_emitted_until_blank_line() {
        let mut sse = Sse::default();
        let mut events = vec![];
        // A full `event:`/`data:` pair but no terminating blank line yet:
        // nothing may be emitted (this is what caused the "incomplete bulks").
        sse.ingest(
            b"event: TrackStarted\ndata: {\"type\":\"TrackStarted\",\"data\":{\"track\":{\"id\":1}}}",
            &mut events,
        );
        assert!(events.is_empty());

        sse.ingest(b"\n\n", &mut events);
        assert_eq!(
            events,
            [SseEvent {
                event: "TrackStarted".into(),
                data: r#"{"type":"TrackStarted","data":{"track":{"id":1}}}"#.into(),
            }]
        );
    }

    #[test]
    fn crlf_split_across_chunks_is_single_event() {
        // CRLF whose `\r` and `\n` land in different chunks must still count
        // as ONE blank-line terminator (no double-emit, no lost event).
        let mut sse = Sse::default();
        let mut events = vec![];
        sse.ingest(b"event: VolumeChanged\r\ndata: 0.5\r", &mut events);
        assert!(events.is_empty());

        sse.ingest(b"\n", &mut events);
        assert_eq!(
            events,
            [SseEvent {
                event: "VolumeChanged".into(),
                data: "0.5".into(),
            }]
        );
    }

    #[test]
    fn malformed_utf8_is_replaced_not_panicked() {
        // The stream is external: invalid UTF-8 must be replaced with U+FFFD
        // in place, never assumed valid. One U+FFFD per broken run, as in
        // `String::from_utf8_lossy`. The data value is FFFD FFFD `0.5`.
        let got = run_case(&[
            b"event: VolumeChanged\r\ndata: ",
            b"\xff\x80", // bad lead, lone continuation
            b"0.5\r\n\r\n",
        ]);
        assert_eq!(
            got,
            [("VolumeChanged".into(), "\u{FFFD}\u{FFFD}0.5".into(),)]
        );
    }

    #[test]
    fn multibyte_sequence_split_across_chunks_is_reassembled() {
        // Chunk boundaries are byte-level: a multi-byte character can be split
        // across chunks. The decoder must hold the lead bytes and reassemble,
        // not replace them.
        // é = C3 A9 ; • = E2 80 A2 ; 🎵 = F0 9F 8E B5
        // The data is `Caf` + é + ` m` + • + `u` + 🎵. Each multi-byte char is
        // split across a chunk boundary; the continuation byte leads the next
        // chunk (the byte-level boundary falls mid-sequence).
        let got = run_case(&[
            b"event: TrackStarted\r\ndata: Caf",
            &[0xC3],             // é split 1+1
            &[0xA9, 0x20, 0x6D], // A9 completes é, then " m"
            &[0xE2, 0x80],       // • split 2+1
            &[0xA2, 0x75],       // A2 completes •, then "u"
            &[0xF0, 0x9F],       // 🎵 split 2+2
            &[0x8E, 0xB5],       // completes 🎵
            b"\r\n\r\n",
        ]);
        assert_eq!(
            got,
            vec![(
                "TrackStarted".into(),
                "Caf\u{e9} m\u{2022}u\u{1F3B5}".into(),
            )]
        );
    }
}
