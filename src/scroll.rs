//! Ping-pong scrolling algorithm with edge dwell.
//!
//! For each display row, this module tracks a fractional scroll offset and
//! updates the visible window at each animation tick.

/// Direction of scrolling.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Direction {
    /// Scrolling right (text moves left, offset increases)
    Right,
    /// Scrolling left (text moves right, offset decreases)
    Left,
}

/// State of a scrolling row.
#[derive(Debug, PartialEq)]
pub struct ScrollState {
    /// Text to scroll (sanitized bytes).
    pub text: Vec<u8>,
    /// Number of visible character cells.
    pub width: usize,
    /// Fractional scroll offset (in cells). 0 = text starts at left edge.
    pub offset: f32,
    /// Scroll speed in cells per second.
    pub speed: f32,
    /// Seconds to pause at each edge before reversing direction.
    pub dwell_secs: f32,
    /// Current scroll direction.
    pub direction: Direction,
    /// Remaining dwell time at the current edge.
    pub dwell_remaining: f32,
    /// Last visible window start index (for dirty-page detection).
    pub last_window: usize,
}

impl ScrollState {
    /// Create a new scroll state. Panics if width is 0 or text is empty.
    pub fn new(text: Vec<u8>, width: usize, speed: f32, dwell_secs: f32) -> Self {
        assert!(width > 0);
        assert!(!text.is_empty());
        ScrollState {
            text,
            width,
            offset: 0.0,
            speed,
            dwell_secs,
            direction: Direction::Right,
            dwell_remaining: dwell_secs,
            last_window: 0,
        }
    }

    /// Update the scroll offset for one tick.
    /// Returns true if the integer window index changed (row is dirty).
    pub fn tick(&mut self, dt: f32) -> bool {
        if self.text.len() <= self.width {
            // No scrolling needed, static display
            return false;
        }

        self.dwell_remaining -= dt;

        let max_offset = (self.text.len() - self.width) as f32;

        if max_offset <= 0.0 {
            return false;
        }

        match self.direction {
            Direction::Right => {
                if self.dwell_remaining > 0.0 {
                    // Still dwelling at left edge
                    return false;
                }
                self.offset += self.speed * dt;
                if self.offset >= max_offset {
                    self.offset = max_offset;
                    self.direction = Direction::Left;
                    self.dwell_remaining = self.dwell_secs;
                }
            }
            Direction::Left => {
                if self.dwell_remaining > 0.0 {
                    // Still dwelling at right edge
                    return false;
                }
                self.offset -= self.speed * dt;
                if self.offset <= 0.0 {
                    self.offset = 0.0;
                    self.direction = Direction::Right;
                    self.dwell_remaining = self.dwell_secs;
                }
            }
        }

        let window = self.offset as usize;
        let dirty = window != self.last_window;
        self.last_window = window;
        dirty
    }

    /// Get the visible characters for the current window.
    pub fn visible(&self) -> &[u8] {
        let window_start = self.offset as usize;
        let end = (window_start + self.width).min(self.text.len());
        if window_start >= self.text.len() {
            &[]
        } else {
            &self.text[window_start..end]
        }
    }
}
