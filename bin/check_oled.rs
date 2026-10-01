#[path = "../src/config.rs"]
#[allow(unused)]
mod config;

cfg_select! {
    feature = "simu" => {
        #[path = "../src/display/simu.rs"]
        mod simu;
        pub use simu::*;
    }
    _ => {
        #[path = "../src/display/driver.rs"]
        #[allow(unused)]
        mod driver;
        pub use driver::*;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let line1 = "|> line 1 test".to_string();
    let line2 = "|| line 2 test".to_string();

    let mut oled = Hd44780::new(&config::DisplayConfig::default()).await?;

    oled.write_line(LineNb::One, line1.chars()).await;
    oled.write_line(LineNb::Two, line2.chars()).await;

    tokio::signal::ctrl_c().await?;
    oled.off().await;

    Ok(())
}
