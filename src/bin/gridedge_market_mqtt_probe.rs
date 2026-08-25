use anyhow::{bail, Context, Result};
use clap::Parser;
use gridedge_t::market_mqtt::MarketMqttClient;
use std::{path::PathBuf, time::Duration};

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 8883)]
    port: u16,
    #[arg(long, default_value = "gridedge-publisher")]
    username: String,
    #[arg(long)]
    password_file: PathBuf,
    #[arg(long)]
    ca_file: PathBuf,
    #[arg(long, default_value = "gridedge-paper-002256")]
    client_id: String,
    #[arg(long, default_value = "XSHE")]
    venue: String,
    #[arg(long, default_value = "002256")]
    symbol: String,
    #[arg(long, default_value_t = 0)]
    hold_seconds: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.hold_seconds > 120 {
        bail!("hold-seconds must not exceed 120")
    }
    let mut client = MarketMqttClient::connect(
        &args.host,
        args.port,
        &args.username,
        &args.password_file,
        &args.ca_file,
        &args.client_id,
        &args.venue,
        &args.symbol,
    )?;
    client.await_ready(Duration::from_secs(10))?;
    println!("market MQTT 5 subscriptions ready");
    if args.hold_seconds > 0 {
        std::thread::sleep(Duration::from_secs(args.hold_seconds));
        let message = client
            .receive(Duration::from_secs(10))?
            .context("market MQTT pump buffered no message while the main thread was held")?;
        println!(
            "market MQTT background pump buffered topic={} bytes={}",
            message.topic,
            message.payload.len()
        );
    }
    Ok(())
}
