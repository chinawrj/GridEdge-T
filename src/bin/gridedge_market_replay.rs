use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::Parser;
use gridedge_t::{data::MarketBar, web_market::WebTradeBarBuilder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

#[derive(Debug, Parser)]
#[command(about = "Replay canonical MQTT market bytes into audited five-minute bars")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value_t = 5)]
    interval_minutes: u32,
    #[arg(long)]
    session_date: NaiveDate,
    /// Optional canonical CSV for feeding the exact rebuilt bars into the strategy replay.
    #[arg(long)]
    bars_csv_output: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    topic: String,
    #[serde(default)]
    payload_hex: Option<String>,
    #[serde(default)]
    payload: Option<Vec<u8>>,
}

impl WireRecord {
    fn payload_bytes(self, line_index: usize) -> Result<(String, Vec<u8>)> {
        let payload = match (self.payload_hex, self.payload) {
            (Some(value), None) => hex::decode(value)
                .with_context(|| format!("invalid payload hex on line {line_index}"))?,
            (None, Some(value)) => value,
            _ => bail!("line {line_index} must contain exactly one payload representation"),
        };
        Ok((self.topic, payload))
    }
}

#[derive(Debug, Serialize)]
struct ReplayAudit {
    schema: &'static str,
    symbol: String,
    session_date: NaiveDate,
    interval_minutes: u32,
    input_event_count: usize,
    covered_through_us: u64,
    bar_count: usize,
    bars_sha256: String,
    bars: Vec<MarketBar>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let input = std::fs::File::open(&args.input)
        .with_context(|| format!("failed to open {}", args.input.display()))?;
    let mut builder = WebTradeBarBuilder::new(args.symbol.clone(), args.interval_minutes)?;
    let mut bars = Vec::new();
    let mut input_event_count = 0usize;
    for (line_index, line) in BufReader::new(input).lines().enumerate() {
        let line = line.with_context(|| format!("failed to read input line {}", line_index + 1))?;
        if line.trim().is_empty() {
            bail!("market replay input contains an empty line")
        }
        let record: WireRecord = serde_json::from_str(&line)
            .with_context(|| format!("invalid market replay line {}", line_index + 1))?;
        let (topic, payload) = record.payload_bytes(line_index + 1)?;
        bars.extend(builder.ingest(&topic, &payload)?);
        input_event_count = input_event_count
            .checked_add(1)
            .context("market replay event count overflow")?;
    }
    if input_event_count == 0 || bars.is_empty() {
        bail!("market replay produced no auditable bars")
    }
    if !builder.has_complete_session(args.session_date) {
        bail!("market replay lacks a complete-session watermark")
    }
    if bars
        .windows(2)
        .any(|pair| pair[0].timestamp >= pair[1].timestamp)
    {
        bail!("market replay bars are not strictly increasing")
    }
    let covered_through_us = builder
        .covered_through_us()
        .context("market replay lacks a source completion watermark")?;
    let bars_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&bars)?));
    let report = ReplayAudit {
        schema: "gridedge.market-replay-audit.v1",
        symbol: args.symbol,
        session_date: args.session_date,
        interval_minutes: args.interval_minutes,
        input_event_count,
        covered_through_us,
        bar_count: bars.len(),
        bars_sha256,
        bars,
    };
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)
        .with_context(|| format!("refusing to overwrite {}", args.output.display()))?;
    serde_json::to_writer_pretty(&mut output, &report)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    if let Some(path) = args.bars_csv_output {
        if path == args.output {
            bail!("market replay audit and bar CSV outputs must be different")
        }
        let mut csv_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| format!("refusing to overwrite {}", path.display()))?;
        {
            let mut writer = csv::Writer::from_writer(&mut csv_file);
            for bar in &report.bars {
                writer.serialize(bar)?;
            }
            writer.flush()?;
        }
        csv_file.sync_all()?;
    }
    println!(
        "market replay complete: events={} bars={} covered_through_us={} bars_sha256={}",
        report.input_event_count, report.bar_count, report.covered_through_us, report.bars_sha256
    );
    Ok(())
}
