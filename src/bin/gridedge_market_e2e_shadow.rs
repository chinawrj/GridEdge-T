use anyhow::{bail, Context, Result};
use chrono::{Local, Utc};
use clap::Parser;
use gridedge_t::{data::MarketBar, market_mqtt::MarketMqttClient, web_market::WebTradeBarBuilder};
use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "gridedge-market-e2e-shadow",
    about = "Physically isolated, money-disabled market-path E2E consumer"
)]
struct Args {
    #[arg(long)]
    contract: PathBuf,
    #[arg(long, default_value_t = 600)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 5)]
    interval_minutes: u32,
}

#[derive(Debug, Deserialize)]
struct Contract {
    mode: String,
    nonce: String,
    root: PathBuf,
    mqtt_host: String,
    mqtt_port: u16,
    mqtt_namespace: String,
    worker_mqtt_username: String,
    worker_mqtt_client_id: String,
    market_event_log: PathBuf,
    bar_log: PathBuf,
    ledger: PathBuf,
    money_actions_enabled: bool,
}

#[derive(Debug, Serialize)]
struct DurableMessage<'a> {
    contract_sha256: &'a str,
    physical_namespace: &'a str,
    topic: &'a str,
    payload_hex: String,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    if !(600..=1_800).contains(&args.duration_seconds) {
        bail!("isolated E2E duration must be within 600..=1800 seconds")
    }
    validate_interval(args.interval_minutes)?;
    let (contract, contract_sha256) = load_contract(&args.contract)?;
    ensure_fresh_evidence(&contract)?;
    let root = contract.root.as_path();
    let binary_sha256 = hex::encode(Sha256::digest(fs::read(std::env::current_exe()?)?));
    let password_file = root.join("secrets/worker.password");
    let ca_file = root.join("certs/ca.crt");
    let mut ledger = open_ledger(
        &contract.ledger,
        &contract.nonce,
        &contract_sha256,
        &binary_sha256,
    )?;
    let mut builder = WebTradeBarBuilder::new("002256.SZ", args.interval_minutes)?;
    let mut market = MarketMqttClient::connect(
        &contract.mqtt_host,
        contract.mqtt_port,
        &contract.worker_mqtt_username,
        &password_file,
        &ca_file,
        &contract.worker_mqtt_client_id,
        &contract.mqtt_namespace,
        "XSHE",
        "002256",
    )?;
    market.await_ready(Duration::from_secs(10))?;

    let started = Instant::now();
    let mut message_count = 0_u64;
    let mut bar_count = 0_u64;
    let mut source_observation_count = 0_u64;
    let mut first_message_released_bar = false;
    let mut previous_observed_at_us = None;
    let mut maximum_source_observation_gap_us = 0_u64;
    let mut maximum_committed_receipt_gap_us = 0_u64;
    let mut first_observed_at_us = None;
    let mut last_observed_at_us = None;
    let mut first_observation_elapsed = None;
    let mut previous_committed_receipt_instant = None;
    let mut maximum_observation_age_us = 0_u64;
    while started.elapsed() < Duration::from_secs(args.duration_seconds) {
        let Some(message) = market.receive(Duration::from_secs(2))? else {
            continue;
        };
        append_json_line(
            &contract.market_event_log,
            &DurableMessage {
                contract_sha256: &contract_sha256,
                physical_namespace: &contract.mqtt_namespace,
                topic: &message.topic,
                payload_hex: hex::encode(&message.payload),
            },
        )?;
        let receipt = builder.ingest_with_receipt_at(
            &message.topic,
            &message.payload,
            Local::now().naive_local(),
        )?;
        message_count += 1;
        if message_count == 1 && !receipt.bars.is_empty() {
            first_message_released_bar = true;
        }
        if let Some(observation) = receipt.source_observation {
            source_observation_count += 1;
            let committed_received_instant = Instant::now();
            let committed_received_at_us = u64::try_from(Utc::now().timestamp_micros())?;
            let observation_age_us = committed_received_at_us
                .checked_sub(observation.observed_at_us)
                .context("isolated E2E source observation is in the future")?;
            maximum_observation_age_us = maximum_observation_age_us.max(observation_age_us);
            first_observed_at_us.get_or_insert(observation.observed_at_us);
            last_observed_at_us = Some(observation.observed_at_us);
            first_observation_elapsed
                .get_or_insert(committed_received_instant.duration_since(started));
            if let Some(previous) = previous_observed_at_us {
                maximum_source_observation_gap_us = maximum_source_observation_gap_us.max(
                    observation
                        .observed_at_us
                        .checked_sub(previous)
                        .context("isolated E2E source observation moved backwards")?,
                );
            }
            if let Some(previous) = previous_committed_receipt_instant {
                maximum_committed_receipt_gap_us =
                    maximum_committed_receipt_gap_us.max(u64::try_from(
                        committed_received_instant
                            .duration_since(previous)
                            .as_micros(),
                    )?);
            }
            previous_observed_at_us = Some(observation.observed_at_us);
            previous_committed_receipt_instant = Some(committed_received_instant);
        }
        for bar in receipt.bars {
            append_json_line(&contract.bar_log, &bar)?;
            append_bar_triplet(&mut ledger, &bar)?;
            bar_count += 1;
        }
    }
    if message_count == 0 {
        bail!("isolated E2E received no PostgreSQL committed market event")
    }
    if first_message_released_bar {
        bail!("an empty midstream consumer emitted a bar from its first event")
    }
    if source_observation_count < 2 {
        bail!("isolated E2E did not observe two reviewed source observations")
    }
    let first_gap_us = u64::try_from(
        first_observation_elapsed
            .context("isolated E2E lacks first observation")?
            .as_micros(),
    )?;
    let tail_gap_us = u64::try_from(
        Instant::now()
            .duration_since(
                previous_committed_receipt_instant
                    .context("isolated E2E lacks committed observation receipt")?,
            )
            .as_micros(),
    )?;
    validate_observation_health([
        first_gap_us,
        tail_gap_us,
        maximum_observation_age_us,
        maximum_source_observation_gap_us,
        maximum_committed_receipt_gap_us,
    ])?;
    if bar_count == 0 {
        bail!("isolated E2E did not complete a local five-minute bar")
    }
    let ledger_head = verify_ledger(&ledger, bar_count)?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "bars": bar_count,
            "messages": message_count,
            "mode": contract.mode,
            "money_actions_enabled": false,
            "shadow_market_path_only": true,
            "strategy_evaluated": false,
            "source_observations": source_observation_count,
            "first_observed_at_us": first_observed_at_us,
            "last_observed_at_us": last_observed_at_us,
            "maximum_source_observation_gap_us": maximum_source_observation_gap_us,
            "maximum_committed_receipt_gap_us": maximum_committed_receipt_gap_us,
            "maximum_observation_age_us": maximum_observation_age_us,
            "start_to_first_observation_us": first_gap_us,
            "last_observation_to_end_us": tail_gap_us,
            "triplets": bar_count * 3,
            "ledger_head_sequence": ledger_head.0,
            "ledger_head_sha256": ledger_head.1,
            "shadow_binary_sha256": binary_sha256,
        }))?
    );
    Ok(())
}

fn validate_interval(interval_minutes: u32) -> Result<()> {
    if interval_minutes != 5 {
        bail!("isolated E2E interval is fixed at five minutes")
    }
    Ok(())
}

fn validate_observation_health(gaps_us: [u64; 5]) -> Result<()> {
    const ACCEPTANCE_GAP_US: u64 = 45_000_000;
    if gaps_us.into_iter().any(|gap| gap > ACCEPTANCE_GAP_US) {
        bail!("isolated E2E source/committed observation freshness exceeded 45 seconds")
    }
    Ok(())
}

fn load_contract(path: &Path) -> Result<(Contract, String)> {
    let bytes = fs::read(path)
        .with_context(|| format!("cannot read isolated contract {}", path.display()))?;
    let contract: Contract = serde_json::from_slice(&bytes)?;
    let contract_sha256 = hex::encode(Sha256::digest(&bytes));
    let uuid_text = contract
        .nonce
        .strip_prefix("e2e-0629-")
        .context("isolated nonce lacks reviewed prefix")?;
    let uuid = Uuid::parse_str(uuid_text).context("isolated nonce is not UUID")?;
    if uuid.get_version_num() != 4 || uuid.hyphenated().to_string() != uuid_text {
        bail!("isolated nonce must contain canonical lowercase UUIDv4")
    }
    let expected_root = PathBuf::from(format!("/tmp/gridedge-market-e2e-{}", contract.nonce));
    let contract_parent = path
        .parent()
        .context("isolated contract has no parent")?
        .canonicalize()?;
    let root = contract.root.canonicalize()?;
    if root != expected_root.canonicalize()?
        || path.canonicalize()? != root.join("contract.json")
        || contract_parent != root
        || contract.mode != "ISOLATED_READ_ONLY_E2E"
        || contract.money_actions_enabled
        || contract.mqtt_host != "127.0.0.1"
        || contract.mqtt_port != 18_883
        || contract.worker_mqtt_username != "gridedge-e2e-worker"
        || contract.worker_mqtt_client_id != format!("{}-worker", contract.nonce)
        || contract.mqtt_namespace != format!("gridedge-e2e/{}", contract.nonce)
        || contract.mqtt_namespace.starts_with("gridedge/market")
    {
        bail!("isolated market E2E contract is not atomic")
    }
    let state = root.join("state").canonicalize()?;
    if expected_root.is_symlink() || root.join("state").is_symlink() {
        bail!("isolated E2E root or state directory must not be a symbolic link")
    }
    for (evidence, basename) in [
        (&contract.market_event_log, "market-events.ndjson"),
        (&contract.bar_log, "bars.ndjson"),
        (&contract.ledger, "ledger.sqlite3"),
    ] {
        if evidence != &state.join(basename)
            || evidence
                .parent()
                .and_then(|parent| parent.canonicalize().ok())
                .as_deref()
                != Some(state.as_path())
        {
            bail!("isolated E2E evidence escaped the nonce state root")
        }
    }
    Ok((contract, contract_sha256))
}

fn ensure_fresh_evidence(contract: &Contract) -> Result<()> {
    for path in [
        &contract.market_event_log,
        &contract.bar_log,
        &contract.ledger,
    ] {
        if path.exists() && path.metadata()?.len() != 0 {
            bail!(
                "isolated E2E evidence path is not fresh: {}",
                path.display()
            )
        }
    }
    Ok(())
}

fn open_ledger(
    path: &Path,
    nonce: &str,
    contract_sha256: &str,
    binary_sha256: &str,
) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         CREATE TABLE e2e_metadata(
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE e2e_events(
             sequence_number INTEGER PRIMARY KEY,
             event_type TEXT NOT NULL CHECK(event_type IN(
                 'MARKET_DATA_RECEIVED',
                 'MARKET_BAR_DECISIONS_COMMITTED',
                 'MARKET_BAR_PROCESSED'
             )),
             bar_timestamp TEXT NOT NULL,
             payload_json TEXT NOT NULL,
             previous_sha256 TEXT,
             event_sha256 TEXT NOT NULL UNIQUE
         );",
    )?;
    connection.execute(
        "INSERT INTO e2e_metadata(key,value) VALUES('mode','ISOLATED_READ_ONLY_E2E')",
        [],
    )?;
    connection.execute(
        "INSERT INTO e2e_metadata(key,value) VALUES('money_actions_enabled','false')",
        [],
    )?;
    connection.execute(
        "INSERT INTO e2e_metadata(key,value) VALUES('nonce',?1)",
        [nonce],
    )?;
    connection.execute(
        "INSERT INTO e2e_metadata(key,value) VALUES('contract_sha256',?1)",
        [contract_sha256],
    )?;
    connection.execute(
        "INSERT INTO e2e_metadata(key,value) VALUES('shadow_binary_sha256',?1)",
        [binary_sha256],
    )?;
    Ok(connection)
}

fn append_bar_triplet(connection: &mut Connection, bar: &MarketBar) -> Result<()> {
    let transaction = connection.transaction()?;
    for event_type in [
        "MARKET_DATA_RECEIVED",
        "MARKET_BAR_DECISIONS_COMMITTED",
        "MARKET_BAR_PROCESSED",
    ] {
        append_e2e_event(&transaction, event_type, bar)?;
    }
    transaction.commit()?;
    Ok(())
}

fn append_e2e_event(
    transaction: &Transaction<'_>,
    event_type: &str,
    bar: &MarketBar,
) -> Result<()> {
    let (next_sequence, previous_sha): (i64, Option<String>) = transaction.query_row(
        "SELECT COALESCE(MAX(sequence_number),0)+1,
                (SELECT event_sha256 FROM e2e_events ORDER BY sequence_number DESC LIMIT 1)
           FROM e2e_events",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let payload = serde_json::to_string(&json!({
        "bar": bar,
        "event_type": event_type,
        "money_actions_enabled": false,
        "sequence_number": next_sequence,
    }))?;
    let mut hasher = Sha256::new();
    hasher.update(previous_sha.as_deref().unwrap_or("GENESIS").as_bytes());
    hasher.update(payload.as_bytes());
    let event_sha = hex::encode(hasher.finalize());
    transaction.execute(
        "INSERT INTO e2e_events(
             sequence_number,event_type,bar_timestamp,payload_json,previous_sha256,event_sha256
         ) VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            next_sequence,
            event_type,
            bar.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
            payload,
            previous_sha,
            event_sha,
        ],
    )?;
    Ok(())
}

fn verify_ledger(connection: &Connection, bar_count: u64) -> Result<(u64, String)> {
    let (events, received, decisions, processed): (u64, u64, u64, u64) = connection.query_row(
        "SELECT COUNT(*),
                SUM(event_type='MARKET_DATA_RECEIVED'),
                SUM(event_type='MARKET_BAR_DECISIONS_COMMITTED'),
                SUM(event_type='MARKET_BAR_PROCESSED')
           FROM e2e_events",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if events != bar_count * 3
        || received != bar_count
        || decisions != bar_count
        || processed != bar_count
    {
        bail!("isolated E2E ledger triplet conservation failed")
    }
    let money_actions: String = connection.query_row(
        "SELECT value FROM e2e_metadata WHERE key='money_actions_enabled'",
        [],
        |row| row.get(0),
    )?;
    if money_actions != "false" {
        bail!("isolated E2E ledger enabled money actions")
    }
    let mut statement = connection.prepare(
        "SELECT sequence_number,event_type,bar_timestamp,payload_json,
                previous_sha256,event_sha256
           FROM e2e_events ORDER BY sequence_number",
    )?;
    let mut rows = statement.query([])?;
    let expected_types = [
        "MARKET_DATA_RECEIVED",
        "MARKET_BAR_DECISIONS_COMMITTED",
        "MARKET_BAR_PROCESSED",
    ];
    let mut expected_sequence = 1_u64;
    let mut previous_sha: Option<String> = None;
    let mut group_timestamp: Option<String> = None;
    let mut group_bar: Option<serde_json::Value> = None;
    while let Some(row) = rows.next()? {
        let sequence: u64 = row.get(0)?;
        let event_type: String = row.get(1)?;
        let bar_timestamp: String = row.get(2)?;
        let payload: String = row.get(3)?;
        let stored_previous: Option<String> = row.get(4)?;
        let stored_sha: String = row.get(5)?;
        if sequence != expected_sequence
            || event_type != expected_types[((sequence - 1) % 3) as usize]
            || stored_previous != previous_sha
        {
            bail!("isolated E2E ledger sequence, triplet order, or previous hash is invalid")
        }
        let document: serde_json::Value = serde_json::from_str(&payload)?;
        if document.get("event_type").and_then(|value| value.as_str()) != Some(event_type.as_str())
            || document
                .get("sequence_number")
                .and_then(serde_json::Value::as_u64)
                != Some(sequence)
            || document
                .get("money_actions_enabled")
                .and_then(serde_json::Value::as_bool)
                != Some(false)
        {
            bail!("isolated E2E event payload does not bind its ledger row")
        }
        let payload_bar = document
            .get("bar")
            .context("isolated E2E event payload lacks bar")?;
        if payload_bar
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            != Some(bar_timestamp.as_str())
            || payload_bar
                .get("symbol")
                .and_then(serde_json::Value::as_str)
                != Some("002256.SZ")
        {
            bail!("isolated E2E payload bar identity differs from its ledger row")
        }
        if (sequence - 1).is_multiple_of(3) {
            group_timestamp = Some(bar_timestamp.clone());
            group_bar = Some(payload_bar.clone());
        } else if group_timestamp.as_deref() != Some(bar_timestamp.as_str())
            || group_bar.as_ref() != Some(payload_bar)
        {
            bail!("isolated E2E triplet does not bind one identical completed bar")
        }
        let mut hasher = Sha256::new();
        hasher.update(previous_sha.as_deref().unwrap_or("GENESIS").as_bytes());
        hasher.update(payload.as_bytes());
        if hex::encode(hasher.finalize()) != stored_sha {
            bail!("isolated E2E ledger event hash is invalid")
        }
        previous_sha = Some(stored_sha);
        expected_sequence += 1;
    }
    Ok((expected_sequence - 1, previous_sha.unwrap_or_default()))
}

fn append_json_line(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    fn bar() -> MarketBar {
        MarketBar {
            timestamp: NaiveDate::from_ymd_opt(2026, 8, 31)
                .unwrap()
                .and_hms_opt(9, 35, 0)
                .unwrap(),
            symbol: "002256.SZ".to_owned(),
            open: Decimal::new(340, 2),
            high: Decimal::new(341, 2),
            low: Decimal::new(339, 2),
            close: Decimal::new(340, 2),
            volume: 100,
            amount: Some(Decimal::new(340, 0)),
        }
    }

    #[test]
    fn each_shadow_bar_atomically_appends_one_complete_processing_triplet() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("ledger.sqlite3");
        let mut connection =
            open_ledger(&path, "e2e-0629-test", &"a".repeat(64), &"b".repeat(64)).unwrap();
        append_bar_triplet(&mut connection, &bar()).unwrap();
        verify_ledger(&connection, 1).unwrap();
        let chain_breaks: u64 = connection
            .query_row(
                "SELECT COUNT(*) FROM e2e_events current
                  LEFT JOIN e2e_events previous
                    ON previous.sequence_number=current.sequence_number-1
                 WHERE current.sequence_number>1
                   AND current.previous_sha256<>previous.event_sha256",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(chain_breaks, 0);
    }

    #[test]
    fn shadow_interval_is_exactly_five_minutes() {
        assert!(validate_interval(5).is_ok());
        for invalid in [0, 1, 2, 3, 10, 15, 30] {
            assert!(validate_interval(invalid).is_err());
        }
    }

    #[test]
    fn observation_health_rejects_start_tail_stale_source_and_committed_silence() {
        assert!(validate_observation_health([45_000_000; 5]).is_ok());
        for index in 0..5 {
            let mut gaps = [1_u64; 5];
            gaps[index] = 45_000_001;
            assert!(validate_observation_health(gaps).is_err(), "index={index}");
        }
    }

    #[test]
    fn production_verifier_rejects_payload_hash_order_and_bar_binding_tampering() {
        for tamper in ["payload", "hash", "order", "bar"] {
            let directory = tempdir().unwrap();
            let path = directory.path().join("ledger.sqlite3");
            let mut connection =
                open_ledger(&path, "e2e-0629-test", &"a".repeat(64), &"b".repeat(64)).unwrap();
            append_bar_triplet(&mut connection, &bar()).unwrap();
            match tamper {
                "payload" => connection
                    .execute("UPDATE e2e_events SET payload_json='{}' WHERE sequence_number=2", [])
                    .unwrap(),
                "hash" => connection
                    .execute("UPDATE e2e_events SET event_sha256=?1 WHERE sequence_number=2", [&"b".repeat(64)])
                    .unwrap(),
                "order" => connection
                    .execute("UPDATE e2e_events SET event_type='MARKET_BAR_PROCESSED' WHERE sequence_number=2", [])
                    .unwrap(),
                "bar" => connection
                    .execute("UPDATE e2e_events SET bar_timestamp='2026-08-31 09:40:00' WHERE sequence_number=2", [])
                    .unwrap(),
                _ => unreachable!(),
            };
            assert!(verify_ledger(&connection, 1).is_err(), "tamper={tamper}");
        }

        let directory = tempdir().unwrap();
        let path = directory.path().join("ledger.sqlite3");
        let mut connection =
            open_ledger(&path, "e2e-0629-test", &"a".repeat(64), &"b".repeat(64)).unwrap();
        append_bar_triplet(&mut connection, &bar()).unwrap();
        let previous: String = connection
            .query_row(
                "SELECT event_sha256 FROM e2e_events WHERE sequence_number=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut second: serde_json::Value = connection
            .query_row(
                "SELECT payload_json FROM e2e_events WHERE sequence_number=2",
                [],
                |row| row.get::<_, String>(0),
            )
            .map(|value| serde_json::from_str(&value).unwrap())
            .unwrap();
        second["bar"]["close"] = json!("3.41");
        let second_payload = serde_json::to_string(&second).unwrap();
        let second_sha = hex::encode(Sha256::digest(
            [previous.as_bytes(), second_payload.as_bytes()].concat(),
        ));
        let third_payload: String = connection
            .query_row(
                "SELECT payload_json FROM e2e_events WHERE sequence_number=3",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let third_sha = hex::encode(Sha256::digest(
            [second_sha.as_bytes(), third_payload.as_bytes()].concat(),
        ));
        connection
            .execute(
                "UPDATE e2e_events SET payload_json=?1,event_sha256=?2 WHERE sequence_number=2",
                params![second_payload, second_sha],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE e2e_events SET previous_sha256=?1,event_sha256=?2 WHERE sequence_number=3",
                params![second_sha, third_sha],
            )
            .unwrap();
        assert!(verify_ledger(&connection, 1).is_err());
    }

    #[test]
    fn shadow_ledger_has_no_order_or_android_write_vocabulary() {
        let source = include_str!("gridedge_market_e2e_shadow.rs");
        let subject = source.split("#[cfg(test)]").next().unwrap();
        for forbidden in [
            "OrderIntent",
            "run_outbox",
            "AndroidThsSimulationUiDriver",
            "prepare_order",
            "submit_order",
            "cancel_order",
        ] {
            assert!(!subject.contains(forbidden));
        }
    }
}
