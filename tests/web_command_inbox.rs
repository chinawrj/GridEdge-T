use gridedge_t::{
    config::Config,
    data::CsvReplayFeed,
    domain::StrategyState,
    gate,
    journal::{SqliteStore, StateReader},
    profit::{unrealized_grid_valuation, UNREALIZED_VALUATION_POLICY_VERSION},
    service::GridAutomationService,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier, Mutex, MutexGuard},
    thread,
    time::{Duration, Instant},
};
use tempfile::TempDir;

const API_VERSION: &str = "gridedge.api/v1";
const API_TOKEN: &str = "durable-inbox-test-token";
static WEB_PROCESS_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

impl HttpResponse {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|error| {
            panic!(
                "response body is not JSON: {error}; body={}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

#[derive(Clone, Debug)]
struct TestClient {
    port: u16,
}

impl TestClient {
    fn get(&self, path: &str) -> HttpResponse {
        self.request("GET", path, None)
    }

    fn command(&self, payload: &Value) -> HttpResponse {
        self.request("POST", "/api/v1/commands", Some(payload))
    }

    fn command_with_read_timeout(&self, payload: &Value, timeout: Duration) -> HttpResponse {
        let body = serde_json::to_vec(payload).unwrap();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .write_all(&self.request_bytes("POST", "/api/v1/commands", &body, true))
            .unwrap();
        stream.flush().unwrap();
        parse_http_response(&read_complete_http_response_with_timeout(
            &mut stream,
            timeout,
        ))
    }

    fn pending_commands(&self) -> HttpResponse {
        self.request("GET", "/api/v1/pending-commands", None)
    }

    fn retry_pending(&self, run_id: &str, request_id: &str) -> HttpResponse {
        self.request(
            "POST",
            "/api/v1/pending-commands/retry",
            Some(&json!({"run_id": run_id, "request_id": request_id})),
        )
    }

    fn command_without_token(&self, payload: &Value) -> HttpResponse {
        self.request_with_authentication("POST", "/api/v1/commands", Some(payload), false)
    }

    fn get_without_token(&self, path: &str) -> HttpResponse {
        self.request_with_authentication("GET", path, None, false)
    }

    fn post_form(&self, path: &str, fields: &[(&str, &str)]) -> HttpResponse {
        let body = fields
            .iter()
            .map(|(name, value)| format!("{}={}", form_component(name), form_component(value)))
            .collect::<Vec<_>>()
            .join("&")
            .into_bytes();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\nOrigin: http://127.0.0.1:{}\r\nSec-Fetch-Site: same-origin\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.port,
            self.port,
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        stream.write_all(&body).unwrap();
        stream.flush().unwrap();
        parse_http_response(&read_complete_http_response(&mut stream))
    }

    fn command_without_reading_response(&self, payload: &Value) -> TcpStream {
        let body = serde_json::to_vec(payload).unwrap();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .write_all(&self.request_bytes("POST", "/api/v1/commands", &body, true))
            .unwrap();
        stream.flush().unwrap();
        stream
    }

    fn request(&self, method: &str, path: &str, payload: Option<&Value>) -> HttpResponse {
        self.request_with_authentication(method, path, payload, true)
    }

    fn request_with_authentication(
        &self,
        method: &str,
        path: &str,
        payload: Option<&Value>,
        authenticated: bool,
    ) -> HttpResponse {
        let body = payload
            .map(|value| serde_json::to_vec(value).unwrap())
            .unwrap_or_default();
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        stream
            .write_all(&self.request_bytes(method, path, &body, authenticated))
            .unwrap();
        stream.flush().unwrap();
        let response = read_complete_http_response(&mut stream);
        parse_http_response(&response)
    }

    fn request_bytes(&self, method: &str, path: &str, body: &[u8], authenticated: bool) -> Vec<u8> {
        let authentication = if authenticated {
            format!("X-GridEdge-Api-Token: {API_TOKEN}\r\n")
        } else {
            String::new()
        };
        format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n{authentication}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.port,
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect()
    }
}

struct ServerProcess {
    child: Child,
    client: TestClient,
}

impl ServerProcess {
    fn start(config: &Path, data: &Path) -> Self {
        let port = unused_loopback_port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_gridedge"))
            .args([
                "web",
                "--config",
                config.to_str().unwrap(),
                "--data",
                data.to_str().unwrap(),
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .env("GRIDEDGE_API_TOKEN", API_TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let client = TestClient { port };
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("web server exited before readiness: {status}");
            }
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                let ready = client.get("/ready");
                if ready.status == 200 {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "web server did not become ready");
            thread::sleep(Duration::from_millis(10));
        }
        Self { child, client }
    }

    fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
            self.child.wait().unwrap();
        }
    }
}

fn spawn_web_without_readiness_wait(config: &Path, data: &Path) -> (Child, TestClient) {
    let port = unused_loopback_port();
    let child = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "web",
            "--config",
            config.to_str().unwrap(),
            "--data",
            data.to_str().unwrap(),
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .env("GRIDEDGE_API_TOKEN", API_TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    (child, TestClient { port })
}

fn assert_web_preflight_fails_before_readiness(
    mut child: Child,
    client: &TestClient,
    expected_error: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert!(!status.success(), "invalid database started successfully");
            assert!(
                stderr.contains(expected_error),
                "startup error did not identify {expected_error:?}: {stderr}"
            );
            return;
        }
        if TcpStream::connect(("127.0.0.1", client.port)).is_ok() {
            assert_ne!(
                client.get("/ready").status,
                200,
                "invalid database was advertised as ready"
            );
            assert_ne!(
                client.get("/api/v1/runs").status,
                200,
                "invalid database served an authenticated business read"
            );
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("invalid database left a live non-ready core: {stderr}");
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn readiness_fixture(database: &Path) -> (TempDir, PathBuf, PathBuf) {
    let temporary = TempDir::new().unwrap();
    let source = fs::read_to_string("configs/default.yaml").unwrap();
    let config = temporary.path().join("readiness.yaml");
    fs::write(
        &config,
        source.replace(
            "database: \"gridedge.db\"",
            &format!("database: {:?}", database.to_string_lossy()),
        ),
    )
    .unwrap();
    let data = fs::canonicalize("tests/fixtures/sample.csv").unwrap();
    (temporary, config, data)
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Harness {
    server: ServerProcess,
    config: PathBuf,
    data: PathBuf,
    database: PathBuf,
    _temporary: TempDir,
    _serial: MutexGuard<'static, ()>,
}

impl Harness {
    fn new() -> Self {
        Self::build(None)
    }

    fn with_mutable_data() -> Self {
        Self::build(Some((
            "mutable-sample.csv",
            fs::read_to_string("tests/fixtures/sample.csv").unwrap(),
        )))
    }

    fn with_custom_data(filename: &str, contents: String) -> Self {
        Self::build(Some((filename, contents)))
    }

    fn build(data_override: Option<(&str, String)>) -> Self {
        Self::build_seeded(data_override, None)
    }

    fn with_legacy_pending_start(run_id: &str, request_id: &str) -> Self {
        Self::build_seeded(None, Some((run_id, request_id)))
    }

    fn build_seeded(
        data_override: Option<(&str, String)>,
        legacy_pending_start: Option<(&str, &str)>,
    ) -> Self {
        let serial = WEB_PROCESS_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("command-inbox.db");
        let source = fs::read_to_string("configs/default.yaml").unwrap();
        let config_text = source.replace(
            "database: \"gridedge.db\"",
            &format!("database: {:?}", database.to_string_lossy()),
        );
        let config = temporary.path().join("config.yaml");
        fs::write(&config, config_text).unwrap();
        if let Some((run_id, request_id)) = legacy_pending_start {
            let connection = rusqlite::Connection::open(&database).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE web_playback_control (
                         run_id TEXT PRIMARY KEY,
                         command_version INTEGER NOT NULL,
                         active INTEGER NOT NULL CHECK(active IN (0,1)),
                         interval_ms INTEGER NOT NULL,
                         updated_at TEXT NOT NULL
                     );
                     CREATE TABLE web_command_inbox (
                         run_id TEXT NOT NULL,
                         request_id TEXT NOT NULL,
                         request_sha256 TEXT NOT NULL,
                         command TEXT NOT NULL,
                         expected_sequence INTEGER NOT NULL,
                         expected_version INTEGER NOT NULL,
                         accepted_version INTEGER NOT NULL,
                         target_processed_bars INTEGER NOT NULL,
                         receipt_state TEXT NOT NULL CHECK(receipt_state IN ('PENDING','COMPLETED')),
                         response_json TEXT,
                         created_at TEXT NOT NULL,
                         updated_at TEXT NOT NULL,
                         PRIMARY KEY(run_id, request_id),
                         UNIQUE(request_id)
                     );",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO web_playback_control(
                         run_id,command_version,active,interval_ms,updated_at
                     ) VALUES(?1,1,0,0,'2026-08-16 00:00:00')",
                    [run_id],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO web_command_inbox(
                         run_id,request_id,request_sha256,command,expected_sequence,
                         expected_version,accepted_version,target_processed_bars,
                         receipt_state,response_json,created_at,updated_at
                     ) VALUES(?1,?2,?3,'start',0,0,1,0,'PENDING',NULL,
                              '2026-08-16 00:00:00','2026-08-16 00:00:00')",
                    rusqlite::params![run_id, request_id, "0".repeat(64)],
                )
                .unwrap();
        }
        let data = if let Some((filename, contents)) = data_override {
            let data = temporary.path().join(filename);
            fs::write(&data, contents).unwrap();
            data
        } else {
            fs::canonicalize("tests/fixtures/sample.csv").unwrap()
        };
        let server = ServerProcess::start(&config, &data);
        Self {
            server,
            config,
            data,
            database,
            _temporary: temporary,
            _serial: serial,
        }
    }

    fn restart(&mut self) {
        self.server.stop();
        self.server = ServerProcess::start(&self.config, &self.data);
    }

    fn client(&self) -> TestClient {
        self.server.client.clone()
    }

    fn database(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(&self.database).unwrap()
    }
}

fn replace_file_fragments(path: &Path, replacements: &[(&str, &str)]) {
    let mut text = fs::read_to_string(path).unwrap();
    for (before, after) in replacements {
        assert_eq!(text.matches(before).count(), 1, "missing unique {before}");
        text = text.replacen(before, after, 1);
    }
    fs::write(path, text).unwrap();
}

fn form_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                vec![char::from(byte)]
            }
            b' ' => vec!['+'],
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn unused_loopback_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn parse_http_response(bytes: &[u8]) -> HttpResponse {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap_or_else(|| {
            panic!(
                "incomplete HTTP response: {}",
                String::from_utf8_lossy(bytes)
            )
        });
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap();
    let mut body = bytes[header_end + 4..].to_vec();
    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        body = decode_chunked(&body);
    }
    HttpResponse { status, body }
}

fn read_complete_http_response(stream: &mut TcpStream) -> Vec<u8> {
    read_complete_http_response_with_timeout(stream, Duration::from_secs(5))
}

fn read_complete_http_response_with_timeout(stream: &mut TcpStream, timeout: Duration) -> Vec<u8> {
    stream.set_read_timeout(Some(timeout)).unwrap();
    let mut response = Vec::new();
    let mut buffer = [0_u8; 8_192];
    loop {
        let size = stream.read(&mut buffer).unwrap_or_else(|error| {
            panic!(
                "HTTP response timed out: {error}; partial={}",
                String::from_utf8_lossy(&response)
            )
        });
        if size == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..size]);
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&response[..header_end]);
        if let Some(content_length) = headers.lines().find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        }) {
            if response.len() >= header_end + 4 + content_length {
                break;
            }
        } else if headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
            && response[header_end + 4..].ends_with(b"0\r\n\r\n")
        {
            break;
        }
    }
    response
}

fn decode_chunked(bytes: &[u8]) -> Vec<u8> {
    let mut remaining = bytes;
    let mut decoded = Vec::new();
    loop {
        let line_end = remaining
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("chunk size line");
        let size = usize::from_str_radix(std::str::from_utf8(&remaining[..line_end]).unwrap(), 16)
            .unwrap();
        remaining = &remaining[line_end + 2..];
        if size == 0 {
            break;
        }
        decoded.extend_from_slice(&remaining[..size]);
        remaining = &remaining[size + 2..];
    }
    decoded
}

fn command_request(
    request_id: &str,
    command: &str,
    run_id: &str,
    expected_sequence: i64,
    expected_version: i64,
) -> Value {
    json!({
        "api_version": API_VERSION,
        "request_id": request_id,
        "expected_sequence": expected_sequence,
        "expected_version": expected_version,
        "command": command,
        "run_id": run_id,
        "dataset": null,
        "speed_ms": null
    })
}

fn start_run(client: &TestClient, run_id: &str) -> Value {
    start_run_with_dataset(client, run_id, "sample.csv")
}

fn start_run_with_dataset(client: &TestClient, run_id: &str, dataset: &str) -> Value {
    let mut request = command_request(&format!("start-{run_id}"), "start", run_id, 0, 0);
    request["dataset"] = json!(dataset);
    let response = client.command(&request);
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let result = response.json();
    assert_eq!(result["api_version"], API_VERSION);
    assert!(result["accepted_sequence"].is_i64());
    assert!(result["accepted_version"].is_i64());
    result
}

#[test]
fn unknown_explicit_dataset_is_rejected_before_any_durable_write() {
    let harness = Harness::new();
    let client = harness.client();
    let mut request = command_request("unknown-dataset", "start", "unknown-dataset", 0, 0);
    request["dataset"] = json!("absent.csv");
    let response = client.command(&request);
    assert_eq!(response.status, 422);
    let database = harness.database();
    let mutation_rows: i64 = database
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events) +
               (SELECT COUNT(*) FROM web_command_inbox) +
               (SELECT COUNT(*) FROM web_playback_control)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mutation_rows, 0);
}

fn html_hidden_value(html: &str, name: &str) -> String {
    let prefix = format!("name=\"{name}\" value=\"");
    let start = html.find(&prefix).unwrap() + prefix.len();
    let end = html[start..].find('"').unwrap() + start;
    html[start..end].to_owned()
}

fn snapshot(client: &TestClient, run_id: &str) -> Value {
    let response = client.get(&format!("/api/v1/runs/{run_id}"));
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    response.json()
}

fn pending_command_batch(client: &TestClient) -> Value {
    let response = client.pending_commands();
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    response.json()
}

fn snapshot_bytes(client: &TestClient, run_id: &str) -> Vec<u8> {
    let response = client.get(&format!("/api/v1/runs/{run_id}"));
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    response.body
}

fn opportunity_page(
    client: &TestClient,
    run_id: &str,
    after: i64,
    through: i64,
    limit: usize,
) -> Value {
    let response = client.get(&format!(
        "/api/v1/runs/{run_id}/opportunities?after={after}&through={through}&limit={limit}"
    ));
    assert_eq!(
        response.status,
        200,
        "opportunity history response: {}",
        String::from_utf8_lossy(&response.body)
    );
    let page = response.json();
    assert_eq!(page["api_version"], API_VERSION);
    assert_eq!(page["run_id"], run_id);
    assert_eq!(page["through_sequence"].as_i64(), Some(through));
    let standard_quantity = page["standard_quantity"]
        .as_i64()
        .expect("opportunity page must expose its audited share unit");
    assert!(page["opportunities"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| { record["standard_quantity"].as_i64() == Some(standard_quantity) }));
    page
}

fn complete_opportunity_history(client: &TestClient, run_id: &str, through: i64) -> Value {
    let mut after = 0;
    let mut records = Vec::new();
    let mut final_counts = None;
    for _ in 0..10_000 {
        let page = opportunity_page(client, run_id, after, through, 2);
        let counts = page["counts"].clone();
        if let Some(previous) = &final_counts {
            assert_eq!(&counts, previous, "page totals changed inside one prefix");
        } else {
            final_counts = Some(counts);
        }
        let page_records = page["opportunities"]
            .as_array()
            .expect("opportunities must be an array");
        for record in page_records {
            assert_eq!(record["run_id"], run_id);
            assert!(record["opportunity_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty()));
            assert!(record["touch_sequence"]
                .as_i64()
                .is_some_and(|seq| seq > after));
            assert!(record["resolution_sequence"]
                .as_i64()
                .is_some_and(|seq| seq <= through));
            assert!(record["processed_sequence"].as_i64().is_some_and(|seq| {
                seq >= record["resolution_sequence"].as_i64().unwrap() && seq <= through
            }));
            let grid_index = record["grid_index"]
                .as_i64()
                .expect("opportunity grid index must be an integer");
            assert_eq!(
                record["direction"],
                if grid_index < 0 { "BUY" } else { "SELL" }
            );
            assert!(matches!(
                record["resolution"].as_str(),
                Some("GRANTED" | "SKIPPED")
            ));
            if record["resolution"] == "GRANTED" {
                let quantities = [
                    "gross_available_quantity",
                    "platform_residual_quantity",
                    "algorithm_authorized_quantity",
                    "exercise_quantity",
                    "defer_quantity",
                    "platform_blocked_quantity",
                    "order_intent_quantity",
                    "remaining_decision_quantity",
                ]
                .map(|field| {
                    record[field]
                        .as_i64()
                        .unwrap_or_else(|| panic!("granted opportunity lacks {field}"))
                });
                let [gross, residual, authorized, exercise, deferred, blocked, intent, remaining] =
                    quantities;
                assert_eq!(record["decision_contract_version"], 3);
                assert_eq!(gross, residual + authorized);
                assert_eq!(authorized, exercise + deferred);
                assert_eq!(intent, exercise - blocked);
                assert_eq!(remaining, deferred + blocked);
                assert!(record["right_id"].as_str().is_some());
                assert!(record["decision_id"].as_str().is_some());
                assert!(record["reason"].is_null());
                assert!(record["reason_audit_status"].is_null());
                let capacity = &record["pre_trade_capacity"];
                assert!(capacity.is_object());
                for field in [
                    "eligible_quantity",
                    "t_plus_one_blocked_quantity",
                    "risk_blocked_quantity",
                    "no_profit_blocked_quantity",
                ] {
                    assert!(capacity[field]
                        .as_i64()
                        .is_some_and(|quantity| quantity >= 0));
                }
                for field in [
                    "eligible_lot_ids",
                    "t_plus_one_blocked_lot_ids",
                    "risk_blocked_lot_ids",
                    "no_profit_blocked_lot_ids",
                    "source_right_ids",
                    "tranche_ids",
                ] {
                    assert!(capacity[field].is_array(), "capacity lacks {field}");
                }
                for partial in record["partial_blocks"].as_array().unwrap() {
                    for (quantity_field, lot_field) in [
                        ("t_plus_one_blocked_quantity", "t_plus_one_blocked_lot_ids"),
                        ("risk_blocked_quantity", "risk_blocked_lot_ids"),
                        ("no_profit_blocked_quantity", "no_profit_blocked_lot_ids"),
                    ] {
                        assert_eq!(partial[quantity_field], capacity[quantity_field]);
                        assert_eq!(partial[lot_field], capacity[lot_field]);
                    }
                }
            } else {
                assert!(record["right_id"].is_null());
                assert!(record["decision_id"].is_null());
                assert!(record["reason"]
                    .as_str()
                    .is_some_and(|reason| !reason.trim().is_empty()));
                assert_eq!(record["reason_audit_status"], "RECORDED_UNVERIFIED");
                for field in [
                    "decision_contract_version",
                    "gross_available_quantity",
                    "platform_residual_quantity",
                    "algorithm_authorized_quantity",
                    "exercise_quantity",
                    "defer_quantity",
                    "platform_blocked_quantity",
                    "order_intent_quantity",
                    "remaining_decision_quantity",
                ] {
                    assert!(record[field].is_null(), "skip unexpectedly exposes {field}");
                }
            }
            records.push(record.clone());
        }
        let next = page["next_sequence"]
            .as_i64()
            .expect("opportunity page must contain an integer next_sequence");
        if page["complete"].as_bool() == Some(true) {
            assert!(next <= through);
            return json!({
                "api_version": API_VERSION,
                "run_id": run_id,
                "through_sequence": through,
                "counts": final_counts.unwrap(),
                "opportunities": records,
            });
        }
        assert!(
            next > after,
            "non-terminal opportunity page did not advance"
        );
        assert!(
            next <= through,
            "opportunity cursor advanced beyond its prefix"
        );
        after = next;
    }
    panic!("opportunity pagination did not terminate");
}

fn assert_cursor(snapshot: &Value, expected: u64) {
    assert_eq!(
        snapshot["progress"]["processed_bars"].as_u64(),
        Some(expected)
    );
}

fn synthetic_intraday_csv(bar_count: usize) -> String {
    assert!((1..=121).contains(&bar_count));
    let mut csv = String::from("timestamp,symbol,open,high,low,close,volume,amount\n");
    let start_minutes = 9 * 60 + 30;
    for index in 0..bar_count {
        let minute = start_minutes + index;
        let hour = minute / 60;
        let minute = minute % 60;
        let high = if index == 40 { "10.99" } else { "10.10" };
        let low = if index == 41 { "9.01" } else { "9.90" };
        csv.push_str(&format!(
            "2026-01-05 {hour:02}:{minute:02}:00,600000.SH,10.00,{high},{low},10.00,1000,10000\n"
        ));
    }
    csv
}

fn long_identity_attack_csv(bar_count: usize) -> String {
    use chrono::Datelike;

    let mut csv = String::from("timestamp,symbol,open,high,low,close,volume,amount\n");
    let mut date = chrono::NaiveDate::from_ymd_opt(2025, 1, 6).unwrap();
    let mut emitted = 0;
    while emitted < bar_count {
        if date.weekday().number_from_monday() <= 5 {
            for slot in 0..48 {
                if emitted == bar_count {
                    break;
                }
                let minute = if slot < 24 {
                    9 * 60 + 35 + slot * 5
                } else {
                    13 * 60 + 5 + (slot - 24) * 5
                };
                csv.push_str(&format!(
                    "{} {:02}:{:02}:00,600000.SH,10.00,10.01,9.99,10.00,1000,10000\n",
                    date,
                    minute / 60,
                    minute % 60
                ));
                emitted += 1;
            }
        }
        date = date.succ_opt().unwrap();
    }
    csv
}

fn wait_for_cursor(client: &TestClient, run_id: &str, minimum: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let value = snapshot(client, run_id);
        if value["progress"]["processed_bars"]
            .as_u64()
            .is_some_and(|cursor| cursor >= minimum)
        {
            return value;
        }
        assert!(Instant::now() < deadline, "replay cursor did not advance");
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn command_contract_requires_api_and_optimistic_versions() {
    let harness = Harness::new();
    let client = harness.client();

    for (run_id, mut request) in [
        (
            "missing-api-version",
            json!({
                "request_id": "missing-version",
                "expected_sequence": 0,
                "expected_version": 0,
                "command": "start",
                "run_id": "missing-api-version",
                "dataset": "sample.csv"
            }),
        ),
        (
            "unknown-api-version",
            json!({
                "api_version": "gridedge.api/v999",
                "request_id": "unknown-version",
                "expected_sequence": 0,
                "expected_version": 0,
                "command": "start",
                "run_id": "unknown-api-version",
                "dataset": "sample.csv"
            }),
        ),
    ] {
        request["run_id"] = json!(run_id);
        let response = client.command(&request);
        assert_eq!(
            response.status, 422,
            "request unexpectedly accepted: {request}"
        );
    }

    let accepted = start_run(&client, "contract-run");
    let current = snapshot(&client, "contract-run");
    assert_eq!(current["api_version"], API_VERSION);
    assert!(current["command_version"].is_i64());
    assert_eq!(accepted["accepted_sequence"], current["sequence"]);
    assert_eq!(accepted["accepted_version"], current["command_version"]);
}

#[test]
fn every_api_read_and_command_requires_the_internal_token() {
    let harness = Harness::new();
    let client = harness.client();
    let request = command_request("unauthorized-start", "start", "unauthorized", 0, 0);
    assert_eq!(client.get_without_token("/api/v1/runs").status, 403);
    assert_eq!(client.command_without_token(&request).status, 403);
    assert_eq!(client.get("/api/v1/runs").json(), json!([]));
}

#[test]
fn native_start_and_replay_forms_reject_an_absent_dataset_without_any_run_write() {
    let harness = Harness::new();
    let client = harness.client();
    let dashboard = client.get("/");
    assert_eq!(dashboard.status, 200);
    let csrf = html_hidden_value(&String::from_utf8(dashboard.body).unwrap(), "csrf_token");

    for (path, run_id, request_id) in [
        (
            "/actions/step/start",
            "absent-step-form",
            "absent-step-form-request",
        ),
        (
            "/actions/replay",
            "absent-replay-form",
            "absent-replay-form-request",
        ),
    ] {
        let response = client.post_form(
            path,
            &[
                ("run_id", run_id),
                ("dataset", "absent.csv"),
                ("request_id", request_id),
                ("expected_sequence", "0"),
                ("expected_version", "0"),
                ("csrf_token", &csrf),
            ],
        );
        assert_eq!(response.status, 303);
        let database = harness.database();
        let event_count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                [run_id],
                |row| row.get(0),
            )
            .unwrap();
        let receipt_count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM web_command_inbox WHERE run_id=?1",
                [run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((event_count, receipt_count), (0, 0));
    }
}

#[test]
fn healthy_core_is_ready_only_after_migration_and_an_authenticated_business_probe() {
    let harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/health").status, 200);
    let ready = client.get("/ready");
    assert_eq!(
        ready.status,
        200,
        "{}",
        String::from_utf8_lossy(&ready.body)
    );
    assert_eq!(client.get_without_token("/api/v1/runs").status, 403);
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let version: i64 = harness
        .database()
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 13);
}

#[test]
fn migration_13_creates_one_immutable_database_identity() {
    let harness = Harness::new();
    let database = harness.database();
    let identities: Vec<(i64, String)> = database
        .prepare("SELECT singleton,instance_id FROM database_identity")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(identities.len(), 1);
    assert_eq!(identities[0].0, 1);
    assert_eq!(identities[0].1.len(), 32);
    assert!(
        identities[0]
            .1
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "database identity is not canonical lowercase hex"
    );
    let immutable_triggers: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='trigger' AND name IN (
               'database_identity_is_immutable_insert',
               'database_identity_is_immutable_update',
               'database_identity_is_immutable_delete')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(immutable_triggers, 3);
    for mutation in [
        "INSERT INTO database_identity(singleton,instance_id)
         VALUES(1,lower(hex(randomblob(16))))",
        "INSERT OR REPLACE INTO database_identity(singleton,instance_id)
         VALUES(1,lower(hex(randomblob(16))))",
        "UPDATE database_identity SET instance_id=lower(hex(randomblob(16))) WHERE singleton=1",
        "DELETE FROM database_identity WHERE singleton=1",
    ] {
        let error = database.execute(mutation, []).unwrap_err();
        assert!(error.to_string().contains("database identity is immutable"));
    }
    let unchanged: (i64, String) = database
        .query_row(
            "SELECT singleton,instance_id FROM database_identity",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(unchanged, identities[0]);
}

#[test]
fn liveness_stays_independent_and_lost_readiness_requires_restart() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/health").status, 200);
    assert_eq!(client.get("/ready").status, 200);
    let database = harness.database();
    database
        .execute(
            "INSERT INTO schema_migrations(version,applied_at)
             VALUES(99,'2026-08-17 00:00:00')",
            [],
        )
        .unwrap();
    assert_eq!(client.get("/health").status, 200);
    let unavailable = client.get("/ready");
    assert_eq!(unavailable.status, 503);
    assert_eq!(String::from_utf8_lossy(&unavailable.body), "not ready");
    database
        .execute("DELETE FROM schema_migrations WHERE version=99", [])
        .unwrap();
    assert_eq!(client.get("/ready").status, 503);
    harness.server.stop();
    harness.server = ServerProcess::start(&harness.config, &harness.data);
    assert_eq!(harness.client().get("/ready").status, 200);
}

#[test]
fn replacing_a_ready_database_fails_every_business_entry_without_creating_a_new_ledger() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "database-identity");
    let started = snapshot(&client, "database-identity");
    let step = command_request(
        "database-identity-step",
        "step",
        "database-identity",
        started["sequence"].as_i64().unwrap(),
        started["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    let before = snapshot(&client, "database-identity");
    let original_head = before["sequence"].as_i64().unwrap();
    let original = harness.database.with_extension("db.original");
    fs::rename(&harness.database, &original).unwrap();
    fs::write(&harness.database, []).unwrap();

    let runs = client.get("/api/v1/runs");
    assert_eq!(
        runs.status,
        503,
        "replacement database served runs: {}",
        String::from_utf8_lossy(&runs.body)
    );
    let snapshot_response = client.get("/api/v1/runs/database-identity");
    assert_eq!(snapshot_response.status, 503);
    let command = command_request(
        "database-identity-second-step",
        "step",
        "database-identity",
        original_head,
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&command).status, 503);
    assert_eq!(client.get("/health").status, 200);
    assert_eq!(client.get("/ready").status, 503);
    assert_eq!(
        fs::metadata(&harness.database).unwrap().len(),
        0,
        "business/readiness probe initialized the replacement ledger"
    );
    let original_connection = rusqlite::Connection::open(&original).unwrap();
    let head: i64 = original_connection
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='database-identity'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(head, original_head);

    harness.server.stop();
    fs::remove_file(&harness.database).unwrap();
    fs::rename(original, &harness.database).unwrap();
}

#[cfg(unix)]
#[test]
fn overwriting_a_ready_database_in_place_with_another_valid_ledger_revokes_readiness_and_play() {
    use std::os::unix::fs::MetadataExt;

    let harness = Harness::new();
    let replacement_dir = TempDir::new().unwrap();
    let replacement_database = replacement_dir.path().join("replacement.db");
    let source = fs::read_to_string("configs/default.yaml").unwrap();
    let replacement_config_text = source.replace(
        "database: \"gridedge.db\"",
        &format!("database: {:?}", replacement_database.to_string_lossy()),
    );
    let replacement_config = replacement_dir.path().join("replacement.yaml");
    fs::write(&replacement_config, replacement_config_text).unwrap();
    let mut replacement_server = ServerProcess::start(&replacement_config, &harness.data);
    let replacement_client = replacement_server.client.clone();
    start_run(&replacement_client, "replacement-ledger-run");
    replacement_server.stop();
    let replacement_connection = rusqlite::Connection::open(&replacement_database).unwrap();
    replacement_connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let replacement_identity: String = replacement_connection
        .query_row(
            "SELECT instance_id FROM database_identity WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let replacement_facts: (i64, i64, i64) = replacement_connection
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events),
               (SELECT COUNT(*) FROM web_command_inbox),
               (SELECT MAX(sequence_number) FROM events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    drop(replacement_connection);

    let client = harness.client();
    start_run(&client, "in-place-active-play");
    let started = snapshot(&client, "in-place-active-play");
    let mut play = command_request(
        "in-place-active-play-command",
        "play",
        "in-place-active-play",
        started["sequence"].as_i64().unwrap(),
        started["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    assert_eq!(client.command(&play).status, 200);
    wait_for_cursor(&client, "in-place-active-play", 1);
    harness
        .database()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let original_inode = fs::metadata(&harness.database).unwrap().ino();
    fs::copy(&replacement_database, &harness.database).unwrap();
    assert_eq!(
        fs::metadata(&harness.database).unwrap().ino(),
        original_inode,
        "fixture replaced the inode instead of attacking the durable UUID boundary"
    );

    thread::sleep(Duration::from_millis(750));
    let overwritten = rusqlite::Connection::open(&harness.database).unwrap();
    let identity: String = overwritten
        .query_row(
            "SELECT instance_id FROM database_identity WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let facts: (i64, i64, i64) = overwritten
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events),
               (SELECT COUNT(*) FROM web_command_inbox),
               (SELECT MAX(sequence_number) FROM events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(identity, replacement_identity);
    assert_eq!(facts, replacement_facts, "old PLAY wrote into ledger B");
    drop(overwritten);
    assert_eq!(client.get("/ready").status, 503);
    assert_eq!(client.get("/api/v1/runs").status, 503);
}

#[cfg(unix)]
#[test]
fn finish_detects_an_in_place_valid_ledger_overwrite_without_an_http_probe_or_writing_ledger_b() {
    use std::os::unix::fs::MetadataExt;

    let harness =
        Harness::with_custom_data("finish-identity-long.csv", long_identity_attack_csv(5_000));
    let replacement_dir = TempDir::new().unwrap();
    let replacement_database = replacement_dir.path().join("finish-replacement.db");
    let source = fs::read_to_string("configs/default.yaml").unwrap();
    let replacement_config_text = source.replace(
        "database: \"gridedge.db\"",
        &format!("database: {:?}", replacement_database.to_string_lossy()),
    );
    let replacement_config = replacement_dir.path().join("finish-replacement.yaml");
    fs::write(&replacement_config, replacement_config_text).unwrap();
    let mut replacement_server = ServerProcess::start(&replacement_config, &harness.data);
    start_run_with_dataset(
        &replacement_server.client,
        "finish-replacement-ledger",
        "finish-identity-long.csv",
    );
    replacement_server.stop();
    let replacement_connection = rusqlite::Connection::open(&replacement_database).unwrap();
    replacement_connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let replacement_facts: (String, i64, i64, i64) = replacement_connection
        .query_row(
            "SELECT
               (SELECT instance_id FROM database_identity WHERE singleton=1),
               (SELECT COUNT(*) FROM events),
               (SELECT COUNT(*) FROM web_command_inbox),
               (SELECT MAX(sequence_number) FROM events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    drop(replacement_connection);

    let client = harness.client();
    start_run_with_dataset(
        &client,
        "finish-identity-attack",
        "finish-identity-long.csv",
    );
    let before = snapshot(&client, "finish-identity-attack");
    let finish = command_request(
        "finish-identity-attack-command",
        "finish",
        "finish-identity-attack",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let finish_client = client.clone();
    let finish_thread = thread::spawn(move || finish_client.command(&finish));

    let observer = harness.database();
    observer.busy_timeout(Duration::from_secs(1)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (pending, processed): (i64, i64) = observer
            .query_row(
                "SELECT
                   (SELECT COUNT(*) FROM web_command_inbox
                     WHERE request_id='finish-identity-attack-command'
                       AND receipt_state='PENDING'),
                   (SELECT COUNT(*) FROM events
                     WHERE run_id='finish-identity-attack'
                       AND event_type='MARKET_BAR_PROCESSED')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        if pending == 1 && processed >= 10 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "FINISH completed before the in-flight identity attack could be injected"
        );
        thread::sleep(Duration::from_millis(2));
    }
    observer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(observer);
    let original_inode = fs::metadata(&harness.database).unwrap().ino();
    fs::copy(&replacement_database, &harness.database).unwrap();
    assert_eq!(
        fs::metadata(&harness.database).unwrap().ino(),
        original_inode
    );

    let response = finish_thread
        .join()
        .expect("FINISH client thread panicked before receiving an HTTP response");
    assert!(
        response.status >= 500,
        "FINISH reported success after its ledger was replaced: {}",
        String::from_utf8_lossy(&response.body)
    );
    thread::sleep(Duration::from_millis(300));
    for _ in 0..2 {
        assert_eq!(client.get("/ready").status, 503);
        assert_eq!(client.get("/api/v1/runs").status, 503);
    }
    let overwritten = rusqlite::Connection::open(&harness.database).unwrap();
    let facts: (String, i64, i64, i64) = overwritten
        .query_row(
            "SELECT
               (SELECT instance_id FROM database_identity WHERE singleton=1),
               (SELECT COUNT(*) FROM events),
               (SELECT COUNT(*) FROM web_command_inbox),
               (SELECT MAX(sequence_number) FROM events)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(facts, replacement_facts, "FINISH wrote into ledger B");
}

#[test]
fn readiness_probes_are_read_only_for_schema_and_business_state() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "readiness-read-only");
    let before = harness
        .database()
        .query_row(
            "SELECT
           (SELECT GROUP_CONCAT(version || ':' || applied_at, '|')
              FROM schema_migrations ORDER BY version),
           (SELECT COUNT(*) FROM events WHERE run_id='readiness-read-only'),
           (SELECT MAX(sequence_number) FROM events WHERE run_id='readiness-read-only'),
           (SELECT COUNT(*) FROM snapshots WHERE run_id='readiness-read-only'),
           (SELECT COUNT(*) FROM web_command_inbox WHERE run_id='readiness-read-only')",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .unwrap();
    for _ in 0..20 {
        assert_eq!(client.get("/ready").status, 200);
    }
    let after = harness
        .database()
        .query_row(
            "SELECT
           (SELECT GROUP_CONCAT(version || ':' || applied_at, '|')
              FROM schema_migrations ORDER BY version),
           (SELECT COUNT(*) FROM events WHERE run_id='readiness-read-only'),
           (SELECT MAX(sequence_number) FROM events WHERE run_id='readiness-read-only'),
           (SELECT COUNT(*) FROM snapshots WHERE run_id='readiness-read-only'),
           (SELECT COUNT(*) FROM web_command_inbox WHERE run_id='readiness-read-only')",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(after, before);
}

#[test]
fn database_identity_failure_stops_active_play_before_another_bar() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "identity-play");
    let before = snapshot(&client, "identity-play");
    let mut play = command_request(
        "identity-play-start",
        "play",
        "identity-play",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    assert_eq!(client.command(&play).status, 200);
    wait_for_cursor(&client, "identity-play", 1);

    let observer = harness.database();
    let original = harness.database.with_extension("db.play-original");
    fs::rename(&harness.database, &original).unwrap();
    fs::write(&harness.database, []).unwrap();
    assert_eq!(client.get("/api/v1/runs").status, 503);
    let stopped_head: i64 = observer
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='identity-play'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    thread::sleep(Duration::from_millis(750));
    let later_head: i64 = observer
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='identity-play'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        later_head, stopped_head,
        "PLAY advanced after identity failure"
    );
    assert_eq!(fs::metadata(&harness.database).unwrap().len(), 0);

    drop(observer);
    harness.server.stop();
    fs::remove_file(&harness.database).unwrap();
    fs::rename(original, &harness.database).unwrap();
}

#[test]
fn corrupted_sqlite_is_rejected_before_the_core_can_become_ready() {
    let _serial = WEB_PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let database_dir = TempDir::new().unwrap();
    let database = database_dir.path().join("corrupt.db");
    fs::write(&database, b"not a sqlite database").unwrap();
    let (_config_dir, config, data) = readiness_fixture(&database);
    let (child, client) = spawn_web_without_readiness_wait(&config, &data);
    assert_web_preflight_fails_before_readiness(child, &client, "file is not a database");
}

#[test]
fn future_and_noncontiguous_migration_histories_are_rejected_before_readiness() {
    let _serial = WEB_PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for (label, versions) in [("future", vec![14]), ("gap", vec![1, 3])] {
        let database_dir = TempDir::new().unwrap();
        let database = database_dir.path().join(format!("{label}.db"));
        let connection = rusqlite::Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations (
                     version INTEGER PRIMARY KEY,
                     applied_at TEXT NOT NULL
                 );",
            )
            .unwrap();
        for version in versions {
            connection
                .execute(
                    "INSERT INTO schema_migrations(version,applied_at)
                     VALUES(?1,'2026-08-17 00:00:00')",
                    [version],
                )
                .unwrap();
        }
        drop(connection);
        let (_config_dir, config, data) = readiness_fixture(&database);
        let (child, client) = spawn_web_without_readiness_wait(&config, &data);
        assert_web_preflight_fails_before_readiness(
            child,
            &client,
            "unsupported or non-contiguous database schema version",
        );
    }
}

#[test]
fn migration_failure_rolls_back_and_never_exposes_a_ready_core() {
    let _serial = WEB_PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let database_dir = TempDir::new().unwrap();
    let database = database_dir.path().join("migration-failure.db");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE schema_migrations (
                 version INTEGER PRIMARY KEY,
                 applied_at TEXT NOT NULL
             );
             CREATE TABLE events(dummy TEXT);",
        )
        .unwrap();
    drop(connection);
    let (_config_dir, config, data) = readiness_fixture(&database);
    let (child, client) = spawn_web_without_readiness_wait(&config, &data);
    assert_web_preflight_fails_before_readiness(child, &client, "events");
    let connection = rusqlite::Connection::open(&database).unwrap();
    let migrations: i64 = connection
        .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        migrations, 0,
        "failed migration partially advanced its version"
    );
}

#[cfg(unix)]
#[test]
fn sigterm_drains_the_core_cleanly_and_releases_its_database_lease() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/ready").status, 200);
    start_run(&client, "sigterm-active-play");
    let started = snapshot(&client, "sigterm-active-play");
    let mut play = command_request(
        "sigterm-active-play-command",
        "play",
        "sigterm-active-play",
        started["sequence"].as_i64().unwrap(),
        started["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(2_000);
    assert_eq!(client.command(&play).status, 200);
    let playing = wait_for_cursor(&client, "sigterm-active-play", 1);
    let head_before_signal = playing["sequence"].as_i64().unwrap();
    let cursor_before_signal = playing["progress"]["processed_bars"].as_u64().unwrap();
    let playback_active: i64 = harness
        .database()
        .query_row(
            "SELECT active FROM web_playback_control WHERE run_id='sigterm-active-play'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(playback_active, 1, "PLAY was not active before SIGTERM");

    let result = unsafe { libc::kill(harness.server.child.id() as i32, libc::SIGTERM) };
    assert_eq!(result, 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = harness.server.child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "core ignored SIGTERM");
        thread::sleep(Duration::from_millis(20));
    };
    assert!(
        status.success(),
        "SIGTERM was not a clean shutdown: {status}"
    );
    let head_after_exit: i64 = harness
        .database()
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='sigterm-active-play'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(head_after_exit, head_before_signal);
    harness.server = ServerProcess::start(&harness.config, &harness.data);
    let restarted_client = harness.client();
    assert_eq!(restarted_client.get("/ready").status, 200);
    let restarted = snapshot(&restarted_client, "sigterm-active-play");
    assert_eq!(restarted["sequence"], head_after_exit);
    assert_eq!(
        restarted["progress"]["processed_bars"],
        cursor_before_signal
    );
}

#[test]
fn a_second_web_core_for_the_same_database_fails_before_health_ready() {
    let harness = Harness::new();
    let port = unused_loopback_port();
    let output = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "web",
            "--config",
            harness.config.to_str().unwrap(),
            "--data",
            harness.data.to_str().unwrap(),
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .env("GRIDEDGE_API_TOKEN", API_TOKEN)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("another GridEdge-T Web service already owns database"));
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[test]
fn a_running_core_keeps_its_startup_config_and_database_after_the_file_is_rewritten() {
    let harness = Harness::new();
    let client_a = harness.client();
    start_run(&client_a, "frozen-config-a");

    let database_b = harness.database.with_file_name("frozen-config-b.db");
    let config_b = harness.config.with_file_name("frozen-config-b.yaml");
    let original_config = fs::read_to_string(&harness.config).unwrap();
    let config_b_text = original_config.replace(
        harness.database.to_string_lossy().as_ref(),
        database_b.to_string_lossy().as_ref(),
    );
    fs::write(&config_b, &config_b_text).unwrap();
    let server_b = ServerProcess::start(&config_b, &harness.data);
    let client_b = server_b.client.clone();
    start_run(&client_b, "frozen-config-b");

    fs::write(&harness.config, config_b_text).unwrap();
    assert_eq!(
        client_a.get("/api/v1/runs").json(),
        json!(["frozen-config-a"])
    );
    assert_eq!(
        client_b.get("/api/v1/runs").json(),
        json!(["frozen-config-b"])
    );
    assert_eq!(client_a.get("/api/v1/runs/frozen-config-b").status, 404);
    let before = snapshot(&client_a, "frozen-config-a");
    let step = command_request(
        "frozen-config-a-step",
        "step",
        "frozen-config-a",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client_a.command(&step).status, 200);
    assert_cursor(&snapshot(&client_a, "frozen-config-a"), 1);
    assert_eq!(
        snapshot(&client_b, "frozen-config-b")["progress"]["processed_bars"],
        0
    );
}

#[cfg(unix)]
#[test]
fn a_two_layer_symlink_alias_cannot_bypass_the_same_database_web_lease() {
    use std::os::unix::fs::symlink;

    let harness = Harness::new();
    assert!(harness.database.exists());
    let first_alias = harness.database.with_file_name("command-inbox-alias-1.db");
    let second_alias = harness.database.with_file_name("command-inbox-alias-2.db");
    symlink(&harness.database, &first_alias).unwrap();
    symlink(&first_alias, &second_alias).unwrap();
    assert!(first_alias.is_symlink());
    assert!(second_alias.is_symlink());
    assert!(first_alias.exists());
    assert!(second_alias.exists());
    let alias_config = harness
        .config
        .with_file_name("config-with-database-alias.yaml");
    let config_text = fs::read_to_string(&harness.config).unwrap().replace(
        harness.database.to_string_lossy().as_ref(),
        second_alias.to_string_lossy().as_ref(),
    );
    fs::write(&alias_config, config_text).unwrap();

    let port = unused_loopback_port();
    let output = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "web",
            "--config",
            alias_config.to_str().unwrap(),
            "--data",
            harness.data.to_str().unwrap(),
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .env("GRIDEDGE_API_TOKEN", API_TOKEN)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("another GridEdge-T Web service already owns database"));
    assert!(TcpStream::connect(("127.0.0.1", port)).is_err());
}

#[test]
fn duplicate_step_returns_the_durable_response_without_advancing_twice() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "duplicate-step");
    let before = snapshot(&client, "duplicate-step");
    let request = command_request(
        "step-once",
        "step",
        "duplicate-step",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );

    let first = client.command(&request);
    assert_eq!(first.status, 200);
    let first_body = first.json();
    let after_first = snapshot(&client, "duplicate-step");
    assert_cursor(&after_first, 1);

    let retry = client.command(&request);
    assert_eq!(retry.status, 200);
    assert_eq!(retry.json(), first_body);
    let after_retry = snapshot(&client, "duplicate-step");
    assert_eq!(after_retry, after_first);
    assert_cursor(&after_retry, 1);
}

#[test]
fn same_request_id_with_different_payload_is_a_409_and_atomic() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "conflicting-request");
    let before = snapshot(&client, "conflicting-request");
    let request = command_request(
        "conflict-id",
        "step",
        "conflicting-request",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&request).status, 200);
    let after_step = snapshot(&client, "conflicting-request");

    let mut conflict = request;
    conflict["command"] = json!("finish");
    let rejected = client.command(&conflict);
    assert_eq!(rejected.status, 409);
    assert_eq!(snapshot(&client, "conflicting-request"), after_step);
}

#[test]
fn request_id_is_global_and_cannot_be_reused_for_another_run() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "global-id-a");
    start_run(&client, "global-id-b");
    let before_a = snapshot(&client, "global-id-a");
    let before_b = snapshot(&client, "global-id-b");
    let request_a = command_request(
        "globally-unique-step",
        "step",
        "global-id-a",
        before_a["sequence"].as_i64().unwrap(),
        before_a["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&request_a).status, 200);
    let after_a = snapshot(&client, "global-id-a");
    assert_cursor(&after_a, 1);

    let request_b = command_request(
        "globally-unique-step",
        "step",
        "global-id-b",
        before_b["sequence"].as_i64().unwrap(),
        before_b["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&request_b).status, 409);
    assert_eq!(snapshot(&client, "global-id-b"), before_b);
    assert_eq!(snapshot(&client, "global-id-a"), after_a);
}

#[test]
fn two_clients_at_one_expected_version_have_exactly_one_winner() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "concurrent-step");
    let before = snapshot(&client, "concurrent-step");
    let sequence = before["sequence"].as_i64().unwrap();
    let version = before["command_version"].as_i64().unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let handles: Vec<_> = ["concurrent-a", "concurrent-b"]
        .into_iter()
        .map(|request_id| {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let request =
                    command_request(request_id, "step", "concurrent-step", sequence, version);
                barrier.wait();
                client.command(&request)
            })
        })
        .collect();
    barrier.wait();
    let mut statuses: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().status)
        .collect();
    statuses.sort_unstable();
    assert_eq!(statuses, vec![200, 409]);
    let after = snapshot(&client, "concurrent-step");
    assert_cursor(&after, 1);
    assert_eq!(after["command_version"].as_i64(), Some(version + 1));
}

#[test]
fn a_lost_step_response_survives_process_restart_without_reexecution() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "lost-response");
    let before = snapshot(&client, "lost-response");
    let request = command_request(
        "lost-step-response",
        "step",
        "lost-response",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let _discarded_response = client.command_without_reading_response(&request);
    let committed = wait_for_cursor(&client, "lost-response", 1);
    assert_cursor(&committed, 1);

    harness.restart();
    let restarted = harness.client();
    let retry = restarted.command(&request);
    assert_eq!(retry.status, 200);
    let response = retry.json();
    assert_eq!(response["accepted_sequence"], committed["sequence"]);
    assert_eq!(response["accepted_version"], committed["command_version"]);
    assert_eq!(snapshot(&restarted, "lost-response"), committed);
}

#[test]
fn a_timed_out_finish_duplicate_joins_one_inflight_execution_and_one_durable_receipt() {
    let harness = Harness::with_custom_data("timeout-finish.csv", long_identity_attack_csv(5_000));
    let client = harness.client();
    start_run_with_dataset(&client, "timeout-finish", "timeout-finish.csv");
    let before = snapshot(&client, "timeout-finish");
    let finish = command_request(
        "timeout-finish-request",
        "finish",
        "timeout-finish",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );

    let abandoned = client.command_without_reading_response(&finish);
    let observer = harness.database();
    observer.busy_timeout(Duration::from_secs(1)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (pending, processed): (i64, i64) = observer
            .query_row(
                "SELECT
                   (SELECT COUNT(*) FROM web_command_inbox
                     WHERE request_id='timeout-finish-request'
                       AND receipt_state='PENDING'),
                   (SELECT COUNT(*) FROM events
                     WHERE run_id='timeout-finish'
                       AND event_type='MARKET_BAR_PROCESSED')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        if pending == 1 && processed >= 10 {
            assert!(processed < 5_000, "FINISH fixture was not long-running");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed-out FINISH did not leave an observable in-flight durable claim"
        );
        thread::sleep(Duration::from_millis(2));
    }
    drop(abandoned);

    let recovered = client.command_with_read_timeout(&finish, Duration::from_secs(30));
    assert_eq!(
        recovered.status,
        200,
        "duplicate FINISH did not join the in-flight command: {}",
        String::from_utf8_lossy(&recovered.body)
    );
    let durable_response = recovered.body.clone();
    let terminal = snapshot(&client, "timeout-finish");
    assert_cursor(&terminal, 5_000);
    assert_eq!(terminal["state"]["duplicate_events"], 0);
    assert_eq!(terminal["playback"]["active"], false);

    let replayed = client.command(&finish);
    assert_eq!(replayed.status, 200);
    assert_eq!(replayed.body, durable_response);
    let facts: (i64, i64, i64, i64, i64, i64, i64) = observer
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='timeout-finish-request'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='timeout-finish-request'
                   AND receipt_state='COMPLETED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='timeout-finish' AND event_type='MARKET_DATA_RECEIVED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='timeout-finish'
                   AND event_type='MARKET_BAR_DECISIONS_COMMITTED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='timeout-finish' AND event_type='MARKET_BAR_PROCESSED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='timeout-finish' AND event_type='SERVICE_STOPPED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='timeout-finish' AND event_type='ERROR_RECORDED')",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(facts, (1, 1, 5_000, 5_000, 5_000, 1, 0));
}

#[test]
fn running_server_uses_frozen_bars_but_restart_rejects_changed_dataset_bytes() {
    let mut harness = Harness::with_mutable_data();
    let client = harness.client();
    start_run_with_dataset(&client, "frozen-bars", "mutable-sample.csv");
    let initial = snapshot(&client, "frozen-bars");
    let first_step = command_request(
        "frozen-first-step",
        "step",
        "frozen-bars",
        initial["sequence"].as_i64().unwrap(),
        initial["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&first_step).status, 200);
    let after_first = snapshot(&client, "frozen-bars");
    assert_cursor(&after_first, 1);

    let original = fs::read_to_string(&harness.data).unwrap();
    let changed = original.replacen(
        "2026-01-05 09:31:00,600000.SH,10.00,10.00,9.79,9.82,110000,1080200",
        "2026-01-05 09:31:00,600000.SH,10.00,88.88,9.79,77.77,110000,1080200",
        1,
    );
    assert_ne!(changed, original);
    assert_eq!(changed.lines().count(), original.lines().count());
    fs::write(&harness.data, changed).unwrap();

    let second_step = command_request(
        "frozen-second-step",
        "step",
        "frozen-bars",
        after_first["sequence"].as_i64().unwrap(),
        after_first["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&second_step).status, 200);
    let frozen = snapshot(&client, "frozen-bars");
    assert_cursor(&frozen, 2);
    assert_eq!(frozen["state"]["last_price"], "9.82");

    harness.restart();
    let restarted = harness.client();
    let unsafe_continuation = command_request(
        "changed-dataset-step",
        "step",
        "frozen-bars",
        frozen["sequence"].as_i64().unwrap(),
        frozen["command_version"].as_i64().unwrap(),
    );
    let rejected = restarted.command(&unsafe_continuation);
    assert_eq!(rejected.status, 409);
    let error = rejected.json();
    assert_eq!(error["api_version"], API_VERSION);
    assert_eq!(error["code"], "COMMAND_CONFLICT");
    assert!(error["message"]
        .as_str()
        .is_some_and(|message| message.contains("replay dataset changed")));
    let after_rejection = snapshot(&restarted, "frozen-bars");
    assert_eq!(after_rejection["sequence"], frozen["sequence"]);
    assert_eq!(
        after_rejection["command_version"],
        frozen["command_version"]
    );
    assert_eq!(after_rejection["progress"], frozen["progress"]);
    assert_eq!(after_rejection["state"]["last_price"], "9.82");
    let receipt_count: i64 = harness
        .database()
        .query_row(
            "SELECT COUNT(*) FROM web_command_inbox WHERE request_id='changed-dataset-step'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipt_count, 0);
}

#[test]
fn bars_api_exposes_only_the_processed_prefix_bound_to_the_run_descriptor() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "visible-bars");
    let initial = snapshot(&client, "visible-bars");
    let descriptor = &initial["progress"]["descriptor"];

    let empty = client.get("/api/v1/runs/visible-bars/bars?max_points=100");
    assert_eq!(empty.status, 200);
    let empty = empty.json();
    assert_eq!(empty["api_version"], API_VERSION);
    assert_eq!(empty["run_id"], "visible-bars");
    assert_eq!(empty["dataset_id"], descriptor["dataset_id"]);
    assert_eq!(empty["data_sha256"], descriptor["data_sha256"]);
    assert_eq!(empty["visible_bars"], 0);
    assert_eq!(empty["total_bars"], descriptor["total_bars"]);
    assert_eq!(empty["sampled"], json!([]));

    let request = command_request(
        "visible-bars-step",
        "step",
        "visible-bars",
        initial["sequence"].as_i64().unwrap(),
        initial["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&request).status, 200);
    let one = client
        .get("/api/v1/runs/visible-bars/bars?max_points=100")
        .json();
    assert_eq!(one["visible_bars"], 1);
    assert_eq!(one["sampled"].as_array().unwrap().len(), 1);
    assert_eq!(one["sampled"][0]["timestamp"], "2026-01-05 09:30:00");
    assert_eq!(one["sampled"][0]["close"], "10.00");
    let serialized = serde_json::to_string(&one["sampled"]).unwrap();
    assert!(!serialized.contains("2026-01-05 09:31:00"));
    assert!(!serialized.contains("9.82"));
}

#[test]
fn bars_api_ohlc_aggregation_preserves_intrabar_high_and_low() {
    let harness = Harness::with_custom_data("aggregate-bars.csv", synthetic_intraday_csv(101));
    let client = harness.client();
    start_run_with_dataset(&client, "aggregate-bars", "aggregate-bars.csv");
    let before = snapshot(&client, "aggregate-bars");
    let finish = command_request(
        "aggregate-bars-finish",
        "finish",
        "aggregate-bars",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 200);
    let terminal = snapshot(&client, "aggregate-bars");
    assert_cursor(&terminal, 101);

    let response = client.get("/api/v1/runs/aggregate-bars/bars?max_points=100");
    assert_eq!(response.status, 200);
    let batch = response.json();
    assert_eq!(batch["visible_bars"], 101);
    assert_eq!(batch["total_bars"], 101);
    assert_eq!(
        batch["dataset_id"],
        terminal["progress"]["descriptor"]["dataset_id"]
    );
    assert_eq!(
        batch["data_sha256"],
        terminal["progress"]["descriptor"]["data_sha256"]
    );
    let sampled = batch["sampled"].as_array().unwrap();
    assert!(sampled.len() <= 100);
    assert!(sampled.iter().any(|bar| bar["high"] == "10.99"));
    assert!(sampled.iter().any(|bar| bar["low"] == "9.01"));
}

#[test]
fn opportunity_api_pages_the_complete_short_history_and_isolates_runs_across_restart() {
    let mut harness = Harness::new();
    let client = harness.client();
    let mut terminal_snapshots = Vec::new();
    for run_id in ["opportunity-run-a", "opportunity-run-b"] {
        start_run(&client, run_id);
        let started = snapshot(&client, run_id);
        let finish = command_request(
            &format!("finish-{run_id}"),
            "finish",
            run_id,
            started["sequence"].as_i64().unwrap(),
            started["command_version"].as_i64().unwrap(),
        );
        assert_eq!(client.command(&finish).status, 200);
        let terminal = snapshot(&client, run_id);
        assert_cursor(&terminal, 21);
        terminal_snapshots.push((run_id, terminal));
    }

    let histories = terminal_snapshots
        .iter()
        .map(|(run_id, terminal)| {
            let through = terminal["sequence"].as_i64().unwrap();
            (
                *run_id,
                complete_opportunity_history(&client, run_id, through),
            )
        })
        .collect::<Vec<_>>();
    for (run_id, history) in &histories {
        assert_eq!(
            history["counts"],
            json!({"touched": 12, "granted": 8, "skipped": 4, "legacy_unbound": 0})
        );
        let records = history["opportunities"].as_array().unwrap();
        assert_eq!(records.len(), 12);
        assert_eq!(
            records
                .iter()
                .filter(|record| record["resolution"] == "GRANTED")
                .count(),
            8
        );
        let skipped = records
            .iter()
            .filter(|record| record["resolution"] == "SKIPPED")
            .collect::<Vec<_>>();
        assert_eq!(skipped.len(), 4);
        assert!(skipped.iter().all(|record| record["reason"]
            .as_str()
            .is_some_and(|reason| !reason.trim().is_empty())));
        assert!(records.iter().all(|record| record["run_id"] == *run_id));
    }
    let first_ids = histories[0].1["opportunities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["opportunity_id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let second_ids = histories[1].1["opportunities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["opportunity_id"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(first_ids.is_disjoint(&second_ids));

    harness.restart();
    let restarted = harness.client();
    for ((run_id, terminal), (_, hot)) in terminal_snapshots.iter().zip(&histories) {
        let cold = complete_opportunity_history(
            &restarted,
            run_id,
            terminal["sequence"].as_i64().unwrap(),
        );
        assert_eq!(&cold, hot, "cold opportunity history changed for {run_id}");
    }
}

#[test]
fn opportunity_api_is_bounded_by_the_requested_processed_prefix_without_future_leakage() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "opportunity-prefix");

    let mut frozen_prefixes = Vec::new();
    for step in 1..=5 {
        let before = snapshot(&client, "opportunity-prefix");
        let request = command_request(
            &format!("opportunity-prefix-step-{step}"),
            "step",
            "opportunity-prefix",
            before["sequence"].as_i64().unwrap(),
            before["command_version"].as_i64().unwrap(),
        );
        assert_eq!(client.command(&request).status, 200);
        let after = snapshot(&client, "opportunity-prefix");
        assert_cursor(&after, step);
        let through = after["sequence"].as_i64().unwrap();
        let history = complete_opportunity_history(&client, "opportunity-prefix", through);
        let records = history["opportunities"].as_array().unwrap();
        assert!(records.iter().all(|record| record["resolution_sequence"]
            .as_i64()
            .is_some_and(|sequence| sequence <= through)));
        assert_eq!(
            history["counts"]["touched"].as_u64(),
            Some(records.len() as u64)
        );
        frozen_prefixes.push((through, history));
    }

    let before_finish = snapshot(&client, "opportunity-prefix");
    let finish = command_request(
        "opportunity-prefix-finish",
        "finish",
        "opportunity-prefix",
        before_finish["sequence"].as_i64().unwrap(),
        before_finish["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 200);
    assert_cursor(&snapshot(&client, "opportunity-prefix"), 21);
    for (through, expected) in frozen_prefixes {
        assert_eq!(
            complete_opportunity_history(&client, "opportunity-prefix", through),
            expected,
            "later bars leaked into the frozen processed prefix at {through}"
        );
    }

    let missing_token = client.get_without_token(
        "/api/v1/runs/opportunity-prefix/opportunities?after=0&through=1&limit=2",
    );
    assert_eq!(missing_token.status, 403);
}

#[test]
fn opportunity_api_hides_an_atomic_touch_resolution_until_its_bar_is_processed() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "opportunity-partial-bar");
    let started = snapshot(&client, "opportunity-partial-bar");
    let head = started["sequence"].as_i64().unwrap();
    let database = harness.database();
    let (cycle_id, symbol, config_version): (String, String, String) = database
        .query_row(
            "SELECT cycle_id,symbol,config_version FROM events
             WHERE run_id='opportunity-partial-bar' AND event_type='GRID_CYCLE_STARTED'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let correlation = "partial-opportunity-correlation";
    let event_time = "2026-01-08 10:00:00";
    database
        .execute(
            "INSERT INTO events(
               event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,
               recorded_at,sequence_number,correlation_id,causation_id,idempotency_key,
               payload,config_version
             ) VALUES(?1,'GRID_LEVEL_TOUCHED',2,'opportunity-partial-bar',?2,?3,?4,
                      ?4,?5,?6,NULL,'partial-touch',?7,?8)",
            rusqlite::params![
                "partial-touch-event",
                cycle_id,
                symbol,
                event_time,
                head + 1,
                correlation,
                json!({"grid_index": -1, "price": "9.80"}).to_string(),
                config_version,
            ],
        )
        .unwrap();
    database
        .execute(
            "INSERT INTO events(
               event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,
               recorded_at,sequence_number,correlation_id,causation_id,idempotency_key,
               payload,config_version
             ) VALUES(?1,'GRID_LEVEL_SKIPPED',2,'opportunity-partial-bar',?2,?3,?4,
                      ?4,?5,?6,?7,'partial-skip',?8,?9)",
            rusqlite::params![
                "partial-skip-event",
                cycle_id,
                symbol,
                event_time,
                head + 2,
                correlation,
                "partial-touch-event",
                json!({"grid_index": -1, "reason": "OBSERVATION_BOUNDARY"}).to_string(),
                config_version,
            ],
        )
        .unwrap();

    let hidden = opportunity_page(&client, "opportunity-partial-bar", 0, head + 2, 10);
    assert_eq!(
        hidden["counts"],
        json!({"touched": 0, "granted": 0, "skipped": 0, "legacy_unbound": 0})
    );
    assert_eq!(hidden["opportunities"], json!([]));

    database
        .execute(
            "INSERT INTO events(
               event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,
               recorded_at,sequence_number,correlation_id,causation_id,idempotency_key,
               payload,config_version
             ) VALUES(?1,'MARKET_BAR_PROCESSED',2,'opportunity-partial-bar',?2,?3,?4,
                      ?4,?5,'partial-market',?6,'partial-processed','{}',?7)",
            rusqlite::params![
                "partial-processed-event",
                cycle_id,
                symbol,
                event_time,
                head + 3,
                "partial-skip-event",
                config_version,
            ],
        )
        .unwrap();
    let visible = complete_opportunity_history(&client, "opportunity-partial-bar", head + 3);
    assert_eq!(
        visible["counts"],
        json!({"touched": 1, "granted": 0, "skipped": 1, "legacy_unbound": 0})
    );
    assert_eq!(
        visible["opportunities"][0]["reason"],
        "OBSERVATION_BOUNDARY"
    );
}

#[test]
fn opportunity_api_preserves_full_t_plus_one_capacity_and_lot_sources() {
    let csv = "timestamp,symbol,open,high,low,close,volume,amount\n\
2026-01-05 09:30:00,600000.SH,10,10.01,9.99,10,100000,1000000\n\
2026-01-05 09:31:00,600000.SH,10,10.01,9.79,9.82,100000,982000\n\
2026-01-05 09:32:00,600000.SH,9.82,10.21,9.81,10.15,100000,1015000\n";
    let harness = Harness::with_custom_data("full-t1.csv", csv.to_owned());
    let client = harness.client();
    start_run_with_dataset(&client, "full-t1-opportunity", "full-t1.csv");
    let started = snapshot(&client, "full-t1-opportunity");
    let finish = command_request(
        "full-t1-finish",
        "finish",
        "full-t1-opportunity",
        started["sequence"].as_i64().unwrap(),
        started["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 200);
    let terminal = snapshot(&client, "full-t1-opportunity");
    assert_cursor(&terminal, 3);
    let history = complete_opportunity_history(
        &client,
        "full-t1-opportunity",
        terminal["sequence"].as_i64().unwrap(),
    );
    let full_t1 = history["opportunities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| {
            record["terminal_kind"] == "BLOCKED"
                && record["terminal_reason"] == "SELL_BLOCKED_T_PLUS_ONE"
        })
        .expect("same-day acquired lot did not produce an auditable full T+1 block");
    assert_eq!(full_t1["direction"], "SELL");
    assert_eq!(full_t1["algorithm_succeeded"], true);
    assert_eq!(full_t1["gross_available_quantity"], 0);
    assert_eq!(full_t1["platform_residual_quantity"], 0);
    assert_eq!(full_t1["algorithm_authorized_quantity"], 0);
    assert_eq!(full_t1["exercise_quantity"], 0);
    assert_eq!(full_t1["defer_quantity"], 0);
    assert_eq!(full_t1["platform_blocked_quantity"], 0);
    assert_eq!(full_t1["order_intent_quantity"], 0);
    assert_eq!(full_t1["remaining_decision_quantity"], 0);
    let capacity = &full_t1["pre_trade_capacity"];
    assert_eq!(capacity["eligible_quantity"], 0);
    assert_eq!(capacity["t_plus_one_blocked_quantity"], 1500);
    assert_eq!(capacity["risk_blocked_quantity"], 0);
    assert_eq!(capacity["no_profit_blocked_quantity"], 0);
    assert_eq!(
        capacity["t_plus_one_blocked_lot_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(capacity["source_right_ids"].is_array());
    assert!(capacity["tranche_ids"].is_array());
    assert_eq!(full_t1["partial_blocks"], json!([]));
}

#[test]
fn frozen_certification_opportunities_remain_pageable_without_current_quantity_reinterpretation() {
    let _serial = WEB_PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let temporary = TempDir::new().unwrap();
    let data =
        fs::canonicalize("artifacts/certification-v10/002256.SZ_5m_raw_20230814_20260814.csv")
            .unwrap();
    let cases = [
        (
            "v6",
            "artifacts/certification-v6/zhaoxin_5m_certification_v6.yaml",
            "artifacts/certification-v6/gridedge-certification-v6.db",
            1,
        ),
        (
            "v8",
            "artifacts/certification-v8/zhaoxin_5m_quantity_v8.yaml",
            "artifacts/certification-v8/gridedge-quantity-v8.db",
            2,
        ),
        (
            "v9",
            "artifacts/certification-v9/zhaoxin_5m_quantity_v9.yaml",
            "artifacts/certification-v9/gridedge-quantity-v9.db",
            2,
        ),
        (
            "v10",
            "artifacts/certification-v10/zhaoxin_5m_quantity_v10.yaml",
            "artifacts/certification-v10/gridedge-quantity-v10.db",
            2,
        ),
    ];

    for (label, source_config, source_database, contract_version) in cases {
        let database = temporary.path().join(format!("legacy-{label}.db"));
        fs::copy(source_database, &database).unwrap();
        let source = fs::read_to_string(source_config).unwrap();
        let database_line = source
            .lines()
            .find(|line| line.trim_start().starts_with("database:"))
            .unwrap();
        let config = temporary.path().join(format!("legacy-{label}.yaml"));
        fs::write(
            &config,
            source.replacen(
                database_line,
                &format!("database: {:?}", database.to_string_lossy()),
                1,
            ),
        )
        .unwrap();
        let connection = rusqlite::Connection::open(&database).unwrap();
        let (run_id, through): (String, i64) = connection
            .query_row(
                "SELECT run_id, MAX(sequence_number) FROM events GROUP BY run_id",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        drop(connection);

        let mut server = ServerProcess::start(&config, &data);
        let client = server.client.clone();
        let mut after = 0;
        let mut records = Vec::new();
        loop {
            let page = opportunity_page(&client, &run_id, after, through, 250);
            records.extend(page["opportunities"].as_array().unwrap().iter().cloned());
            if page["complete"] == true {
                let counts = &page["counts"];
                assert_eq!(
                    counts["touched"].as_u64().unwrap() as usize,
                    records.len(),
                    "{label} did not return its complete opportunity history"
                );
                break;
            }
            let next = page["next_sequence"].as_i64().unwrap();
            assert!(next > after, "{label} opportunity cursor stalled");
            after = next;
        }
        let grants = records
            .iter()
            .filter(|record| record["resolution"] == "GRANTED")
            .collect::<Vec<_>>();
        let unbound = records
            .iter()
            .filter(|record| record["resolution"] == "LEGACY_UNBOUND")
            .collect::<Vec<_>>();
        if matches!(label, "v6" | "v8") {
            assert!(
                !unbound.is_empty(),
                "{label} silently guessed how to bind ambiguous legacy touches"
            );
        }
        assert!(unbound.iter().all(|record| {
            record["semantics"] == "LEGACY_RECORDED"
                && record["reason"] == "LEGACY_TOUCH_RESOLUTION_UNBOUND"
                && record["reason_audit_status"] == "LEGACY_UNBOUND"
                && record["right_id"].is_null()
                && record["decision_id"].is_null()
                && record["terminal_kind"].is_null()
        }));
        assert!(!grants.is_empty(), "{label} lacks its recorded grants");
        for grant in grants {
            assert_eq!(grant["semantics"], "LEGACY_RECORDED");
            assert_eq!(grant["decision_contract_version"], contract_version);
            assert!(grant["terminal_kind"].as_str().is_some());
            for field in [
                "gross_available_quantity",
                "platform_residual_quantity",
                "algorithm_authorized_quantity",
                "platform_blocked_quantity",
                "remaining_decision_quantity",
            ] {
                assert!(
                    grant[field].is_null(),
                    "{label} legacy grant reinterpreted {field} as a current fact"
                );
            }
        }
        server.stop();
    }
}

#[test]
fn opportunity_queries_use_the_m12_type_market_and_correlation_indexes() {
    let harness = Harness::new();
    start_run(&harness.client(), "plan-run");
    let database = harness.database();
    let explain = |sql: &str| {
        let mut statement = database.prepare(sql).unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };

    let anchors = explain(
        "EXPLAIN QUERY PLAN
         SELECT touched.sequence_number,MIN(processed.sequence_number)
         FROM events AS touched
         JOIN events AS processed
           ON processed.run_id=touched.run_id
          AND processed.event_type='MARKET_BAR_PROCESSED'
          AND processed.symbol=touched.symbol
          AND processed.event_time=touched.event_time
          AND processed.sequence_number>touched.sequence_number
          AND processed.sequence_number<=1000000
         WHERE touched.run_id='plan-run'
           AND touched.event_type='GRID_LEVEL_TOUCHED'
           AND touched.sequence_number>0
           AND touched.sequence_number<=1000000
         GROUP BY touched.sequence_number
         ORDER BY touched.sequence_number
         LIMIT 251",
    );
    assert!(anchors.iter().any(|detail| {
        detail.contains("SEARCH touched USING INDEX idx_events_run_type_sequence")
    }));
    assert!(anchors.iter().any(|detail| {
        detail.contains("SEARCH processed USING COVERING INDEX idx_events_market_identity")
    }));

    let correlation = explain(
        "EXPLAIN QUERY PLAN
         SELECT sequence_number FROM events
         WHERE run_id='plan-run'
           AND correlation_id='opportunity-correlation'
           AND sequence_number<=1000000
         ORDER BY sequence_number",
    );
    assert!(correlation.iter().any(|detail| {
        detail.contains("USING COVERING INDEX idx_events_run_correlation_sequence")
    }));

    let completed_touches = explain(
        "EXPLAIN QUERY PLAN
         SELECT touched.schema_version,touched.symbol,touched.event_time,
                touched.correlation_id,json_extract(touched.payload,'$.grid_index')
         FROM events AS touched
         WHERE touched.run_id='plan-run'
           AND touched.event_type='GRID_LEVEL_TOUCHED'
           AND touched.sequence_number<=1000000
           AND EXISTS (
             SELECT 1 FROM events AS processed
             WHERE processed.run_id=touched.run_id
               AND processed.event_type='MARKET_BAR_PROCESSED'
               AND processed.symbol=touched.symbol
               AND processed.event_time=touched.event_time
               AND processed.sequence_number>touched.sequence_number
               AND processed.sequence_number<=1000000
           )",
    );
    assert!(completed_touches.iter().any(|detail| {
        detail.contains("SEARCH touched USING INDEX idx_events_run_type_sequence")
    }));
    assert!(completed_touches.iter().any(|detail| {
        detail.contains("SEARCH processed")
            && detail.contains("USING COVERING INDEX idx_events_market_identity")
    }));
    let resolutions = explain(
        "EXPLAIN QUERY PLAN
         SELECT event_type,symbol,event_time,correlation_id,
                json_extract(payload,'$.grid_index')
         FROM events
         WHERE run_id='plan-run'
           AND event_type IN ('GRID_RIGHT_GRANTED','GRID_LEVEL_SKIPPED')
           AND sequence_number<=1000000",
    );
    assert!(resolutions
        .iter()
        .any(|detail| { detail.contains("USING INDEX idx_events_run_type_sequence") }));
    let legacy_chain = explain(
        "EXPLAIN QUERY PLAN
         SELECT sequence_number FROM events
         WHERE run_id='plan-run'
           AND sequence_number>=1
           AND sequence_number<=1000000
         ORDER BY sequence_number",
    );
    assert!(legacy_chain
        .iter()
        .any(|detail| { detail.contains("USING COVERING INDEX sqlite_autoindex_events_2") }));
}

#[test]
fn bars_api_rejects_legacy_runs_without_a_durable_dataset_binding() {
    let harness = Harness::new();
    let config = Config::load(&harness.config).unwrap();
    let mut store = SqliteStore::open(&config.database).unwrap();
    store.migrate().unwrap();
    let policy = gate::from_config(&config).unwrap();
    let mut service = GridAutomationService::start_new(
        config.clone(),
        store,
        policy,
        Some("legacy-unbound".to_owned()),
    )
    .unwrap();
    let mut feed = CsvReplayFeed::load(&harness.data, &config.symbol).unwrap();
    service.run_feed(&mut feed).unwrap();
    service.stop().unwrap();
    let database = harness.database();
    let (event_count, descriptor_count): (i64, i64) = database
        .query_row(
            "SELECT COUNT(*),SUM(event_type='REPLAY_INITIALIZED')
             FROM events WHERE run_id='legacy-unbound'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(
        event_count > 0,
        "legacy fixture must contain a real journal"
    );
    assert_eq!(descriptor_count, 0);

    let response = harness
        .client()
        .get("/api/v1/runs/legacy-unbound/bars?max_points=100");
    assert_eq!(response.status, 409);
    let error = response.json();
    assert_eq!(error["api_version"], API_VERSION);
    assert_eq!(error["code"], "COMMAND_CONFLICT");
    assert!(error["message"]
        .as_str()
        .is_some_and(|message| message.contains("no durable dataset binding")));
}

#[test]
fn cli_replay_persists_one_dataset_descriptor_before_market_and_serves_bars() {
    let harness = Harness::new();
    let output = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "replay",
            "--config",
            harness.config.to_str().unwrap(),
            "--data",
            harness.data.to_str().unwrap(),
            "--run-id",
            "cli-bound",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "CLI replay failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let database = harness.database();
    let (descriptor_count, descriptor_sequence, first_market_sequence): (i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='REPLAY_INITIALIZED'),
               MIN(CASE WHEN event_type='REPLAY_INITIALIZED' THEN sequence_number END),
               MIN(CASE WHEN event_type='MARKET_DATA_RECEIVED' THEN sequence_number END)
             FROM events WHERE run_id='cli-bound'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(descriptor_count, 1);
    assert!(descriptor_sequence < first_market_sequence);
    let payload: String = database
        .query_row(
            "SELECT payload FROM events
             WHERE run_id='cli-bound' AND event_type='REPLAY_INITIALIZED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let descriptor: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(descriptor["dataset_id"], "sample.csv");
    assert_eq!(
        descriptor["symbol"],
        Config::load(&harness.config).unwrap().symbol
    );
    let expected_bars = CsvReplayFeed::load(
        &harness.data,
        &Config::load(&harness.config).unwrap().symbol,
    )
    .unwrap()
    .bars()
    .len();
    assert_eq!(descriptor["total_bars"], expected_bars);
    assert_eq!(descriptor["data_sha256"].as_str().unwrap().len(), 64);

    let bars = harness
        .client()
        .get("/api/v1/runs/cli-bound/bars?max_points=100");
    assert_eq!(bars.status, 200);
    let bars = bars.json();
    assert_eq!(bars["visible_bars"], expected_bars);
    assert_eq!(bars["sampled"].as_array().unwrap().len(), expected_bars);
}

#[test]
fn native_step_forms_create_completed_durable_inbox_receipts() {
    let harness = Harness::new();
    let client = harness.client();
    let dashboard = client.get("/");
    assert_eq!(dashboard.status, 200);
    let html = String::from_utf8(dashboard.body).unwrap();
    let csrf = html_hidden_value(&html, "csrf_token");

    let start = client.post_form(
        "/actions/step/start",
        &[
            ("run_id", "native-form"),
            ("dataset", "sample.csv"),
            ("request_id", "native-form-start"),
            ("expected_sequence", "0"),
            ("expected_version", "0"),
            ("csrf_token", &csrf),
        ],
    );
    assert_eq!(start.status, 303);
    let created = snapshot(&client, "native-form");

    let sequence = created["sequence"].as_i64().unwrap().to_string();
    let version = created["command_version"].as_i64().unwrap().to_string();
    let next = client.post_form(
        "/actions/step/next",
        &[
            ("run_id", "native-form"),
            ("dataset", ""),
            ("request_id", "native-form-next"),
            ("expected_sequence", &sequence),
            ("expected_version", &version),
            ("csrf_token", &csrf),
        ],
    );
    assert_eq!(next.status, 303);
    assert_cursor(&snapshot(&client, "native-form"), 1);

    let database = harness.database();
    let mut statement = database
        .prepare(
            "SELECT request_id,command,receipt_state,response_json
             FROM web_command_inbox WHERE run_id='native-form' ORDER BY accepted_version",
        )
        .unwrap();
    let receipts: Vec<(String, String, String, Option<String>)> = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(receipts.len(), 2);
    assert_eq!(receipts[0].0, "native-form-start");
    assert_eq!(receipts[0].1, "start");
    assert_eq!(receipts[1].0, "native-form-next");
    assert_eq!(receipts[1].1, "step");
    assert!(receipts
        .iter()
        .all(|receipt| receipt.2 == "COMPLETED" && receipt.3.is_some()));
}

#[test]
fn pending_inbox_migration_preserves_legacy_rows_but_never_guesses_their_request() {
    let harness = Harness::with_legacy_pending_start("legacy-pending", "legacy-pending-start");
    let discovered = harness.client().pending_commands();
    assert_eq!(
        discovered.status,
        200,
        "{}",
        String::from_utf8_lossy(&discovered.body)
    );
    let database = harness.database();
    let columns: Vec<String> = database
        .prepare("PRAGMA table_info(web_command_inbox)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(columns.iter().any(|column| column == "request_json"));
    assert!(columns.iter().any(|column| column == "plan_sha256"));
    assert!(columns.iter().any(|column| column == "config_sha256"));
    assert!(columns.iter().any(|column| column == "algorithm_sha256"));

    let discovered = discovered.json();
    assert_eq!(discovered["api_version"], API_VERSION);
    assert_eq!(discovered["commands"].as_array().unwrap().len(), 1);
    assert_eq!(discovered["commands"][0]["run_id"], "legacy-pending");
    assert_eq!(
        discovered["commands"][0]["request_id"],
        "legacy-pending-start"
    );
    assert_eq!(discovered["commands"][0]["command"], "start");
    assert_eq!(discovered["commands"][0]["recovery_state"], "blocked");

    let retry = harness
        .client()
        .retry_pending("legacy-pending", "legacy-pending-start");
    assert_eq!(retry.status, 409);
    let event_count: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='legacy-pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 0);
}

#[test]
fn physical_migration_8_pending_row_upgrades_to_runtime_identity_columns_but_stays_blocked() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_migration_8_pending_start
             BEFORE INSERT ON events
             WHEN NEW.run_id='migration-8-pending'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected migration-8 START failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "migration-8-pending-request",
        "start",
        "migration-8-pending",
        0,
        0,
    );
    request["dataset"] = json!("sample.csv");
    assert_eq!(client.command(&request).status, 500);
    database
        .execute_batch("DROP TRIGGER fail_migration_8_pending_start;")
        .unwrap();
    harness.server.stop();
    drop(database);

    {
        let database = harness.database();
        database
            .execute_batch(
                "DELETE FROM schema_migrations WHERE version IN (9,10,11,12,13);
                 DROP TRIGGER database_identity_is_immutable_insert;
                 DROP TRIGGER database_identity_is_immutable_update;
                 DROP TRIGGER database_identity_is_immutable_delete;
                 DROP TABLE database_identity;
                 DROP INDEX idx_events_market_identity;
                 DROP INDEX idx_events_run_type_sequence;
                 CREATE INDEX IF NOT EXISTS idx_events_run_sequence
                   ON events(run_id,sequence_number);
                 CREATE INDEX IF NOT EXISTS idx_events_market_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_DATA_RECEIVED';
                 CREATE INDEX IF NOT EXISTS idx_events_processed_bar_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_BAR_PROCESSED';
                 ALTER TABLE web_command_inbox DROP COLUMN config_sha256;
                 ALTER TABLE web_command_inbox DROP COLUMN algorithm_sha256;",
            )
            .unwrap();
        let (version, count): (i64, i64) = database
            .query_row(
                "SELECT MAX(version),COUNT(*) FROM schema_migrations",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!((version, count), (8, 8));
        let columns: Vec<String> = database
            .prepare("PRAGMA table_info(web_command_inbox)")
            .unwrap()
            .query_map([], |row| row.get(1))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(columns.iter().any(|column| column == "request_json"));
        assert!(columns.iter().any(|column| column == "plan_sha256"));
        assert!(!columns.iter().any(|column| column == "config_sha256"));
        assert!(!columns.iter().any(|column| column == "algorithm_sha256"));
    }

    harness.restart();
    let client = harness.client();
    let discovered = pending_command_batch(&client);
    assert_eq!(discovered["commands"].as_array().unwrap().len(), 1);
    assert_eq!(discovered["commands"][0]["run_id"], "migration-8-pending");
    assert_eq!(discovered["commands"][0]["recovery_state"], "blocked");
    let database = harness.database();
    let version: i64 = database
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 13);
    let columns: Vec<String> = database
        .prepare("PRAGMA table_info(web_command_inbox)")
        .unwrap()
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(columns.iter().any(|column| column == "config_sha256"));
    assert!(columns.iter().any(|column| column == "algorithm_sha256"));
    let type_index: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='index' AND name='idx_events_run_type_sequence'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(type_index, 1);
    let market_identity_index: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type='index' AND name='idx_events_market_identity'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(market_identity_index, 1);
    for removed in [
        "idx_events_run_sequence",
        "idx_events_market_run",
        "idx_events_processed_bar_run",
    ] {
        let count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [removed],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "M12 must remove redundant {removed}");
    }
    let events: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='migration-8-pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(events, 0);
}

#[test]
fn physical_migration_10_upgrades_through_current_and_keeps_required_query_plans() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    harness.server.stop();

    {
        let database = harness.database();
        database
            .execute_batch(
                "DELETE FROM schema_migrations WHERE version IN (11,12,13);
                 DROP TRIGGER database_identity_is_immutable_insert;
                 DROP TRIGGER database_identity_is_immutable_update;
                 DROP TRIGGER database_identity_is_immutable_delete;
                 DROP TABLE database_identity;
                 DROP INDEX idx_events_market_identity;
                 CREATE INDEX IF NOT EXISTS idx_events_run_sequence
                   ON events(run_id,sequence_number);
                 CREATE INDEX IF NOT EXISTS idx_events_market_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_DATA_RECEIVED';
                 CREATE INDEX IF NOT EXISTS idx_events_processed_bar_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_BAR_PROCESSED';",
            )
            .unwrap();
        let version: i64 = database
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, 10);
    }

    harness.restart();
    assert_eq!(harness.client().get("/api/v1/runs").status, 200);
    let database = harness.database();
    let version: i64 = database
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(version, 13);
    for removed in [
        "idx_events_run_sequence",
        "idx_events_market_run",
        "idx_events_processed_bar_run",
    ] {
        let count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [removed],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "M12 must remove redundant {removed}");
    }
    for required in [
        "idx_events_run_type_sequence",
        "idx_events_market_identity",
        "idx_events_run_correlation_sequence",
        "sqlite_autoindex_events_2",
    ] {
        let count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [required],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "M12 removed required {required}");
    }
    let query_plan: Vec<String> = database
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT MIN(received.sequence_number)
             FROM events AS received
             WHERE received.run_id=?1 AND received.event_type=?2
               AND NOT EXISTS (
                 SELECT 1 FROM events AS processed
                 WHERE processed.run_id=received.run_id
                   AND processed.event_type=?3
                   AND processed.symbol=received.symbol
                   AND processed.event_time=received.event_time
               )",
        )
        .unwrap()
        .query_map(
            rusqlite::params![
                "query-plan-run",
                "MARKET_DATA_RECEIVED",
                "MARKET_BAR_PROCESSED"
            ],
            |row| row.get(3),
        )
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        query_plan.iter().any(|detail| {
            detail.contains("received")
                && detail.contains("USING INDEX idx_events_run_type_sequence")
                && detail.contains("run_id=?")
                && detail.contains("event_type=?")
        }),
        "outer incomplete-bar lookup did not use M10 index: {query_plan:?}"
    );
    assert!(
        query_plan.iter().any(|detail| {
            detail.contains("processed")
                && detail.contains("USING COVERING INDEX idx_events_market_identity")
                && detail.contains("run_id=?")
                && detail.contains("event_type=?")
                && detail.contains("symbol=?")
                && detail.contains("event_time=?")
        }),
        "inner incomplete-bar lookup did not use M11 index: {query_plan:?}"
    );
    let load_after_plan: Vec<String> = database
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT event_id FROM events
             WHERE run_id=?1 AND sequence_number>?2
             ORDER BY sequence_number LIMIT ?3",
        )
        .unwrap()
        .query_map(rusqlite::params!["query-plan-run", 0, 100], |row| {
            row.get(3)
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        load_after_plan
            .iter()
            .any(|detail| detail.contains("USING INDEX sqlite_autoindex_events_2")),
        "load_after did not fall back to the UNIQUE(run,sequence) index: {load_after_plan:?}"
    );
}

#[test]
fn physical_migration_11_through_current_removes_only_redundant_indexes_and_is_idempotent() {
    let mut harness = Harness::new();
    let client = harness.client();
    let started = start_run(&client, "migration-11-to-12");
    let step = command_request(
        "migration-11-to-12-step",
        "step",
        "migration-11-to-12",
        started["accepted_sequence"].as_i64().unwrap(),
        started["accepted_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    harness.server.stop();

    let before = {
        let database = harness.database();
        database
            .execute_batch(
                "DELETE FROM schema_migrations WHERE version IN (12,13);
                 DROP TRIGGER database_identity_is_immutable_insert;
                 DROP TRIGGER database_identity_is_immutable_update;
                 DROP TRIGGER database_identity_is_immutable_delete;
                 DROP TABLE database_identity;
                 CREATE INDEX idx_events_run_sequence
                   ON events(run_id,sequence_number);
                 CREATE INDEX idx_events_market_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_DATA_RECEIVED';
                 CREATE INDEX idx_events_processed_bar_run
                   ON events(run_id,sequence_number)
                   WHERE event_type='MARKET_BAR_PROCESSED';",
            )
            .unwrap();
        let version: i64 = database
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, 11);
        for redundant in [
            "idx_events_run_sequence",
            "idx_events_market_run",
            "idx_events_processed_bar_run",
        ] {
            let count: i64 = database
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [redundant],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1);
        }
        database
            .query_row(
                "SELECT
                   (SELECT COUNT(*) FROM events WHERE run_id='migration-11-to-12'),
                   (SELECT MAX(sequence_number) FROM events WHERE run_id='migration-11-to-12'),
                   (SELECT COUNT(*) FROM snapshots WHERE run_id='migration-11-to-12'),
                   (SELECT COUNT(*) FROM web_command_inbox WHERE run_id='migration-11-to-12')",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap()
    };

    for restart in 1..=2 {
        harness.restart();
        assert_eq!(harness.client().get("/api/v1/runs").status, 200);
        let database = harness.database();
        let version: i64 = database
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, 13);
        for redundant in [
            "idx_events_run_sequence",
            "idx_events_market_run",
            "idx_events_processed_bar_run",
        ] {
            let count: i64 = database
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [redundant],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 0, "restart {restart} restored {redundant}");
        }
        let after: (i64, i64, i64, i64) = database
            .query_row(
                "SELECT
                   (SELECT COUNT(*) FROM events WHERE run_id='migration-11-to-12'),
                   (SELECT MAX(sequence_number) FROM events WHERE run_id='migration-11-to-12'),
                   (SELECT COUNT(*) FROM snapshots WHERE run_id='migration-11-to-12'),
                   (SELECT COUNT(*) FROM web_command_inbox WHERE run_id='migration-11-to-12')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(after, before);
        if restart == 1 {
            let explain = |sql: &str| {
                let mut statement = database.prepare(sql).unwrap();
                statement
                    .query_map([], |row| row.get::<_, String>(3))
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap()
            };
            let anchors = explain(
                "EXPLAIN QUERY PLAN
                 SELECT touched.sequence_number,MIN(processed.sequence_number)
                 FROM events AS touched
                 JOIN events AS processed
                   ON processed.run_id=touched.run_id
                  AND processed.event_type='MARKET_BAR_PROCESSED'
                  AND processed.symbol=touched.symbol
                  AND processed.event_time=touched.event_time
                  AND processed.sequence_number>touched.sequence_number
                  AND processed.sequence_number<=1000000
                 WHERE touched.run_id='migration-11-to-12'
                   AND touched.event_type='GRID_LEVEL_TOUCHED'
                   AND touched.sequence_number>0
                   AND touched.sequence_number<=1000000
                 GROUP BY touched.sequence_number
                 ORDER BY touched.sequence_number LIMIT 251",
            );
            assert!(anchors.iter().any(|detail| {
                detail.contains("SEARCH touched USING INDEX idx_events_run_type_sequence")
            }));
            assert!(anchors.iter().any(|detail| {
                detail.contains("SEARCH processed USING COVERING INDEX idx_events_market_identity")
            }));
            let correlation = explain(
                "EXPLAIN QUERY PLAN
                 SELECT sequence_number FROM events
                 WHERE run_id='migration-11-to-12'
                   AND correlation_id='opportunity-correlation'
                   AND sequence_number<=1000000
                 ORDER BY sequence_number",
            );
            assert!(correlation.iter().any(|detail| {
                detail.contains("USING COVERING INDEX idx_events_run_correlation_sequence")
            }));
            let sequence = explain(
                "EXPLAIN QUERY PLAN
                 SELECT event_id FROM events
                 WHERE run_id='migration-11-to-12' AND sequence_number>0
                 ORDER BY sequence_number LIMIT 1",
            );
            assert!(sequence
                .iter()
                .any(|detail| detail.contains("USING INDEX sqlite_autoindex_events_2")));
        }
    }
}

#[test]
fn no_event_start_is_discoverable_after_restart_and_retries_from_its_stored_request() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_self_service_start
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-self-service-start'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected START business failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "pending-self-service-start-request",
        "start",
        "pending-self-service-start",
        0,
        0,
    );
    request["dataset"] = json!("sample.csv");
    let failed = client.command(&request);
    assert_eq!(failed.status, 500);
    assert_eq!(client.get("/api/v1/runs").json(), json!([]));

    let (request_json, plan_sha256, state, events): (String, String, String, i64) = database
        .query_row(
            "SELECT request_json,plan_sha256,receipt_state,
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-self-service-start')
             FROM web_command_inbox
             WHERE run_id='pending-self-service-start'
              AND request_id='pending-self-service-start-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&request_json).unwrap(),
        request
    );
    assert_eq!(plan_sha256.len(), 64);
    assert!(plan_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!((state.as_str(), events), ("PENDING", 0));

    let discovered = pending_command_batch(&client);
    assert_eq!(discovered["commands"].as_array().unwrap().len(), 1);
    assert_eq!(
        discovered["commands"][0],
        json!({
            "run_id": "pending-self-service-start",
            "request_id": "pending-self-service-start-request",
            "command": "start",
            "accepted_version": 1,
            "recovery_state": "retryable"
        })
    );

    database
        .execute_batch("DROP TRIGGER fail_self_service_start;")
        .unwrap();
    harness.restart();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").json(), json!([]));
    assert_eq!(
        pending_command_batch(&client)["commands"][0]["request_id"],
        "pending-self-service-start-request"
    );
    let expansive_retry = client.request(
        "POST",
        "/api/v1/pending-commands/retry",
        Some(&json!({
            "run_id": "pending-self-service-start",
            "request_id": "pending-self-service-start-request",
            "dataset": "sample.csv"
        })),
    );
    assert_eq!(expansive_retry.status, 422);

    let retried = client.retry_pending(
        "pending-self-service-start",
        "pending-self-service-start-request",
    );
    assert_eq!(retried.status, 200);
    assert_eq!(retried.json()["accepted_version"], 1);
    let (config_count, descriptor_count, receipt_count): (i64, i64, i64) = harness
        .database()
        .query_row(
            "SELECT
               SUM(event_type='CONFIG_SNAPSHOTTED'),
               SUM(event_type='REPLAY_INITIALIZED'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='pending-self-service-start-request'
                  AND receipt_state='COMPLETED')
             FROM events WHERE run_id='pending-self-service-start'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((config_count, descriptor_count, receipt_count), (1, 1, 1));
    assert!(pending_command_batch(&client)["commands"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn committed_start_receipt_recovers_from_frozen_ledger_after_data_and_config_drift() {
    let mut harness = Harness::with_mutable_data();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_committed_start_receipt_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='committed-start-receipt-request'
              AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected START receipt completion failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "committed-start-receipt-request",
        "start",
        "committed-start-receipt",
        0,
        0,
    );
    request["dataset"] = json!("mutable-sample.csv");
    let failed = client.command(&request);
    assert_eq!(failed.status, 500);
    assert_eq!(failed.json()["code"], "INTERNAL_ERROR");

    let events_before: Vec<(i64, String, String, String, String)> = database
        .prepare(
            "SELECT sequence_number,event_id,event_type,idempotency_key,payload
             FROM events WHERE run_id='committed-start-receipt'
             ORDER BY sequence_number",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let (config_count, algorithm_count, descriptor_count): (i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='CONFIG_SNAPSHOTTED'),
               SUM(event_type='ALGORITHM_REGISTERED'),
               SUM(event_type='REPLAY_INITIALIZED')
             FROM events WHERE run_id='committed-start-receipt'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((config_count, algorithm_count, descriptor_count), (1, 1, 1));
    assert!(!events_before.is_empty());
    let (receipt_state, response_json, config_sha256, algorithm_sha256): (
        String,
        Option<String>,
        String,
        String,
    ) = database
        .query_row(
            "SELECT receipt_state,response_json,config_sha256,algorithm_sha256
             FROM web_command_inbox
             WHERE run_id='committed-start-receipt'
              AND request_id='committed-start-receipt-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(receipt_state, "PENDING");
    assert_eq!(response_json, None);
    assert_eq!(config_sha256.len(), 64);
    assert_eq!(algorithm_sha256.len(), 64);

    database
        .execute_batch("DROP TRIGGER fail_committed_start_receipt_completion;")
        .unwrap();
    harness.server.stop();
    let original_data = fs::read_to_string(&harness.data).unwrap();
    let changed_data = original_data.replacen(
        "10.00,10.01,9.99,10.00,100000,1000000",
        "10.00,10.01,9.99,10.00,100001,1000000",
        1,
    );
    assert_ne!(changed_data, original_data);
    fs::write(&harness.data, changed_data).unwrap();
    let original_config = fs::read_to_string(&harness.config).unwrap();
    let changed_config =
        original_config.replacen("anchor_price: \"10.00\"", "anchor_price: \"10.01\"", 1);
    assert_ne!(changed_config, original_config);
    fs::write(&harness.config, changed_config).unwrap();
    harness.restart();
    let client = harness.client();

    let discovered = client.pending_commands();
    assert_eq!(
        discovered.status,
        200,
        "{}",
        String::from_utf8_lossy(&discovered.body)
    );
    assert!(discovered.json()["commands"].as_array().unwrap().is_empty());
    let recovered =
        client.retry_pending("committed-start-receipt", "committed-start-receipt-request");
    assert_eq!(
        recovered.status,
        200,
        "{}",
        String::from_utf8_lossy(&recovered.body)
    );
    assert_eq!(recovered.json()["accepted_version"], 1);
    assert_eq!(
        recovered.json()["accepted_sequence"],
        events_before.last().unwrap().0
    );

    let database = harness.database();
    let events_after: Vec<(i64, String, String, String, String)> = database
        .prepare(
            "SELECT sequence_number,event_id,event_type,idempotency_key,payload
             FROM events WHERE run_id='committed-start-receipt'
             ORDER BY sequence_number",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(events_after, events_before);
    let (state, stored_response, stored_config_sha, stored_algorithm_sha): (
        String,
        String,
        String,
        String,
    ) = database
        .query_row(
            "SELECT receipt_state,response_json,config_sha256,algorithm_sha256
             FROM web_command_inbox
             WHERE run_id='committed-start-receipt'
              AND request_id='committed-start-receipt-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state, "COMPLETED");
    assert_eq!(
        serde_json::from_str::<Value>(&stored_response).unwrap(),
        recovered.json()
    );
    assert_eq!(stored_config_sha, config_sha256);
    assert_eq!(stored_algorithm_sha, algorithm_sha256);
}

#[test]
fn uncommitted_start_is_blocked_after_configuration_identity_changes() {
    let mut harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_uncommitted_start_before_business
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-config-drift'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected pre-business START failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "pending-config-drift-request",
        "start",
        "pending-config-drift",
        0,
        0,
    );
    request["dataset"] = json!("sample.csv");
    assert_eq!(client.command(&request).status, 500);
    let (state, response_json, plan_sha, config_sha, algorithm_sha, events): (
        String,
        Option<String>,
        String,
        String,
        String,
        i64,
    ) = database
        .query_row(
            "SELECT receipt_state,response_json,plan_sha256,config_sha256,algorithm_sha256,
                    (SELECT COUNT(*) FROM events WHERE run_id='pending-config-drift')
             FROM web_command_inbox
             WHERE run_id='pending-config-drift'
              AND request_id='pending-config-drift-request'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), response_json, events),
        ("PENDING", None, 0)
    );
    for identity in [&plan_sha, &config_sha, &algorithm_sha] {
        assert_eq!(identity.len(), 64);
        assert!(identity.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    database
        .execute_batch("DROP TRIGGER fail_uncommitted_start_before_business;")
        .unwrap();
    harness.server.stop();
    let original_config = fs::read_to_string(&harness.config).unwrap();
    let changed_config =
        original_config.replacen("anchor_price: \"10.00\"", "anchor_price: \"10.01\"", 1);
    assert_ne!(changed_config, original_config);
    fs::write(&harness.config, changed_config).unwrap();
    harness.restart();
    let client = harness.client();

    let discovered = pending_command_batch(&client);
    assert_eq!(discovered["commands"].as_array().unwrap().len(), 1);
    assert_eq!(discovered["commands"][0]["recovery_state"], "blocked");
    let retry = client.retry_pending("pending-config-drift", "pending-config-drift-request");
    assert_eq!(retry.status, 409);
    assert_eq!(retry.json()["code"], "PENDING_PLAN_CONFLICT");
    let database = harness.database();
    let (state, response_json, events): (String, Option<String>, i64) = database
        .query_row(
            "SELECT receipt_state,response_json,
                    (SELECT COUNT(*) FROM events WHERE run_id='pending-config-drift')
             FROM web_command_inbox
             WHERE run_id='pending-config-drift'
              AND request_id='pending-config-drift-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), response_json, events),
        ("PENDING", None, 0)
    );
}

#[test]
fn concurrent_self_service_retries_share_one_stored_request_and_one_response() {
    let harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_concurrent_pending_start
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-concurrent-start'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected concurrent START failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "pending-concurrent-start-request",
        "start",
        "pending-concurrent-start",
        0,
        0,
    );
    request["dataset"] = json!("sample.csv");
    assert_eq!(client.command(&request).status, 500);
    database
        .execute_batch("DROP TRIGGER fail_concurrent_pending_start;")
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let client = client.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            client.retry_pending(
                "pending-concurrent-start",
                "pending-concurrent-start-request",
            )
        }));
    }
    barrier.wait();
    let responses: Vec<HttpResponse> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert!(responses.iter().all(|response| response.status == 200));
    assert_eq!(responses[0].json(), responses[1].json());
    let (receipts, descriptors): (i64, i64) = database
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='pending-concurrent-start-request'
                  AND receipt_state='COMPLETED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='pending-concurrent-start'
                  AND event_type='REPLAY_INITIALIZED')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((receipts, descriptors), (1, 1));
}

#[test]
fn self_service_retry_completes_partial_step_and_finish_without_duplicate_bars() {
    let harness = Harness::new();
    let client = harness.client();
    let database = harness.database();
    start_run(&client, "pending-step-self-service");
    let before = snapshot(&client, "pending-step-self-service");
    database
        .execute_batch(
            "CREATE TRIGGER fail_pending_step_bar
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-step-self-service'
              AND NEW.event_type='MARKET_BAR_PROCESSED'
             BEGIN SELECT RAISE(ABORT, 'injected STEP bar failure'); END;",
        )
        .unwrap();
    let step = command_request(
        "pending-step-self-service-request",
        "step",
        "pending-step-self-service",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 500);
    let partial: (i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='MARKET_DATA_RECEIVED'),
               SUM(event_type='MARKET_BAR_PROCESSED')
             FROM events WHERE run_id='pending-step-self-service'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(partial, (1, 0));
    assert_eq!(
        pending_command_batch(&client)["commands"][0]["command"],
        "step"
    );
    database
        .execute_batch("DROP TRIGGER fail_pending_step_bar;")
        .unwrap();
    assert_eq!(
        client
            .retry_pending(
                "pending-step-self-service",
                "pending-step-self-service-request"
            )
            .status,
        200
    );
    let completed_step: (i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='MARKET_DATA_RECEIVED'),
               SUM(event_type='MARKET_BAR_PROCESSED')
             FROM events WHERE run_id='pending-step-self-service'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(completed_step, (1, 1));

    start_run(&client, "pending-finish-self-service");
    let before_finish = snapshot(&client, "pending-finish-self-service");
    database
        .execute_batch(
            "CREATE TRIGGER fail_pending_finish_bar
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-finish-self-service'
              AND NEW.event_type='MARKET_BAR_PROCESSED'
             BEGIN SELECT RAISE(ABORT, 'injected FINISH bar failure'); END;",
        )
        .unwrap();
    let finish = command_request(
        "pending-finish-self-service-request",
        "finish",
        "pending-finish-self-service",
        before_finish["sequence"].as_i64().unwrap(),
        before_finish["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 500);
    assert_eq!(
        pending_command_batch(&client)["commands"][0]["command"],
        "finish"
    );
    database
        .execute_batch("DROP TRIGGER fail_pending_finish_bar;")
        .unwrap();
    assert_eq!(
        client
            .retry_pending(
                "pending-finish-self-service",
                "pending-finish-self-service-request"
            )
            .status,
        200
    );
    let (received, processed, receipt_count): (i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='MARKET_DATA_RECEIVED'),
               SUM(event_type='MARKET_BAR_PROCESSED'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='pending-finish-self-service-request'
                  AND receipt_state='COMPLETED')
             FROM events WHERE run_id='pending-finish-self-service'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((received, processed, receipt_count), (21, 21, 1));
}

#[test]
fn finish_pending_after_the_last_bar_requires_one_durable_stop_before_completion() {
    let harness = Harness::new();
    let client = harness.client();
    let run_id = "pending-finish-stop-boundary";
    let request_id = "pending-finish-stop-boundary-request";
    start_run(&client, run_id);
    let before = snapshot(&client, run_id);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_pending_finish_service_stop
             BEFORE INSERT ON events
             WHEN NEW.run_id='pending-finish-stop-boundary'
              AND NEW.event_type='SERVICE_STOPPED'
             BEGIN SELECT RAISE(ABORT, 'injected FINISH stop failure'); END;",
        )
        .unwrap();
    let finish = command_request(
        request_id,
        "finish",
        run_id,
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );

    let failed = client.command(&finish);
    assert_eq!(failed.status, 500);
    let before_discovery: (String, i64, i64, i64, i64) = database
        .query_row(
            "SELECT receipt_state,
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_DATA_RECEIVED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_BAR_DECISIONS_COMMITTED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_BAR_PROCESSED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='SERVICE_STOPPED')
             FROM web_command_inbox
             WHERE run_id='pending-finish-stop-boundary'
              AND request_id='pending-finish-stop-boundary-request'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(before_discovery, ("PENDING".to_owned(), 21, 21, 21, 0));

    let pending = pending_command_batch(&client);
    assert_eq!(pending["commands"].as_array().unwrap().len(), 1);
    assert_eq!(pending["commands"][0]["run_id"], run_id);
    assert_eq!(pending["commands"][0]["request_id"], request_id);
    assert_eq!(pending["commands"][0]["command"], "finish");
    assert_eq!(pending["commands"][0]["recovery_state"], "retryable");
    let after_discovery: (String, i64) = database
        .query_row(
            "SELECT receipt_state,
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='SERVICE_STOPPED')
             FROM web_command_inbox
             WHERE run_id='pending-finish-stop-boundary'
              AND request_id='pending-finish-stop-boundary-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(after_discovery, ("PENDING".to_owned(), 0));

    database
        .execute_batch("DROP TRIGGER fail_pending_finish_service_stop;")
        .unwrap();
    let recovered = client.retry_pending(run_id, request_id);
    assert_eq!(
        recovered.status,
        200,
        "{}",
        String::from_utf8_lossy(&recovered.body)
    );
    let response = recovered.json();
    let final_facts: (String, i64, i64, i64, i64, i64, i64, String) = database
        .query_row(
            "SELECT inbox.receipt_state,
                    json_extract(inbox.response_json,'$.accepted_sequence'),
                    stopped.sequence_number,
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_DATA_RECEIVED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_BAR_DECISIONS_COMMITTED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='MARKET_BAR_PROCESSED'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='pending-finish-stop-boundary'
                       AND event_type='SERVICE_STOPPED'),
                    inbox.response_json
             FROM web_command_inbox AS inbox
             JOIN events AS stopped
               ON stopped.run_id=inbox.run_id
              AND stopped.event_type='SERVICE_STOPPED'
             WHERE inbox.run_id='pending-finish-stop-boundary'
              AND inbox.request_id='pending-finish-stop-boundary-request'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(final_facts.0, "COMPLETED");
    assert!(final_facts.1 >= final_facts.2);
    assert_eq!(
        (final_facts.3, final_facts.4, final_facts.5, final_facts.6),
        (21, 21, 21, 1)
    );
    assert_eq!(response["accepted_sequence"], final_facts.1);
    assert_eq!(
        serde_json::from_str::<Value>(&final_facts.7).unwrap(),
        response
    );
    assert!(pending_command_batch(&client)["commands"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn finish_completes_a_current_safe_run_without_creating_an_order() {
    let harness = Harness::new();
    let client = harness.client();
    let run_id = "finish-current-safe";
    let request_id = "finish-current-safe-request";
    start_run(&client, run_id);
    let database = harness.database();

    let stored_snapshot: String = database
        .query_row(
            "SELECT snapshot_json FROM paper_accounts WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )
        .unwrap();
    let mut broker: Value = serde_json::from_str(&stored_snapshot).unwrap();
    broker["cash"]["available"] = json!("200001.00");
    database
        .execute(
            "UPDATE paper_accounts SET snapshot_json=?1 WHERE run_id=?2",
            rusqlite::params![serde_json::to_string(&broker).unwrap(), run_id],
        )
        .unwrap();

    let dashboard = client.get(&format!("/?run_id={run_id}"));
    assert_eq!(dashboard.status, 200);
    let csrf = html_hidden_value(&String::from_utf8(dashboard.body).unwrap(), "csrf_token");
    let reconciliation = client.post_form(
        "/actions/reconcile",
        &[
            ("run_id", run_id),
            ("dataset", ""),
            ("request_id", "unused-reconcile-form-request"),
            ("expected_sequence", "0"),
            ("expected_version", "0"),
            ("csrf_token", &csrf),
        ],
    );
    assert_eq!(reconciliation.status, 303);
    let safe = snapshot(&client, run_id);
    assert_eq!(safe["state"]["mode"], "SAFE");
    assert!(safe["state"]["orders"].as_object().unwrap().is_empty());

    let finish = command_request(
        request_id,
        "finish",
        run_id,
        safe["sequence"].as_i64().unwrap(),
        safe["command_version"].as_i64().unwrap(),
    );
    let completed = client.command(&finish);
    assert_eq!(
        completed.status,
        200,
        "{}",
        String::from_utf8_lossy(&completed.body)
    );
    let terminal = snapshot(&client, run_id);
    assert_cursor(&terminal, 21);
    assert_eq!(terminal["state"]["mode"], "SAFE");
    assert!(terminal["state"]["orders"].as_object().unwrap().is_empty());

    let facts: (i64, i64, i64, i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='MARKET_DATA_RECEIVED'),
               SUM(event_type='MARKET_BAR_DECISIONS_COMMITTED'),
               SUM(event_type='MARKET_BAR_PROCESSED'),
               SUM(event_type='SERVICE_STOPPED'),
               SUM(event_type='ORDER_INTENT_CREATED'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE run_id='finish-current-safe'
                  AND request_id='finish-current-safe-request'
                  AND receipt_state='COMPLETED')
             FROM events WHERE run_id='finish-current-safe'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(facts, (21, 21, 21, 1, 0, 1));
    assert!(
        completed.json()["accepted_sequence"].as_i64().unwrap()
            >= terminal["sequence"].as_i64().unwrap()
    );
}

#[test]
fn finish_completes_a_current_read_only_run_without_creating_an_order() {
    let harness = Harness::new();
    let client = harness.client();
    let run_id = "finish-current-read-only";
    start_run(&client, run_id);
    let started = snapshot(&client, run_id);
    let first_step = command_request(
        "finish-current-read-only-step",
        "step",
        run_id,
        started["sequence"].as_i64().unwrap(),
        started["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&first_step).status, 200);
    let before = snapshot(&client, run_id);
    assert_cursor(&before, 1);
    let database = harness.database();
    let corrupted = database
        .execute(
            "UPDATE snapshots SET checksum='corrupt' WHERE run_id=?1",
            [run_id],
        )
        .unwrap();
    assert!(corrupted >= 1);

    let finish = command_request(
        "finish-current-read-only-request",
        "finish",
        run_id,
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let completed = client.command(&finish);
    assert_eq!(
        completed.status,
        200,
        "{}",
        String::from_utf8_lossy(&completed.body)
    );
    let terminal = snapshot(&client, run_id);
    assert_cursor(&terminal, 21);
    assert_eq!(terminal["state"]["mode"], "READ_ONLY");
    assert!(terminal["state"]["orders"].as_object().unwrap().is_empty());

    let facts: (i64, i64, i64, i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='MARKET_DATA_RECEIVED'),
               SUM(event_type='MARKET_BAR_DECISIONS_COMMITTED'),
               SUM(event_type='MARKET_BAR_PROCESSED'),
               SUM(event_type='SERVICE_STOPPED'),
               SUM(event_type='ORDER_INTENT_CREATED'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE run_id='finish-current-read-only'
                  AND request_id='finish-current-read-only-request'
                  AND receipt_state='COMPLETED')
             FROM events WHERE run_id='finish-current-read-only'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(facts, (21, 21, 21, 1, 0, 1));
}

#[test]
fn tampered_stored_pending_plan_is_visible_but_retry_fails_closed() {
    let harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_tampered_pending_start
             BEFORE INSERT ON events
             WHEN NEW.run_id LIKE 'tampered-pending-%'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected tampered START failure'); END;",
        )
        .unwrap();
    let attacks = [
        (
            "request-sha",
            "request_sha256='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
        ),
        ("command", "command='step'"),
        ("expected-sequence", "expected_sequence=1"),
        ("expected-version", "expected_version=1"),
        ("accepted-version", "accepted_version=2"),
        ("target-bars", "target_processed_bars=1"),
        (
            "plan-sha",
            "plan_sha256='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
        ),
        (
            "config-sha",
            "config_sha256='cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'",
        ),
        (
            "algorithm-sha",
            "algorithm_sha256='dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd'",
        ),
        ("request-json", "request_json='{}'"),
    ];
    for (label, mutation) in attacks {
        let run_id = format!("tampered-pending-{label}");
        let request_id = format!("tampered-pending-{label}-request");
        let mut request = command_request(&request_id, "start", &run_id, 0, 0);
        request["dataset"] = json!("sample.csv");
        assert_eq!(client.command(&request).status, 500, "attack={label}");
        database
            .execute(
                &format!(
                    "UPDATE web_command_inbox SET {mutation}
                     WHERE run_id=?1 AND request_id=?2"
                ),
                rusqlite::params![run_id, request_id],
            )
            .unwrap();
    }
    database
        .execute_batch("DROP TRIGGER fail_tampered_pending_start;")
        .unwrap();

    let discovered = client.pending_commands();
    assert_eq!(discovered.status, 200);
    let discovered = discovered.json();
    assert_eq!(
        discovered["commands"].as_array().unwrap().len(),
        attacks.len()
    );
    for (label, _) in attacks {
        let run_id = format!("tampered-pending-{label}");
        let request_id = format!("tampered-pending-{label}-request");
        let view = discovered["commands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|view| view["run_id"] == run_id)
            .unwrap_or_else(|| panic!("missing pending view for {label}"));
        assert_eq!(view["recovery_state"], "blocked", "attack={label}");
        let retry = client.retry_pending(&run_id, &request_id);
        assert_eq!(retry.status, 409, "attack={label}");
        assert_eq!(retry.json()["code"], "PENDING_PLAN_CONFLICT");
        let event_count: i64 = database
            .query_row(
                "SELECT COUNT(*) FROM events WHERE run_id=?1",
                [&run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 0, "attack={label}");
    }
}

#[test]
fn failed_command_claim_has_no_state_cursor_or_version_side_effect() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "claim-failure");
    let before = snapshot(&client, "claim-failure");
    let request = command_request(
        "claim-must-fail-once",
        "step",
        "claim-failure",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_web_command_claim_once
             BEFORE INSERT ON web_command_inbox
             WHEN NEW.request_id='claim-must-fail-once'
             BEGIN SELECT RAISE(ABORT, 'injected command claim failure'); END;",
        )
        .unwrap();

    let failed = client.command(&request);
    assert_ne!(failed.status, 200);
    assert_eq!(snapshot(&client, "claim-failure"), before);
    database
        .execute_batch("DROP TRIGGER fail_web_command_claim_once;")
        .unwrap();

    let retry = client.command(&request);
    assert_eq!(retry.status, 200);
    let after = snapshot(&client, "claim-failure");
    assert_cursor(&after, 1);
    assert_eq!(
        after["command_version"].as_i64(),
        Some(before["command_version"].as_i64().unwrap() + 1)
    );
}

#[test]
fn start_descriptor_failure_rolls_back_the_bootstrap_and_same_request_retries_once() {
    let harness = Harness::new();
    let client = harness.client();
    assert_eq!(client.get("/api/v1/runs").status, 200);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_replay_descriptor_once
             BEFORE INSERT ON events
             WHEN NEW.run_id='descriptor-atomic-start'
              AND NEW.event_type='REPLAY_INITIALIZED'
             BEGIN SELECT RAISE(ABORT, 'injected replay descriptor failure'); END;",
        )
        .unwrap();
    let mut request = command_request(
        "descriptor-atomic-start-request",
        "start",
        "descriptor-atomic-start",
        0,
        0,
    );
    request["dataset"] = json!("sample.csv");
    let failed = client.command(&request);
    assert_eq!(failed.status, 500);
    assert_eq!(failed.json()["code"], "INTERNAL_ERROR");
    let event_count: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='descriptor-atomic-start'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 0);
    let pending: (String, Option<String>, i64) = database
        .query_row(
            "SELECT receipt_state,response_json,accepted_version
             FROM web_command_inbox
             WHERE run_id='descriptor-atomic-start'
              AND request_id='descriptor-atomic-start-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(pending, ("PENDING".to_owned(), None, 1));
    let playback: (i64, i64) = database
        .query_row(
            "SELECT command_version,active FROM web_playback_control
             WHERE run_id='descriptor-atomic-start'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(playback, (1, 0));

    database
        .execute_batch("DROP TRIGGER fail_replay_descriptor_once;")
        .unwrap();
    let retried = client.command(&request);
    assert_eq!(retried.status, 200);
    let retried = retried.json();
    assert_eq!(retried["accepted_version"], 1);
    let step = command_request(
        "descriptor-atomic-first-step",
        "step",
        "descriptor-atomic-start",
        retried["accepted_sequence"].as_i64().unwrap(),
        retried["accepted_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    let (descriptor_count, descriptor_sequence, first_market_sequence): (i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(event_type='REPLAY_INITIALIZED'),
               MIN(CASE WHEN event_type='REPLAY_INITIALIZED' THEN sequence_number END),
               MIN(CASE WHEN event_type='MARKET_DATA_RECEIVED' THEN sequence_number END)
             FROM events WHERE run_id='descriptor-atomic-start'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(descriptor_count, 1);
    assert!(descriptor_sequence < first_market_sequence);
    let completed: (String, i64) = database
        .query_row(
            "SELECT receipt_state,COUNT(*) FROM web_command_inbox
             WHERE run_id='descriptor-atomic-start'
              AND request_id='descriptor-atomic-start-request'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(completed, ("COMPLETED".to_owned(), 1));
}

#[test]
fn failed_receipt_completion_retries_without_repeating_the_business_step() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "completion-failure");
    let before = snapshot(&client, "completion-failure");
    let request = command_request(
        "completion-must-fail-once",
        "step",
        "completion-failure",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_web_command_completion_once
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='completion-must-fail-once' AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected command completion failure'); END;",
        )
        .unwrap();

    let failed = client.command(&request);
    assert_ne!(failed.status, 200);
    let (processed, receipt_state, response_json, accepted_version): (
        i64,
        String,
        Option<String>,
        i64,
    ) = database
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events
                 WHERE run_id='completion-failure'
                  AND event_type='MARKET_BAR_PROCESSED'),
               receipt_state,response_json,accepted_version
             FROM web_command_inbox
             WHERE run_id='completion-failure'
              AND request_id='completion-must-fail-once'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(processed, 1);
    assert_eq!(receipt_state, "PENDING");
    assert_eq!(response_json, None);
    assert_eq!(
        accepted_version,
        before["command_version"].as_i64().unwrap() + 1
    );
    database
        .execute_batch("DROP TRIGGER fail_web_command_completion_once;")
        .unwrap();

    let retry = client.command(&request);
    assert_eq!(retry.status, 200);
    let committed = snapshot(&client, "completion-failure");
    assert_cursor(&committed, 1);
    assert_eq!(
        committed["command_version"].as_i64(),
        Some(before["command_version"].as_i64().unwrap() + 1)
    );
    let duplicate = client.command(&request);
    assert_eq!(duplicate.status, 200);
    assert_eq!(duplicate.json(), retry.json());
    assert_eq!(snapshot(&client, "completion-failure"), committed);
}

fn leave_completed_step_with_pending_receipt(
    harness: &Harness,
    run_id: &str,
    request_id: &str,
) -> Value {
    let client = harness.client();
    start_run(&client, run_id);
    let before = snapshot(&client, run_id);
    let request = command_request(
        request_id,
        "step",
        run_id,
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let trigger = format!(
        "CREATE TRIGGER fail_non_play_receipt_completion
         BEFORE UPDATE OF receipt_state ON web_command_inbox
         WHEN NEW.request_id='{request_id}' AND NEW.receipt_state='COMPLETED'
         BEGIN SELECT RAISE(ABORT, 'injected non-PLAY completion failure'); END;"
    );
    let database = harness.database();
    database.execute_batch(&trigger).unwrap();

    let failed = client.command(&request);
    assert_ne!(failed.status, 200);
    let (processed, receipt_state, response_json, target_bars, accepted_version): (
        i64,
        String,
        Option<String>,
        i64,
        i64,
    ) = database
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events
                 WHERE run_id=?1 AND event_type='MARKET_BAR_PROCESSED'),
               receipt_state,response_json,target_processed_bars,accepted_version
             FROM web_command_inbox
             WHERE run_id=?1 AND request_id=?2",
            rusqlite::params![run_id, request_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(processed, 1, "the STEP business effect must be durable");
    assert_eq!(receipt_state, "PENDING");
    assert_eq!(response_json, None);
    assert_eq!(target_bars, 1);
    assert_eq!(
        accepted_version,
        before["command_version"].as_i64().unwrap() + 1
    );
    database
        .execute_batch("DROP TRIGGER fail_non_play_receipt_completion;")
        .unwrap();
    before
}

fn assert_receipt_completed(harness: &Harness, run_id: &str, request_id: &str) {
    let database = harness.database();
    let (state, response_count, receipt_count): (String, i64, i64) = database
        .query_row(
            "SELECT receipt_state,response_json IS NOT NULL,COUNT(*)
             FROM web_command_inbox WHERE run_id=?1 AND request_id=?2",
            rusqlite::params![run_id, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), response_count, receipt_count),
        ("COMPLETED", 1, 1)
    );
}

#[test]
fn concurrent_command_and_retry_endpoints_complete_one_committed_step_with_identical_receipts() {
    let harness = Harness::new();
    let run_id = "concurrent-step-completion";
    let request_id = "concurrent-step-completion-request";
    let before = leave_completed_step_with_pending_receipt(&harness, run_id, request_id);
    let request = command_request(
        request_id,
        "step",
        run_id,
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let barrier = Arc::new(Barrier::new(3));
    let command_worker = {
        let client = harness.client();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            client.command(&request)
        })
    };
    let retry_worker = {
        let client = harness.client();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            client.retry_pending(run_id, request_id)
        })
    };
    barrier.wait();
    let command_response = command_worker.join().unwrap();
    let retry_response = retry_worker.join().unwrap();
    assert_eq!(command_response.status, 200);
    assert_eq!(retry_response.status, 200);
    assert_eq!(command_response.body, retry_response.body);
    let database = harness.database();
    let (stored_response, processed, receipts): (String, i64, i64) = database
        .query_row(
            "SELECT response_json,
                    (SELECT COUNT(*) FROM events
                      WHERE run_id=?1 AND event_type='MARKET_BAR_PROCESSED'),
                    COUNT(*)
             FROM web_command_inbox
             WHERE run_id=?1 AND request_id=?2 AND receipt_state='COMPLETED'",
            rusqlite::params![run_id, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(command_response.body, stored_response.as_bytes());
    assert_eq!((processed, receipts), (1, 1));
}

#[test]
fn concurrent_command_and_retry_endpoints_complete_one_committed_play_with_identical_receipts() {
    let harness = Harness::new();
    let client = harness.client();
    let run_id = "concurrent-play-completion";
    let request_id = "concurrent-play-completion-request";
    start_run(&client, run_id);
    let before = snapshot(&client, run_id);
    let mut request = command_request(
        request_id,
        "play",
        run_id,
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    request["speed_ms"] = json!(2000);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_concurrent_play_receipt_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='concurrent-play-completion-request'
              AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected PLAY receipt completion failure'); END;",
        )
        .unwrap();
    assert_eq!(client.command(&request).status, 500);
    let processed_before: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='concurrent-play-completion'
              AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(processed_before, 0);
    database
        .execute_batch("DROP TRIGGER fail_concurrent_play_receipt_completion;")
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let command_worker = {
        let client = harness.client();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            client.command(&request)
        })
    };
    let retry_worker = {
        let client = harness.client();
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            client.retry_pending(run_id, request_id)
        })
    };
    barrier.wait();
    let command_response = command_worker.join().unwrap();
    let retry_response = retry_worker.join().unwrap();
    assert_eq!(command_response.status, 200);
    assert_eq!(retry_response.status, 200);
    assert_eq!(command_response.body, retry_response.body);
    let stored_response: String = database
        .query_row(
            "SELECT response_json FROM web_command_inbox
             WHERE run_id=?1 AND request_id=?2 AND receipt_state='COMPLETED'",
            rusqlite::params![run_id, request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(command_response.body, stored_response.as_bytes());

    let current = snapshot(&client, run_id);
    let pause = command_request(
        "concurrent-play-completion-pause",
        "pause",
        run_id,
        current["sequence"].as_i64().unwrap(),
        current["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&pause).status, 200);
}

fn submit_next_step_after_recovery(client: &TestClient, run_id: &str, recovered: &Value) {
    let next = command_request(
        &format!("{run_id}-next-request"),
        "step",
        run_id,
        recovered["sequence"].as_i64().unwrap(),
        recovered["command_version"].as_i64().unwrap(),
    );
    let response = client.command(&next);
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_cursor(&snapshot(client, run_id), 2);
}

#[test]
fn snapshot_refresh_completes_an_old_pending_step_before_the_page_uses_a_new_request_id() {
    let harness = Harness::new();
    let run_id = "pending-step-refresh";
    let request_id = "pending-step-refresh-original";
    let before = leave_completed_step_with_pending_receipt(&harness, run_id, request_id);
    let client = harness.client();

    let refreshed = snapshot(&client, run_id);
    assert_cursor(&refreshed, 1);
    assert_eq!(
        refreshed["command_version"].as_i64(),
        Some(before["command_version"].as_i64().unwrap() + 1)
    );
    assert_receipt_completed(&harness, run_id, request_id);
    submit_next_step_after_recovery(&client, run_id, &refreshed);
}

#[test]
fn a_new_step_request_completes_an_old_pending_step_without_reexecuting_it() {
    let harness = Harness::new();
    let run_id = "pending-step-new-request";
    let request_id = "pending-step-new-request-original";
    let before = leave_completed_step_with_pending_receipt(&harness, run_id, request_id);
    let database = harness.database();
    let (durable_sequence, durable_version): (i64, i64) = database
        .query_row(
            "SELECT
               (SELECT MAX(sequence_number) FROM events WHERE run_id=?1),
               command_version
             FROM web_playback_control WHERE run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        durable_version,
        before["command_version"].as_i64().unwrap() + 1
    );
    let next = command_request(
        "pending-step-new-request-second",
        "step",
        run_id,
        durable_sequence,
        durable_version,
    );
    let client = harness.client();
    let response = client.command(&next);
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    assert_receipt_completed(&harness, run_id, request_id);
    assert_cursor(&snapshot(&client, run_id), 2);
}

#[test]
fn restart_completes_an_old_pending_step_before_serving_or_accepting_new_work() {
    let mut harness = Harness::new();
    let run_id = "pending-step-restart";
    let request_id = "pending-step-restart-original";
    let before = leave_completed_step_with_pending_receipt(&harness, run_id, request_id);
    harness.restart();
    let client = harness.client();

    let recovered = snapshot(&client, run_id);
    assert_cursor(&recovered, 1);
    assert_eq!(
        recovered["command_version"].as_i64(),
        Some(before["command_version"].as_i64().unwrap() + 1)
    );
    assert_receipt_completed(&harness, run_id, request_id);
    submit_next_step_after_recovery(&client, run_id, &recovered);
}

#[test]
fn pending_play_is_recovered_before_a_new_pause_and_never_runs_unreceipted() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "pending-play");
    let before = snapshot(&client, "pending-play");
    let mut play = command_request(
        "pending-play-original",
        "play",
        "pending-play",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_play_receipt_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='pending-play-original' AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected PLAY completion failure'); END;",
        )
        .unwrap();
    assert_ne!(client.command(&play).status, 200);
    thread::sleep(Duration::from_millis(350));
    let processed: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='pending-play' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(processed, 0);
    database
        .execute_batch("DROP TRIGGER fail_play_receipt_completion;")
        .unwrap();

    let pause = command_request(
        "pending-play-new-pause",
        "pause",
        "pending-play",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap() + 1,
    );
    let response = client.command(&pause);
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    let after = snapshot(&client, "pending-play");
    assert_eq!(after["playback"]["active"], false);
    assert_cursor(&after, 0);
    let pending: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM web_command_inbox
             WHERE run_id='pending-play' AND receipt_state='PENDING'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);
}

#[test]
fn any_new_command_recovers_pending_play_and_starts_its_worker_before_conflicting() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "pending-play-step");
    let before = snapshot(&client, "pending-play-step");
    let mut play = command_request(
        "pending-play-step-original",
        "play",
        "pending-play-step",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_pending_play_step_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='pending-play-step-original' AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected pending PLAY completion failure'); END;",
        )
        .unwrap();
    assert_ne!(client.command(&play).status, 200);
    thread::sleep(Duration::from_millis(350));
    let processed_before_recovery: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='pending-play-step' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(processed_before_recovery, 0);
    database
        .execute_batch("DROP TRIGGER fail_pending_play_step_completion;")
        .unwrap();

    let step = command_request(
        "pending-play-conflicting-step",
        "step",
        "pending-play-step",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap() + 1,
    );
    let conflict = client.command(&step);
    assert_eq!(conflict.status, 409);
    assert_eq!(conflict.json()["code"], "COMMAND_CONFLICT");
    let advanced = wait_for_cursor(&client, "pending-play-step", 1);
    assert_eq!(advanced["playback"]["active"], true);
    let receipt_states: Vec<String> = database
        .prepare(
            "SELECT receipt_state FROM web_command_inbox
             WHERE run_id='pending-play-step' ORDER BY accepted_version",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(receipt_states, vec!["COMPLETED", "COMPLETED"]);

    let pause = command_request(
        "pending-play-step-pause",
        "pause",
        "pending-play-step",
        before["sequence"].as_i64().unwrap(),
        advanced["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&pause).status, 200);
}

#[test]
fn retrying_an_old_completed_start_also_recovers_pending_play_and_starts_its_worker() {
    let harness = Harness::new();
    let client = harness.client();
    let mut start = command_request(
        "old-completed-start",
        "start",
        "pending-play-old-start",
        0,
        0,
    );
    start["dataset"] = json!("sample.csv");
    let first_start = client.command(&start);
    assert_eq!(first_start.status, 200);
    let first_start = first_start.json();
    let before_play = snapshot(&client, "pending-play-old-start");
    let mut play = command_request(
        "pending-play-before-old-start-retry",
        "play",
        "pending-play-old-start",
        before_play["sequence"].as_i64().unwrap(),
        before_play["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_old_start_pending_play_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='pending-play-before-old-start-retry'
              AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected pending PLAY completion failure'); END;",
        )
        .unwrap();
    assert_ne!(client.command(&play).status, 200);
    thread::sleep(Duration::from_millis(350));
    let processed_before_recovery: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='pending-play-old-start' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(processed_before_recovery, 0);
    database
        .execute_batch("DROP TRIGGER fail_old_start_pending_play_completion;")
        .unwrap();

    let old_start_retry = client.command(&start);
    assert_eq!(old_start_retry.status, 200);
    assert_eq!(old_start_retry.json(), first_start);
    let advanced = wait_for_cursor(&client, "pending-play-old-start", 1);
    assert_eq!(advanced["playback"]["active"], true);
    let pending: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM web_command_inbox
             WHERE run_id='pending-play-old-start' AND receipt_state='PENDING'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 0);

    let pause = command_request(
        "pending-play-old-start-pause",
        "pause",
        "pending-play-old-start",
        before_play["sequence"].as_i64().unwrap(),
        advanced["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&pause).status, 200);
}

#[test]
fn play_retry_is_one_worker_and_pause_is_bound_to_the_active_generation() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "play-generation");
    let before_play = snapshot(&client, "play-generation");
    let mut play = command_request(
        "play-generation-1",
        "play",
        "play-generation",
        before_play["sequence"].as_i64().unwrap(),
        before_play["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(250);
    let first_play = client.command(&play);
    assert_eq!(first_play.status, 200);
    let first_play_body = first_play.json();
    let play_version = first_play_body["accepted_version"].as_i64().unwrap();
    assert_eq!(client.command(&play).json(), first_play_body);

    let advanced = wait_for_cursor(&client, "play-generation", 1);
    assert_eq!(advanced["playback"]["active"], true);
    assert_eq!(
        advanced["playback"]["command_version"].as_i64(),
        Some(play_version)
    );

    let pause = command_request(
        "pause-generation-1",
        "pause",
        "play-generation",
        first_play_body["accepted_sequence"].as_i64().unwrap(),
        play_version,
    );
    let pause_response = client.command(&pause);
    assert_eq!(pause_response.status, 200);
    let paused = snapshot(&client, "play-generation");
    assert_eq!(paused["playback"]["active"], false);

    let mut play_again = command_request(
        "play-generation-2",
        "play",
        "play-generation",
        paused["sequence"].as_i64().unwrap(),
        paused["command_version"].as_i64().unwrap(),
    );
    play_again["speed_ms"] = json!(250);
    let second_play = client.command(&play_again);
    assert_eq!(second_play.status, 200);
    let second_play_body = second_play.json();
    let second_version = second_play_body["accepted_version"].as_i64().unwrap();

    let stale_pause = command_request(
        "stale-pause",
        "pause",
        "play-generation",
        second_play_body["accepted_sequence"].as_i64().unwrap(),
        play_version,
    );
    assert_eq!(client.command(&stale_pause).status, 409);
    assert_eq!(
        snapshot(&client, "play-generation")["playback"]["active"],
        true
    );

    let future_pause = command_request(
        "future-pause",
        "pause",
        "play-generation",
        second_play_body["accepted_sequence"].as_i64().unwrap(),
        second_version + 1,
    );
    assert_eq!(client.command(&future_pause).status, 409);
    assert_eq!(
        snapshot(&client, "play-generation")["playback"]["active"],
        true
    );

    let valid_pause = command_request(
        "pause-generation-2",
        "pause",
        "play-generation",
        second_play_body["accepted_sequence"].as_i64().unwrap(),
        second_version,
    );
    assert_eq!(client.command(&valid_pause).status, 200);
    assert_eq!(
        snapshot(&client, "play-generation")["playback"]["active"],
        false
    );
}

#[test]
fn worker_stops_before_another_bar_when_its_durable_generation_becomes_stale() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "stale-worker-generation");
    let before = snapshot(&client, "stale-worker-generation");
    let mut play = command_request(
        "stale-worker-play",
        "play",
        "stale-worker-generation",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(1_000);
    let accepted = client.command(&play);
    assert_eq!(accepted.status, 200);
    let generation = accepted.json()["accepted_version"].as_i64().unwrap();
    wait_for_cursor(&client, "stale-worker-generation", 1);

    let database = harness.database();
    let replaced = database
        .execute(
            "UPDATE web_playback_control
             SET command_version=?2,active=1,updated_at='test-new-generation'
             WHERE run_id=?1 AND command_version=?3 AND active=1",
            rusqlite::params!["stale-worker-generation", generation + 1, generation],
        )
        .unwrap();
    assert_eq!(replaced, 1);
    let cursor_at_replacement: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='stale-worker-generation' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cursor_at_replacement, 1);

    thread::sleep(Duration::from_millis(1_200));
    let cursor_after_wait: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='stale-worker-generation' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cursor_after_wait, cursor_at_replacement);
    let durable: (i64, i64) = database
        .query_row(
            "SELECT command_version,active FROM web_playback_control
             WHERE run_id='stale-worker-generation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(durable, (generation + 1, 1));
}

#[test]
fn snapshot_same_head_is_byte_identical_for_sequential_and_concurrent_readers() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "cache-same-head");
    let before = snapshot(&client, "cache-same-head");
    let step = command_request(
        "cache-same-head-step",
        "step",
        "cache-same-head",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);

    let expected = snapshot_bytes(&client, "cache-same-head");
    for _ in 0..5 {
        assert_eq!(snapshot_bytes(&client, "cache-same-head"), expected);
    }

    let barrier = Arc::new(Barrier::new(5));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                snapshot_bytes(&client, "cache-same-head")
            })
        })
        .collect();
    barrier.wait();
    for handle in handles {
        assert_eq!(handle.join().unwrap(), expected);
    }
}

#[test]
fn snapshot_refreshes_after_step_play_pause_and_direct_control_changes() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "cache-invalidation");
    let initial = snapshot(&client, "cache-invalidation");
    let initial_bytes = snapshot_bytes(&client, "cache-invalidation");

    let step = command_request(
        "cache-invalidation-step",
        "step",
        "cache-invalidation",
        initial["sequence"].as_i64().unwrap(),
        initial["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    let stepped = snapshot(&client, "cache-invalidation");
    assert_cursor(&stepped, 1);
    assert_ne!(snapshot_bytes(&client, "cache-invalidation"), initial_bytes);

    let mut play = command_request(
        "cache-invalidation-play",
        "play",
        "cache-invalidation",
        stepped["sequence"].as_i64().unwrap(),
        stepped["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(2_000);
    let play_response = client.command(&play);
    assert_eq!(play_response.status, 200);
    let accepted_play = play_response.json();
    let playing = snapshot(&client, "cache-invalidation");
    assert_eq!(playing["playback"]["active"], true);
    assert_eq!(
        playing["command_version"],
        accepted_play["accepted_version"]
    );

    let pause = command_request(
        "cache-invalidation-pause",
        "pause",
        "cache-invalidation",
        accepted_play["accepted_sequence"].as_i64().unwrap(),
        accepted_play["accepted_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&pause).status, 200);
    let paused = snapshot(&client, "cache-invalidation");
    assert_eq!(paused["playback"]["active"], false);
    assert!(
        paused["command_version"].as_i64().unwrap() > playing["command_version"].as_i64().unwrap()
    );

    harness
        .database()
        .execute(
            "UPDATE web_playback_control SET interval_ms=500,updated_at='cache-control-change'
             WHERE run_id='cache-invalidation'",
            [],
        )
        .unwrap();
    let control_changed = snapshot(&client, "cache-invalidation");
    assert_eq!(control_changed["sequence"], paused["sequence"]);
    assert_eq!(control_changed["state"], paused["state"]);
    assert_eq!(control_changed["playback"]["interval_ms"], 500);
    assert_ne!(control_changed, paused);
}

#[test]
fn snapshot_cache_contract_isolated_by_run_and_frozen_dataset_identity() {
    let mut harness = Harness::with_custom_data(
        "cache-a.csv",
        fs::read_to_string("tests/fixtures/sample.csv").unwrap(),
    );
    harness.server.stop();
    let second_dataset = harness.data.with_file_name("cache-b.csv");
    let changed =
        fs::read_to_string(&harness.data)
            .unwrap()
            .replacen("100000,1000000", "100001,1000000", 1);
    fs::write(&second_dataset, changed).unwrap();
    harness.restart();
    let client = harness.client();

    start_run_with_dataset(&client, "cache-run-a", "cache-a.csv");
    start_run_with_dataset(&client, "cache-run-b", "cache-b.csv");
    let before_a = snapshot(&client, "cache-run-a");
    let before_b = snapshot_bytes(&client, "cache-run-b");
    let descriptor_a = before_a["progress"]["descriptor"].clone();
    let descriptor_b = snapshot(&client, "cache-run-b")["progress"]["descriptor"].clone();
    assert_eq!(descriptor_a["dataset_id"], "cache-a.csv");
    assert_eq!(descriptor_b["dataset_id"], "cache-b.csv");
    assert_ne!(descriptor_a["data_sha256"], descriptor_b["data_sha256"]);

    let step_a = command_request(
        "cache-run-a-step",
        "step",
        "cache-run-a",
        before_a["sequence"].as_i64().unwrap(),
        before_a["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step_a).status, 200);
    assert_cursor(&snapshot(&client, "cache-run-a"), 1);
    assert_eq!(snapshot_bytes(&client, "cache-run-b"), before_b);
}

#[test]
fn ledger_run_context_seeds_web_and_cli_reads_and_rejects_drift_before_step_claim() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "durable-run-context");
    let original = snapshot(&client, "durable-run-context");
    assert_cursor(&original, 0);
    assert_eq!(original["state"]["cash"]["available"], "200000.00");
    assert_eq!(original["state"]["position"]["total"], 10000);
    assert_eq!(original["state"]["position"]["sellable"], 10000);
    assert_eq!(original["state"]["symbol"], "600000.SH");
    assert_eq!(original["state"]["anchor_price"], "10.00");
    let snapshots: i64 = harness
        .database()
        .query_row(
            "SELECT COUNT(*) FROM snapshots WHERE run_id='durable-run-context'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        snapshots, 0,
        "START-only recovery must not depend on a snapshot"
    );

    harness.server.stop();
    replace_file_fragments(
        &harness.config,
        &[
            ("initial_cash: \"200000.00\"", "initial_cash: \"123.45\""),
            ("initial_position: 10000", "initial_position: 7777"),
            ("initial_sellable: 10000", "initial_sellable: 6666"),
        ],
    );
    harness.restart();
    let client = harness.client();
    let recovered = snapshot(&client, "durable-run-context");
    assert_eq!(recovered, original);

    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            harness.config.to_str().unwrap(),
            "--run-id",
            "durable-run-context",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli: Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(cli["available_cash"], "200000.00");
    assert_eq!(cli["total_position"], 10000);
    assert_eq!(cli["sellable_position"], 10000);
    assert_eq!(cli["symbol"], "600000.SH");
    assert_eq!(cli["anchor"], "10.00");

    let event_count_before: i64 = harness
        .database()
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='durable-run-context'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let step = command_request(
        "durable-run-context-step",
        "step",
        "durable-run-context",
        recovered["sequence"].as_i64().unwrap(),
        recovered["command_version"].as_i64().unwrap(),
    );
    let rejected = client.command(&step);
    assert_eq!(rejected.status, 409);
    assert_eq!(rejected.json()["code"], "COMMAND_CONFLICT");
    let (events_after, receipts): (i64, i64) = harness
        .database()
        .query_row(
            "SELECT
               (SELECT COUNT(*) FROM events WHERE run_id='durable-run-context'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='durable-run-context-step')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((events_after, receipts), (event_count_before, 0));

    let read_only_config = harness
        .config
        .with_file_name("read-only-identity-drift.yaml");
    fs::copy(&harness.config, &read_only_config).unwrap();
    replace_file_fragments(
        &read_only_config,
        &[
            ("symbol: \"600000.SH\"", "symbol: \"000001.SZ\""),
            ("anchor_price: \"10.00\"", "anchor_price: \"88.88\""),
        ],
    );
    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            read_only_config.to_str().unwrap(),
            "--run-id",
            "durable-run-context",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli: Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(cli["symbol"], "600000.SH");
    assert_eq!(cli["anchor"], "10.00");
    assert_eq!(cli["available_cash"], "200000.00");
    assert_eq!(cli["total_position"], 10000);
}

#[test]
fn web_snapshot_uses_durable_context_after_corrupt_and_deleted_snapshots() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "durable-context-snapshot-fallback");
    let before = snapshot(&client, "durable-context-snapshot-fallback");
    let step = command_request(
        "durable-context-snapshot-fallback-step",
        "step",
        "durable-context-snapshot-fallback",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    let hot = snapshot_bytes(&client, "durable-context-snapshot-fallback");
    harness.server.stop();
    harness
        .database()
        .execute(
            "UPDATE snapshots SET checksum='corrupt'
             WHERE run_id='durable-context-snapshot-fallback'",
            [],
        )
        .unwrap();
    harness.restart();
    assert_eq!(
        snapshot_bytes(&harness.client(), "durable-context-snapshot-fallback"),
        hot
    );

    harness.server.stop();
    harness
        .database()
        .execute(
            "DELETE FROM snapshots WHERE run_id='durable-context-snapshot-fallback'",
            [],
        )
        .unwrap();
    harness.restart();
    assert_eq!(
        snapshot_bytes(&harness.client(), "durable-context-snapshot-fallback"),
        hot
    );
}

#[test]
fn incomplete_bar_public_prefix_uses_durable_initial_account_after_runtime_drift() {
    let mut harness = Harness::with_custom_data(
        "durable-prefix.csv",
        concat!(
            "timestamp,symbol,open,high,low,close,volume,amount\n",
            "2026-01-05 09:30:00,600000.SH,10.00,10.01,9.86,9.87,1000,9870\n",
            "2026-01-05 09:31:00,600000.SH,9.87,10.01,9.86,10.00,1000,10000\n",
        )
        .to_owned(),
    );
    let client = harness.client();
    start_run_with_dataset(&client, "durable-prefix", "durable-prefix.csv");
    let before = snapshot_bytes(&client, "durable-prefix");
    let before_json: Value = serde_json::from_slice(&before).unwrap();
    let step = command_request(
        "durable-prefix-step",
        "step",
        "durable-prefix",
        before_json["sequence"].as_i64().unwrap(),
        before_json["command_version"].as_i64().unwrap(),
    );
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_durable_prefix_completion
             BEFORE INSERT ON events
             WHEN NEW.run_id='durable-prefix' AND NEW.event_type='MARKET_BAR_PROCESSED'
             BEGIN SELECT RAISE(ABORT, 'injected durable-prefix failure'); END;",
        )
        .unwrap();
    assert_eq!(client.command(&step).status, 500);
    let journal_head: i64 = database
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='durable-prefix'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(journal_head > before_json["sequence"].as_i64().unwrap());
    drop(database);
    harness.server.stop();
    replace_file_fragments(
        &harness.config,
        &[
            ("initial_cash: \"200000.00\"", "initial_cash: \"123.45\""),
            ("initial_position: 10000", "initial_position: 7777"),
            ("initial_sellable: 10000", "initial_sellable: 6666"),
        ],
    );
    harness.restart();
    let recovered = snapshot(&harness.client(), "durable-prefix");
    for field in ["sequence", "state", "progress", "performance"] {
        assert_eq!(
            recovered[field], before_json[field],
            "business field {field}"
        );
    }
    assert_eq!(
        recovered["command_version"].as_i64().unwrap(),
        before_json["command_version"].as_i64().unwrap() + 1
    );
    assert_eq!(
        recovered["playback"]["command_version"],
        recovered["command_version"]
    );
    assert_eq!(recovered["playback"]["active"], false);
}

#[test]
fn tampered_run_started_identity_is_rejected_by_web_and_cli() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "tampered-run-context");
    let head_before: i64 = harness
        .database()
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='tampered-run-context'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    harness.server.stop();
    let database = harness.database();
    database
        .execute_batch("DROP TRIGGER events_are_append_only_update;")
        .unwrap();
    assert_eq!(
        database
            .execute(
                "UPDATE events
                 SET payload=json_set(payload,'$.initial_cash','199999.99')
                 WHERE run_id='tampered-run-context' AND event_type='RUN_STARTED'",
                [],
            )
            .unwrap(),
        1
    );
    drop(database);
    harness.restart();
    let rejected = harness.client().get("/api/v1/runs/tampered-run-context");
    assert_eq!(rejected.status, 500);
    assert_eq!(rejected.json()["code"], "INTERNAL_ERROR");
    let head_after: i64 = harness
        .database()
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='tampered-run-context'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(head_after, head_before);
    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            harness.config.to_str().unwrap(),
            "--run-id",
            "tampered-run-context",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr)
        .contains("RUN_STARTED and CONFIG_SNAPSHOTTED identities differ"));
}

#[test]
fn missing_current_config_hash_is_rejected_even_when_a_valid_snapshot_exists() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "missing-config-content-hash");
    let before = snapshot(&client, "missing-config-content-hash");
    let step = command_request(
        "missing-config-content-hash-step",
        "step",
        "missing-config-content-hash",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&step).status, 200);
    let database = harness.database();
    let (snapshot_state, snapshot_checksum): (String, String) = database
        .query_row(
            "SELECT state_json,checksum FROM snapshots
             WHERE run_id='missing-config-content-hash'
             ORDER BY sequence_number DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        hex::encode(Sha256::digest(snapshot_state.as_bytes())),
        snapshot_checksum
    );
    let head_before: i64 = database
        .query_row(
            "SELECT MAX(sequence_number) FROM events
             WHERE run_id='missing-config-content-hash'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    harness.server.stop();
    database
        .execute_batch("DROP TRIGGER events_are_append_only_update;")
        .unwrap();
    assert_eq!(
        database
            .execute(
                "UPDATE events SET payload=json_remove(payload,'$._content_sha256')
                 WHERE run_id='missing-config-content-hash'
                  AND event_type='CONFIG_SNAPSHOTTED'",
                [],
            )
            .unwrap(),
        1
    );
    drop(database);

    harness.restart();
    let rejected = harness
        .client()
        .get("/api/v1/runs/missing-config-content-hash");
    assert_eq!(rejected.status, 500);
    assert_eq!(rejected.json()["code"], "INTERNAL_ERROR");
    let head_after: i64 = harness
        .database()
        .query_row(
            "SELECT MAX(sequence_number) FROM events
             WHERE run_id='missing-config-content-hash'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(head_after, head_before);
    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            harness.config.to_str().unwrap(),
            "--run-id",
            "missing-config-content-hash",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr)
        .contains("current durable run configuration lacks its content hash"));
}

#[test]
fn missing_algorithm_bootstrap_is_rejected_by_web_and_cli_without_writes() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "missing-algorithm-bootstrap");
    harness.server.stop();
    let database = harness.database();
    let count_before: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='missing-algorithm-bootstrap'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    database
        .execute_batch("DROP TRIGGER events_are_append_only_delete;")
        .unwrap();
    assert_eq!(
        database
            .execute(
                "DELETE FROM events WHERE run_id='missing-algorithm-bootstrap'
                  AND event_type='ALGORITHM_REGISTERED'",
                [],
            )
            .unwrap(),
        1
    );
    drop(database);

    harness.restart();
    let rejected = harness
        .client()
        .get("/api/v1/runs/missing-algorithm-bootstrap");
    assert_eq!(rejected.status, 500);
    assert_eq!(rejected.json()["code"], "INTERNAL_ERROR");
    let count_after: i64 = harness
        .database()
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='missing-algorithm-bootstrap'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count_after, count_before - 1);
    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            harness.config.to_str().unwrap(),
            "--run-id",
            "missing-algorithm-bootstrap",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr)
        .contains("current run requires one complete durable identity bootstrap"));
}

#[test]
fn out_of_order_algorithm_bootstrap_is_rejected_by_web_and_cli_without_writes() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "out-of-order-algorithm-bootstrap");
    harness.server.stop();
    let database = harness.database();
    let (config_sequence, algorithm_sequence): (i64, i64) = database
        .query_row(
            "SELECT
               MAX(CASE WHEN event_type='CONFIG_SNAPSHOTTED' THEN sequence_number END),
               MAX(CASE WHEN event_type='ALGORITHM_REGISTERED' THEN sequence_number END)
             FROM events WHERE run_id='out-of-order-algorithm-bootstrap'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let count_before: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='out-of-order-algorithm-bootstrap'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    database
        .execute_batch("DROP TRIGGER events_are_append_only_update;")
        .unwrap();
    database
        .execute(
            "UPDATE events SET sequence_number=-1
             WHERE run_id='out-of-order-algorithm-bootstrap'
              AND event_type='ALGORITHM_REGISTERED'",
            [],
        )
        .unwrap();
    database
        .execute(
            "UPDATE events SET sequence_number=?1
             WHERE run_id='out-of-order-algorithm-bootstrap'
              AND event_type='CONFIG_SNAPSHOTTED'",
            [algorithm_sequence],
        )
        .unwrap();
    database
        .execute(
            "UPDATE events SET sequence_number=?1
             WHERE run_id='out-of-order-algorithm-bootstrap'
              AND event_type='ALGORITHM_REGISTERED'",
            [config_sequence],
        )
        .unwrap();
    drop(database);

    harness.restart();
    let rejected = harness
        .client()
        .get("/api/v1/runs/out-of-order-algorithm-bootstrap");
    assert_eq!(rejected.status, 500);
    assert_eq!(rejected.json()["code"], "INTERNAL_ERROR");
    let count_after: i64 = harness
        .database()
        .query_row(
            "SELECT COUNT(*) FROM events WHERE run_id='out-of-order-algorithm-bootstrap'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count_after, count_before);
    let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
        .args([
            "status",
            "--config",
            harness.config.to_str().unwrap(),
            "--run-id",
            "out-of-order-algorithm-bootstrap",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!cli.status.success());
    assert!(String::from_utf8_lossy(&cli.stderr)
        .contains("durable run identity bootstrap is out of order"));
}

#[test]
fn noncanonical_algorithm_identity_hashes_fail_closed_in_web_and_cli_without_writes() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "invalid-algorithm-hash");
    harness.server.stop();
    let database = harness.database();
    database
        .execute_batch("DROP TRIGGER events_are_append_only_update;")
        .unwrap();
    let (original_payload, head_before, count_before): (String, i64, i64) = database
        .query_row(
            "SELECT payload,
                    (SELECT MAX(sequence_number) FROM events
                      WHERE run_id='invalid-algorithm-hash'),
                    (SELECT COUNT(*) FROM events
                      WHERE run_id='invalid-algorithm-hash')
             FROM events WHERE run_id='invalid-algorithm-hash'
              AND event_type='ALGORITHM_REGISTERED'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let invalid_hashes = [
        String::new(),
        "a".repeat(63),
        "A".repeat(64),
        "g".repeat(64),
    ];
    for field in ["artifact_sha256", "environment_sha256", "platform_sha256"] {
        for invalid in &invalid_hashes {
            let mut payload: Value = serde_json::from_str(&original_payload).unwrap();
            payload[field] = json!(invalid);
            assert_eq!(
                database
                    .execute(
                        "UPDATE events SET payload=?1
                         WHERE run_id='invalid-algorithm-hash'
                          AND event_type='ALGORITHM_REGISTERED'",
                        [serde_json::to_string(&payload).unwrap()],
                    )
                    .unwrap(),
                1
            );
            harness.restart();
            let rejected = harness.client().get("/api/v1/runs/invalid-algorithm-hash");
            assert_eq!(rejected.status, 500, "field={field} value={invalid}");
            assert_eq!(rejected.json()["code"], "INTERNAL_ERROR");
            let cli = Command::new(env!("CARGO_BIN_EXE_gridedge"))
                .args([
                    "status",
                    "--config",
                    harness.config.to_str().unwrap(),
                    "--run-id",
                    "invalid-algorithm-hash",
                    "--json",
                ])
                .output()
                .unwrap();
            assert!(!cli.status.success(), "field={field} value={invalid}");
            assert!(String::from_utf8_lossy(&cli.stderr)
                .contains("current algorithm manifest lacks a canonical SHA-256 identity"));
            let (head_after, count_after): (i64, i64) = database
                .query_row(
                    "SELECT MAX(sequence_number),COUNT(*) FROM events
                     WHERE run_id='invalid-algorithm-hash'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!((head_after, count_after), (head_before, count_before));
            harness.server.stop();
        }
    }
}

#[test]
fn snapshot_pending_play_failure_is_not_cached_and_never_exposes_partial_progress() {
    let harness = Harness::new();
    let client = harness.client();
    start_run(&client, "cache-pending-play");
    let before = snapshot(&client, "cache-pending-play");
    let mut play = command_request(
        "cache-pending-play-command",
        "play",
        "cache-pending-play",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    play["speed_ms"] = json!(2_000);
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_cache_play_completion
             BEFORE UPDATE OF receipt_state ON web_command_inbox
             WHEN NEW.request_id='cache-pending-play-command' AND NEW.receipt_state='COMPLETED'
             BEGIN SELECT RAISE(ABORT, 'injected cache PLAY receipt failure'); END;",
        )
        .unwrap();
    assert_ne!(client.command(&play).status, 200);
    thread::sleep(Duration::from_millis(300));
    let first_snapshot = client.get("/api/v1/runs/cache-pending-play");
    assert_ne!(first_snapshot.status, 200);
    let processed_while_pending: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='cache-pending-play' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(processed_while_pending, 0);

    database
        .execute_batch("DROP TRIGGER fail_cache_play_completion;")
        .unwrap();
    let recovered = snapshot(&client, "cache-pending-play");
    assert_eq!(recovered["playback"]["active"], true);
    assert_cursor(&recovered, 0);
    let receipt_state: String = database
        .query_row(
            "SELECT receipt_state FROM web_command_inbox
             WHERE request_id='cache-pending-play-command'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(receipt_state, "COMPLETED");
    let advanced = wait_for_cursor(&client, "cache-pending-play", 1);
    let pause = command_request(
        "cache-pending-play-pause",
        "pause",
        "cache-pending-play",
        advanced["sequence"].as_i64().unwrap(),
        advanced["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&pause).status, 200);
}

#[test]
fn snapshot_hides_partial_market_bar_until_the_same_step_receipt_completes() {
    let harness = Harness::with_custom_data(
        "cache-partial.csv",
        concat!(
            "timestamp,symbol,open,high,low,close,volume,amount\n",
            "2026-01-05 09:30:00,600000.SH,10.00,10.01,9.86,9.87,1000,9870\n",
            "2026-01-05 09:31:00,600000.SH,9.87,10.01,9.86,10.00,1000,10000\n",
        )
        .to_owned(),
    );
    let client = harness.client();
    start_run_with_dataset(&client, "cache-partial-step", "cache-partial.csv");
    let before = snapshot(&client, "cache-partial-step");
    assert_cursor(&before, 0);
    let step = command_request(
        "cache-partial-step-request",
        "step",
        "cache-partial-step",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    let database = harness.database();
    database
        .execute_batch(
            "CREATE TRIGGER fail_partial_market_bar_completion
             BEFORE INSERT ON events
             WHEN NEW.run_id='cache-partial-step'
              AND NEW.event_type='MARKET_BAR_PROCESSED'
             BEGIN SELECT RAISE(ABORT, 'injected partial bar completion failure'); END;",
        )
        .unwrap();

    let failed = client.command(&step);
    assert_ne!(failed.status, 200);
    let (received, processed, journal_head): (i64, i64, i64) = database
        .query_row(
            "SELECT
               SUM(CASE WHEN event_type='MARKET_DATA_RECEIVED' THEN 1 ELSE 0 END),
               SUM(CASE WHEN event_type='MARKET_BAR_PROCESSED' THEN 1 ELSE 0 END),
               MAX(sequence_number)
             FROM events WHERE run_id='cache-partial-step'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(received, 1);
    assert_eq!(processed, 0);
    assert!(journal_head > before["sequence"].as_i64().unwrap());
    let partial_bytes = snapshot_bytes(&client, "cache-partial-step");
    let partial: Value = serde_json::from_slice(&partial_bytes).unwrap();
    assert_eq!(partial["sequence"], before["sequence"]);
    assert_eq!(partial["state"], before["state"]);
    assert_eq!(partial["progress"], before["progress"]);
    assert_eq!(partial["performance"], before["performance"]);
    assert_eq!(
        partial["state"]["last_price"],
        before["state"]["last_price"]
    );
    assert!(!String::from_utf8_lossy(&partial_bytes).contains("9.87"));
    assert_eq!(snapshot_bytes(&client, "cache-partial-step"), partial_bytes);

    database
        .execute_batch("DROP TRIGGER fail_partial_market_bar_completion;")
        .unwrap();
    let recovered_response = client.command(&step);
    assert_eq!(
        recovered_response.status,
        200,
        "{}",
        String::from_utf8_lossy(&recovered_response.body)
    );
    let recovered = snapshot(&client, "cache-partial-step");
    assert_cursor(&recovered, 1);
    assert!(recovered["sequence"].as_i64().unwrap() > before["sequence"].as_i64().unwrap());
    assert_eq!(recovered["state"]["last_price"], "9.87");
    let (received_after, processed_after, receipt_count, completed_receipts): (i64, i64, i64, i64) =
        database
            .query_row(
                "SELECT
               (SELECT COUNT(*) FROM events
                 WHERE run_id='cache-partial-step' AND event_type='MARKET_DATA_RECEIVED'),
               (SELECT COUNT(*) FROM events
                 WHERE run_id='cache-partial-step' AND event_type='MARKET_BAR_PROCESSED'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='cache-partial-step-request'),
               (SELECT COUNT(*) FROM web_command_inbox
                 WHERE request_id='cache-partial-step-request' AND receipt_state='COMPLETED')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    assert_eq!((received_after, processed_after), (1, 1));
    assert_eq!((receipt_count, completed_receipts), (1, 1));
    let stable = snapshot_bytes(&client, "cache-partial-step");
    assert_eq!(snapshot_bytes(&client, "cache-partial-step"), stable);
}

#[test]
fn snapshot_after_restart_matches_an_independent_ledger_recovery_field_for_field() {
    let mut harness = Harness::new();
    let client = harness.client();
    start_run(&client, "cache-cold-restart");
    let before = snapshot(&client, "cache-cold-restart");
    let finish = command_request(
        "cache-cold-restart-finish",
        "finish",
        "cache-cold-restart",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 200);
    let hot = snapshot(&client, "cache-cold-restart");

    harness.restart();
    let cold = snapshot(&harness.client(), "cache-cold-restart");
    assert_eq!(cold, hot);

    let config = Config::load(&harness.config).unwrap();
    let database = harness.database();
    let cycle_id: String = database
        .query_row(
            "SELECT cycle_id FROM events WHERE run_id='cache-cold-restart'
             ORDER BY sequence_number LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let initial_state = StrategyState::new(
        "cache-cold-restart".to_owned(),
        cycle_id,
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    );
    let store = SqliteStore::open(&config.database).unwrap();
    let (recovered_state, rebuilt_sequence) = store.rebuild(initial_state).unwrap();
    let realized = recovered_state.realized_pnl;
    let valuation = unrealized_grid_valuation(&recovered_state);
    let valuation_policy_version = recovered_state
        .audited_profit_guard_policy
        .as_ref()
        .map(|_| UNREALIZED_VALUATION_POLICY_VERSION);
    let sequence: i64 = database
        .query_row(
            "SELECT MAX(sequence_number) FROM events WHERE run_id='cache-cold-restart'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rebuilt_sequence, sequence);
    let descriptor_json: String = database
        .query_row(
            "SELECT payload FROM events
             WHERE run_id='cache-cold-restart' AND event_type='REPLAY_INITIALIZED'
             ORDER BY sequence_number LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let processed_bars: i64 = database
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE run_id='cache-cold-restart' AND event_type='MARKET_BAR_PROCESSED'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let (command_version, active, interval_ms): (i64, i64, i64) = database
        .query_row(
            "SELECT command_version,active,interval_ms FROM web_playback_control
             WHERE run_id='cache-cold-restart'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let baseline = json!({
        "api_version": API_VERSION,
        "sequence": sequence,
        "lot_size": config.lot_size,
        "standard_quantity": config.standard_quantity,
        "state": recovered_state,
        "performance": {
            "realized_grid_pnl": realized.to_string(),
            "unrealized_grid_pnl": valuation.mark_to_market_unrealized.map(|value| value.to_string()),
            "total_grid_pnl": valuation.mark_to_market_unrealized.map(|value| (realized + value).to_string()),
            "valuation_policy_version": valuation_policy_version,
            "mark_price": valuation.mark_price.map(|value| value.to_string()),
            "mark_to_market_unrealized_grid_pnl": valuation.mark_to_market_unrealized.map(|value| value.to_string()),
            "total_mark_to_market_grid_pnl": valuation.mark_to_market_unrealized.map(|value| (realized + value).to_string()),
            "conservative_exit_unrealized_grid_pnl": valuation.conservative_exit_unrealized.map(|value| value.to_string()),
            "total_conservative_exit_grid_pnl": valuation.conservative_exit_unrealized.map(|value| (realized + value).to_string()),
            "conservative_exit_adjustment": valuation.conservative_exit_adjustment.map(|value| value.to_string()),
            "unpriced_open_quantity": valuation.unpriced_open_quantity,
        },
        "progress": {
            "descriptor": serde_json::from_str::<Value>(&descriptor_json).unwrap(),
            "processed_bars": processed_bars,
        },
        "playback": {
            "active": active != 0,
            "interval_ms": interval_ms,
            "command_version": command_version,
        },
        "command_version": command_version,
    });
    assert_eq!(cold, baseline);
}

#[test]
fn snapshot_reports_exact_mark_to_market_and_per_lot_conservative_exit_and_recovers_cold() {
    let mut harness = Harness::with_custom_data(
        "valuation-three-bars.csv",
        concat!(
            "timestamp,symbol,open,high,low,close,volume,amount\n",
            "2026-01-05 09:30:00,600000.SH,10.00,10.00,10.00,10.00,1000,10000\n",
            "2026-01-05 09:31:00,600000.SH,10.00,10.00,9.80,9.80,1000,9800\n",
            "2026-01-05 09:32:00,600000.SH,9.81,9.82,9.81,9.82,1000,9820\n",
        )
        .to_owned(),
    );
    let client = harness.client();
    start_run_with_dataset(&client, "valuation-snapshot", "valuation-three-bars.csv");
    let before = snapshot(&client, "valuation-snapshot");
    assert_eq!(before["performance"]["unrealized_grid_pnl"], Value::Null);
    assert_eq!(before["performance"]["total_grid_pnl"], Value::Null);
    assert_eq!(before["performance"]["mark_price"], Value::Null);
    assert_eq!(
        before["performance"]["mark_to_market_unrealized_grid_pnl"],
        Value::Null
    );
    assert_eq!(
        before["performance"]["conservative_exit_unrealized_grid_pnl"],
        Value::Null
    );
    let finish = command_request(
        "valuation-snapshot-finish",
        "finish",
        "valuation-snapshot",
        before["sequence"].as_i64().unwrap(),
        before["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&finish).status, 200);
    let hot_bytes = snapshot_bytes(&client, "valuation-snapshot");
    let hot: Value = serde_json::from_slice(&hot_bytes).unwrap();
    assert_cursor(&hot, 3);
    assert_eq!(hot["state"]["last_price"], "9.82");
    assert_eq!(hot["state"]["lots"].as_object().unwrap().len(), 1);
    assert_eq!(
        hot["performance"]["unrealized_grid_pnl"],
        hot["performance"]["mark_to_market_unrealized_grid_pnl"]
    );
    assert_eq!(
        hot["performance"]["total_grid_pnl"],
        hot["performance"]["total_mark_to_market_grid_pnl"]
    );
    assert_eq!(
        hot["performance"],
        json!({
            "realized_grid_pnl": "0",
            "unrealized_grid_pnl": "10.00",
            "total_grid_pnl": "10.00",
            "valuation_policy_version": UNREALIZED_VALUATION_POLICY_VERSION,
            "mark_price": "9.82",
            "mark_to_market_unrealized_grid_pnl": "10.00",
            "total_mark_to_market_grid_pnl": "10.00",
            "conservative_exit_unrealized_grid_pnl": "-24.72",
            "total_conservative_exit_grid_pnl": "-24.72",
            "conservative_exit_adjustment": "-34.72",
            "unpriced_open_quantity": 0,
        })
    );
    assert_eq!(snapshot_bytes(&client, "valuation-snapshot"), hot_bytes);

    harness.restart();
    let cold_bytes = snapshot_bytes(&harness.client(), "valuation-snapshot");
    assert_eq!(cold_bytes, hot_bytes);
}

#[test]
fn snapshot_cache_never_exposes_future_ohlc_before_its_bar_is_processed() {
    let harness = Harness::with_custom_data(
        "cache-future.csv",
        concat!(
            "timestamp,symbol,open,high,low,close,volume,amount\n",
            "2026-01-05 09:30:00,600000.SH,10.00,10.01,9.99,10.00,1000,10000\n",
            "2026-01-05 09:31:00,600000.SH,10.00,12.34,10.00,12.34,1000,12340\n",
        )
        .to_owned(),
    );
    let client = harness.client();
    start_run_with_dataset(&client, "cache-no-future", "cache-future.csv");
    let initial = snapshot(&client, "cache-no-future");
    assert_eq!(initial["performance"]["unrealized_grid_pnl"], Value::Null);
    assert_eq!(initial["performance"]["total_grid_pnl"], Value::Null);
    assert_eq!(initial["performance"]["mark_price"], Value::Null);
    assert_eq!(
        initial["performance"]["mark_to_market_unrealized_grid_pnl"],
        Value::Null
    );
    assert_eq!(
        initial["performance"]["conservative_exit_unrealized_grid_pnl"],
        Value::Null
    );
    let first = command_request(
        "cache-no-future-first",
        "step",
        "cache-no-future",
        initial["sequence"].as_i64().unwrap(),
        initial["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&first).status, 200);
    let after_first = snapshot_bytes(&client, "cache-no-future");
    assert!(!String::from_utf8_lossy(&after_first).contains("12.34"));

    let first_snapshot: Value = serde_json::from_slice(&after_first).unwrap();
    let second = command_request(
        "cache-no-future-second",
        "step",
        "cache-no-future",
        first_snapshot["sequence"].as_i64().unwrap(),
        first_snapshot["command_version"].as_i64().unwrap(),
    );
    assert_eq!(client.command(&second).status, 200);
    let terminal = snapshot(&client, "cache-no-future");
    assert_cursor(&terminal, 2);
    assert_eq!(terminal["state"]["last_price"], "12.34");
}
