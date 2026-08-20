use crate::{
    config::Config,
    data::{CsvReplayFeed, MarketBar},
    decision::{algorithm_from_config, AlgorithmManifest, GridRightStatus},
    domain::{GridLevelStatus, OrderStatus, ServiceMode, StrategyState},
    journal::{
        web_command_plan_sha256, EventReader, PendingWebCommand, SqliteStore, StateReader,
        WebCommandClaim, WebCommandReceipt, WebPlaybackControl,
    },
    service::{compare_states, GridAutomationService, ReplayDescriptor},
};
use anyhow::{bail, Context, Result};
use axum::{
    body::Bytes,
    extract::{Form, Path as AxumPath, Query, Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Write,
    fs::{self, File, OpenOptions},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};
#[cfg(unix)]
use std::{os::fd::AsRawFd, os::unix::fs::MetadataExt};
use tokio::sync::{watch, Mutex};
use uuid::Uuid;

#[derive(Clone)]
struct DatasetOption {
    id: String,
    label: String,
    sha256: String,
    bars: Arc<Vec<MarketBar>>,
}

type CommandTaskKey = (String, String);
type CommandTaskMap = HashMap<CommandTaskKey, Arc<ActiveCommandTask>>;

#[derive(Clone)]
struct WebState {
    config: Config,
    database: Arc<WebDatabaseGuard>,
    datasets: Vec<DatasetOption>,
    default_dataset: String,
    run_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    command_tasks: Arc<Mutex<CommandTaskMap>>,
    command_task_count: watch::Sender<usize>,
    snapshot_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    snapshot_cache: Arc<StdMutex<SnapshotCache>>,
    playbacks: Arc<Mutex<HashMap<String, Arc<PlaybackControl>>>>,
    allowed_host: String,
    csrf_token: String,
    api_token: String,
}

struct ActiveCommandTask {
    request: ApiCommandRequest,
    outcome: watch::Sender<Option<CommandTaskOutcome>>,
}

#[derive(Clone)]
enum CommandTaskOutcome {
    Completed(ApiCommandResponse),
    Failed(CommandTaskFailure),
}

#[derive(Clone)]
struct CommandTaskFailure {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ActiveCommandTask {
    fn new(request: ApiCommandRequest) -> Self {
        let (outcome, _) = watch::channel(None);
        Self { request, outcome }
    }

    async fn wait(&self) -> Result<ApiCommandResponse, WebError> {
        let mut outcome = self.outcome.subscribe();
        loop {
            if let Some(result) = outcome.borrow_and_update().clone() {
                return match result {
                    CommandTaskOutcome::Completed(response) => Ok(response),
                    CommandTaskOutcome::Failed(failure) => Err(failure.into_web_error()),
                };
            }
            outcome
                .changed()
                .await
                .map_err(|_| WebError::conflict("command task ended without a durable outcome"))?;
        }
    }
}

impl From<WebError> for CommandTaskFailure {
    fn from(error: WebError) -> Self {
        Self {
            status: error.status,
            code: error.code,
            message: error.error.to_string(),
        }
    }
}

impl CommandTaskFailure {
    fn into_web_error(self) -> WebError {
        WebError {
            status: self.status,
            code: self.code,
            error: anyhow::anyhow!(self.message),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WebDatabaseIdentity {
    canonical_path: PathBuf,
    instance_id: String,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

struct WebDatabaseGuard {
    path: PathBuf,
    identity: WebDatabaseIdentity,
    ready: AtomicBool,
}

impl WebDatabaseGuard {
    fn capture(path: &Path, instance_id: String) -> Result<Self> {
        Ok(Self {
            path: path.to_owned(),
            identity: web_database_identity(path, instance_id)?,
            ready: AtomicBool::new(true),
        })
    }

    fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    fn verify_identity(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            bail!("Web database readiness was revoked; restart is required")
        }
        let current = web_database_identity(&self.path, self.identity.instance_id.clone())?;
        if current.canonical_path != self.identity.canonical_path || {
            #[cfg(unix)]
            {
                current.device != self.identity.device || current.inode != self.identity.inode
            }
            #[cfg(not(unix))]
            {
                false
            }
        } {
            bail!("Web database file identity changed; restart is required")
        }
        Ok(())
    }

    fn open_store(&self) -> Result<SqliteStore> {
        let result = (|| {
            self.probe_identity()?;
            let store = SqliteStore::open_existing(&self.path)?;
            store.verify_current_schema()?;
            if store.database_instance_id()? != self.identity.instance_id {
                bail!("Web database instance identity changed; restart is required")
            }
            store.run_ids()?;
            self.verify_identity()?;
            Ok(store)
        })();
        if result.is_err() {
            self.mark_not_ready();
        }
        result
    }

    fn probe_identity(&self) -> Result<()> {
        let result = (|| {
            self.verify_identity()?;
            let store = SqliteStore::open_existing_read_only(&self.path)?;
            store.verify_current_schema()?;
            if store.database_instance_id()? != self.identity.instance_id {
                bail!("Web database instance identity changed; restart is required")
            }
            self.verify_identity()
        })();
        if result.is_err() {
            self.mark_not_ready();
        }
        result
    }
}

fn web_database_identity(path: &Path, instance_id: String) -> Result<WebDatabaseIdentity> {
    let canonical_path = fs::canonicalize(path)
        .with_context(|| format!("failed to resolve Web database {}", path.display()))?;
    let metadata = fs::metadata(&canonical_path)
        .with_context(|| format!("failed to inspect Web database {}", path.display()))?;
    Ok(WebDatabaseIdentity {
        canonical_path,
        instance_id,
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

const SNAPSHOT_CACHE_MAX_ENTRIES: usize = 16;
const SNAPSHOT_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;
const SNAPSHOT_CACHE_MAX_ITEM_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotStamp {
    sequence: i64,
    duplicate_events: u64,
    processed_bars: usize,
    descriptor_identity_sha256: Option<String>,
    playback: WebPlaybackControl,
}

#[derive(Debug)]
struct CachedSnapshot {
    stamp: SnapshotStamp,
    json: Arc<[u8]>,
}

#[derive(Debug, Default)]
struct SnapshotCache {
    entries: HashMap<String, CachedSnapshot>,
    least_recently_used: VecDeque<String>,
    total_bytes: usize,
}

impl SnapshotCache {
    fn get(&mut self, run_id: &str, stamp: &SnapshotStamp) -> Option<Arc<[u8]>> {
        let cached = self
            .entries
            .get(run_id)
            .filter(|cached| &cached.stamp == stamp)
            .map(|cached| Arc::clone(&cached.json))?;
        self.touch(run_id);
        Some(cached)
    }

    fn insert(&mut self, run_id: String, stamp: SnapshotStamp, json: Arc<[u8]>) {
        if json.len() > SNAPSHOT_CACHE_MAX_ITEM_BYTES {
            self.remove(&run_id);
            return;
        }
        self.remove(&run_id);
        self.total_bytes += json.len();
        self.least_recently_used.push_back(run_id.clone());
        self.entries.insert(run_id, CachedSnapshot { stamp, json });
        while self.entries.len() > SNAPSHOT_CACHE_MAX_ENTRIES
            || self.total_bytes > SNAPSHOT_CACHE_MAX_BYTES
        {
            let Some(oldest) = self.least_recently_used.pop_front() else {
                break;
            };
            if let Some(removed) = self.entries.remove(&oldest) {
                self.total_bytes = self.total_bytes.saturating_sub(removed.json.len());
            }
        }
    }

    fn touch(&mut self, run_id: &str) {
        if let Some(position) = self
            .least_recently_used
            .iter()
            .position(|candidate| candidate == run_id)
        {
            self.least_recently_used.remove(position);
        }
        self.least_recently_used.push_back(run_id.to_owned());
    }

    fn remove(&mut self, run_id: &str) {
        if let Some(removed) = self.entries.remove(run_id) {
            self.total_bytes = self.total_bytes.saturating_sub(removed.json.len());
        }
        if let Some(position) = self
            .least_recently_used
            .iter()
            .position(|candidate| candidate == run_id)
        {
            self.least_recently_used.remove(position);
        }
    }
}

fn cached_snapshot_json(
    cache: &StdMutex<SnapshotCache>,
    run_id: &str,
    stamp: SnapshotStamp,
    cacheable: bool,
    build: impl FnOnce() -> Result<Arc<[u8]>>,
) -> Result<Arc<[u8]>> {
    if cacheable {
        let mut cache = cache
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot cache lock poisoned"))?;
        if let Some(json) = cache.get(run_id, &stamp) {
            return Ok(json);
        }
    }
    let json = build()?;
    if cacheable {
        cache
            .lock()
            .map_err(|_| anyhow::anyhow!("snapshot cache lock poisoned"))?
            .insert(run_id.to_owned(), stamp, Arc::clone(&json));
    }
    Ok(json)
}

struct WebDatabaseLease {
    _file: File,
}

impl WebDatabaseLease {
    fn acquire(database: &Path) -> Result<Self> {
        let identity = database_lease_identity(database)?;
        let extension = identity
            .extension()
            .and_then(|value| value.to_str())
            .map_or_else(
                || "web.lock".to_owned(),
                |value| format!("{value}.web.lock"),
            );
        let path = identity.with_extension(extension);
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("failed to open Web database lease {}", path.display()))?;
        #[cfg(unix)]
        {
            // SAFETY: flock only observes the live file descriptor, which is retained by Self.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result != 0 {
                bail!(
                    "another GridEdge-T Web service already owns database {}",
                    database.display()
                )
            }
        }
        Ok(Self { _file: file })
    }
}

fn database_lease_identity(database: &Path) -> Result<PathBuf> {
    let mut candidate = if database.is_absolute() {
        database.to_path_buf()
    } else {
        std::env::current_dir()?.join(database)
    };
    for _ in 0..32 {
        match fs::symlink_metadata(&candidate) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let target = fs::read_link(&candidate)?;
                candidate = if target.is_absolute() {
                    target
                } else {
                    candidate
                        .parent()
                        .context("database symlink has no parent")?
                        .join(target)
                };
            }
            Ok(_) => {
                return fs::canonicalize(&candidate).context("failed to resolve database identity");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = candidate.parent().context("database path has no parent")?;
                let parent = fs::canonicalize(parent)?;
                return Ok(parent.join(
                    candidate
                        .file_name()
                        .context("database path has no filename")?,
                ));
            }
            Err(error) => return Err(error).context("failed to inspect database identity"),
        }
    }
    bail!("database symlink chain is cyclic or too deep")
}

#[cfg(unix)]
impl Drop for WebDatabaseLease {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains valid for the full lifetime of this guard.
        unsafe {
            libc::flock(self._file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[derive(Debug)]
struct PlaybackControl {
    cancelled: AtomicBool,
    interval_ms: u64,
    generation: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
struct PlaybackView {
    active: bool,
    interval_ms: u64,
    command_version: i64,
}

#[derive(Debug, Clone, Serialize)]
struct ReplayProgress {
    descriptor: ReplayDescriptor,
    processed_bars: usize,
}

impl ReplayProgress {
    fn is_complete(&self) -> bool {
        self.processed_bars >= self.descriptor.total_bars
    }
}

#[derive(Debug, Deserialize, Default)]
struct DashboardQuery {
    run_id: Option<String>,
    search: Option<String>,
    notice: Option<String>,
    dataset: Option<String>,
    window: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunForm {
    run_id: String,
    #[serde(default)]
    dataset: String,
    request_id: String,
    expected_sequence: i64,
    expected_version: i64,
    csrf_token: String,
}

#[derive(Debug, Deserialize)]
struct PlaybackForm {
    run_id: String,
    speed_ms: u64,
    request_id: String,
    expected_sequence: i64,
    expected_version: i64,
    csrf_token: String,
}

#[derive(Debug, Deserialize)]
struct PendingRetryForm {
    run_id: String,
    request_id: String,
    csrf_token: String,
}

#[derive(Debug, Deserialize)]
struct ResumeForm {
    run_id: String,
    reason: String,
    csrf_token: String,
}

#[derive(Debug, Deserialize)]
struct ApiEventsQuery {
    #[serde(default)]
    after: i64,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ApiOpportunitiesQuery {
    #[serde(default)]
    after: i64,
    through: i64,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ApiBarsQuery {
    max_points: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ApiRunSnapshot {
    api_version: &'static str,
    sequence: i64,
    lot_size: i64,
    standard_quantity: i64,
    state: StrategyState,
    performance: ApiPerformance,
    progress: Option<ReplayProgress>,
    playback: PlaybackView,
    command_version: i64,
}

#[derive(Debug, Serialize)]
struct ApiPerformance {
    realized_grid_pnl: String,
    /// Compatibility aliases for the explicitly named mark-to-market fields.
    unrealized_grid_pnl: Option<String>,
    total_grid_pnl: Option<String>,
    valuation_policy_version: Option<&'static str>,
    mark_price: Option<String>,
    mark_to_market_unrealized_grid_pnl: Option<String>,
    total_mark_to_market_grid_pnl: Option<String>,
    conservative_exit_unrealized_grid_pnl: Option<String>,
    total_conservative_exit_grid_pnl: Option<String>,
    conservative_exit_adjustment: Option<String>,
    unpriced_open_quantity: i64,
}

#[derive(Debug, Serialize)]
struct ApiEventBatch {
    api_version: &'static str,
    events: Vec<crate::event::EventEnvelope>,
    next_sequence: i64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
struct ApiOpportunityCounts {
    touched: usize,
    granted: usize,
    skipped: usize,
    legacy_unbound: usize,
}

#[derive(Debug, Serialize)]
struct ApiOpportunityRecord {
    opportunity_id: String,
    run_id: String,
    cycle_id: String,
    symbol: String,
    event_time: String,
    grid_index: i64,
    grid_price: Option<String>,
    touch_sequence: i64,
    resolution_sequence: i64,
    processed_sequence: i64,
    resolution: &'static str,
    semantics: &'static str,
    standard_quantity: i64,
    direction: Option<String>,
    right_id: Option<String>,
    decision_id: Option<String>,
    reason: Option<String>,
    reason_audit_status: Option<&'static str>,
    terminal_kind: Option<&'static str>,
    terminal_reason: Option<String>,
    algorithm_succeeded: Option<bool>,
    pre_trade_capacity: Option<ApiOpportunityPreTradeCapacity>,
    partial_blocks: Vec<ApiOpportunityPartialBlock>,
    decision_contract_version: Option<i64>,
    gross_available_quantity: Option<i64>,
    platform_residual_quantity: Option<i64>,
    algorithm_authorized_quantity: Option<i64>,
    algorithm_offered_quantity: Option<i64>,
    predecision_blocked_quantity: Option<i64>,
    exercise_quantity: Option<i64>,
    defer_quantity: Option<i64>,
    platform_blocked_quantity: Option<i64>,
    postdecision_blocked_quantity: Option<i64>,
    order_intent_quantity: Option<i64>,
    remaining_decision_quantity: Option<i64>,
    market_score: Option<String>,
    market_signal_passed: Option<bool>,
    cash_affordable_units: Option<i64>,
    sellable_inventory_units: Option<i64>,
    resource_units: Option<i64>,
    target_units: Option<i64>,
    pending_buy_quantity: Option<i64>,
    position_exposure_quantity: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ApiOpportunityPreTradeCapacity {
    eligible_quantity: i64,
    eligible_lot_ids: Vec<String>,
    t_plus_one_blocked_quantity: i64,
    t_plus_one_blocked_lot_ids: Vec<String>,
    risk_blocked_quantity: i64,
    risk_blocked_lot_ids: Vec<String>,
    no_profit_blocked_quantity: i64,
    no_profit_blocked_lot_ids: Vec<String>,
    source_right_ids: Vec<String>,
    tranche_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ApiOpportunityPartialBlock {
    sequence: i64,
    reason: String,
    t_plus_one_blocked_quantity: i64,
    t_plus_one_blocked_lot_ids: Vec<String>,
    risk_blocked_quantity: i64,
    risk_blocked_lot_ids: Vec<String>,
    no_profit_blocked_quantity: i64,
    no_profit_blocked_lot_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ApiOpportunityPage {
    api_version: &'static str,
    run_id: String,
    through_sequence: i64,
    standard_quantity: i64,
    opportunities: Vec<ApiOpportunityRecord>,
    next_sequence: i64,
    complete: bool,
    counts: ApiOpportunityCounts,
}

#[derive(Debug, Serialize)]
struct ApiBarBatch {
    api_version: &'static str,
    run_id: String,
    dataset_id: String,
    data_sha256: String,
    visible_bars: usize,
    total_bars: usize,
    sampled: Vec<MarketBar>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ApiCommandKind {
    Start,
    Step,
    Play,
    Pause,
    Finish,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ApiCommandRequest {
    api_version: String,
    request_id: String,
    command: ApiCommandKind,
    run_id: String,
    dataset: Option<String>,
    speed_ms: Option<u64>,
    expected_sequence: i64,
    expected_version: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct ApiCommandResponse {
    api_version: String,
    request_id: String,
    command: ApiCommandKind,
    run_id: String,
    accepted: bool,
    message: String,
    accepted_sequence: i64,
    accepted_version: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiPendingRetryRequest {
    run_id: String,
    request_id: String,
}

#[derive(Debug, Serialize)]
struct ApiPendingCommandView {
    run_id: String,
    request_id: String,
    command: String,
    accepted_version: i64,
    recovery_state: &'static str,
}

#[derive(Debug, Serialize)]
struct ApiPendingCommandBatch {
    api_version: &'static str,
    commands: Vec<ApiPendingCommandView>,
}

pub async fn serve(
    config_path: PathBuf,
    data_path: PathBuf,
    host: String,
    port: u16,
) -> Result<()> {
    let address: SocketAddr = format!("{host}:{port}")
        .parse()
        .context("invalid web listen address")?;
    if !address.ip().is_loopback() {
        bail!("the MVP dashboard may only listen on a loopback address")
    }
    let config = Config::load(&config_path)?;
    let _database_lease = WebDatabaseLease::acquire(Path::new(&config.database))?;
    let database_instance_id =
        prepare_web_database(&config).context("Web database failed its startup readiness check")?;
    let database = Arc::new(WebDatabaseGuard::capture(
        Path::new(&config.database),
        database_instance_id,
    )?);
    let datasets = discover_datasets(&data_path, &config.symbol)?;
    let default_dataset = dataset_id(&data_path)?;
    let csrf_token = Uuid::new_v4().to_string();
    let (command_task_count, _) = watch::channel(0_usize);
    let state = Arc::new(WebState {
        config,
        database,
        datasets,
        default_dataset,
        run_locks: Arc::new(Mutex::new(HashMap::new())),
        command_tasks: Arc::new(Mutex::new(HashMap::new())),
        command_task_count,
        snapshot_locks: Arc::new(Mutex::new(HashMap::new())),
        snapshot_cache: Arc::new(StdMutex::new(SnapshotCache::default())),
        playbacks: Arc::new(Mutex::new(HashMap::new())),
        allowed_host: address.to_string(),
        api_token: std::env::var("GRIDEDGE_API_TOKEN").unwrap_or_else(|_| csrf_token.clone()),
        csrf_token,
    });
    let shutdown_state = Arc::clone(&state);
    let app = Router::new()
        .route("/", get(dashboard))
        .route("/assets/dashboard.js", get(dashboard_script))
        .route("/api/v1/runs", get(api_runs))
        .route("/api/v1/runs/{run_id}", get(api_run_snapshot))
        .route("/api/v1/runs/{run_id}/events", get(api_run_events))
        .route(
            "/api/v1/runs/{run_id}/opportunities",
            get(api_run_opportunities),
        )
        .route("/api/v1/runs/{run_id}/bars", get(api_run_bars))
        .route("/api/v1/commands", post(api_command))
        .route("/api/v1/pending-commands", get(api_pending_commands))
        .route(
            "/api/v1/pending-commands/retry",
            post(api_retry_pending_command),
        )
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(readiness))
        .route("/actions/replay", post(replay_action))
        .route("/actions/step/start", post(step_start_action))
        .route("/actions/step/next", post(step_next_action))
        .route("/actions/step/play", post(step_play_action))
        .route("/actions/step/pause", post(step_pause_action))
        .route("/actions/step/finish", post(step_finish_action))
        .route("/actions/commands/retry", post(retry_pending_action))
        .route("/actions/rebuild", post(rebuild_action))
        .route("/actions/reconcile", post(reconcile_action))
        .route("/actions/resume", post(resume_action))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            security_boundary,
        ))
        .with_state(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("GridEdge-T dashboard: http://{address}/");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown_state.database.mark_not_ready();
            let playbacks = shutdown_state.playbacks.lock().await;
            for control in playbacks.values() {
                control.cancelled.store(true, Ordering::Release);
            }
        })
        .await?;
    wait_for_command_tasks(&state).await;
    Ok(())
}

async fn wait_for_command_tasks(app: &WebState) {
    let mut active = app.command_task_count.subscribe();
    while *active.borrow_and_update() != 0 {
        if active.changed().await.is_err() {
            break;
        }
    }
}

fn prepare_web_database(config: &Config) -> Result<String> {
    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    store.verify_current_schema()?;
    store.run_ids()?;
    store.database_instance_id()
}

fn open_web_store(app: &WebState) -> std::result::Result<SqliteStore, WebError> {
    app.database
        .open_store()
        .map_err(WebError::database_unavailable)
}

async fn readiness(State(app): State<Arc<WebState>>) -> Response {
    match app.database.open_store() {
        Ok(_) => (StatusCode::OK, "ready").into_response(),
        Err(error) => {
            eprintln!("Web database readiness failed: {error:#}");
            (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn dashboard_script() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        DYNAMIC_DASHBOARD_SCRIPT,
    )
        .into_response()
}

async fn api_runs(State(app): State<Arc<WebState>>) -> Result<Json<Vec<String>>, WebError> {
    let store = open_web_store(&app)?;
    Ok(Json(store.run_ids()?))
}

async fn api_run_snapshot(
    State(app): State<Arc<WebState>>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Response, WebError> {
    let run_id = sanitize_run_id(&run_id);
    if run_id.is_empty() {
        return Err(WebError::validation("invalid run id"));
    }
    let snapshot_lock = snapshot_lock(&app, &run_id).await;
    let _snapshot_guard = snapshot_lock.lock().await;
    let config = app.config.clone();
    let mut store = open_web_store(&app)?;
    recover_committed_pending_receipt(&app, &mut store, &run_id)?;
    if !store.run_ids()?.contains(&run_id) {
        return Err(WebError::not_found("run not found"));
    }
    let projection_config = crate::run_context::RunContext::load(&store, &run_id)?
        .map(|context| context.config)
        .unwrap_or_else(|| config.clone());
    let cache = Arc::clone(&app.snapshot_cache);
    let run_for_projection = run_id.clone();
    let (json, durable_control) = store.with_consistent_read(|store| {
        let progress = replay_progress_from_store(store, &run_for_projection)?;
        let descriptor_identity_sha256 = progress
            .as_ref()
            .map(|progress| {
                serde_json::to_vec(&progress.descriptor)
                    .map(|bytes| hex::encode(Sha256::digest(bytes)))
            })
            .transpose()?;
        let durable_control = store.web_playback_control(&run_for_projection)?;
        let stamp = SnapshotStamp {
            sequence: store.latest_sequence(&run_for_projection)?,
            duplicate_events: store.duplicate_count(&run_for_projection)?,
            processed_bars: progress
                .as_ref()
                .map_or(0, |progress| progress.processed_bars),
            descriptor_identity_sha256,
            playback: durable_control,
        };
        let pending_receipt = store.pending_web_command(&run_for_projection)?;
        let has_pending_receipt = pending_receipt.is_some();
        let first_incomplete_market_sequence = if progress.is_some() {
            store.first_incomplete_market_sequence(&run_for_projection)?
        } else {
            None
        };
        let has_incomplete_bar = first_incomplete_market_sequence.is_some();
        let json = cached_snapshot_json(
            &cache,
            &run_for_projection,
            stamp.clone(),
            !has_pending_receipt && !has_incomplete_bar,
            || {
                let initial = initial_state(&projection_config, &run_for_projection);
                let (state, sequence) =
                    if let Some(incomplete_sequence) = first_incomplete_market_sequence {
                        let completed_prefix =
                            pending_receipt
                                .as_ref()
                                .map_or(incomplete_sequence - 1, |pending| {
                                    pending
                                        .receipt
                                        .expected_sequence
                                        .min(incomplete_sequence - 1)
                                });
                        store.rebuild_through_sequence(initial, completed_prefix)?
                    } else {
                        store.rebuild(initial)?
                    };
                if (!has_incomplete_bar && sequence != stamp.sequence)
                    || sequence > stamp.sequence
                    || state.duplicate_events != stamp.duplicate_events
                {
                    bail!("snapshot projection does not match its durable journal stamp")
                }
                let realized_grid_pnl = state.realized_pnl;
                let valuation = crate::profit::unrealized_grid_valuation(&state);
                let valuation_policy_version = state
                    .audited_profit_guard_policy
                    .as_ref()
                    .map(|_| crate::profit::UNREALIZED_VALUATION_POLICY_VERSION);
                let snapshot = ApiRunSnapshot {
                    api_version: "gridedge.api/v1",
                    sequence,
                    lot_size: projection_config.lot_size,
                    standard_quantity: projection_config.standard_quantity,
                    state,
                    performance: ApiPerformance {
                        realized_grid_pnl: realized_grid_pnl.to_string(),
                        unrealized_grid_pnl: valuation
                            .mark_to_market_unrealized
                            .map(|value| value.to_string()),
                        total_grid_pnl: valuation
                            .mark_to_market_unrealized
                            .and_then(|value| realized_grid_pnl.checked_add(value))
                            .map(|value| value.to_string()),
                        valuation_policy_version,
                        mark_price: valuation.mark_price.map(|value| value.to_string()),
                        mark_to_market_unrealized_grid_pnl: valuation
                            .mark_to_market_unrealized
                            .map(|value| value.to_string()),
                        total_mark_to_market_grid_pnl: valuation
                            .mark_to_market_unrealized
                            .and_then(|value| realized_grid_pnl.checked_add(value))
                            .map(|value| value.to_string()),
                        conservative_exit_unrealized_grid_pnl: valuation
                            .conservative_exit_unrealized
                            .map(|value| value.to_string()),
                        total_conservative_exit_grid_pnl: valuation
                            .conservative_exit_unrealized
                            .and_then(|value| realized_grid_pnl.checked_add(value))
                            .map(|value| value.to_string()),
                        conservative_exit_adjustment: valuation
                            .conservative_exit_adjustment
                            .map(|value| value.to_string()),
                        unpriced_open_quantity: valuation.unpriced_open_quantity,
                    },
                    progress,
                    playback: PlaybackView {
                        active: durable_control.active,
                        interval_ms: durable_control.interval_ms,
                        command_version: durable_control.command_version,
                    },
                    command_version: durable_control.command_version,
                };
                Ok(Arc::from(serde_json::to_vec(&snapshot)?))
            },
        )?;
        Ok((json, durable_control))
    })?;
    if durable_control.active {
        ensure_playback_worker(Arc::clone(&app), run_id.clone(), durable_control).await?;
    }
    Ok((
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        Bytes::from_owner(json),
    )
        .into_response())
}

fn replay_progress_from_store(store: &SqliteStore, run_id: &str) -> Result<Option<ReplayProgress>> {
    let Some(payload) =
        store.first_payload_by_type(run_id, crate::event::EventType::ReplayInitialized)?
    else {
        return Ok(None);
    };
    let descriptor: ReplayDescriptor = serde_json::from_value(payload)?;
    let processed_bars =
        store.event_count_by_type(run_id, crate::event::EventType::MarketBarProcessed)?;
    Ok(Some(ReplayProgress {
        descriptor,
        processed_bars,
    }))
}

fn finish_terminal_evidence(
    store: &SqliteStore,
    run_id: &str,
    target_processed_bars: usize,
) -> Result<bool> {
    let (processed, latest_processed) =
        store.event_type_summary(run_id, crate::event::EventType::MarketBarProcessed)?;
    if processed < target_processed_bars {
        return Ok(false);
    }
    if processed > target_processed_bars {
        bail!("FINISH processed-bar count overtook its durable target")
    }
    if store.first_incomplete_market_sequence(run_id)?.is_some() {
        return Ok(false);
    }
    let (stopped, stopped_sequence) =
        store.event_type_summary(run_id, crate::event::EventType::ServiceStopped)?;
    if stopped == 0 {
        return Ok(false);
    }
    if stopped != 1
        || stopped_sequence.is_none()
        || latest_processed.is_none()
        || stopped_sequence <= latest_processed
    {
        bail!("FINISH terminal evidence is inconsistent")
    }
    let context = crate::run_context::RunContext::load(store, run_id)?
        .context("FINISH run lacks a durable reconstruction context")?;
    let (state, rebuilt_sequence) = store.rebuild(context.initial_state())?;
    if state.mode == ServiceMode::Running
        || rebuilt_sequence < stopped_sequence.context("missing FINISH stop sequence")?
    {
        bail!("FINISH terminal projection is still running")
    }
    Ok(true)
}

async fn api_run_events(
    State(app): State<Arc<WebState>>,
    AxumPath(run_id): AxumPath<String>,
    Query(query): Query<ApiEventsQuery>,
) -> Result<Json<ApiEventBatch>, WebError> {
    let run_id = sanitize_run_id(&run_id);
    if run_id.is_empty() || query.after < 0 {
        return Err(WebError::validation("invalid event cursor"));
    }
    let store = open_web_store(&app)?;
    let limit = query.limit.unwrap_or(250).clamp(1, 1_000);
    let events = store.load_after_limited(&run_id, query.after, limit)?;
    let next_sequence = events
        .last()
        .map(|event| event.sequence_number)
        .unwrap_or(query.after);
    Ok(Json(ApiEventBatch {
        api_version: "gridedge.api/v1",
        events,
        next_sequence,
    }))
}

async fn api_run_opportunities(
    State(app): State<Arc<WebState>>,
    AxumPath(run_id): AxumPath<String>,
    Query(query): Query<ApiOpportunitiesQuery>,
) -> Result<Json<ApiOpportunityPage>, WebError> {
    let run_id = sanitize_run_id(&run_id);
    if run_id.is_empty() || query.after < 0 || query.through < 0 || query.after > query.through {
        return Err(WebError::validation("invalid opportunity prefix"));
    }
    let limit = query.limit.unwrap_or(100);
    if !(1..=250).contains(&limit) {
        return Err(WebError::validation(
            "opportunity limit must be between 1 and 250",
        ));
    }
    let config = app.config.clone();
    let mut store = open_web_store(&app)?;
    if !store.run_ids()?.contains(&run_id) {
        return Err(WebError::not_found("run not found"));
    }
    let run_context = crate::run_context::RunContext::load(&store, &run_id)?;
    let standard_quantity = run_context
        .as_ref()
        .map(|context| context.config.standard_quantity)
        .unwrap_or(config.standard_quantity);
    let current_decision_contract = run_context.as_ref().map_or(3, |context| {
        if context.config.gate.kind == "resource_aware" {
            4
        } else {
            3
        }
    });
    let page = store
        .with_consistent_read(|store| {
            let latest = store.latest_sequence(&run_id)?;
            if query.through > latest {
                bail!("opportunity prefix is newer than the durable journal head")
            }
            let (touched, granted, skipped, legacy_unbound, invalid_current) =
                store.completed_opportunity_counts(&run_id, query.through)?;
            if invalid_current != 0 || touched != granted + skipped + legacy_unbound {
                bail!("completed opportunity prefix violates touch-resolution conservation")
            }
            let mut anchors = store.completed_opportunity_anchors(
                &run_id,
                query.after,
                query.through,
                limit + 1,
            )?;
            let complete = anchors.len() <= limit;
            anchors.truncate(limit);
            let opportunities = anchors
                .into_iter()
                .map(|(touch, processed_sequence)| {
                    let events =
                        store.opportunity_events_for_touch(&run_id, &touch, processed_sequence)?;
                    project_opportunity(
                        touch,
                        processed_sequence,
                        events,
                        standard_quantity,
                        current_decision_contract,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let next_sequence = opportunities
                .last()
                .map_or(query.after, |item| item.touch_sequence);
            if !complete && next_sequence <= query.after {
                bail!("opportunity cursor did not advance")
            }
            Ok(ApiOpportunityPage {
                api_version: "gridedge.api/v1",
                run_id: run_id.clone(),
                through_sequence: query.through,
                standard_quantity,
                opportunities,
                next_sequence,
                complete,
                counts: ApiOpportunityCounts {
                    touched,
                    granted,
                    skipped,
                    legacy_unbound,
                },
            })
        })
        .map_err(WebError::opportunity_conflict)?;
    Ok(Json(page))
}

fn project_opportunity(
    touch: crate::event::EventEnvelope,
    processed_sequence: i64,
    events: Vec<crate::event::EventEnvelope>,
    standard_quantity: i64,
    current_decision_contract: i64,
) -> Result<ApiOpportunityRecord> {
    use crate::event::EventType;

    let grid_index = json_i64(&touch.payload, &["grid_index"])?;
    if grid_index == 0 {
        bail!("grid center cannot be a mechanical trading opportunity")
    }
    let grid_price = json_optional_string(&touch.payload, &["price"])?;
    let direction = if grid_index < 0 { "BUY" } else { "SELL" };
    let all_related = events.iter().collect::<Vec<_>>();
    let legacy_touch = touch.schema_version < 2;
    if !legacy_touch && grid_price.is_none() {
        bail!("current opportunity touch lacks its grid price")
    }
    let touches = all_related
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridLevelTouched
                && (!legacy_touch || event.event_id == touch.event_id)
        })
        .copied()
        .collect::<Vec<_>>();
    let matches_legacy_grid = |event: &&crate::event::EventEnvelope| {
        !legacy_touch
            || json_optional_i64(&event.payload, &["grid_index"])
                .ok()
                .flatten()
                == Some(grid_index)
    };
    let skips = all_related
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridLevelSkipped && matches_legacy_grid(event)
        })
        .copied()
        .collect::<Vec<_>>();
    let grants = all_related
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridRightGranted && matches_legacy_grid(event)
        })
        .copied()
        .collect::<Vec<_>>();
    if touches.len() != 1 {
        bail!("opportunity must contain exactly one touch")
    }
    if legacy_touch && skips.len() + grants.len() != 1 {
        return Ok(project_legacy_unbound_opportunity(
            touch,
            processed_sequence,
            grid_index,
            grid_price,
            direction,
            standard_quantity,
            skips
                .iter()
                .chain(grants.iter())
                .map(|event| event.sequence_number)
                .collect(),
        ));
    }
    if (skips.len(), grants.len()) == (0, 0) || !skips.is_empty() && !grants.is_empty() {
        bail!("opportunity must contain one touch and exactly one grant or skip")
    }
    if skips.len() == 1 {
        let skip = skips[0];
        let related = all_related
            .iter()
            .filter(|event| event.correlation_id == skip.correlation_id)
            .copied()
            .collect::<Vec<_>>();
        if related.iter().any(|event| {
            matches!(
                event.event_type,
                EventType::GateDecisionMade | EventType::OrderIntentCreated
            )
        }) {
            bail!("skipped opportunity contains a decision or order intent")
        }
        let skip_grid = json_i64(&skip.payload, &["grid_index"])?;
        if skip_grid != grid_index {
            bail!("skip does not resolve its touched grid level")
        }
        let reason = json_string(&skip.payload, &["reason"])?;
        if !matches!(
            reason.as_str(),
            "OBSERVATION_BOUNDARY"
                | "RIGHT_ALREADY_EXERCISED_AT_LEVEL"
                | "RIGHT_CARRIED_TO_DEEPER_LEVEL"
                | "BUY_NO_AVAILABLE_CAPACITY"
                | "SELL_NO_STRATEGY_CAPACITY"
                | "AMBIGUOUS_INTRABAR_PATH"
                | "SERVICE_MODE_SAFE"
                | "SERVICE_MODE_READONLY"
        ) {
            bail!("opportunity contains an unknown skip reason")
        }
        return Ok(ApiOpportunityRecord {
            opportunity_id: touch.event_id,
            run_id: touch.run_id,
            cycle_id: touch.cycle_id,
            symbol: touch.symbol,
            event_time: touch.event_time.to_string(),
            grid_index,
            grid_price,
            touch_sequence: touch.sequence_number,
            resolution_sequence: skip.sequence_number,
            processed_sequence,
            resolution: "SKIPPED",
            semantics: if touch.schema_version == 2 && skip.schema_version == 2 {
                if current_decision_contract == 4 {
                    "CURRENT_V4"
                } else {
                    "CURRENT_V3"
                }
            } else {
                "LEGACY_RECORDED"
            },
            standard_quantity,
            direction: Some(direction.to_owned()),
            right_id: None,
            decision_id: None,
            reason: Some(reason),
            reason_audit_status: Some("RECORDED_UNVERIFIED"),
            terminal_kind: None,
            terminal_reason: None,
            algorithm_succeeded: None,
            pre_trade_capacity: None,
            partial_blocks: Vec::new(),
            decision_contract_version: None,
            gross_available_quantity: None,
            platform_residual_quantity: None,
            algorithm_authorized_quantity: None,
            algorithm_offered_quantity: None,
            predecision_blocked_quantity: None,
            exercise_quantity: None,
            defer_quantity: None,
            platform_blocked_quantity: None,
            postdecision_blocked_quantity: None,
            order_intent_quantity: None,
            remaining_decision_quantity: None,
            market_score: None,
            market_signal_passed: None,
            cash_affordable_units: None,
            sellable_inventory_units: None,
            resource_units: None,
            target_units: None,
            pending_buy_quantity: None,
            position_exposure_quantity: None,
        });
    }
    if grants.len() != 1 {
        bail!("opportunity contains duplicate grants")
    }
    let grant = grants[0];
    let related = all_related
        .iter()
        .filter(|event| event.correlation_id == grant.correlation_id)
        .copied()
        .collect::<Vec<_>>();
    let right_id = json_string(&grant.payload, &["right_id"])?;
    let recorded_direction = json_string(&grant.payload, &["direction"])?;
    if recorded_direction != direction {
        bail!("granted opportunity direction does not match its grid index")
    }
    let decisions = related
        .iter()
        .filter(|event| event.event_type == EventType::GateDecisionMade)
        .copied()
        .collect::<Vec<_>>();
    if decisions.len() != 1 {
        bail!("granted opportunity must contain exactly one decision")
    }
    let decision = decisions[0];
    let decision_right = json_string(&decision.payload, &["response", "right_id"])?;
    if decision_right != right_id {
        bail!("decision does not belong to the granted right")
    }
    let contract_version = json_i64(&decision.payload, &["response", "contract_version"])?;
    if !matches!(contract_version, 1..=4) {
        bail!("opportunity decision uses an unsupported future contract")
    }
    let decision_direction = json_string(&decision.payload, &["request", "context", "direction"])?;
    if decision_direction != direction {
        bail!("decision direction does not match its granted opportunity")
    }
    if contract_version < 3 {
        return project_legacy_granted_opportunity(
            touch,
            processed_sequence,
            grid_index,
            grid_price,
            direction,
            standard_quantity,
            right_id,
            decision,
            &related,
            contract_version,
        );
    }
    if standard_quantity <= 0 {
        bail!("current opportunity lacks a positive audited standard quantity")
    }
    let outcome_kind = json_string(&decision.payload, &["response", "outcome", "kind"])?;
    let exercise = match outcome_kind.as_str() {
        "DEFER" => json_optional_i64(
            &decision.payload,
            &["response", "outcome", "exercise_quantity"],
        )?
        .unwrap_or(0),
        "EXERCISE" => json_i64(
            &decision.payload,
            &["response", "outcome", "exercise_quantity"],
        )?,
        _ => bail!("opportunity decision has an unknown typed outcome"),
    };
    let deferred = json_i64(
        &decision.payload,
        &["response", "outcome", "defer_quantity"],
    )?;
    let gross = json_i64(
        &decision.payload,
        &["request", "context", "gross_available_quantity"],
    )?;
    let residual = json_i64(
        &decision.payload,
        &["request", "context", "platform_residual_quantity"],
    )?;
    let authorized = json_i64(
        &decision.payload,
        &["request", "context", "algorithm_authorized_quantity"],
    )?;
    let terminal = related
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::GridRightDeferred
                    | EventType::GridRightResidualHeld
                    | EventType::GridRightReserved
            ) || (event.event_type == EventType::GridRightBlocked
                && event
                    .payload
                    .get("partial")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true))
        })
        .copied()
        .collect::<Vec<_>>();
    if terminal.len() != 1 {
        bail!("granted opportunity must contain exactly one terminal disposition")
    }
    let terminal = terminal[0];
    let terminal_right = json_string(&terminal.payload, &["right_id"])?;
    let decision_id = json_string(&terminal.payload, &["decision_id"])?;
    if terminal_right != right_id {
        bail!("terminal disposition does not belong to the granted right")
    }
    let terminal_kind = opportunity_terminal_kind(terminal.event_type)?;
    let terminal_reason = json_optional_string(&terminal.payload, &["reason"])?;
    let pre_trade_capacity = project_pre_trade_capacity(grant)?;
    let partial_blocks = project_partial_blocks(&related, &right_id)?;
    if partial_blocks.len() > 1 {
        bail!("opportunity contains duplicate partial platform blocks")
    }
    if let Some(partial) = partial_blocks.first() {
        if partial.t_plus_one_blocked_quantity != pre_trade_capacity.t_plus_one_blocked_quantity
            || partial.t_plus_one_blocked_lot_ids != pre_trade_capacity.t_plus_one_blocked_lot_ids
            || partial.risk_blocked_quantity != pre_trade_capacity.risk_blocked_quantity
            || partial.risk_blocked_lot_ids != pre_trade_capacity.risk_blocked_lot_ids
            || partial.no_profit_blocked_quantity != pre_trade_capacity.no_profit_blocked_quantity
            || partial.no_profit_blocked_lot_ids != pre_trade_capacity.no_profit_blocked_lot_ids
        {
            bail!("partial platform block differs from the granted pre-trade capacity")
        }
    }
    let algorithm_succeeded = decision
        .payload
        .get("algorithm_succeeded")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("current decision lacks its algorithm status"))?;
    let terminal_gross = json_i64(&terminal.payload, &["gross_available_quantity"])?;
    let terminal_residual = json_i64(&terminal.payload, &["platform_residual_quantity"])?;
    let terminal_authorized = json_i64(&terminal.payload, &["algorithm_authorized_quantity"])?;
    let (offered, pre_blocked, post_blocked) = if contract_version == 4 {
        (
            json_i64(&terminal.payload, &["algorithm_offered_quantity"])?,
            json_i64(&terminal.payload, &["predecision_blocked_quantity"])?,
            json_i64(&terminal.payload, &["postdecision_blocked_quantity"])?,
        )
    } else {
        (
            authorized,
            0,
            json_i64(&terminal.payload, &["platform_blocked_quantity"])?,
        )
    };
    let blocked = json_i64(&terminal.payload, &["platform_blocked_quantity"])?;
    let intent_quantity = json_i64(&terminal.payload, &["order_intent_quantity"])?;
    let remaining = json_i64(&terminal.payload, &["remaining_decision_quantity"])?;
    if (gross, residual, authorized) != (terminal_gross, terminal_residual, terminal_authorized)
        || residual
            .checked_add(authorized)
            .is_none_or(|value| gross != value)
        || pre_blocked
            .checked_add(offered)
            .is_none_or(|value| authorized != value)
        || exercise
            .checked_add(deferred)
            .is_none_or(|value| offered != value)
        || intent_quantity
            .checked_add(post_blocked)
            .is_none_or(|value| exercise != value)
        || pre_blocked
            .checked_add(post_blocked)
            .is_none_or(|value| blocked != value)
        || deferred
            .checked_add(blocked)
            .is_none_or(|value| remaining != value)
        || gross < 0
        || residual < 0
        || authorized < 0
        || offered < 0
        || pre_blocked < 0
        || exercise < 0
        || deferred < 0
        || blocked < 0
        || post_blocked < 0
        || intent_quantity < 0
        || remaining < 0
        || post_blocked > exercise
        || residual >= standard_quantity
        || [
            authorized,
            offered,
            pre_blocked,
            exercise,
            deferred,
            blocked,
            post_blocked,
            intent_quantity,
            remaining,
        ]
        .iter()
        .any(|quantity| quantity % standard_quantity != 0)
    {
        bail!("opportunity quantity partition is not canonical")
    }
    let intents = related
        .iter()
        .filter(|event| event.event_type == EventType::OrderIntentCreated)
        .copied()
        .collect::<Vec<_>>();
    if (intent_quantity == 0 && !intents.is_empty()) || (intent_quantity > 0 && intents.len() != 1)
    {
        bail!("opportunity intent does not match its approved quantity")
    }
    if let Some(intent) = intents.first() {
        let intent_right = json_string(&intent.payload, &["intent", "right_id"])?;
        let intent_direction = json_string(&intent.payload, &["intent", "direction"])?;
        let quantity = json_i64(&intent.payload, &["intent", "quantity"])?;
        if intent_right != right_id || intent_direction != direction || quantity != intent_quantity
        {
            bail!("opportunity order intent is not bound to its decision")
        }
    }
    let expected_decision_schema = 4;
    let current_semantics = touch.schema_version == 2
        && grant.schema_version == 2
        && decision.schema_version == expected_decision_schema
        && contract_version == current_decision_contract
        && terminal.schema_version == crate::event::current_event_schema(terminal.event_type)
        && intents.iter().all(|intent| {
            intent.schema_version == crate::event::current_event_schema(intent.event_type)
        });
    Ok(ApiOpportunityRecord {
        opportunity_id: touch.event_id,
        run_id: touch.run_id,
        cycle_id: touch.cycle_id,
        symbol: touch.symbol,
        event_time: touch.event_time.to_string(),
        grid_index,
        grid_price,
        touch_sequence: touch.sequence_number,
        resolution_sequence: terminal.sequence_number,
        processed_sequence,
        resolution: "GRANTED",
        semantics: if current_semantics {
            if contract_version == 4 {
                "CURRENT_V4"
            } else {
                "CURRENT_V3"
            }
        } else {
            "LEGACY_RECORDED"
        },
        standard_quantity,
        direction: Some(direction.to_owned()),
        right_id: Some(right_id),
        decision_id: Some(decision_id),
        reason: None,
        reason_audit_status: None,
        terminal_kind: Some(terminal_kind),
        terminal_reason,
        algorithm_succeeded: Some(algorithm_succeeded),
        pre_trade_capacity: Some(pre_trade_capacity),
        partial_blocks,
        decision_contract_version: Some(contract_version),
        gross_available_quantity: Some(gross),
        platform_residual_quantity: Some(residual),
        algorithm_authorized_quantity: Some(authorized),
        algorithm_offered_quantity: Some(offered),
        predecision_blocked_quantity: Some(pre_blocked),
        exercise_quantity: Some(exercise),
        defer_quantity: Some(deferred),
        platform_blocked_quantity: Some(blocked),
        postdecision_blocked_quantity: Some(post_blocked),
        order_intent_quantity: Some(intent_quantity),
        remaining_decision_quantity: Some(remaining),
        market_score: if contract_version == 4 {
            Some(json_string(
                &decision.payload,
                &["request", "context", "funds_inventory", "market_score"],
            )?)
        } else {
            None
        },
        market_signal_passed: if contract_version == 4 {
            Some(json_bool(
                &decision.payload,
                &[
                    "request",
                    "context",
                    "funds_inventory",
                    "market_signal_passed",
                ],
            )?)
        } else {
            None
        },
        cash_affordable_units: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &[
                    "request",
                    "context",
                    "funds_inventory",
                    "cash_affordable_units",
                ],
            )?)
        } else {
            None
        },
        sellable_inventory_units: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &[
                    "request",
                    "context",
                    "funds_inventory",
                    "sellable_inventory_units",
                ],
            )?)
        } else {
            None
        },
        resource_units: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &["request", "context", "funds_inventory", "resource_units"],
            )?)
        } else {
            None
        },
        target_units: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &["request", "context", "funds_inventory", "target_units"],
            )?)
        } else {
            None
        },
        pending_buy_quantity: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &[
                    "request",
                    "context",
                    "funds_inventory",
                    "pending_buy_quantity",
                ],
            )?)
        } else {
            None
        },
        position_exposure_quantity: if contract_version == 4 {
            Some(json_i64(
                &decision.payload,
                &[
                    "request",
                    "context",
                    "funds_inventory",
                    "position_exposure_quantity",
                ],
            )?)
        } else {
            None
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn project_legacy_unbound_opportunity(
    touch: crate::event::EventEnvelope,
    processed_sequence: i64,
    grid_index: i64,
    grid_price: Option<String>,
    direction: &str,
    standard_quantity: i64,
    resolution_sequences: Vec<i64>,
) -> ApiOpportunityRecord {
    let resolution_sequence = resolution_sequences
        .into_iter()
        .max()
        .filter(|sequence| *sequence > touch.sequence_number)
        .unwrap_or(processed_sequence);
    ApiOpportunityRecord {
        opportunity_id: touch.event_id,
        run_id: touch.run_id,
        cycle_id: touch.cycle_id,
        symbol: touch.symbol,
        event_time: touch.event_time.to_string(),
        grid_index,
        grid_price,
        touch_sequence: touch.sequence_number,
        resolution_sequence,
        processed_sequence,
        resolution: "LEGACY_UNBOUND",
        semantics: "LEGACY_RECORDED",
        standard_quantity,
        direction: Some(direction.to_owned()),
        right_id: None,
        decision_id: None,
        reason: Some("LEGACY_TOUCH_RESOLUTION_UNBOUND".to_owned()),
        reason_audit_status: Some("LEGACY_UNBOUND"),
        terminal_kind: None,
        terminal_reason: None,
        algorithm_succeeded: None,
        pre_trade_capacity: None,
        partial_blocks: Vec::new(),
        decision_contract_version: None,
        gross_available_quantity: None,
        platform_residual_quantity: None,
        algorithm_authorized_quantity: None,
        algorithm_offered_quantity: None,
        predecision_blocked_quantity: None,
        exercise_quantity: None,
        defer_quantity: None,
        platform_blocked_quantity: None,
        postdecision_blocked_quantity: None,
        order_intent_quantity: None,
        remaining_decision_quantity: None,
        market_score: None,
        market_signal_passed: None,
        cash_affordable_units: None,
        sellable_inventory_units: None,
        resource_units: None,
        target_units: None,
        pending_buy_quantity: None,
        position_exposure_quantity: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn project_legacy_granted_opportunity(
    touch: crate::event::EventEnvelope,
    processed_sequence: i64,
    grid_index: i64,
    grid_price: Option<String>,
    direction: &str,
    standard_quantity: i64,
    right_id: String,
    decision: &crate::event::EventEnvelope,
    related: &[&crate::event::EventEnvelope],
    contract_version: i64,
) -> Result<ApiOpportunityRecord> {
    use crate::event::EventType;

    let terminals = related
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                EventType::GridRightDeferred
                    | EventType::GridRightResidualHeld
                    | EventType::GridRightReserved
            ) || (event.event_type == EventType::GridRightBlocked
                && event
                    .payload
                    .get("partial")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true))
        })
        .copied()
        .collect::<Vec<_>>();
    if terminals.len() != 1 {
        bail!("legacy granted opportunity lacks a unique recorded disposition")
    }
    let terminal = terminals[0];
    let terminal_right = json_string(&terminal.payload, &["right_id"])?;
    let decision_id = json_string(&terminal.payload, &["decision_id"])?;
    if terminal_right != right_id {
        bail!("legacy disposition does not belong to its granted right")
    }
    let intents = related
        .iter()
        .filter(|event| event.event_type == EventType::OrderIntentCreated)
        .copied()
        .collect::<Vec<_>>();
    if intents.len() > 1 {
        bail!("legacy opportunity contains duplicate order intents")
    }
    let intent_quantity = intents
        .first()
        .map(|intent| {
            let intent_right = json_string(&intent.payload, &["intent", "right_id"])?;
            let intent_direction = json_string(&intent.payload, &["intent", "direction"])?;
            if intent_right != right_id || intent_direction != direction {
                bail!("legacy order intent is not bound to its recorded opportunity")
            }
            json_i64(&intent.payload, &["intent", "quantity"])
        })
        .transpose()?;
    Ok(ApiOpportunityRecord {
        opportunity_id: touch.event_id,
        run_id: touch.run_id,
        cycle_id: touch.cycle_id,
        symbol: touch.symbol,
        event_time: touch.event_time.to_string(),
        grid_index,
        grid_price,
        touch_sequence: touch.sequence_number,
        resolution_sequence: terminal.sequence_number,
        processed_sequence,
        resolution: "GRANTED",
        semantics: "LEGACY_RECORDED",
        standard_quantity,
        direction: Some(direction.to_owned()),
        right_id: Some(right_id.clone()),
        decision_id: Some(decision_id),
        reason: None,
        reason_audit_status: None,
        terminal_kind: Some(opportunity_terminal_kind(terminal.event_type)?),
        terminal_reason: json_optional_string(&terminal.payload, &["reason"])?,
        algorithm_succeeded: decision
            .payload
            .get("algorithm_succeeded")
            .and_then(serde_json::Value::as_bool),
        pre_trade_capacity: None,
        partial_blocks: project_partial_blocks(related, &right_id)?,
        decision_contract_version: Some(contract_version),
        gross_available_quantity: None,
        platform_residual_quantity: None,
        algorithm_authorized_quantity: None,
        algorithm_offered_quantity: None,
        predecision_blocked_quantity: None,
        exercise_quantity: json_optional_i64(
            &decision.payload,
            &["response", "outcome", "exercise_quantity"],
        )?,
        defer_quantity: json_optional_i64(
            &decision.payload,
            &["response", "outcome", "defer_quantity"],
        )?,
        platform_blocked_quantity: None,
        postdecision_blocked_quantity: None,
        order_intent_quantity: intent_quantity,
        remaining_decision_quantity: None,
        market_score: None,
        market_signal_passed: None,
        cash_affordable_units: None,
        sellable_inventory_units: None,
        resource_units: None,
        target_units: None,
        pending_buy_quantity: None,
        position_exposure_quantity: None,
    })
}

fn project_pre_trade_capacity(
    grant: &crate::event::EventEnvelope,
) -> Result<ApiOpportunityPreTradeCapacity> {
    let capacity = json_at(&grant.payload, &["capacity"])?;
    let result = ApiOpportunityPreTradeCapacity {
        eligible_quantity: json_i64(capacity, &["eligible_quantity"])?,
        eligible_lot_ids: json_optional_string_array(capacity, &["eligible_lot_ids"])?
            .unwrap_or_default(),
        t_plus_one_blocked_quantity: json_optional_i64(capacity, &["t_plus_one_blocked_quantity"])?
            .unwrap_or(0),
        t_plus_one_blocked_lot_ids: json_optional_string_array(
            capacity,
            &["t_plus_one_blocked_lot_ids"],
        )?
        .unwrap_or_default(),
        risk_blocked_quantity: json_optional_i64(capacity, &["risk_blocked_quantity"])?
            .unwrap_or(0),
        risk_blocked_lot_ids: json_optional_string_array(capacity, &["risk_blocked_lot_ids"])?
            .unwrap_or_default(),
        no_profit_blocked_quantity: json_optional_i64(capacity, &["no_profit_blocked_quantity"])?
            .unwrap_or(0),
        no_profit_blocked_lot_ids: json_optional_string_array(
            capacity,
            &["no_profit_blocked_lot_ids"],
        )?
        .unwrap_or_default(),
        source_right_ids: json_optional_string_array(capacity, &["source_right_ids"])?
            .unwrap_or_default(),
        tranche_ids: json_optional_string_array(capacity, &["tranche_ids"])?.unwrap_or_default(),
    };
    if result.eligible_quantity < 0
        || result.t_plus_one_blocked_quantity < 0
        || result.risk_blocked_quantity < 0
        || result.no_profit_blocked_quantity < 0
    {
        bail!("granted pre-trade capacity contains a negative quantity")
    }
    Ok(result)
}

fn opportunity_terminal_kind(event_type: crate::event::EventType) -> Result<&'static str> {
    use crate::event::EventType;
    match event_type {
        EventType::GridRightDeferred => Ok("DEFERRED"),
        EventType::GridRightResidualHeld => Ok("RESIDUAL_HELD"),
        EventType::GridRightBlocked => Ok("BLOCKED"),
        EventType::GridRightReserved => Ok("RESERVED"),
        _ => bail!("opportunity has an unknown terminal disposition"),
    }
}

fn project_partial_blocks(
    related: &[&crate::event::EventEnvelope],
    right_id: &str,
) -> Result<Vec<ApiOpportunityPartialBlock>> {
    use crate::event::EventType;
    related
        .iter()
        .filter(|event| {
            event.event_type == EventType::GridRightBlocked
                && event
                    .payload
                    .get("partial")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
        })
        .map(|event| {
            if json_string(&event.payload, &["right_id"])? != right_id {
                bail!("partial block does not belong to its granted right")
            }
            let block = ApiOpportunityPartialBlock {
                sequence: event.sequence_number,
                reason: json_string(&event.payload, &["reason"])?,
                t_plus_one_blocked_quantity: json_optional_i64(
                    &event.payload,
                    &["t_plus_one_blocked_quantity"],
                )?
                .unwrap_or(0),
                t_plus_one_blocked_lot_ids: json_optional_string_array(
                    &event.payload,
                    &["t_plus_one_blocked_lot_ids"],
                )?
                .unwrap_or_default(),
                risk_blocked_quantity: json_optional_i64(
                    &event.payload,
                    &["risk_blocked_quantity"],
                )?
                .unwrap_or(0),
                risk_blocked_lot_ids: json_optional_string_array(
                    &event.payload,
                    &["risk_blocked_lot_ids"],
                )?
                .unwrap_or_default(),
                no_profit_blocked_quantity: json_optional_i64(
                    &event.payload,
                    &["no_profit_blocked_quantity"],
                )?
                .unwrap_or(0),
                no_profit_blocked_lot_ids: json_optional_string_array(
                    &event.payload,
                    &["no_profit_blocked_lot_ids"],
                )?
                .unwrap_or_default(),
            };
            if block.t_plus_one_blocked_quantity < 0
                || block.risk_blocked_quantity < 0
                || block.no_profit_blocked_quantity < 0
            {
                bail!("partial block contains a negative quantity")
            }
            Ok(block)
        })
        .collect()
}

fn json_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Result<&'a serde_json::Value> {
    path.iter().try_fold(value, |current, key| {
        current
            .get(*key)
            .ok_or_else(|| anyhow::anyhow!("opportunity payload is missing {key}"))
    })
}

fn json_i64(value: &serde_json::Value, path: &[&str]) -> Result<i64> {
    json_at(value, path)?
        .as_i64()
        .ok_or_else(|| anyhow::anyhow!("opportunity quantity is not an integer"))
}

fn json_bool(value: &serde_json::Value, path: &[&str]) -> Result<bool> {
    let mut current = value;
    for key in path {
        current = current
            .get(*key)
            .with_context(|| format!("missing JSON field {}", path.join(".")))?;
    }
    current
        .as_bool()
        .with_context(|| format!("JSON field {} is not a boolean", path.join(".")))
}

fn json_optional_i64(value: &serde_json::Value, path: &[&str]) -> Result<Option<i64>> {
    let Some(raw) = path
        .iter()
        .try_fold(value, |current, key| current.get(*key))
    else {
        return Ok(None);
    };
    raw.as_i64()
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("opportunity quantity is not an integer"))
}

fn json_optional_string(value: &serde_json::Value, path: &[&str]) -> Result<Option<String>> {
    let Some(raw) = path
        .iter()
        .try_fold(value, |current, key| current.get(*key))
    else {
        return Ok(None);
    };
    raw.as_str()
        .map(|text| Some(text.to_owned()))
        .ok_or_else(|| anyhow::anyhow!("opportunity text is not a string"))
}

fn json_optional_string_array(
    value: &serde_json::Value,
    path: &[&str],
) -> Result<Option<Vec<String>>> {
    let Some(raw) = path
        .iter()
        .try_fold(value, |current, key| current.get(*key))
    else {
        return Ok(None);
    };
    let values = raw
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("opportunity lot ids are not an array"))?;
    values
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("opportunity lot id is not a string"))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn json_string(value: &serde_json::Value, path: &[&str]) -> Result<String> {
    json_at(value, path)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("opportunity text is not a string"))
}

async fn api_run_bars(
    State(app): State<Arc<WebState>>,
    AxumPath(run_id): AxumPath<String>,
    Query(query): Query<ApiBarsQuery>,
) -> Result<Json<ApiBarBatch>, WebError> {
    let run_id = sanitize_run_id(&run_id);
    if run_id.is_empty() {
        return Err(WebError::validation("invalid run id"));
    }
    let maximum = query.max_points.unwrap_or(1_000);
    if !(100..=1_500).contains(&maximum) {
        return Err(WebError::validation(
            "max_points must be between 100 and 1500",
        ));
    }
    let store = open_web_store(&app)?;
    let progress = replay_progress_from_store(&store, &run_id)?
        .ok_or_else(|| WebError::conflict("run has no durable dataset binding"))?;
    let dataset = replay_dataset(&app.datasets, &progress.descriptor)
        .map_err(|_| WebError::conflict("bound replay dataset is unavailable"))?;
    validate_descriptor(&progress.descriptor, dataset, &dataset.bars)
        .map_err(WebError::conflict_error)?;
    let visible = progress.processed_bars.min(progress.descriptor.total_bars);
    Ok(Json(ApiBarBatch {
        api_version: "gridedge.api/v1",
        run_id,
        dataset_id: progress.descriptor.dataset_id,
        data_sha256: progress.descriptor.data_sha256,
        visible_bars: visible,
        total_bars: progress.descriptor.total_bars,
        sampled: aggregate_bars(&dataset.bars[..visible], maximum),
    }))
}

async fn api_pending_commands(
    State(app): State<Arc<WebState>>,
) -> Result<Json<ApiPendingCommandBatch>, WebError> {
    let mut store = open_web_store(&app)?;
    for (run_id, _) in store.pending_web_commands(1_000)? {
        if let Some(control) = recover_committed_pending_receipt(&app, &mut store, &run_id)? {
            ensure_playback_worker(Arc::clone(&app), run_id, control).await?;
        }
    }
    let commands = store
        .pending_web_commands(1_000)?
        .into_iter()
        .map(|(run_id, pending)| {
            let retryable = match pending.receipt.request_json.as_deref() {
                Some(request_json) => stored_pending_request(
                    &app,
                    &run_id,
                    &pending.request_id,
                    &pending.receipt,
                    request_json,
                )
                .is_ok(),
                None => false,
            };
            Ok(ApiPendingCommandView {
                run_id,
                request_id: pending.request_id,
                command: pending.receipt.command,
                accepted_version: pending.receipt.accepted_version,
                recovery_state: if retryable { "retryable" } else { "blocked" },
            })
        })
        .collect::<std::result::Result<Vec<_>, WebError>>()?;
    Ok(Json(ApiPendingCommandBatch {
        api_version: "gridedge.api/v1",
        commands,
    }))
}

async fn api_retry_pending_command(
    State(app): State<Arc<WebState>>,
    Json(retry): Json<ApiPendingRetryRequest>,
) -> Result<Json<ApiCommandResponse>, WebError> {
    if retry.run_id != sanitize_run_id(&retry.run_id)
        || retry.run_id.is_empty()
        || retry.request_id.trim().is_empty()
    {
        return Err(WebError::validation(
            "invalid pending command retry envelope",
        ));
    }
    Ok(Json(
        retry_stored_pending_command(app, &retry.run_id, &retry.request_id).await?,
    ))
}

async fn retry_stored_pending_command(
    app: Arc<WebState>,
    run_id: &str,
    request_id: &str,
) -> Result<ApiCommandResponse, WebError> {
    let request = {
        let mut store = open_web_store(&app)?;
        if let Some(control) = recover_committed_pending_receipt(&app, &mut store, run_id)? {
            ensure_playback_worker(Arc::clone(&app), run_id.to_owned(), control).await?;
        }
        let receipt = store
            .web_command_receipt(run_id, request_id)?
            .ok_or_else(|| WebError::not_found("pending command not found"))?;
        if let Some(response_json) = receipt.completed_response_json.as_deref() {
            return serde_json::from_str(response_json).map_err(|error| {
                WebError::pending_plan_conflict(format!(
                    "completed pending response is invalid: {error}"
                ))
            });
        }
        let request_json = receipt.request_json.as_deref().ok_or_else(|| {
            WebError::pending_unavailable("legacy pending command has no durable request body")
        })?;
        stored_pending_request(&app, run_id, request_id, &receipt, request_json)?
    };
    dispatch_durable_api_command(app, request).await
}

async fn api_command(
    State(app): State<Arc<WebState>>,
    Json(request): Json<ApiCommandRequest>,
) -> Result<Json<ApiCommandResponse>, WebError> {
    Ok(Json(dispatch_durable_api_command(app, request).await?))
}

async fn dispatch_durable_api_command(
    app: Arc<WebState>,
    request: ApiCommandRequest,
) -> Result<ApiCommandResponse, WebError> {
    validate_api_command_request(&request)?;
    if let Some(response) = completed_command_response(&app, &request).await? {
        return Ok(response);
    }
    if request.command == ApiCommandKind::Pause {
        return execute_durable_api_command(app, request).await;
    }

    let key = (request.run_id.clone(), request.request_id.clone());
    let (task, created) = {
        let mut tasks = app.command_tasks.lock().await;
        if let Some(task) = tasks.get(&key) {
            if task.request != request {
                return Err(WebError::conflict(
                    "request_id was reused with different command content",
                ));
            }
            (Arc::clone(task), false)
        } else {
            let task = Arc::new(ActiveCommandTask::new(request.clone()));
            tasks.insert(key.clone(), Arc::clone(&task));
            app.command_task_count.send_replace(tasks.len());
            (task, true)
        }
    };
    if created {
        let task_app = Arc::clone(&app);
        let task_ref = Arc::clone(&task);
        tokio::spawn(async move {
            let lock = run_lock(&task_app, &request.run_id).await;
            let _guard = lock.lock().await;
            let outcome = match execute_durable_api_command(Arc::clone(&task_app), request).await {
                Ok(response) => CommandTaskOutcome::Completed(response),
                Err(error) => CommandTaskOutcome::Failed(error.into()),
            };
            task_ref.outcome.send_replace(Some(outcome));
            let mut tasks = task_app.command_tasks.lock().await;
            if tasks
                .get(&key)
                .is_some_and(|active| Arc::ptr_eq(active, &task_ref))
            {
                tasks.remove(&key);
            }
            task_app.command_task_count.send_replace(tasks.len());
        });
    }
    task.wait().await
}

fn validate_api_command_request(request: &ApiCommandRequest) -> Result<(), WebError> {
    if request.api_version != "gridedge.api/v1"
        || request.run_id != sanitize_run_id(&request.run_id)
        || request.run_id.is_empty()
        || request.request_id.trim().is_empty()
        || request.request_id.len() > 128
        || request.expected_sequence < 0
        || request.expected_version < 0
    {
        return Err(WebError::validation("invalid API command envelope"));
    }
    Ok(())
}

async fn completed_command_response(
    app: &Arc<WebState>,
    request: &ApiCommandRequest,
) -> Result<Option<ApiCommandResponse>, WebError> {
    let mut store = open_web_store(app)?;
    if let Some(control) = recover_committed_pending_receipt(app, &mut store, &request.run_id)? {
        ensure_playback_worker(Arc::clone(app), request.run_id.clone(), control).await?;
    }
    let Some(receipt) = store.web_command_receipt(&request.run_id, &request.request_id)? else {
        return Ok(None);
    };
    let same_request = if let Some(stored_request_json) = receipt.request_json.as_deref() {
        serde_json::to_string(request).map_err(anyhow::Error::from)? == stored_request_json
    } else {
        receipt.request_sha256 == command_request_sha256(app, request)?
    };
    if !same_request {
        return Err(WebError::conflict(
            "request_id was reused with different command content",
        ));
    }
    let Some(response_json) = receipt.completed_response_json else {
        return Ok(None);
    };
    let response: ApiCommandResponse =
        serde_json::from_str(&response_json).map_err(anyhow::Error::from)?;
    if request.command == ApiCommandKind::Play {
        let control = store.web_playback_control(&request.run_id)?;
        if control.active && control.command_version == receipt.accepted_version {
            ensure_playback_worker(Arc::clone(app), request.run_id.clone(), control).await?;
        }
    }
    Ok(Some(response))
}

async fn execute_durable_api_command(
    app: Arc<WebState>,
    request: ApiCommandRequest,
) -> Result<ApiCommandResponse, WebError> {
    let request_json = serde_json::to_string(&request).map_err(anyhow::Error::from)?;
    let request_sha256 = command_request_sha256(&app, &request)?;
    let (config_sha256, algorithm_sha256) = current_command_runtime_identity(&app)?;
    let mut store = open_web_store(&app)?;
    if let Some(control) = recover_committed_pending_receipt(&app, &mut store, &request.run_id)? {
        ensure_playback_worker(Arc::clone(&app), request.run_id.clone(), control).await?;
    }

    if let Some(receipt) = store.web_command_receipt(&request.run_id, &request.request_id)? {
        if receipt.request_sha256 != request_sha256 {
            return Err(WebError::conflict(
                "request_id was reused with different command content",
            ));
        }
        if let Some(response_json) = receipt.completed_response_json.as_deref() {
            let response: ApiCommandResponse =
                serde_json::from_str(response_json).map_err(anyhow::Error::from)?;
            if request.command == ApiCommandKind::Play {
                let control = store.web_playback_control(&request.run_id)?;
                if control.active && control.command_version == receipt.accepted_version {
                    ensure_playback_worker(Arc::clone(&app), request.run_id.clone(), control)
                        .await?;
                }
            }
            return Ok(response);
        }
        let stored_request_json = receipt.request_json.as_deref().ok_or_else(|| {
            WebError::pending_unavailable("legacy pending command has no durable request body")
        })?;
        stored_pending_request(
            &app,
            &request.run_id,
            &request.request_id,
            &receipt,
            stored_request_json,
        )?;
        return finish_claimed_api_command(app, request, request_sha256, receipt, &mut store).await;
    }

    let control = store.web_playback_control(&request.run_id)?;
    let processed =
        store.event_count_by_type(&request.run_id, crate::event::EventType::MarketBarProcessed)?;
    let target_processed_bars = match request.command {
        ApiCommandKind::Start => {
            if store.latest_sequence(&request.run_id)? != 0 {
                return Err(WebError::conflict("run already exists"));
            }
            0
        }
        ApiCommandKind::Step => {
            let descriptor = validated_command_descriptor(&app, &store, &request.run_id)?;
            processed.saturating_add(1).min(descriptor.total_bars)
        }
        ApiCommandKind::Finish => {
            validated_command_descriptor(&app, &store, &request.run_id)?.total_bars
        }
        ApiCommandKind::Play | ApiCommandKind::Pause => {
            validated_command_descriptor(&app, &store, &request.run_id)?;
            processed
        }
    };
    if request.command != ApiCommandKind::Start {
        let durable_identity = durable_command_runtime_identity(&store, &request.run_id)?
            .ok_or_else(|| WebError::conflict("run has no durable runtime identity"))?;
        if durable_identity != (config_sha256.clone(), algorithm_sha256.clone()) {
            return Err(WebError::conflict(
                "runtime configuration or algorithm differs from the audited run",
            ));
        }
    }
    let interval_ms = if request.command == ApiCommandKind::Play {
        let speed = request.speed_ms.unwrap_or(1_000);
        if !matches!(speed, 250 | 500 | 1_000 | 2_000) {
            return Err(WebError::validation("invalid playback speed"));
        }
        speed
    } else {
        control.interval_ms
    };
    let claim = store
        .claim_web_command(
            &request.run_id,
            &request.request_id,
            &request_sha256,
            api_command_name(request.command),
            request.expected_sequence,
            request.expected_version,
            target_processed_bars,
            request.command != ApiCommandKind::Pause,
            request.command != ApiCommandKind::Pause,
            request.command == ApiCommandKind::Pause,
            request.command == ApiCommandKind::Play,
            interval_ms,
            &request_json,
            &config_sha256,
            &algorithm_sha256,
        )
        .map_err(WebError::conflict_error)?;
    let receipt = match claim {
        WebCommandClaim::Existing(receipt) | WebCommandClaim::Claimed(receipt) => receipt,
    };
    finish_claimed_api_command(app, request, request_sha256, receipt, &mut store).await
}

fn validated_command_descriptor(
    app: &WebState,
    store: &SqliteStore,
    run_id: &str,
) -> Result<ReplayDescriptor, WebError> {
    let descriptor = load_replay_descriptor(store, run_id)
        .map_err(|_| WebError::not_found("replay run not found"))?;
    let dataset = replay_dataset(&app.datasets, &descriptor)
        .map_err(|_| WebError::conflict("bound replay dataset is unavailable"))?;
    validate_descriptor(&descriptor, dataset, &dataset.bars).map_err(WebError::conflict_error)?;
    Ok(descriptor)
}

async fn finish_claimed_api_command(
    app: Arc<WebState>,
    request: ApiCommandRequest,
    request_sha256: String,
    receipt: WebCommandReceipt,
    store: &mut SqliteStore,
) -> Result<ApiCommandResponse, WebError> {
    let message = match request.command {
        ApiCommandKind::Start => {
            if replay_progress_from_store(store, &request.run_id)?.is_none() {
                let dataset = command_start_dataset(&app, request.dataset.as_deref())?;
                let config = app.config.clone();
                let database = Arc::clone(&app.database);
                let run_id = request.run_id.clone();
                tokio::task::spawn_blocking(move || {
                    step_start_sync(config, dataset, run_id, Some(&database))
                })
                .await
                .context("step start task failed")??;
            }
            "step replay created".to_owned()
        }
        ApiCommandKind::Step => {
            let processed = store.event_count_by_type(
                &request.run_id,
                crate::event::EventType::MarketBarProcessed,
            )?;
            if processed < receipt.target_processed_bars {
                if processed + 1 != receipt.target_processed_bars {
                    return Err(WebError::conflict("pending STEP target is inconsistent"));
                }
                let config = app.config.clone();
                let datasets = app.datasets.clone();
                let database = Arc::clone(&app.database);
                let run_id = request.run_id.clone();
                tokio::task::spawn_blocking(move || {
                    step_once_sync(config, datasets, run_id, Some(&database))
                })
                .await
                .context("step task failed")??;
            } else if processed > receipt.target_processed_bars {
                return Err(WebError::conflict("pending STEP was overtaken"));
            }
            let descriptor = load_replay_descriptor(store, &request.run_id)?;
            if receipt.target_processed_bars >= descriptor.total_bars {
                "replay complete".to_owned()
            } else {
                "advanced one bar".to_owned()
            }
        }
        ApiCommandKind::Finish => {
            if !finish_terminal_evidence(store, &request.run_id, receipt.target_processed_bars)? {
                let config = app.config.clone();
                let datasets = app.datasets.clone();
                let database = Arc::clone(&app.database);
                let run_id = request.run_id.clone();
                tokio::task::spawn_blocking(move || {
                    step_finish_sync(config, datasets, run_id, Some(&database))
                })
                .await
                .context("finish task failed")??;
            }
            if !finish_terminal_evidence(store, &request.run_id, receipt.target_processed_bars)? {
                return Err(WebError::conflict(
                    "pending FINISH lacks durable terminal evidence",
                ));
            }
            "replay complete".to_owned()
        }
        ApiCommandKind::Play => {
            let control = store.web_playback_control(&request.run_id)?;
            if control.command_version != receipt.accepted_version || !control.active {
                return Err(WebError::conflict(
                    "pending PLAY generation is no longer active",
                ));
            }
            "automatic playback started".to_owned()
        }
        ApiCommandKind::Pause => {
            {
                let playbacks = app.playbacks.lock().await;
                if let Some(control) = playbacks.get(&request.run_id) {
                    if control.generation == receipt.expected_version {
                        control.cancelled.store(true, Ordering::Release);
                    }
                }
            }
            let lock = run_lock(&app, &request.run_id).await;
            let _guard = lock.lock().await;
            "playback pause requested".to_owned()
        }
    };
    let accepted_sequence = store.latest_sequence(&request.run_id)?;
    let response = ApiCommandResponse {
        api_version: "gridedge.api/v1".to_owned(),
        request_id: request.request_id.clone(),
        command: request.command,
        run_id: request.run_id.clone(),
        accepted: true,
        message,
        accepted_sequence,
        accepted_version: receipt.accepted_version,
    };
    let durable_response_json = store.complete_web_command(
        &request.run_id,
        &request.request_id,
        &request_sha256,
        &serde_json::to_string(&response).map_err(anyhow::Error::from)?,
    )?;
    let response: ApiCommandResponse =
        serde_json::from_str(&durable_response_json).map_err(anyhow::Error::from)?;
    if request.command == ApiCommandKind::Play {
        let control = store.web_playback_control(&request.run_id)?;
        ensure_playback_worker(Arc::clone(&app), request.run_id.clone(), control).await?;
    }
    Ok(response)
}

fn recover_committed_pending_receipt(
    _app: &WebState,
    store: &mut SqliteStore,
    run_id: &str,
) -> Result<Option<WebPlaybackControl>, WebError> {
    let Some(PendingWebCommand {
        request_id,
        receipt,
    }) = store.pending_web_command(run_id)?
    else {
        return Ok(None);
    };
    let Some(request_json) = receipt.request_json.as_deref() else {
        return Ok(None);
    };
    let Ok(request) = parse_pending_request(run_id, &request_id, &receipt, request_json) else {
        return Ok(None);
    };
    let command = match receipt.command.as_str() {
        "start" => ApiCommandKind::Start,
        "step" => ApiCommandKind::Step,
        "finish" => ApiCommandKind::Finish,
        "play" => ApiCommandKind::Play,
        "pause" => ApiCommandKind::Pause,
        _ => return Err(WebError::conflict("pending command kind is invalid")),
    };
    let progress = replay_progress_from_store(store, run_id)?;
    let control = store.web_playback_control(run_id)?;
    let (committed, message) = match command {
        ApiCommandKind::Start => (progress.is_some(), "step replay created"),
        ApiCommandKind::Step => {
            let Some(progress) = progress.as_ref() else {
                return Ok(None);
            };
            (
                progress.processed_bars == receipt.target_processed_bars,
                if receipt.target_processed_bars >= progress.descriptor.total_bars {
                    "replay complete"
                } else {
                    "advanced one bar"
                },
            )
        }
        ApiCommandKind::Finish => {
            let committed = progress
                .as_ref()
                .is_some_and(|progress| progress.processed_bars == receipt.target_processed_bars)
                && finish_terminal_evidence(store, run_id, receipt.target_processed_bars)?;
            (committed, "replay complete")
        }
        ApiCommandKind::Play => (
            control.active && control.command_version == receipt.accepted_version,
            "automatic playback started",
        ),
        ApiCommandKind::Pause => (
            !control.active && control.command_version == receipt.accepted_version,
            "playback pause requested",
        ),
    };
    if !committed {
        return Ok(None);
    }
    if validate_committed_pending_request(store, run_id, &receipt, &request).is_err() {
        return Ok(None);
    }
    let response = ApiCommandResponse {
        api_version: "gridedge.api/v1".to_owned(),
        request_id: request_id.clone(),
        command,
        run_id: run_id.to_owned(),
        accepted: true,
        message: message.to_owned(),
        accepted_sequence: if command == ApiCommandKind::Play {
            receipt.expected_sequence
        } else {
            store.latest_sequence(run_id)?
        },
        accepted_version: receipt.accepted_version,
    };
    let durable_response_json = store.complete_web_command(
        run_id,
        &request_id,
        &receipt.request_sha256,
        &serde_json::to_string(&response).map_err(anyhow::Error::from)?,
    )?;
    let durable_response: ApiCommandResponse = serde_json::from_str(&durable_response_json)
        .map_err(|error| WebError::pending_plan_conflict(error.to_string()))?;
    if durable_response.request_id != request_id
        || durable_response.run_id != run_id
        || durable_response.command != command
        || durable_response.accepted_version != receipt.accepted_version
    {
        return Err(WebError::pending_plan_conflict(
            "completed command response differs from its durable plan",
        ));
    }
    Ok((command == ApiCommandKind::Play).then_some(control))
}

fn command_start_dataset(
    app: &WebState,
    requested: Option<&str>,
) -> Result<DatasetOption, WebError> {
    let id = requested.unwrap_or(&app.default_dataset);
    app.datasets
        .iter()
        .find(|dataset| dataset.id == id)
        .cloned()
        .ok_or_else(|| WebError::validation("requested dataset is unavailable"))
}

fn command_request_sha256(app: &WebState, request: &ApiCommandRequest) -> Result<String, WebError> {
    Ok(hex::encode(Sha256::digest(
        command_request_plan_json(app, request)?.as_bytes(),
    )))
}

fn command_request_plan_json(
    app: &WebState,
    request: &ApiCommandRequest,
) -> Result<String, WebError> {
    if request.command == ApiCommandKind::Start {
        let dataset = command_start_dataset(app, request.dataset.as_deref())?;
        return command_request_plan_json_with_dataset(
            request,
            &dataset.id,
            &dataset.sha256,
            dataset.bars.len(),
            dataset.bars.first().map(|bar| bar.timestamp),
            dataset.bars.last().map(|bar| bar.timestamp),
        );
    }
    let bytes = serde_json::to_vec(request).map_err(anyhow::Error::from)?;
    String::from_utf8(bytes).map_err(|error| WebError::from(anyhow::Error::from(error)))
}

fn command_request_plan_json_with_dataset(
    request: &ApiCommandRequest,
    dataset_id: &str,
    data_sha256: &str,
    total_bars: usize,
    first_timestamp: Option<chrono::NaiveDateTime>,
    last_timestamp: Option<chrono::NaiveDateTime>,
) -> Result<String, WebError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "request": request,
        "dataset_plan": {
            "dataset_id": dataset_id,
            "data_sha256": data_sha256,
            "total_bars": total_bars,
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
        }
    }))
    .map_err(anyhow::Error::from)?;
    String::from_utf8(bytes).map_err(|error| WebError::from(anyhow::Error::from(error)))
}

fn algorithm_manifest_sha256(manifest: &AlgorithmManifest) -> Result<String, WebError> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(manifest).map_err(anyhow::Error::from)?,
    )))
}

fn current_command_runtime_identity(app: &WebState) -> Result<(String, String), WebError> {
    let config_sha256 = app.config.content_sha256()?;
    let algorithm = algorithm_from_config(&app.config)?;
    let algorithm_sha256 = algorithm_manifest_sha256(&algorithm.manifest())?;
    Ok((config_sha256, algorithm_sha256))
}

fn durable_command_runtime_identity(
    store: &SqliteStore,
    run_id: &str,
) -> Result<Option<(String, String)>, WebError> {
    let Some(context) = crate::run_context::RunContext::load(store, run_id)
        .map_err(|error| WebError::pending_plan_conflict(error.to_string()))?
    else {
        return Ok(None);
    };
    let config_sha256 = context
        .config
        .content_sha256()
        .map_err(|error| WebError::pending_plan_conflict(error.to_string()))?;
    let Some(manifest) = context.algorithm_manifest else {
        return Ok(None);
    };
    Ok(Some((config_sha256, algorithm_manifest_sha256(&manifest)?)))
}

fn parse_pending_request(
    run_id: &str,
    request_id: &str,
    receipt: &WebCommandReceipt,
    request_json: &str,
) -> Result<ApiCommandRequest, WebError> {
    let config_sha256 = receipt.config_sha256.as_deref().ok_or_else(|| {
        WebError::pending_unavailable("legacy pending command has no configuration identity")
    })?;
    let algorithm_sha256 = receipt.algorithm_sha256.as_deref().ok_or_else(|| {
        WebError::pending_unavailable("legacy pending command has no algorithm identity")
    })?;
    let expected_plan_sha256 = web_command_plan_sha256(
        &receipt.request_sha256,
        &receipt.command,
        receipt.expected_sequence,
        receipt.expected_version,
        receipt.accepted_version,
        receipt.target_processed_bars,
        (config_sha256, algorithm_sha256),
    );
    if receipt.plan_sha256.as_deref() != Some(&expected_plan_sha256) {
        return Err(WebError::pending_plan_conflict(
            "pending command plan does not match its content hash",
        ));
    }
    let request: ApiCommandRequest = serde_json::from_str(request_json)
        .map_err(|error| WebError::pending_plan_conflict(error.to_string()))?;
    if request.run_id != run_id || request.request_id != request_id {
        return Err(WebError::pending_plan_conflict(
            "pending command request identity does not match its inbox row",
        ));
    }
    if receipt.command != api_command_name(request.command)
        || receipt.expected_sequence != request.expected_sequence
        || receipt.expected_version != request.expected_version
        || receipt.expected_version.checked_add(1) != Some(receipt.accepted_version)
    {
        return Err(WebError::pending_plan_conflict(
            "pending command coordinates do not match its stored request",
        ));
    }
    validate_api_command_request(&request)
        .map_err(|error| WebError::pending_plan_conflict(error.error.to_string()))?;
    let canonical_json = serde_json::to_string(&request).map_err(anyhow::Error::from)?;
    if canonical_json != request_json {
        return Err(WebError::pending_plan_conflict(
            "pending command request body is not canonical",
        ));
    }
    Ok(request)
}

fn validate_committed_pending_request(
    store: &SqliteStore,
    run_id: &str,
    receipt: &WebCommandReceipt,
    request: &ApiCommandRequest,
) -> Result<(), WebError> {
    let expected_request_sha256 = if request.command == ApiCommandKind::Start {
        let descriptor = load_replay_descriptor(store, run_id)
            .map_err(|error| WebError::pending_plan_conflict(error.to_string()))?;
        hex::encode(Sha256::digest(
            command_request_plan_json_with_dataset(
                request,
                &descriptor.dataset_id,
                &descriptor.data_sha256,
                descriptor.total_bars,
                Some(descriptor.first_timestamp),
                Some(descriptor.last_timestamp),
            )?
            .as_bytes(),
        ))
    } else {
        hex::encode(Sha256::digest(
            serde_json::to_vec(request).map_err(anyhow::Error::from)?,
        ))
    };
    let durable_identity = durable_command_runtime_identity(store, run_id)?.ok_or_else(|| {
        WebError::pending_plan_conflict("committed command lacks durable runtime identity")
    })?;
    if expected_request_sha256 != receipt.request_sha256
        || receipt.config_sha256.as_deref() != Some(durable_identity.0.as_str())
        || receipt.algorithm_sha256.as_deref() != Some(durable_identity.1.as_str())
    {
        return Err(WebError::pending_plan_conflict(
            "committed command differs from its durable request plan",
        ));
    }
    Ok(())
}

fn stored_pending_request(
    app: &WebState,
    run_id: &str,
    request_id: &str,
    receipt: &WebCommandReceipt,
    request_json: &str,
) -> Result<ApiCommandRequest, WebError> {
    let request = parse_pending_request(run_id, request_id, receipt, request_json)?;
    let current_request_sha256 = command_request_sha256(app, &request)
        .map_err(|error| WebError::pending_plan_conflict(error.error.to_string()))?;
    let current_identity = current_command_runtime_identity(app)
        .map_err(|error| WebError::pending_plan_conflict(error.error.to_string()))?;
    if current_request_sha256 != receipt.request_sha256
        || receipt.config_sha256.as_deref() != Some(current_identity.0.as_str())
        || receipt.algorithm_sha256.as_deref() != Some(current_identity.1.as_str())
    {
        return Err(WebError::pending_plan_conflict(
            "pending command dataset, configuration, or algorithm identity changed",
        ));
    }
    Ok(request)
}

fn api_command_name(command: ApiCommandKind) -> &'static str {
    match command {
        ApiCommandKind::Start => "start",
        ApiCommandKind::Step => "step",
        ApiCommandKind::Play => "play",
        ApiCommandKind::Pause => "pause",
        ApiCommandKind::Finish => "finish",
    }
}

async fn ensure_playback_worker(
    app: Arc<WebState>,
    run_id: String,
    durable: WebPlaybackControl,
) -> Result<(), WebError> {
    if !durable.active {
        return Ok(());
    }
    let store = open_web_store(&app)?;
    if store.web_playback_control(&run_id)? != durable
        || !store.completed_playback_generation(&run_id, durable.command_version)?
    {
        return Ok(());
    }
    let control = {
        let mut playbacks = app.playbacks.lock().await;
        if let Some(existing) = playbacks.get(&run_id) {
            if existing.generation == durable.command_version
                && !existing.cancelled.load(Ordering::Acquire)
            {
                return Ok(());
            }
            existing.cancelled.store(true, Ordering::Release);
        }
        let control = Arc::new(PlaybackControl {
            cancelled: AtomicBool::new(false),
            interval_ms: durable.interval_ms,
            generation: durable.command_version,
        });
        playbacks.insert(run_id.clone(), Arc::clone(&control));
        control
    };

    let app_for_task = Arc::clone(&app);
    tokio::spawn(async move {
        let lock = run_lock(&app_for_task, &run_id).await;
        let _guard = lock.lock().await;
        let config = app_for_task.config.clone();
        let datasets = app_for_task.datasets.clone();
        let worker_control = Arc::clone(&control);
        let database = Arc::clone(&app_for_task.database);
        let worker_run_id = run_id.clone();
        let result = tokio::task::spawn_blocking(move || {
            step_play_sync(
                config,
                datasets,
                worker_run_id,
                worker_control.interval_ms,
                Some(worker_control.generation),
                &worker_control.cancelled,
                Some(&database),
            )
        })
        .await;
        if !matches!(result, Ok(Ok(_))) {
            eprintln!("automatic API playback failed for {run_id}");
        }
        if let Ok(mut store) = app_for_task.database.open_store() {
            let _ = store.deactivate_web_playback_if_version(&run_id, control.generation);
        }
        let mut playbacks = app_for_task.playbacks.lock().await;
        if playbacks
            .get(&run_id)
            .is_some_and(|current| Arc::ptr_eq(current, &control))
        {
            playbacks.remove(&run_id);
        }
    });
    Ok(())
}

async fn security_boundary(
    State(app): State<Arc<WebState>>,
    request: Request,
    next: Next,
) -> Response {
    let is_api = request.uri().path().starts_with("/api/v1/");
    let api_authorized = is_api
        && request
            .headers()
            .get("x-gridedge-api-token")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == app.api_token);
    if (is_api && !api_authorized)
        || (!is_api
            && !request_headers_are_trusted(request.method(), request.headers(), &app.allowed_host))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !matches!(request.uri().path(), "/health" | "/ready") {
        if let Err(error) = app.database.verify_identity() {
            app.database.mark_not_ready();
            eprintln!("Web database identity check failed: {error:#}");
            return WebError::database_unavailable(error).into_response();
        }
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; style-src 'unsafe-inline'; img-src 'self' data:; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(cookie) = HeaderValue::from_str(&format!(
        "gridedge_csrf={}; Path=/; HttpOnly; SameSite=Strict",
        app.csrf_token
    )) {
        headers.insert(header::SET_COOKIE, cookie);
    }
    response
}

fn request_headers_are_trusted(
    method: &Method,
    headers: &axum::http::HeaderMap,
    allowed_host: &str,
) -> bool {
    if headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        != Some(allowed_host)
    {
        return false;
    }
    if *method == Method::GET || *method == Method::HEAD {
        return true;
    }
    let expected_origin = format!("http://{allowed_host}");
    if headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        != Some(expected_origin.as_str())
    {
        return false;
    }
    headers
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| matches!(value, "same-origin" | "none"))
}

fn csrf_is_valid(app: &WebState, token: &str) -> bool {
    !token.is_empty() && token.as_bytes() == app.csrf_token.as_bytes()
}

async fn run_lock(app: &WebState, run_id: &str) -> Arc<Mutex<()>> {
    let mut locks = app.run_locks.lock().await;
    Arc::clone(
        locks
            .entry(run_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

async fn snapshot_lock(app: &WebState, run_id: &str) -> Arc<Mutex<()>> {
    let mut locks = app.snapshot_locks.lock().await;
    Arc::clone(
        locks
            .entry(run_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

async fn dashboard(
    State(app): State<Arc<WebState>>,
    Query(query): Query<DashboardQuery>,
) -> std::result::Result<Html<String>, WebError> {
    let config = app.config.clone();
    let mut store = open_web_store(&app)?;
    for (run_id, _) in store.pending_web_commands(1_000)? {
        if let Some(control) = recover_committed_pending_receipt(&app, &mut store, &run_id)? {
            ensure_playback_worker(Arc::clone(&app), run_id, control).await?;
        }
    }
    let runs = store.run_ids()?;
    let selected = query
        .run_id
        .clone()
        .filter(|id| runs.contains(id))
        .or_else(|| runs.first().cloned());
    if let Some(run_id) = selected.as_ref() {
        recover_committed_pending_receipt(&app, &mut store, run_id)?;
    }
    let selected_config = selected
        .as_ref()
        .map(|run_id| crate::run_context::RunContext::load(&store, run_id))
        .transpose()?
        .flatten()
        .map(|context| context.config)
        .unwrap_or_else(|| config.clone());
    let state = selected
        .as_ref()
        .map(|run_id| store.rebuild(initial_state(&selected_config, run_id)))
        .transpose()?
        .map(|(state, _)| state);
    let events = selected
        .as_ref()
        .map(|run_id| store.load_after(run_id, 0))
        .transpose()?
        .unwrap_or_default();
    let progress = replay_progress_from_events(&events)?;
    let selected_dataset = progress
        .as_ref()
        .map(|progress| progress.descriptor.dataset_id.clone())
        .filter(|id| app.datasets.iter().any(|dataset| dataset.id == *id))
        .unwrap_or_else(|| selected_dataset_id(&app, query.dataset.as_deref()));
    let bars = if selected.is_some() && progress.is_none() {
        Vec::new()
    } else {
        app.datasets
            .iter()
            .find(|dataset| dataset.id == selected_dataset)
            .map(|dataset| dataset.bars.as_ref().clone())
            .unwrap_or_default()
    };
    if let Some(progress) = progress.as_ref() {
        validate_replay_dataset(&app, progress, &bars)?;
    }
    let playback = if let Some(run_id) = selected.as_ref() {
        let control = store.web_playback_control(run_id)?;
        if control.active {
            ensure_playback_worker(Arc::clone(&app), run_id.clone(), control).await?;
        }
        PlaybackView {
            active: control.active,
            interval_ms: control.interval_ms,
            command_version: control.command_version,
        }
    } else {
        PlaybackView::default()
    };
    let pending_commands = store
        .pending_web_commands(1_000)?
        .into_iter()
        .map(|(run_id, pending)| {
            let retryable = pending.receipt.request_json.as_deref().is_some_and(|json| {
                stored_pending_request(&app, &run_id, &pending.request_id, &pending.receipt, json)
                    .is_ok()
            });
            ApiPendingCommandView {
                run_id,
                request_id: pending.request_id,
                command: pending.receipt.command,
                accepted_version: pending.receipt.accepted_version,
                recovery_state: if retryable { "retryable" } else { "blocked" },
            }
        })
        .collect::<Vec<_>>();
    Ok(Html(render_dashboard(
        &selected_config,
        &runs,
        state.as_ref(),
        &events,
        &bars,
        &query,
        &app.datasets,
        &selected_dataset,
        progress.as_ref(),
        playback,
        &pending_commands,
        &app.csrf_token,
    )))
}

fn replay_progress_from_events(
    events: &[crate::event::EventEnvelope],
) -> Result<Option<ReplayProgress>> {
    let Some(event) = events
        .iter()
        .find(|event| event.event_type == crate::event::EventType::ReplayInitialized)
    else {
        return Ok(None);
    };
    let descriptor: ReplayDescriptor = serde_json::from_value(event.payload.clone())?;
    let processed_bars = events
        .iter()
        .filter(|event| event.event_type == crate::event::EventType::MarketBarProcessed)
        .count();
    let processed_bars = if processed_bars == 0 {
        events
            .iter()
            .filter(|event| {
                event.event_type == crate::event::EventType::MarketDataReceived
                    && event.schema_version == 1
            })
            .count()
    } else {
        processed_bars
    };
    Ok(Some(ReplayProgress {
        descriptor,
        processed_bars,
    }))
}

fn validate_replay_dataset(
    app: &WebState,
    progress: &ReplayProgress,
    bars: &[MarketBar],
) -> Result<()> {
    let dataset = app
        .datasets
        .iter()
        .find(|dataset| dataset.id == progress.descriptor.dataset_id)
        .context("replay dataset is no longer available")?;
    if dataset.sha256 != progress.descriptor.data_sha256 {
        bail!("replay dataset checksum changed; refusing unsafe continuation")
    }
    if bars.len() != progress.descriptor.total_bars
        || bars.first().map(|bar| bar.timestamp) != Some(progress.descriptor.first_timestamp)
        || bars.last().map(|bar| bar.timestamp) != Some(progress.descriptor.last_timestamp)
    {
        bail!("replay dataset boundaries changed; refusing unsafe continuation")
    }
    Ok(())
}

async fn replay_action(State(app): State<Arc<WebState>>, Form(form): Form<RunForm>) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    if run_id.is_empty() {
        return Redirect::to("/?notice=invalid-run-id").into_response();
    }
    let dataset = form.dataset.trim().to_owned();
    let requested_dataset = (!dataset.is_empty()).then(|| dataset.clone());
    let default_dataset = app.default_dataset.clone();
    let start = form_api_request(
        &form,
        ApiCommandKind::Start,
        requested_dataset.clone(),
        None,
    );
    let start_result = dispatch_durable_api_command(Arc::clone(&app), start).await;
    let notice = match start_result {
        Ok(started) => {
            let finish = ApiCommandRequest {
                api_version: "gridedge.api/v1".to_owned(),
                request_id: format!("{}-finish", form.request_id),
                command: ApiCommandKind::Finish,
                run_id: run_id.clone(),
                dataset: None,
                speed_ms: None,
                expected_sequence: started.accepted_sequence,
                expected_version: started.accepted_version,
            };
            match dispatch_durable_api_command(app, finish).await {
                Ok(_) => "replay-complete",
                Err(_) => "replay-failed",
            }
        }
        Err(_) => "replay-failed",
    };
    Redirect::to(&format!(
        "/?run_id={run_id}&notice={notice}&dataset={}",
        requested_dataset.as_deref().unwrap_or(&default_dataset)
    ))
    .into_response()
}

async fn step_start_action(
    State(app): State<Arc<WebState>>,
    Form(form): Form<RunForm>,
) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    if run_id.is_empty() {
        return Redirect::to("/?notice=invalid-run-id").into_response();
    }
    let dataset_id = form.dataset.trim().to_owned();
    let requested_dataset = (!dataset_id.is_empty()).then(|| dataset_id.clone());
    let request = form_api_request(
        &form,
        ApiCommandKind::Start,
        requested_dataset.clone(),
        None,
    );
    let default_dataset = app.default_dataset.clone();
    let notice = match dispatch_durable_api_command(app, request).await {
        Ok(_) => "step-ready",
        Err(_) => "step-failed",
    };
    Redirect::to(&format!(
        "/?run_id={run_id}&notice={notice}&dataset={}",
        requested_dataset.as_deref().unwrap_or(&default_dataset)
    ))
    .into_response()
}

async fn step_next_action(State(app): State<Arc<WebState>>, Form(form): Form<RunForm>) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let request = form_api_request(&form, ApiCommandKind::Step, None, None);
    let notice = match dispatch_durable_api_command(app, request).await {
        Ok(response) if response.message == "replay complete" => "step-complete",
        Ok(_) => "step-advanced",
        Err(_) => "step-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn step_play_action(
    State(app): State<Arc<WebState>>,
    Form(form): Form<PlaybackForm>,
) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    if run_id.is_empty() || !matches!(form.speed_ms, 250 | 500 | 1000 | 2000) {
        return Redirect::to("/?notice=invalid-playback").into_response();
    }

    let request = ApiCommandRequest {
        api_version: "gridedge.api/v1".to_owned(),
        request_id: form.request_id,
        command: ApiCommandKind::Play,
        run_id: run_id.clone(),
        dataset: None,
        speed_ms: Some(form.speed_ms),
        expected_sequence: form.expected_sequence,
        expected_version: form.expected_version,
    };
    let notice = match dispatch_durable_api_command(app, request).await {
        Ok(_) => "playback-started",
        Err(_) => "invalid-playback",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn step_pause_action(
    State(app): State<Arc<WebState>>,
    Form(form): Form<RunForm>,
) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let request = form_api_request(&form, ApiCommandKind::Pause, None, None);
    let notice = match dispatch_durable_api_command(app, request).await {
        Ok(_) => "playback-paused",
        Err(_) => "playback-not-running",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn step_finish_action(
    State(app): State<Arc<WebState>>,
    Form(form): Form<RunForm>,
) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let request = form_api_request(&form, ApiCommandKind::Finish, None, None);
    let notice = match dispatch_durable_api_command(app, request).await {
        Ok(_) => "step-complete",
        Err(_) => "step-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn retry_pending_action(
    State(app): State<Arc<WebState>>,
    Form(form): Form<PendingRetryForm>,
) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    if run_id.is_empty() || form.request_id.trim().is_empty() {
        return Redirect::to("/?notice=invalid-pending-command").into_response();
    }
    let notice = match retry_stored_pending_command(app, &run_id, &form.request_id).await {
        Ok(_) => "pending-command-recovered",
        Err(_) => "pending-command-recovery-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

fn form_api_request(
    form: &RunForm,
    command: ApiCommandKind,
    dataset: Option<String>,
    speed_ms: Option<u64>,
) -> ApiCommandRequest {
    ApiCommandRequest {
        api_version: "gridedge.api/v1".to_owned(),
        request_id: form.request_id.clone(),
        command,
        run_id: sanitize_run_id(&form.run_id),
        dataset,
        speed_ms,
        expected_sequence: form.expected_sequence,
        expected_version: form.expected_version,
    }
}

async fn rebuild_action(State(app): State<Arc<WebState>>, Form(form): Form<RunForm>) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let lock = run_lock(&app, &run_id).await;
    let _guard = lock.lock().await;
    let config = app.config.clone();
    let database = Arc::clone(&app.database);
    let run_for_task = run_id.clone();
    let result =
        tokio::task::spawn_blocking(move || rebuild_sync(config, run_for_task, Some(&database)))
            .await;
    let notice = match result {
        Ok(Ok(())) => "rebuild-verified",
        Ok(Err(_)) | Err(_) => "rebuild-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn reconcile_action(State(app): State<Arc<WebState>>, Form(form): Form<RunForm>) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let lock = run_lock(&app, &run_id).await;
    let _guard = lock.lock().await;
    let config = app.config.clone();
    let database = Arc::clone(&app.database);
    let run_for_task = run_id.clone();
    let result =
        tokio::task::spawn_blocking(move || reconcile_sync(config, run_for_task, Some(&database)))
            .await;
    let notice = match result {
        Ok(Ok(true)) => "reconciliation-matched",
        Ok(Ok(false)) => "reconciliation-difference",
        Ok(Err(_)) | Err(_) => "reconciliation-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

async fn resume_action(State(app): State<Arc<WebState>>, Form(form): Form<ResumeForm>) -> Response {
    if !csrf_is_valid(&app, &form.csrf_token) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let run_id = sanitize_run_id(&form.run_id);
    let reason = form.reason.trim().to_owned();
    let lock = run_lock(&app, &run_id).await;
    let _guard = lock.lock().await;
    let config = app.config.clone();
    let database = Arc::clone(&app.database);
    let run_for_task = run_id.clone();
    let result = tokio::task::spawn_blocking(move || {
        resume_sync(config, run_for_task, reason, Some(&database))
    })
    .await;
    let notice = match result {
        Ok(Ok(())) => "operator-resumed",
        Ok(Err(_)) | Err(_) => "operator-resume-failed",
    };
    Redirect::to(&format!("/?run_id={run_id}&notice={notice}")).into_response()
}

fn open_runtime_or_test_store(
    config: &Config,
    database: Option<&WebDatabaseGuard>,
) -> Result<SqliteStore> {
    if let Some(database) = database {
        return database.open_store();
    }
    let mut store = SqliteStore::open(&config.database)?;
    store.migrate()?;
    Ok(store)
}

fn step_start_sync(
    config: Config,
    dataset: DatasetOption,
    run_id: String,
    database: Option<&WebDatabaseGuard>,
) -> Result<()> {
    let store = open_runtime_or_test_store(&config, database)?;
    if store.run_ids()?.contains(&run_id) {
        bail!("run already exists")
    }
    let first_timestamp = dataset
        .bars
        .first()
        .context("dataset contains no bars")?
        .timestamp;
    let last_timestamp = dataset
        .bars
        .last()
        .context("dataset contains no bars")?
        .timestamp;
    let descriptor = ReplayDescriptor {
        dataset_id: dataset.id,
        data_sha256: dataset.sha256,
        symbol: config.symbol.clone(),
        total_bars: dataset.bars.len(),
        first_timestamp,
        last_timestamp,
    };
    let algorithm = algorithm_from_config(&config)?;
    GridAutomationService::start_new_replay_with_algorithm(
        config,
        store,
        algorithm,
        Some(run_id),
        &descriptor,
    )?;
    Ok(())
}

fn step_once_sync(
    config: Config,
    datasets: Vec<DatasetOption>,
    run_id: String,
    database: Option<&WebDatabaseGuard>,
) -> Result<bool> {
    let mut store = open_runtime_or_test_store(&config, database)?;
    backfill_legacy_bar_completion(&config, &mut store, &run_id, database)?;
    let descriptor = load_replay_descriptor(&store, &run_id)?;
    let dataset = replay_dataset(&datasets, &descriptor)?;
    validate_descriptor(&descriptor, dataset, &dataset.bars)?;
    let processed =
        store.event_count_by_type(&run_id, crate::event::EventType::MarketBarProcessed)?;
    if processed >= descriptor.total_bars {
        return Ok(true);
    }
    let bar = dataset
        .bars
        .get(processed)
        .cloned()
        .context("replay cursor exceeds dataset")?;
    let algorithm = algorithm_from_config(&config)?;
    let mut service =
        GridAutomationService::recover_with_algorithm(config, store, algorithm, run_id)?;
    service.on_bar(&bar)?;
    let complete = processed + 1 == descriptor.total_bars;
    if complete {
        service.stop()?;
    } else {
        service.save_snapshot()?;
    }
    Ok(complete)
}

fn step_finish_sync(
    config: Config,
    datasets: Vec<DatasetOption>,
    run_id: String,
    database: Option<&WebDatabaseGuard>,
) -> Result<()> {
    let mut store = open_runtime_or_test_store(&config, database)?;
    backfill_legacy_bar_completion(&config, &mut store, &run_id, database)?;
    let descriptor = load_replay_descriptor(&store, &run_id)?;
    let dataset = replay_dataset(&datasets, &descriptor)?;
    validate_descriptor(&descriptor, dataset, &dataset.bars)?;
    let processed =
        store.event_count_by_type(&run_id, crate::event::EventType::MarketBarProcessed)?;
    if processed > descriptor.total_bars {
        bail!("FINISH processed-bar count exceeds the replay descriptor")
    }
    if finish_terminal_evidence(&store, &run_id, descriptor.total_bars)? {
        return Ok(());
    }
    let algorithm = algorithm_from_config(&config)?;
    let mut service =
        GridAutomationService::recover_with_algorithm(config, store, algorithm, run_id)?;
    for bar in &dataset.bars[processed..] {
        if let Some(database) = database {
            database.probe_identity()?;
        }
        service.on_bar(bar)?;
    }
    service.stop()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackResult {
    Paused,
    Complete,
}

fn step_play_sync(
    config: Config,
    datasets: Vec<DatasetOption>,
    run_id: String,
    interval_ms: u64,
    generation: Option<i64>,
    cancelled: &AtomicBool,
    database: Option<&WebDatabaseGuard>,
) -> Result<PlaybackResult> {
    let mut store = open_runtime_or_test_store(&config, database)?;
    backfill_legacy_bar_completion(&config, &mut store, &run_id, database)?;
    let descriptor = load_replay_descriptor(&store, &run_id)?;
    let dataset = replay_dataset(&datasets, &descriptor)?;
    validate_descriptor(&descriptor, dataset, &dataset.bars)?;
    let processed =
        store.event_count_by_type(&run_id, crate::event::EventType::MarketBarProcessed)?;
    if processed >= descriptor.total_bars {
        return Ok(PlaybackResult::Complete);
    }
    if cancelled.load(Ordering::Acquire) {
        return Ok(PlaybackResult::Paused);
    }

    let algorithm = algorithm_from_config(&config)?;
    let mut service =
        GridAutomationService::recover_with_algorithm(config, store, algorithm, run_id)?;
    let control_store = if let Some(database) = database {
        database.open_store()?
    } else {
        let mut store = SqliteStore::open(&service.config.database)?;
        store.migrate()?;
        store
    };
    for (offset, bar) in dataset.bars[processed..].iter().enumerate() {
        if let Some(database) = database {
            // The worker's long-lived SQLite connection still points at the
            // same inode after an in-place overwrite. Re-open the configured
            // path and verify its durable instance UUID before every bar.
            database.probe_identity()?;
        }
        let durable_generation_changed = if let Some(generation) = generation {
            let durable = control_store.web_playback_control(&service.state.run_id)?;
            !durable.active || durable.command_version != generation
        } else {
            false
        };
        if cancelled.load(Ordering::Acquire) || durable_generation_changed {
            return Ok(PlaybackResult::Paused);
        }
        service.on_bar(bar)?;
        let complete = processed + offset + 1 == descriptor.total_bars;
        if complete {
            service.stop()?;
            return Ok(PlaybackResult::Complete);
        }
        service.save_snapshot()?;
        if interruptible_wait(interval_ms, cancelled) {
            return Ok(PlaybackResult::Paused);
        }
    }
    Ok(PlaybackResult::Complete)
}

fn backfill_legacy_bar_completion(
    config: &Config,
    store: &mut SqliteStore,
    run_id: &str,
    database: Option<&WebDatabaseGuard>,
) -> Result<()> {
    let events = store.load_after(run_id, 0)?;
    let (mut state, mut last_sequence) = store.rebuild(initial_state(config, run_id))?;
    for market in events.iter().filter(|event| {
        event.event_type == crate::event::EventType::MarketDataReceived && event.schema_version == 1
    }) {
        if let Some(database) = database {
            database.probe_identity()?;
        }
        let bar: MarketBar = serde_json::from_value(market.payload.clone())?;
        let processed_key = format!("bar-processed:{}:{}", bar.symbol, bar.timestamp);
        if store.has_idempotency_key(run_id, &processed_key)? {
            continue;
        }
        let correlation = format!("market:{}:{}", bar.symbol, bar.timestamp);
        let mut migration_events = vec![
            crate::event::EventEnvelope::new(
                crate::event::EventType::MarketBarDecisionsCommitted,
                run_id,
                &market.cycle_id,
                &bar.symbol,
                bar.timestamp,
                &correlation,
                Some(market.event_id.clone()),
                format!("bar-decisions:{}:{}", bar.symbol, bar.timestamp),
                serde_json::json!({
                    "symbol": bar.symbol,
                    "timestamp": bar.timestamp,
                    "legacy_schema_migration": true
                }),
                &config.config_version,
            ),
            crate::event::EventEnvelope::new(
                crate::event::EventType::MarketBarProcessed,
                run_id,
                &market.cycle_id,
                &bar.symbol,
                bar.timestamp,
                &correlation,
                Some(market.event_id.clone()),
                processed_key,
                serde_json::json!({
                    "symbol": bar.symbol,
                    "timestamp": bar.timestamp,
                    "close": bar.close.to_string(),
                    "legacy_schema_migration": true
                }),
                &config.config_version,
            ),
        ];
        crate::ledger::LedgerWriter::new(store, &mut state, &mut last_sequence)
            .append_batch(&mut migration_events)?;
    }
    Ok(())
}

fn interruptible_wait(interval_ms: u64, cancelled: &AtomicBool) -> bool {
    let mut remaining = interval_ms;
    while remaining > 0 {
        if cancelled.load(Ordering::Acquire) {
            return true;
        }
        let slice = remaining.min(25);
        std::thread::sleep(Duration::from_millis(slice));
        remaining -= slice;
    }
    cancelled.load(Ordering::Acquire)
}

fn load_replay_descriptor(store: &SqliteStore, run_id: &str) -> Result<ReplayDescriptor> {
    let payload = store
        .first_payload_by_type(run_id, crate::event::EventType::ReplayInitialized)?
        .context("run is not a step replay")?;
    serde_json::from_value(payload).context("invalid replay descriptor")
}

fn replay_dataset<'a>(
    datasets: &'a [DatasetOption],
    descriptor: &ReplayDescriptor,
) -> Result<&'a DatasetOption> {
    datasets
        .iter()
        .find(|dataset| dataset.id == descriptor.dataset_id)
        .context("replay dataset is no longer available")
}

fn validate_descriptor(
    descriptor: &ReplayDescriptor,
    dataset: &DatasetOption,
    bars: &[MarketBar],
) -> Result<()> {
    if descriptor.data_sha256 != dataset.sha256
        || (!descriptor.symbol.is_empty() && bars.iter().any(|bar| bar.symbol != descriptor.symbol))
        || descriptor.total_bars != bars.len()
        || bars.first().map(|bar| bar.timestamp) != Some(descriptor.first_timestamp)
        || bars.last().map(|bar| bar.timestamp) != Some(descriptor.last_timestamp)
    {
        bail!("replay dataset changed; refusing unsafe continuation")
    }
    Ok(())
}

fn rebuild_sync(config: Config, run_id: String, database: Option<&WebDatabaseGuard>) -> Result<()> {
    let store = open_runtime_or_test_store(&config, database)?;
    let initial = initial_state(&config, &run_id);
    let (snapshot, _) = store.rebuild(initial.clone())?;
    let (full, _) = store.rebuild_full(initial)?;
    compare_states(&snapshot, &full)
}

fn reconcile_sync(
    config: Config,
    run_id: String,
    database: Option<&WebDatabaseGuard>,
) -> Result<bool> {
    let store = open_runtime_or_test_store(&config, database)?;
    let algorithm = algorithm_from_config(&config)?;
    let mut service =
        GridAutomationService::recover_with_algorithm(config, store, algorithm, run_id)?;
    Ok(service.reconcile()?.matched)
}

fn resume_sync(
    config: Config,
    run_id: String,
    reason: String,
    database: Option<&WebDatabaseGuard>,
) -> Result<()> {
    let store = open_runtime_or_test_store(&config, database)?;
    let algorithm = algorithm_from_config(&config)?;
    let mut service =
        GridAutomationService::recover_with_algorithm(config, store, algorithm, run_id)?;
    service.resume_after_reconciliation(&reason)
}

fn initial_state(config: &Config, run_id: &str) -> StrategyState {
    StrategyState::new(
        run_id.to_owned(),
        String::new(),
        config.symbol.clone(),
        config.anchor_price,
        config.initial_cash,
        config.initial_position,
        config.initial_sellable,
    )
}

fn sanitize_run_id(value: &str) -> String {
    value
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        .take(64)
        .collect()
}

fn dataset_id(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("dataset path has no UTF-8 filename")?;
    let id: String = name
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_')
        })
        .take(160)
        .collect();
    if id.is_empty() {
        bail!("dataset filename is invalid")
    }
    Ok(id)
}

fn discover_datasets(primary: &Path, symbol: &str) -> Result<Vec<DatasetOption>> {
    let mut paths = vec![primary.to_path_buf()];
    if let Some(parent) = primary.parent() {
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) == Some("csv")
                    && path != primary
                {
                    paths.push(path);
                }
            }
        }
    }
    let mut datasets = Vec::new();
    for path in paths {
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if path != primary => {
                eprintln!("ignoring unreadable dataset {}: {error}", path.display());
                continue;
            }
            Err(error) => return Err(error).context("failed to read the primary dataset"),
        };
        let source = path.display().to_string();
        let feed = match CsvReplayFeed::load_bytes(&bytes, &source, symbol) {
            Ok(feed) => feed,
            Err(_) if path != primary => continue,
            Err(error) => return Err(error).context("primary dataset is invalid"),
        };
        let id = dataset_id(&path)?;
        datasets.push(DatasetOption {
            label: id.trim_end_matches(".csv").replace('_', " · "),
            id,
            sha256: hex::encode(Sha256::digest(&bytes)),
            bars: Arc::new(feed.bars().to_vec()),
        });
    }
    datasets.sort_by(|left, right| left.id.cmp(&right.id));
    datasets.dedup_by(|left, right| left.id == right.id);
    Ok(datasets)
}

#[cfg(test)]
fn sha256_file(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read dataset {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn selected_dataset_id(app: &WebState, requested: Option<&str>) -> String {
    requested
        .filter(|id| app.datasets.iter().any(|dataset| dataset.id == *id))
        .unwrap_or(&app.default_dataset)
        .to_owned()
}

#[allow(clippy::too_many_arguments)]
fn render_dashboard(
    config: &Config,
    runs: &[String],
    state: Option<&StrategyState>,
    events: &[crate::event::EventEnvelope],
    bars: &[MarketBar],
    query: &DashboardQuery,
    datasets: &[DatasetOption],
    selected_dataset: &str,
    progress: Option<&ReplayProgress>,
    playback: PlaybackView,
    pending_commands: &[ApiPendingCommandView],
    csrf_token: &str,
) -> String {
    let selected = state.map(|s| s.run_id.as_str()).unwrap_or("");
    let mut html = String::with_capacity(48_000);
    html.push_str("<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    html.push_str("<title>GridEdge-T 模拟控制台</title><style>");
    html.push_str(STYLES);
    html.push_str("</style><script src=\"/assets/dashboard.js\" defer></script>");
    let _ = write!(
        html,
        "</head><body data-playback-active=\"{}\" data-playback-interval=\"{}\"><div class=\"shell\">",
        playback.active,
        playback.interval_ms.max(250)
    );
    render_sidebar(
        &mut html,
        config,
        runs,
        selected,
        datasets,
        selected_dataset,
        csrf_token,
    );
    html.push_str("<main><header class=\"topbar\"><div><div class=\"eyebrow\">RESEARCH / PAPER ONLY</div><h1>网格运行控制台</h1><p>事件驱动 · 可恢复 · 可对账</p></div><div class=\"top-actions\"><a class=\"button ghost\" href=\"/\">刷新</a>");
    if let Some(state) = state {
        if playback.active {
            html.push_str("<span class=\"mode live\"><i></i>自动播放</span>");
        } else if progress.is_some_and(|progress| !progress.is_complete()) {
            html.push_str("<span class=\"mode warn\"><i></i>单步已暂停</span>");
        } else {
            let mode_class = match state.mode {
                ServiceMode::Running => "good",
                ServiceMode::Safe | ServiceMode::ReadOnly => "warn",
                ServiceMode::Stopped => "neutral",
            };
            let _ = write!(
                html,
                "<span class=\"mode {mode_class}\"><i></i>{:?}</span>",
                state.mode
            );
        }
    }
    html.push_str("</div></header>");
    if let Some(notice) = query.notice.as_deref() {
        render_notice(&mut html, notice);
    }
    for pending in pending_commands {
        let _ = write!(
            html,
            "<div class=\"notice warn\"><b>待恢复命令：</b> {} · {} · 控制版本 {}",
            escape(&pending.run_id),
            escape(&pending.command.to_uppercase()),
            pending.accepted_version,
        );
        if pending.recovery_state == "retryable" {
            let _ = write!(
                html,
                "<form method=\"post\" action=\"/actions/commands/retry\"><input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"run_id\" value=\"{}\"><input type=\"hidden\" name=\"request_id\" value=\"{}\"><button>重试原命令</button></form>",
                escape(csrf_token),
                escape(&pending.run_id),
                escape(&pending.request_id),
            );
        } else {
            html.push_str("<span>旧版命令未保存原请求，已安全停止。</span>");
        }
        html.push_str("</div>");
    }
    if state.is_some() && progress.is_none() {
        html.push_str("<div class=\"notice danger\">这个旧运行没有冻结的数据集身份；页面不会把当前默认行情冒充为其历史行情，回放控制也已禁用。</div>");
    }
    if let Some(state) = state {
        let visible_bars = progress
            .map(|progress| &bars[..progress.processed_bars.min(bars.len())])
            .unwrap_or(bars);
        if let Some(progress) = progress {
            render_step_controls(
                &mut html,
                state,
                progress,
                visible_bars,
                playback,
                events.last().map_or(0, |event| event.sequence_number),
                csrf_token,
            );
        }
        render_metrics(&mut html, state, config);
        render_overview(
            &mut html,
            state,
            config,
            visible_bars,
            events,
            datasets,
            selected_dataset,
            query.window.as_deref(),
            progress.is_some(),
            csrf_token,
        );
        html.push_str("<div class=\"section-title\" id=\"rights\"><span class=\"kicker\">GRID RIGHTS LEDGER</span><h2>网格权利与递延结果</h2></div>");
        render_rights(&mut html, state);
        html.push_str("<div class=\"section-title\" id=\"orders\"><span class=\"kicker\">SIMULATION RESULTS</span><h2>订单与成交结果</h2></div>");
        render_orders(&mut html, state);
        html.push_str("<div class=\"section-title\" id=\"lots\"><span class=\"kicker\">POSITION LOTS</span><h2>策略批次与持仓</h2></div>");
        render_lots(&mut html, state);
        html.push_str("<div class=\"section-title\" id=\"events\"><span class=\"kicker\">AUDIT TRAIL</span><h2>事件账本</h2></div>");
        render_events(&mut html, events, query.search.as_deref(), selected);
    } else {
        render_data_preview(
            &mut html,
            config,
            bars,
            datasets,
            selected_dataset,
            query.window.as_deref(),
        );
    }
    html.push_str("<footer>GridEdge-T · 仅限研究、回测和 Paper Trading · 不连接真实券商</footer></main></div></body></html>");
    html
}

fn render_sidebar(
    html: &mut String,
    config: &Config,
    runs: &[String],
    selected: &str,
    datasets: &[DatasetOption],
    selected_dataset: &str,
    csrf_token: &str,
) {
    html.push_str("<aside><a class=\"brand\" href=\"/\"><span class=\"brand-mark\">GE</span><span><b>GridEdge-T</b><small>A-SHARE GRID LAB</small></span></a><div class=\"side-label\">当前标的</div>");
    let _ = write!(
        html,
        "<div class=\"symbol-card\"><strong>{}</strong><span>锚点 ¥{} · 网距 {}%</span></div>",
        escape(&config.symbol),
        config.anchor_price,
        config.grid_ratio * Decimal::from(100)
    );
    html.push_str("<div class=\"side-label\">模拟运行</div><nav class=\"runs\">");
    for run in runs.iter().take(12) {
        let active = if run == selected { " active" } else { "" };
        let _ = write!(
            html,
            "<a class=\"run{active}\" href=\"/?run_id={}&dataset={}\"><i></i><span>{}</span></a>",
            escape(run),
            escape(selected_dataset),
            escape(run)
        );
    }
    if runs.is_empty() {
        html.push_str("<p class=\"muted side-empty\">暂无运行记录</p>");
    }
    let _ = write!(html, "</nav><form class=\"new-run\" method=\"post\" action=\"/actions/replay\"><input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"request_id\" value=\"{}\"><input type=\"hidden\" name=\"expected_sequence\" value=\"0\"><input type=\"hidden\" name=\"expected_version\" value=\"0\"><label for=\"dataset\">回放数据集</label><select id=\"dataset\" name=\"dataset\">", escape(csrf_token), Uuid::new_v4());
    for dataset in datasets {
        let selected = if dataset.id == selected_dataset {
            " selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\"{selected}>{}</option>",
            escape(&dataset.id),
            escape(&dataset.label)
        );
    }
    html.push_str("</select><label for=\"run_id\">新建模拟回放</label><input id=\"run_id\" name=\"run_id\" pattern=\"[A-Za-z0-9_-]+\" maxlength=\"64\" required value=\"");
    let _ = write!(html, "web-{}", chrono::Utc::now().format("%m%d-%H%M%S"));
    html.push_str("\"><button type=\"submit\" formaction=\"/actions/step/start\" class=\"step-start\">▷ 创建单步回放</button><button type=\"submit\">▶ 启动完整回放</button><small>单步模式可手动或自动逐根播放，并从账本恢复进度</small></form><div class=\"safety\"><b>SIMULATION ONLY</b><span>无实盘下单能力</span></div></aside>");
}

fn render_notice(html: &mut String, notice: &str) {
    let (class, message) = match notice {
        "replay-complete" => ("ok", "模拟回放已完成，账本和状态已更新。"),
        "step-ready" => ("ok", "单步回放已创建，尚未读取任何未来 K 线。"),
        "step-advanced" => ("ok", "已处理下一根 K 线并保存恢复点。"),
        "step-complete" => ("ok", "所有 K 线已处理，单步回放完成。"),
        "playback-started" => ("ok", "自动单步播放已启动，页面会持续显示最新一步。"),
        "playback-paused" => ("ok", "自动播放已暂停；当前进度已经写入账本。"),
        "playback-running" => ("ok", "这个回放已经在自动播放。"),
        "playback-stopping" => ("ok", "自动播放正在安全暂停，请稍后继续。"),
        "playback-not-running" => ("ok", "这个回放当前没有自动播放。"),
        "rebuild-verified" => ("ok", "状态重建验证通过：快照与完整事件日志一致。"),
        "reconciliation-matched" => ("ok", "Paper Broker 对账一致。"),
        "reconciliation-difference" => ("danger", "发现对账差异，系统已进入安全模式。"),
        "invalid-run-id" => ("danger", "运行名称只能包含字母、数字、横线和下划线。"),
        "replay-failed" => ("danger", "回放未启动；请换一个未使用的运行名称。"),
        "step-failed" => (
            "danger",
            "单步操作失败；数据集可能已变化或运行状态不允许继续。",
        ),
        "invalid-playback" => ("danger", "播放参数无效，请使用页面提供的速度。"),
        "rebuild-failed" => ("danger", "状态重建验证失败，请检查账本。"),
        _ => ("danger", "操作失败，请查看终端日志。"),
    };
    let _ = write!(html, "<div class=\"notice {class}\">{message}</div>");
}

fn render_step_controls(
    html: &mut String,
    state: &StrategyState,
    progress: &ReplayProgress,
    visible_bars: &[MarketBar],
    playback: PlaybackView,
    current_sequence: i64,
    csrf_token: &str,
) {
    let total = progress.descriptor.total_bars;
    let processed = progress.processed_bars.min(total);
    let percent = if total == 0 {
        100.0
    } else {
        processed as f64 / total as f64 * 100.0
    };
    let current = visible_bars.last();
    let status = if progress.is_complete() {
        "回放完成"
    } else if playback.active {
        "自动逐根播放中"
    } else if processed == 0 {
        "等待第一步"
    } else {
        "已暂停，等待下一步"
    };
    html.push_str("<section class=\"step-console\"><div class=\"step-summary\"><span class=\"kicker\">STEP REPLAY</span><h2>单步回放控制</h2>");
    let _ = write!(
        html,
        "<p>{status} · 已处理 <b>{processed}</b> / {total} 根</p><div class=\"progress-track\"><i style=\"width:{percent:.4}%\"></i></div>"
    );
    html.push_str("</div><div class=\"current-bar\">");
    if let Some(bar) = current {
        let _ = write!(
            html,
            "<span>当前 K 线</span><strong>{}</strong><dl><div><dt>开</dt><dd>{}</dd></div><div><dt>高</dt><dd>{}</dd></div><div><dt>低</dt><dd>{}</dd></div><div><dt>收</dt><dd>{}</dd></div></dl>",
            bar.timestamp.format("%Y-%m-%d %H:%M"),
            bar.open,
            bar.high,
            bar.low,
            bar.close
        );
    } else {
        html.push_str("<span>当前 K 线</span><strong>尚未开始</strong><small>点击“下一根 K 线”后才会揭示第一根行情。</small>");
    }
    html.push_str("</div><div class=\"step-actions\">");
    if playback.active {
        let fields = durable_form_fields(
            &state.run_id,
            csrf_token,
            current_sequence,
            playback.command_version,
        );
        let _ = write!(
            html,
            "<div class=\"playing-now\"><i></i><b>自动播放中</b><span>{} 毫秒 / 根</span></div><form data-dynamic-step method=\"post\" action=\"/actions/step/pause\">{}<button class=\"pause\">暂停播放 <span>当前这根落账后停止</span></button></form>",
            playback.interval_ms,
            fields,
        );
    } else if !progress.is_complete() {
        let play_fields = durable_form_fields(
            &state.run_id,
            csrf_token,
            current_sequence,
            playback.command_version,
        );
        let step_fields = durable_form_fields(
            &state.run_id,
            csrf_token,
            current_sequence,
            playback.command_version,
        );
        let finish_fields = durable_form_fields(
            &state.run_id,
            csrf_token,
            current_sequence,
            playback.command_version,
        );
        let _ = write!(
            html,
            "<form data-dynamic-step class=\"auto-play-form\" method=\"post\" action=\"/actions/step/play\">{}<label>播放速度<select name=\"speed_ms\"><option value=\"1000\" selected>1 秒 / 根</option><option value=\"500\">0.5 秒 / 根</option><option value=\"250\">0.25 秒 / 根</option><option value=\"2000\">2 秒 / 根</option></select></label><button>▶ 自动播放 <span>网页动态逐根推进</span></button></form><form data-dynamic-step method=\"post\" action=\"/actions/step/next\">{}<button class=\"secondary\">下一根 K 线 <span>只推进一步</span></button></form><form data-dynamic-step method=\"post\" action=\"/actions/step/finish\">{}<button class=\"secondary\">运行至结束 <span>处理剩余全部数据</span></button></form>",
            play_fields,
            step_fields,
            finish_fields,
        );
    } else {
        html.push_str("<div class=\"complete-mark\">✓ 全部行情已进入审计账本</div>");
    }
    html.push_str("</div></section>");
}

fn durable_form_fields(
    run_id: &str,
    csrf_token: &str,
    expected_sequence: i64,
    expected_version: i64,
) -> String {
    format!(
        "<input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"run_id\" value=\"{}\"><input type=\"hidden\" name=\"request_id\" value=\"{}\"><input type=\"hidden\" name=\"expected_sequence\" value=\"{}\"><input type=\"hidden\" name=\"expected_version\" value=\"{}\">",
        escape(csrf_token),
        escape(run_id),
        Uuid::new_v4(),
        expected_sequence,
        expected_version,
    )
}

fn render_metrics(html: &mut String, state: &StrategyState, config: &Config) {
    let open_lots = state
        .lots
        .values()
        .filter(|lot| lot.remaining_quantity > 0)
        .count();
    let open_orders = state
        .orders
        .values()
        .filter(|order| {
            !matches!(
                order.status,
                OrderStatus::Filled | OrderStatus::Rejected | OrderStatus::Cancelled
            )
        })
        .count();
    let current_level = state
        .levels
        .values()
        .min_by_key(|level| {
            state
                .last_price
                .map(|price| (price - level.price).abs())
                .unwrap_or(Decimal::MAX)
        })
        .map(|level| format!("{:+}", level.index))
        .unwrap_or_else(|| "—".to_owned());
    let valuation = crate::profit::unrealized_grid_valuation(state);
    let mark_total = valuation
        .mark_to_market_unrealized
        .and_then(|value| state.realized_pnl.checked_add(value));
    let conservative_total = valuation
        .conservative_exit_unrealized
        .and_then(|value| state.realized_pnl.checked_add(value));
    let metrics = [
        (
            "可用现金",
            format!("¥{}", money(state.cash.available)),
            format!("手续费 ¥{}", money(state.cash.total_fees)),
        ),
        (
            "总持仓",
            format!("{} 股", state.position.total),
            format!("可卖 {} 股", state.position.sellable),
        ),
        (
            "已实现网格收益",
            format!("¥{}", money(state.realized_pnl)),
            "累计买卖费用已计入".to_owned(),
        ),
        (
            "盯市总网格收益",
            mark_total
                .map(|value| format!("¥{}", money(value)))
                .unwrap_or_else(|| "—".to_owned()),
            valuation
                .mark_to_market_unrealized
                .map(|value| format!("未实现 ¥{} · 未扣退出成本", money(value)))
                .unwrap_or_else(|| "未实现估值不可用".to_owned()),
        ),
        (
            "逐 lot 保守退出总收益",
            conservative_total
                .map(|value| format!("¥{}", money(value)))
                .unwrap_or_else(|| "—".to_owned()),
            valuation
                .conservative_exit_unrealized
                .map(|value| format!("未实现 ¥{} · 不代表当前可卖", money(value)))
                .unwrap_or_else(|| "退出估值不可用".to_owned()),
        ),
        (
            "当前格位",
            current_level,
            format!("中心 ¥{}", config.anchor_price),
        ),
        (
            "账本事件",
            state.event_count.to_string(),
            format!(
                "重复 {} · 歧义 {}",
                state.duplicate_events, state.ambiguous_bars
            ),
        ),
        (
            "活动对象",
            open_lots.to_string(),
            format!("批次 · 未完成订单 {open_orders}"),
        ),
    ];
    html.push_str("<section class=\"metrics\">");
    for (label, value, sub) in metrics {
        let _ = write!(
            html,
            "<article><span>{label}</span><strong>{value}</strong><small>{sub}</small></article>"
        );
    }
    html.push_str("</section>");
}

#[allow(clippy::too_many_arguments)]
fn render_overview(
    html: &mut String,
    state: &StrategyState,
    config: &Config,
    bars: &[MarketBar],
    events: &[crate::event::EventEnvelope],
    datasets: &[DatasetOption],
    selected_dataset: &str,
    window: Option<&str>,
    dataset_locked: bool,
    csrf_token: &str,
) {
    html.push_str("<section class=\"dashboard-grid\"><article class=\"panel chart-panel\">");
    render_data_toolbar(
        html,
        bars,
        datasets,
        selected_dataset,
        window,
        Some(&state.run_id),
        dataset_locked,
    );
    render_price_chart(html, config, &chart_bars(bars, window));
    html.push_str("</article><article class=\"panel cycle-panel\"><div class=\"panel-head\"><div><span class=\"kicker\">STATE MACHINE</span><h2>当前网格周期</h2></div></div>");
    let _ = write!(html, "<div class=\"cycle-id\"><span>周期 ID</span><code>{}</code></div><div class=\"grid-levels\">", escape(&state.cycle_id));
    for level in state.levels.values().rev() {
        let status = match level.status {
            GridLevelStatus::Armed => "armed",
            GridLevelStatus::Touched => "touched",
            GridLevelStatus::Executed => "executed",
            GridLevelStatus::Skipped => "skipped",
            GridLevelStatus::Rearmed => "rearmed",
        };
        let _ = write!(
            html,
            "<div class=\"level {status}\"><b>{:+}</b><span>¥{}</span><em>{:?}</em></div>",
            level.index, level.price, level.status
        );
    }
    html.push_str("</div></article><article class=\"panel action-panel\"><div class=\"panel-head\"><div><span class=\"kicker\">OPERATIONS</span><h2>可靠性操作</h2></div></div><p>所有操作都会进入审计账本；对账差异不会被静默修正。</p><div class=\"action-stack\">");
    let _ = write!(html, "<form method=\"post\" action=\"/actions/rebuild\"><input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"run_id\" value=\"{}\"><button>校验状态重建 <span>快照 vs 全日志</span></button></form><form method=\"post\" action=\"/actions/reconcile\"><input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"run_id\" value=\"{}\"><button>执行 Paper 对账 <span>现金 / 持仓 / 订单</span></button></form>", escape(csrf_token), escape(&state.run_id), escape(csrf_token), escape(&state.run_id));
    if matches!(state.mode, ServiceMode::Safe | ServiceMode::ReadOnly) {
        let _ = write!(html, "<form method=\"post\" action=\"/actions/resume\"><input type=\"hidden\" name=\"csrf_token\" value=\"{}\"><input type=\"hidden\" name=\"run_id\" value=\"{}\"><input name=\"reason\" required maxlength=\"240\" placeholder=\"恢复原因（必填）\"><button>审核后恢复运行 <span>要求最近一次独立对账一致</span></button></form>", escape(csrf_token), escape(&state.run_id));
    }
    html.push_str("</div>");
    let recovery = state.last_recovery.as_deref().unwrap_or("尚无恢复记录");
    let _ = write!(
        html,
        "<div class=\"audit-note\"><b>最近恢复</b><span>{}</span></div>",
        escape(recovery)
    );
    html.push_str("</article><article class=\"panel timeline-panel\"><div class=\"panel-head\"><div><span class=\"kicker\">LATEST EVENTS</span><h2>最近事件</h2></div>");
    html.push_str("<a href=\"#events\">查看账本 ↓</a></div><div class=\"mini-timeline\">");
    for event in events.iter().rev().take(8) {
        let _ = write!(
            html,
            "<div><i></i><time>{}</time><b>{}</b><span>#{}</span></div>",
            event.event_time.format("%m-%d %H:%M"),
            event.event_type,
            event.sequence_number
        );
    }
    html.push_str("</div></article></section>");
}

fn render_data_preview(
    html: &mut String,
    config: &Config,
    bars: &[MarketBar],
    datasets: &[DatasetOption],
    selected_dataset: &str,
    window: Option<&str>,
) {
    html.push_str("<section class=\"data-hero\"><div><span class=\"kicker\">REAL MARKET DATA READY</span><h2>兆新股份三年 5 分钟行情已就绪</h2><p>先浏览真实历史走势；需要生成完整网格账本时，在左侧为本次模拟命名并启动回放。</p></div>");
    if let (Some(first), Some(last)) = (bars.first(), bars.last()) {
        let _ = write!(
            html,
            "<dl><div><dt>K线数量</dt><dd>{}</dd></div><div><dt>覆盖区间</dt><dd>{} — {}</dd></div><div><dt>频率</dt><dd>5 分钟</dd></div></dl>",
            bars.len(),
            first.timestamp.format("%Y-%m-%d"),
            last.timestamp.format("%Y-%m-%d")
        );
    }
    html.push_str("</section><article class=\"panel chart-panel preview-chart\">");
    render_data_toolbar(html, bars, datasets, selected_dataset, window, None, false);
    render_price_chart(html, config, &chart_bars(bars, window));
    html.push_str("</article>");
}

fn render_data_toolbar(
    html: &mut String,
    bars: &[MarketBar],
    datasets: &[DatasetOption],
    selected_dataset: &str,
    window: Option<&str>,
    run_id: Option<&str>,
    dataset_locked: bool,
) {
    let window = normalized_window(window);
    html.push_str("<div class=\"panel-head data-head\"><div><span class=\"kicker\">PRICE / GRID</span><h2>真实行情与固定网格</h2>");
    if let (Some(first), Some(last)) = (bars.first(), bars.last()) {
        let _ = write!(
            html,
            "<small>{} 根 · {} 至 {}</small>",
            bars.len(),
            first.timestamp.format("%Y-%m-%d"),
            last.timestamp.format("%Y-%m-%d")
        );
    }
    html.push_str("</div><form class=\"data-controls\" method=\"get\">");
    if let Some(run_id) = run_id {
        let _ = write!(
            html,
            "<input type=\"hidden\" name=\"run_id\" value=\"{}\">",
            escape(run_id)
        );
    }
    if dataset_locked {
        let _ = write!(
            html,
            "<input type=\"hidden\" name=\"dataset\" value=\"{}\">",
            escape(selected_dataset)
        );
        html.push_str("<label>数据集<select disabled title=\"单步回放已绑定数据集\">");
    } else {
        html.push_str("<label>数据集<select name=\"dataset\">");
    }
    for dataset in datasets {
        let selected = if dataset.id == selected_dataset {
            " selected"
        } else {
            ""
        };
        let _ = write!(
            html,
            "<option value=\"{}\"{selected}>{}</option>",
            escape(&dataset.id),
            escape(&dataset.label)
        );
    }
    html.push_str("</select></label><label>图表范围<select name=\"window\">");
    for (id, label) in [
        ("1d", "1日"),
        ("5d", "5日"),
        ("20d", "20日"),
        ("all", "三年全景"),
    ] {
        let selected = if id == window { " selected" } else { "" };
        let _ = write!(html, "<option value=\"{id}\"{selected}>{label}</option>");
    }
    html.push_str("</select></label><button>查看</button></form></div>");
}

fn normalized_window(window: Option<&str>) -> &str {
    match window {
        Some("1d" | "5d" | "20d" | "all") => window.unwrap_or("20d"),
        _ => "20d",
    }
}

fn chart_bars(bars: &[MarketBar], window: Option<&str>) -> Vec<MarketBar> {
    let requested = match normalized_window(window) {
        "1d" => 48,
        "5d" => 48 * 5,
        "20d" => 48 * 20,
        _ => bars.len(),
    };
    let start = bars.len().saturating_sub(requested);
    aggregate_bars(&bars[start..], 480)
}

fn aggregate_bars(bars: &[MarketBar], maximum: usize) -> Vec<MarketBar> {
    if bars.len() <= maximum {
        return bars.to_vec();
    }
    let chunk_size = bars.len().div_ceil(maximum);
    bars.chunks(chunk_size)
        .map(|chunk| {
            let first = &chunk[0];
            let last = &chunk[chunk.len() - 1];
            MarketBar {
                timestamp: last.timestamp,
                symbol: first.symbol.clone(),
                open: first.open,
                high: chunk.iter().map(|bar| bar.high).max().unwrap_or(first.high),
                low: chunk.iter().map(|bar| bar.low).min().unwrap_or(first.low),
                close: last.close,
                volume: chunk.iter().map(|bar| bar.volume).sum(),
                amount: if chunk.iter().all(|bar| bar.amount.is_some()) {
                    Some(chunk.iter().filter_map(|bar| bar.amount).sum())
                } else {
                    None
                },
            }
        })
        .collect()
}

fn render_price_chart(html: &mut String, config: &Config, bars: &[MarketBar]) {
    if bars.is_empty() {
        html.push_str("<div class=\"chart-empty\">行情数据不可用</div>");
        return;
    }
    let min_price = bars
        .iter()
        .map(|bar| bar.low)
        .min()
        .unwrap_or(config.anchor_price);
    let max_price = bars
        .iter()
        .map(|bar| bar.high)
        .max()
        .unwrap_or(config.anchor_price);
    let span = (max_price - min_price).to_f64().unwrap_or(1.0).max(0.01);
    html.push_str("<div class=\"chart\"><div class=\"plot\">");
    for index in -config.boundary_levels..=config.boundary_levels {
        if index == 0 {
            continue;
        }
        let Ok(price) = crate::grid::GridSpec::from(config).price(index) else {
            html.push_str("<div class=\"chart-empty\">网格数值范围无效</div>");
            return;
        };
        if price < min_price || price > max_price {
            continue;
        }
        let top = (max_price - price).to_f64().unwrap_or(0.0) / span * 100.0;
        let role = if index.abs() <= config.trade_levels {
            "trade-line"
        } else {
            "boundary-line"
        };
        let _ = write!(html, "<div class=\"gridline {role}\" style=\"top:{top:.2}%\"><span>G{index:+} · {price}</span></div>");
    }
    let width = 100.0 / bars.len() as f64;
    let candle_width = (width * 0.72).max(0.08);
    for (position, bar) in bars.iter().enumerate() {
        let x = (position as f64 + 0.5) * width;
        let high = (max_price - bar.high).to_f64().unwrap_or(0.0) / span * 100.0;
        let low = (max_price - bar.low).to_f64().unwrap_or(0.0) / span * 100.0;
        let open = (max_price - bar.open).to_f64().unwrap_or(0.0) / span * 100.0;
        let close = (max_price - bar.close).to_f64().unwrap_or(0.0) / span * 100.0;
        let top = open.min(close);
        let body_height = (open - close).abs().max(0.9);
        let class = if bar.close >= bar.open { "up" } else { "down" };
        let _ = write!(html, "<div class=\"candle {class}\" style=\"left:{x:.2}%;width:{candle_width:.3}%;--high:{high:.2}%;--low:{low:.2}%;--top:{top:.2}%;--height:{body_height:.2}%\"><i></i><b title=\"{} O:{} H:{} L:{} C:{}\"></b></div>", bar.timestamp.format("%m-%d %H:%M"), bar.open, bar.high, bar.low, bar.close);
    }
    html.push_str("</div><div class=\"axis\"><span>");
    let _ = write!(
        html,
        "{}",
        bars.first()
            .map(|bar| bar.timestamp.format("%m-%d %H:%M").to_string())
            .unwrap_or_default()
    );
    html.push_str("</span><span>次日 T+1</span><span>");
    let _ = write!(
        html,
        "{}",
        bars.last()
            .map(|bar| bar.timestamp.format("%m-%d %H:%M").to_string())
            .unwrap_or_default()
    );
    html.push_str("</span></div></div>");
}

fn render_orders(html: &mut String, state: &StrategyState) {
    html.push_str("<section class=\"panel table-panel\"><div class=\"panel-head\"><div><span class=\"kicker\">ORDER LIFECYCLE</span><h2>订单与成交</h2></div></div><div class=\"table-wrap\"><table><thead><tr><th>订单</th><th>方向</th><th>格位</th><th>委托价</th><th>数量</th><th>已成交</th><th>状态</th></tr></thead><tbody>");
    for order in state.orders.values() {
        let direction = format!("{:?}", order.intent.direction);
        let class = direction.to_lowercase();
        let _ = write!(html, "<tr><td><code>{}</code></td><td><span class=\"tag {class}\">{direction}</span></td><td>{:+}</td><td>¥{}</td><td>{}</td><td>{}</td><td><span class=\"status\">{:?}</span></td></tr>", short_id(&order.order_id), order.intent.grid_index, order.intent.limit_price, order.intent.quantity, order.filled_quantity, order.status);
    }
    html.push_str("</tbody></table></div></section>");
}

fn quantity_units(quantity: i64, standard_quantity: i64) -> String {
    if standard_quantity > 0 && quantity % standard_quantity == 0 {
        format!("{} 份", quantity / standard_quantity)
    } else if standard_quantity > 0 {
        format!("{quantity} 股（不足整份）")
    } else {
        "历史金额制".to_owned()
    }
}

fn render_rights(html: &mut String, state: &StrategyState) {
    let mut rights: Vec<_> = state.grid_rights.values().collect();
    rights.sort_by_key(|right| (right.granted_at, right.grid_index, right.right_id.clone()));
    let deferred = rights
        .iter()
        .filter(|right| right.status == GridRightStatus::Deferred)
        .count();
    let blocked = rights
        .iter()
        .filter(|right| right.status == GridRightStatus::Blocked)
        .count();
    let exercised = rights
        .iter()
        .filter(|right| right.status == GridRightStatus::Exercised)
        .count();
    let no_loss_blocks = rights
        .iter()
        .filter(|right| right.capacity.no_profit_blocked_quantity > 0)
        .count();
    let no_loss_blocked_quantity: i64 = rights
        .iter()
        .map(|right| right.capacity.no_profit_blocked_quantity)
        .sum();
    let active_quantity: i64 = state
        .right_tranches
        .values()
        .map(|tranche| tranche.available_quantity + tranche.reserved_quantity)
        .sum();
    let consumed_quantity: i64 = state
        .right_tranches
        .values()
        .map(|tranche| tranche.consumed_quantity)
        .sum();
    let revoked_quantity: i64 = state
        .right_tranches
        .values()
        .map(|tranche| tranche.revoked_quantity)
        .sum();
    let minted_quantity: i64 = state
        .right_tranches
        .values()
        .map(|tranche| tranche.minted_quantity)
        .sum();
    let units = |quantity: i64| quantity_units(quantity, state.audited_standard_quantity);
    let _ = write!(
        html,
        "<section class=\"rights-summary\"><article><span>授予总数</span><strong>{}</strong></article><article><span>算法递延</span><strong>{deferred}</strong></article><article><span>风控阻断</span><strong>{blocked}</strong></article><article><span>完整行权</span><strong>{exercised}</strong></article><article><span>保本拦截</span><strong>{no_loss_blocks}</strong><small>{no_loss_blocked_quantity} 股未卖</small></article><article><span>铸造份数</span><strong>{}</strong></article><article><span>当前有效份数</span><strong>{}</strong></article><article><span>已有成交份数</span><strong>{}</strong></article><article><span>反弹撤销份数</span><strong>{}</strong></article></section><section class=\"rights-grid\">",
        rights.len(),
        units(minted_quantity),
        units(active_quantity),
        units(consumed_quantity),
        units(revoked_quantity)
    );
    for right in rights.iter().rev().take(36) {
        let direction = format!("{:?}", right.direction);
        let status = format!("{:?}", right.status);
        let (capacity_label, capacity, exercised_label, exercised_value) =
            if right.direction == crate::domain::Direction::Buy {
                if right.capacity.mechanical_quantity_cap > 0 {
                    (
                        "可用股份",
                        format!("{} 股", right.capacity.available_quantity),
                        "已行使股份",
                        format!("{} 股", right.exercised_quantity),
                    )
                } else {
                    (
                        "历史可用额度",
                        format!("¥{}", money(right.capacity.available_budget)),
                        "历史已行使额度",
                        format!("¥{}", money(right.exercised_budget)),
                    )
                }
            } else {
                (
                    "可用股份",
                    format!("{} 股", right.capacity.eligible_quantity),
                    "已行使股份",
                    format!("{} 股", right.exercised_quantity),
                )
            };
        let _ = write!(
            html,
            "<article class=\"right-card {} {}\"><div><span class=\"tag {}\">{direction} {:+}</span><em>{status}</em></div><h3>¥{}</h3><dl><dt>{capacity_label}</dt><dd>{capacity}</dd><dt>{exercised_label}</dt><dd>{exercised_value}</dd><dt>来源网格 / 授权数量</dt><dd>{} · {}</dd><dt>授予时间</dt><dd>{}</dd></dl><small>{}</small></article>",
            direction.to_ascii_lowercase(),
            status.to_ascii_lowercase(),
            direction.to_ascii_lowercase(),
            right.grid_index,
            right.grid_price,
            right
                .capacity
                .accumulated_grid_indices
                .iter()
                .map(|index| format!("{index:+}"))
                .collect::<Vec<_>>()
                .join(" / "),
            units(if right.direction == crate::domain::Direction::Buy {
                right.capacity.available_quantity
            } else {
                right.capacity.eligible_quantity
            }),
            right.granted_at.format("%m-%d %H:%M"),
            short_id(&right.right_id)
        );
    }
    if rights.is_empty() {
        html.push_str("<div class=\"empty inline\"><h2>尚未产生网格权利</h2><p>首根 K 线只建立价格状态；真实跨越网格后才会在这里授予权利。</p></div>");
    }
    html.push_str("</section>");
}

fn render_lots(html: &mut String, state: &StrategyState) {
    html.push_str("<section class=\"lot-grid\">");
    for lot in state.lots.values() {
        let closed = lot.remaining_quantity == 0;
        let class = if closed { "closed" } else { "open" };
        let _ = write!(html, "<article class=\"lot-card {class}\"><div><span class=\"tag\">GRID {:+}</span><em>{}</em></div><h3>{} 股</h3><p>剩余 {} / 原始 {}</p><dl><dt>开仓价</dt><dd>¥{}</dd><dt>开仓日</dt><dd>{}</dd><dt>已实现收益</dt><dd class=\"positive\">¥{}</dd></dl><small>{}</small></article>", lot.grid_index, if closed { "已关闭" } else { "持有中" }, lot.original_quantity, lot.remaining_quantity, lot.original_quantity, lot.open_price, lot.opened_on, money(lot.realized_pnl), escape(&lot.lot_id));
    }
    if state.lots.is_empty() {
        html.push_str("<div class=\"empty inline\"><h2>暂无策略批次</h2></div>");
    }
    html.push_str("</section>");
}

fn render_events(
    html: &mut String,
    events: &[crate::event::EventEnvelope],
    search: Option<&str>,
    run_id: &str,
) {
    let search = search.unwrap_or("").trim().to_ascii_uppercase();
    let filtered: Vec<_> = events
        .iter()
        .rev()
        .filter(|event| {
            search.is_empty()
                || event.event_type.to_string().contains(&search)
                || event.idempotency_key.to_ascii_uppercase().contains(&search)
        })
        .take(250)
        .collect();
    html.push_str("<section class=\"panel event-panel\"><div class=\"panel-head event-head\"><div><span class=\"kicker\">APPEND-ONLY JOURNAL</span><h2>事件账本</h2></div><form method=\"get\"><input type=\"hidden\" name=\"run_id\" value=\"");
    html.push_str(&escape(run_id));
    html.push_str("\"><input name=\"search\" value=\"");
    html.push_str(&escape(search.as_str()));
    html.push_str("\" placeholder=\"筛选事件类型，例如 FILL\"><button>筛选</button></form></div><div class=\"event-list\">");
    for event in filtered {
        let _ = write!(html, "<details><summary><span class=\"seq\">#{}</span><time>{}</time><b>{}</b><code>{}</code></summary><div class=\"event-detail\"><dl><dt>Event ID</dt><dd>{}</dd><dt>Idempotency Key</dt><dd>{}</dd><dt>Correlation</dt><dd>{}</dd></dl><pre>{}</pre></div></details>", event.sequence_number, event.event_time, event.event_type, short_id(&event.event_id), escape(&event.event_id), escape(&event.idempotency_key), escape(&event.correlation_id), escape(&serde_json::to_string_pretty(&event.payload).unwrap_or_default()));
    }
    html.push_str("</div></section>");
}

fn money(value: Decimal) -> String {
    value.round_dp(2).to_string()
}

fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[derive(Debug)]
struct WebError {
    status: StatusCode,
    code: &'static str,
    error: anyhow::Error,
}

impl WebError {
    fn database_unavailable(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "DATABASE_UNAVAILABLE",
            error,
        }
    }

    fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "VALIDATION_ERROR",
            error: anyhow::anyhow!(message.into()),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "COMMAND_CONFLICT",
            error: anyhow::anyhow!(message.into()),
        }
    }

    fn conflict_error(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "COMMAND_CONFLICT",
            error,
        }
    }

    fn opportunity_conflict(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "OPPORTUNITY_INTEGRITY_CONFLICT",
            error,
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            error: anyhow::anyhow!(message.into()),
        }
    }

    fn pending_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "PENDING_RECOVERY_UNAVAILABLE",
            error: anyhow::anyhow!(message.into()),
        }
    }

    fn pending_plan_conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "PENDING_PLAN_CONFLICT",
            error: anyhow::anyhow!(message.into()),
        }
    }
}

impl From<anyhow::Error> for WebError {
    fn from(value: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_ERROR",
            error: value,
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({
                "api_version": "gridedge.api/v1",
                "code": self.code,
                "message": self.error.to_string(),
            })),
        )
            .into_response()
    }
}

const DYNAMIC_DASHBOARD_SCRIPT: &str = r#"
document.addEventListener("DOMContentLoaded", () => {
  let timer = null;
  let requestInFlight = false;
  document.body.dataset.dynamicReady = "true";
  document.body.dataset.dynamicRefreshCount = "0";

  const playbackIsActive = () => document.body.dataset.playbackActive === "true";
  const playbackInterval = () => Math.max(250, Number(document.body.dataset.playbackInterval) || 1000);

  const scheduleRefresh = () => {
    window.clearTimeout(timer);
    if (playbackIsActive()) {
      timer = window.setTimeout(refreshDashboard, playbackInterval());
    }
  };

  const applyDashboard = async (response) => {
    if (!response.ok) throw new Error(`dashboard request failed: ${response.status}`);
    const source = await response.text();
    const next = new DOMParser().parseFromString(source, "text/html");
    const nextMain = next.querySelector("main");
    const currentMain = document.querySelector("main");
    if (!nextMain || !currentMain) throw new Error("dashboard response is incomplete");
    currentMain.replaceWith(nextMain);
    document.body.dataset.playbackActive = next.body.dataset.playbackActive || "false";
    document.body.dataset.playbackInterval = next.body.dataset.playbackInterval || "1000";
    document.body.dataset.dynamicRefreshCount = String(Number(document.body.dataset.dynamicRefreshCount) + 1);
    if (response.url) history.replaceState(null, "", response.url);
    scheduleRefresh();
  };

  async function refreshDashboard() {
    if (requestInFlight || !playbackIsActive()) return scheduleRefresh();
    requestInFlight = true;
    try {
      const response = await fetch(window.location.href, {
        cache: "no-store",
        headers: { "X-GridEdge-Dynamic": "dashboard" }
      });
      await applyDashboard(response);
    } catch (error) {
      console.error(error);
      scheduleRefresh();
    } finally {
      requestInFlight = false;
    }
  }

  document.addEventListener("submit", async (event) => {
    const form = event.target.closest("form[data-dynamic-step]");
    if (!form) return;
    event.preventDefault();
    window.clearTimeout(timer);
    if (requestInFlight) return;
    requestInFlight = true;
    const button = event.submitter;
    if (button) button.disabled = true;
    try {
      const response = await fetch(form.action, {
        method: form.method || "POST",
        body: new URLSearchParams(new FormData(form)),
        cache: "no-store",
        headers: { "X-GridEdge-Dynamic": "dashboard" }
      });
      await applyDashboard(response);
    } catch (error) {
      console.error(error);
      if (button) button.disabled = false;
      scheduleRefresh();
    } finally {
      requestInFlight = false;
    }
  });

  scheduleRefresh();
});
"#;

const STYLES: &str = r#"
:root{--ink:#101c2c;--muted:#6b7687;--line:#dde3e9;--paper:#f4f6f8;--card:#fff;--orange:#f36b35;--green:#178b68;--red:#c84b55;--nav:#111d2b;--blue:#2f6feb}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font-family:Inter,"PingFang SC","Microsoft YaHei",system-ui,sans-serif;font-size:14px}.shell{min-height:100vh;display:grid;grid-template-columns:258px 1fr}aside{background:var(--nav);color:#d9e1e9;padding:26px 20px;display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow:auto}.brand{display:flex;gap:12px;align-items:center;color:#fff;text-decoration:none;margin-bottom:34px}.brand-mark{display:grid;place-items:center;width:42px;height:42px;background:var(--orange);border-radius:10px;font-weight:900;letter-spacing:-1px}.brand b{font-size:18px;display:block}.brand small{font-size:9px;letter-spacing:1.8px;color:#7f91a5}.side-label{font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#738397;margin:18px 6px 9px}.symbol-card{border:1px solid #2c3a4b;background:#172535;padding:14px;border-radius:10px}.symbol-card strong{display:block;color:#fff;font-size:17px}.symbol-card span{font-size:11px;color:#8fa0b2}.runs{display:grid;gap:4px}.run{color:#aebaca;text-decoration:none;padding:10px;border-radius:8px;display:flex;align-items:center;gap:9px;white-space:nowrap;overflow:hidden}.run i{width:6px;height:6px;border:1px solid #718397;border-radius:50%;flex:none}.run span{overflow:hidden;text-overflow:ellipsis}.run:hover,.run.active{background:#203146;color:#fff}.run.active i{background:var(--orange);border-color:var(--orange);box-shadow:0 0 0 4px #f36b3522}.new-run{margin-top:22px;padding-top:20px;border-top:1px solid #2a394a}.new-run label{display:block;font-size:11px;color:#93a1b1;margin-bottom:8px}.new-run input{width:100%;border:1px solid #34475c;background:#0e1825;color:#fff;border-radius:7px;padding:10px;outline:none}.new-run input:focus{border-color:var(--orange)}button,.button{border:0;border-radius:7px;background:var(--orange);color:#fff;font-weight:700;padding:10px 14px;cursor:pointer;text-decoration:none}.new-run button{width:100%;margin-top:8px}.new-run small{display:block;color:#718397;text-align:center;margin-top:7px}.safety{margin-top:auto;border:1px solid #3a4654;border-radius:8px;padding:11px}.safety b{display:block;color:#ffb18f;font-size:10px;letter-spacing:1.4px}.safety span{font-size:11px;color:#8998aa}.side-empty{padding:5px 10px}.muted{color:var(--muted)}main{min-width:0;padding:28px 34px 22px;max-width:1680px;width:100%;margin:0 auto}.topbar{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px}.eyebrow,.kicker{font-size:9px;letter-spacing:1.7px;color:var(--orange);font-weight:800}.topbar h1{font-family:Georgia,"Songti SC",serif;font-size:30px;margin:4px 0 0}.topbar p{margin:3px 0;color:var(--muted)}.top-actions{display:flex;align-items:center;gap:10px}.button.ghost{background:#fff;color:var(--ink);border:1px solid var(--line)}.mode{display:flex;align-items:center;gap:7px;border:1px solid var(--line);background:#fff;border-radius:20px;padding:8px 12px;font-size:11px;font-weight:800}.mode i{width:7px;height:7px;border-radius:50%}.mode.good i{background:var(--green)}.mode.warn i{background:#e6a11e}.mode.neutral i{background:#8090a0}.notice{padding:12px 16px;border-radius:8px;margin:-8px 0 18px;border:1px solid}.notice.ok{background:#eaf8f3;color:#176e56;border-color:#bce7d8}.notice.danger{background:#fff0f0;color:#9f3440;border-color:#f0c7ca}.metrics{display:grid;grid-template-columns:repeat(6,minmax(130px,1fr));gap:10px;margin-bottom:22px}.metrics article{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:15px;min-width:0}.metrics span,.metrics small{display:block;color:var(--muted);font-size:11px}.metrics strong{display:block;font-family:Georgia,serif;font-size:20px;margin:7px 0 4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tabs{display:flex;border-bottom:1px solid var(--line);margin-bottom:18px;gap:26px}.tabs a{color:var(--muted);text-decoration:none;padding:10px 2px 12px;font-weight:700;position:relative}.tabs a.active{color:var(--ink)}.tabs a.active:after{content:"";height:3px;background:var(--orange);position:absolute;left:0;right:0;bottom:-1px}.dashboard-grid{display:grid;grid-template-columns:minmax(0,2fr) minmax(290px,.85fr);gap:14px}.panel{background:var(--card);border:1px solid var(--line);border-radius:11px;padding:19px;min-width:0}.panel-head{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px}.panel-head h2{font-size:16px;margin:4px 0}.panel-head a{color:var(--blue);text-decoration:none;font-size:12px}.legend{font-size:10px;color:var(--muted);display:flex;gap:5px;align-items:center}.legend i{width:8px;height:8px;border-radius:2px;margin-left:7px}.legend .up{background:var(--green)}.legend .down{background:var(--red)}.legend .grid-dot{background:#a6b0bd}.chart-panel{min-height:410px}.chart{height:315px}.plot{height:275px;position:relative;border-bottom:1px solid var(--line);background:repeating-linear-gradient(to bottom,#fff 0,#fff 54px,#edf1f4 55px)}.gridline{position:absolute;left:0;right:0;border-top:1px dashed #bbc4ce;z-index:1}.gridline span{position:absolute;right:0;top:-16px;color:#778494;background:#fff;padding-left:5px;font-size:9px}.gridline.boundary-line{border-color:#e3aa94}.gridline.boundary-line span{color:#bb6645}.candle{position:absolute;top:0;width:1.6%;height:100%;z-index:2;transform:translateX(-50%)}.candle i{position:absolute;left:50%;top:var(--high);height:calc(var(--low) - var(--high));border-left:1px solid currentColor}.candle b{position:absolute;left:18%;right:18%;top:var(--top);height:var(--height);min-height:3px;background:currentColor;border-radius:1px}.candle.up{color:var(--green)}.candle.down{color:var(--red)}.axis{display:flex;justify-content:space-between;color:#8b96a3;font-size:9px;padding-top:8px}.cycle-id{background:#f7f9fa;padding:9px 11px;border-radius:7px;margin-bottom:10px}.cycle-id span{display:block;font-size:9px;color:var(--muted)}.cycle-id code{font-size:10px}.grid-levels{display:grid;gap:4px}.level{display:grid;grid-template-columns:36px 1fr auto;align-items:center;border-left:3px solid #c2cad3;background:#f7f9fa;padding:6px 8px}.level b{font-size:11px}.level span{font-family:Georgia,serif}.level em{font-style:normal;font-size:8px;color:#788493}.level.executed{border-color:var(--green);background:#eef9f5}.level.skipped{border-color:#a8b1bb;opacity:.72}.level.touched{border-color:var(--orange);background:#fff5f1}.level.rearmed{border-color:var(--blue)}.action-panel p{font-size:12px;color:var(--muted);line-height:1.6}.action-stack{display:grid;gap:8px}.action-stack button{width:100%;text-align:left;background:#182739;padding:12px}.action-stack button span{display:block;font-weight:400;font-size:10px;color:#9aabba;margin-top:3px}.audit-note{margin-top:13px;border-top:1px solid var(--line);padding-top:12px}.audit-note b,.audit-note span{display:block;font-size:10px}.audit-note span{color:var(--muted);word-break:break-all;margin-top:4px}.timeline-panel{grid-column:1/2}.mini-timeline{display:grid}.mini-timeline div{display:grid;grid-template-columns:12px 90px 1fr 35px;align-items:center;padding:8px 0;border-top:1px solid #edf0f3;font-size:11px}.mini-timeline i{width:6px;height:6px;background:var(--orange);border-radius:50%}.mini-timeline time,.mini-timeline span{color:var(--muted)}.table-panel,.event-panel{padding:0;overflow:hidden}.table-panel .panel-head,.event-panel .panel-head{padding:19px 19px 0}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse}th{text-align:left;background:#f4f6f8;color:#778393;font-size:10px;text-transform:uppercase;letter-spacing:.6px;padding:11px 14px}td{padding:13px 14px;border-top:1px solid #edf0f3;white-space:nowrap}td code{font-size:10px;color:#6c7887}.tag{display:inline-block;border-radius:4px;background:#eef2f5;padding:3px 6px;font-size:9px;font-weight:800}.tag.buy{color:var(--red);background:#fff0f1}.tag.sell{color:var(--green);background:#eaf8f3}.status{font-size:10px;font-weight:800}.lot-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.lot-card{background:#fff;border:1px solid var(--line);border-top:3px solid var(--green);border-radius:10px;padding:17px}.lot-card.closed{border-top-color:#aab3bd}.lot-card>div{display:flex;justify-content:space-between}.lot-card em{font-size:10px;font-style:normal;color:var(--muted)}.lot-card h3{font-family:Georgia,serif;font-size:24px;margin:18px 0 0}.lot-card p{color:var(--muted);font-size:11px}.lot-card dl{display:grid;grid-template-columns:1fr 1fr;margin:18px 0;gap:7px;border-top:1px solid var(--line);padding-top:12px}.lot-card dt{color:var(--muted);font-size:10px}.lot-card dd{text-align:right;margin:0}.lot-card small{color:#9aa4af;font-size:9px}.positive{color:var(--green)}.event-head form{display:flex;gap:6px}.event-head input{border:1px solid var(--line);border-radius:6px;padding:8px;width:260px}.event-head button{padding:8px 12px}.event-list details{border-top:1px solid var(--line)}.event-list summary{display:grid;grid-template-columns:55px 155px 1fr 90px;align-items:center;gap:10px;padding:12px 18px;cursor:pointer}.event-list summary:hover{background:#f8fafb}.event-list summary time,.event-list summary code,.seq{color:var(--muted);font-size:10px}.event-list summary b{font-size:11px}.event-detail{padding:0 18px 18px 75px}.event-detail dl{display:grid;grid-template-columns:110px 1fr;gap:4px;font-size:10px}.event-detail dt{color:var(--muted)}.event-detail dd{margin:0;word-break:break-all}.event-detail pre{background:#142130;color:#cbd6e1;padding:13px;border-radius:7px;overflow:auto;font-size:10px}.empty{background:#fff;border:1px dashed #cbd2d9;border-radius:12px;text-align:center;padding:70px 20px}.empty.inline{grid-column:1/-1}.empty-icon{width:58px;height:58px;border-radius:15px;background:var(--orange);color:#fff;display:grid;place-items:center;font:700 30px Georgia;margin:auto}.empty h2{font-family:Georgia,serif}.empty p{max-width:530px;margin:auto;color:var(--muted);line-height:1.7}footer{text-align:center;color:#98a1ac;font-size:10px;padding:28px 0 4px}@media(max-width:1150px){.metrics{grid-template-columns:repeat(3,1fr)}.dashboard-grid{grid-template-columns:1fr}.timeline-panel{grid-column:auto}.lot-grid{grid-template-columns:repeat(2,1fr)}}@media(max-width:760px){.shell{display:block}aside{position:static;height:auto}.runs{max-height:130px;overflow:auto}.safety{margin-top:20px}main{padding:20px 14px}.topbar{align-items:flex-start}.top-actions{flex-direction:column}.metrics{grid-template-columns:repeat(2,1fr)}.lot-grid{grid-template-columns:1fr}.tabs{overflow:auto}.event-list summary{grid-template-columns:48px 1fr}.event-list summary time,.event-list summary code{display:none}.event-head{display:block}.event-head form{margin-top:10px}.event-head input{width:100%}}
.new-run select,.data-controls select{width:100%;border:1px solid #34475c;background:#0e1825;color:#fff;border-radius:7px;padding:9px;outline:none}.new-run select{margin-bottom:10px}.data-hero{display:flex;justify-content:space-between;gap:30px;align-items:center;background:#fff;border:1px solid var(--line);border-left:4px solid var(--orange);border-radius:11px;padding:24px 26px;margin-bottom:14px}.data-hero h2{font-family:Georgia,"Songti SC",serif;font-size:24px;margin:5px 0 8px}.data-hero p{color:var(--muted);margin:0;line-height:1.7}.data-hero dl{display:flex;gap:26px;margin:0;white-space:nowrap}.data-hero dt{font-size:9px;color:var(--muted);letter-spacing:.7px}.data-hero dd{font-family:Georgia,serif;font-size:16px;margin:5px 0 0}.preview-chart{min-height:430px}.data-head small{color:var(--muted);font-size:10px}.data-controls{display:flex;gap:7px;align-items:end}.data-controls label{font-size:9px;color:var(--muted);min-width:125px}.data-controls select{display:block;margin-top:4px;background:#fff;color:var(--ink);border-color:var(--line);min-width:125px}.data-controls button{padding:9px 13px}.candle{width:auto}@media(max-width:900px){.data-hero{display:block}.data-hero dl{margin-top:18px;white-space:normal}.data-head{display:block}.data-controls{margin-top:12px;flex-wrap:wrap}}@media(max-width:600px){.data-hero dl{display:grid;gap:10px}.data-controls{display:grid;grid-template-columns:1fr 1fr}.data-controls button{grid-column:1/-1}}
.new-run .step-start{background:#24405e;border:1px solid #3a5b7c}.rights-summary{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px}.rights-summary article{background:#172535;color:#fff;border-radius:10px;padding:14px}.rights-summary span{display:block;color:#91a1b3;font-size:10px}.rights-summary strong{display:block;font:24px Georgia,serif;margin-top:5px}.rights-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.right-card{background:#fff;border:1px solid var(--line);border-top:3px solid var(--blue);border-radius:10px;padding:14px;min-width:0}.right-card.sell{border-top-color:var(--green)}.right-card.deferred{border-top-color:#8995a3}.right-card.blocked{border-top-color:#d59b23;background:#fffaf0}.right-card.released,.right-card.expired{opacity:.72}.right-card>div{display:flex;justify-content:space-between;align-items:center;gap:6px}.right-card em{font-style:normal;font-size:9px;font-weight:800;color:var(--muted)}.right-card h3{font:21px Georgia,serif;margin:14px 0}.right-card dl{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin:0;border-top:1px solid var(--line);padding-top:10px}.right-card dt{color:var(--muted);font-size:9px}.right-card dd{text-align:right;margin:0;font-size:10px;overflow:hidden;text-overflow:ellipsis}.right-card small{display:block;color:#9aa4af;margin-top:10px}.step-console{display:grid;grid-template-columns:minmax(260px,1.25fr) minmax(290px,1fr) minmax(220px,.75fr);gap:18px;align-items:center;background:#172535;color:#fff;border-radius:11px;padding:20px 22px;margin-bottom:14px;box-shadow:0 7px 24px #0e192619}.step-console h2{font-family:Georgia,"Songti SC",serif;font-size:21px;margin:4px 0}.step-summary p{color:#aebccc;margin:5px 0 10px}.step-summary p b{color:#fff}.progress-track{height:7px;border-radius:10px;background:#2c3d50;overflow:hidden}.progress-track i{display:block;height:100%;background:var(--orange);border-radius:10px;min-width:2px;transition:width .4s ease}.current-bar{border-left:1px solid #35465a;padding-left:18px}.current-bar>span,.current-bar>small{display:block;color:#91a1b3;font-size:10px}.current-bar>strong{display:block;font-family:Georgia,serif;font-size:17px;margin:5px 0 9px}.current-bar dl{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin:0}.current-bar dl div{background:#203247;border-radius:6px;padding:6px}.current-bar dt{font-size:8px;color:#8293a5}.current-bar dd{margin:2px 0 0;font-family:Georgia,serif}.step-actions{display:grid;gap:7px}.step-actions button{width:100%;text-align:left}.step-actions button span{display:block;font-size:9px;font-weight:400;opacity:.72;margin-top:2px}.step-actions .secondary{background:#2a3d52}.step-actions .pause{background:#b94b48}.auto-play-form{display:grid;grid-template-columns:1fr 1.25fr;gap:7px}.auto-play-form label{font-size:9px;color:#91a1b3}.auto-play-form select{display:block;width:100%;margin-top:3px;border:1px solid #486078;background:#203247;color:#fff;border-radius:7px;padding:8px}.playing-now{display:grid;grid-template-columns:12px 1fr;align-items:center;background:#163d35;border:1px solid #286454;border-radius:8px;padding:10px}.playing-now i{width:9px;height:9px;border-radius:50%;background:#53d8a6;box-shadow:0 0 0 0 #53d8a688;animation:pulse 1.2s infinite}.playing-now b{font-size:11px}.playing-now span{grid-column:2;color:#9ed6c3;font-size:9px}.mode.live{color:#147a59;border-color:#a9dec9;background:#edfaf5}.mode.live i{background:#20a778;box-shadow:0 0 0 0 #20a77866;animation:pulse 1.2s infinite}@keyframes pulse{70%{box-shadow:0 0 0 7px transparent}100%{box-shadow:0 0 0 0 transparent}}.complete-mark{color:#91dec3;background:#1b3b38;border:1px solid #2b6158;border-radius:8px;padding:14px;text-align:center;font-weight:700}.section-title{margin:30px 2px 12px;padding-top:4px}.section-title h2{font-family:Georgia,"Songti SC",serif;font-size:22px;margin:4px 0}.table-panel+.section-title{margin-top:30px}@media(max-width:1200px){.rights-grid{grid-template-columns:repeat(2,1fr)}}@media(max-width:1100px){.step-console{grid-template-columns:1fr 1fr}.step-actions{grid-column:1/-1;grid-template-columns:1fr 1fr}}@media(max-width:700px){.rights-summary{grid-template-columns:repeat(2,1fr)}.rights-grid{grid-template-columns:1fr}.step-console{display:block}.current-bar{border-left:0;border-top:1px solid #35465a;padding:14px 0 0;margin-top:14px}.step-actions{display:grid;margin-top:14px}.auto-play-form{grid-template-columns:1fr}}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn cache_stamp(sequence: i64) -> SnapshotStamp {
        SnapshotStamp {
            sequence,
            duplicate_events: 0,
            processed_bars: usize::try_from(sequence).unwrap_or_default(),
            descriptor_identity_sha256: Some(format!("descriptor-{sequence}")),
            playback: WebPlaybackControl::default(),
        }
    }

    #[test]
    fn snapshot_cache_requires_the_complete_stamp_and_enforces_lru_limits() {
        let mut cache = SnapshotCache::default();
        let first = Arc::<[u8]>::from(vec![1_u8; 64]);
        cache.insert("run-0".to_owned(), cache_stamp(0), Arc::clone(&first));
        assert!(Arc::ptr_eq(
            &cache.get("run-0", &cache_stamp(0)).unwrap(),
            &first
        ));
        assert!(cache.get("run-0", &cache_stamp(1)).is_none());

        for sequence in 1..=SNAPSHOT_CACHE_MAX_ENTRIES {
            cache.insert(
                format!("run-{sequence}"),
                cache_stamp(i64::try_from(sequence).unwrap()),
                Arc::from(vec![u8::try_from(sequence).unwrap(); 64]),
            );
        }
        assert_eq!(cache.entries.len(), SNAPSHOT_CACHE_MAX_ENTRIES);
        assert!(!cache.entries.contains_key("run-0"));

        cache.insert(
            "oversized".to_owned(),
            cache_stamp(99),
            Arc::from(vec![0_u8; SNAPSHOT_CACHE_MAX_ITEM_BYTES + 1]),
        );
        assert!(!cache.entries.contains_key("oversized"));
        assert!(cache.total_bytes <= SNAPSHOT_CACHE_MAX_BYTES);
    }

    #[test]
    fn snapshot_cache_builds_once_per_stable_stamp_and_never_caches_errors() {
        let cache = StdMutex::new(SnapshotCache::default());
        let builds = AtomicUsize::new(0);
        for _ in 0..5 {
            let json = cached_snapshot_json(&cache, "stable", cache_stamp(7), true, || {
                builds.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Arc::from(Vec::from("stable-json")))
            })
            .unwrap();
            assert_eq!(&*json, b"stable-json");
        }
        assert_eq!(builds.load(AtomicOrdering::SeqCst), 1);

        let failure = cached_snapshot_json(&cache, "error", cache_stamp(8), true, || {
            builds.fetch_add(1, AtomicOrdering::SeqCst);
            anyhow::bail!("projection failed")
        });
        assert!(failure.is_err());
        cached_snapshot_json(&cache, "error", cache_stamp(8), true, || {
            builds.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Arc::from(Vec::from("recovered")))
        })
        .unwrap();
        assert_eq!(builds.load(AtomicOrdering::SeqCst), 3);

        for _ in 0..2 {
            cached_snapshot_json(&cache, "pending", cache_stamp(9), false, || {
                builds.fetch_add(1, AtomicOrdering::SeqCst);
                Ok(Arc::from(Vec::from("uncached")))
            })
            .unwrap();
        }
        assert_eq!(builds.load(AtomicOrdering::SeqCst), 5);
    }

    #[test]
    fn displayed_units_are_share_quantity_not_tranche_count() {
        let two_half_tranches_quantity = 3_000 + 3_000;
        assert_eq!(quantity_units(two_half_tranches_quantity, 6_000), "1 份");
        assert_eq!(quantity_units(3_000, 6_000), "3000 股（不足整份）");
    }

    #[test]
    fn run_ids_are_restricted_to_safe_url_characters() {
        assert_eq!(sanitize_run_id("web run/<42>_ok"), "webrun42_ok");
        assert_eq!(sanitize_run_id("***"), "");
    }

    #[test]
    fn mutation_requests_require_fixed_host_origin_and_csrf_forms() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("127.0.0.1:8787"));
        assert!(request_headers_are_trusted(
            &Method::GET,
            &headers,
            "127.0.0.1:8787"
        ));
        assert!(!request_headers_are_trusted(
            &Method::POST,
            &headers,
            "127.0.0.1:8787"
        ));
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://127.0.0.1:8787"),
        );
        headers.insert("sec-fetch-site", HeaderValue::from_static("same-origin"));
        assert!(request_headers_are_trusted(
            &Method::POST,
            &headers,
            "127.0.0.1:8787"
        ));
        headers.insert(header::HOST, HeaderValue::from_static("attacker.invalid"));
        assert!(!request_headers_are_trusted(
            &Method::POST,
            &headers,
            "127.0.0.1:8787"
        ));
    }

    #[test]
    fn empty_dashboard_is_a_complete_product_page() {
        let config = Config::load("configs/default.yaml").unwrap();
        let datasets = vec![DatasetOption {
            id: "sample.csv".to_owned(),
            label: "sample".to_owned(),
            sha256: sha256_file(Path::new("tests/fixtures/sample.csv")).unwrap(),
            bars: Arc::new(
                CsvReplayFeed::load("tests/fixtures/sample.csv", &config.symbol)
                    .unwrap()
                    .bars()
                    .to_vec(),
            ),
        }];
        let page = render_dashboard(
            &config,
            &[],
            None,
            &[],
            &[],
            &DashboardQuery::default(),
            &datasets,
            "sample.csv",
            None,
            PlaybackView::default(),
            &[],
            "csrf-test",
        );
        assert!(page.contains("GridEdge-T 模拟控制台"));
        assert!(page.contains("三年 5 分钟行情已就绪"));
        assert!(page.contains("SIMULATION ONLY"));
        assert!(page.contains("name=\"csrf_token\" value=\"csrf-test\""));
        assert!(page.contains("<script src=\"/assets/dashboard.js\" defer></script>"));
        assert!(!page.contains("http-equiv=\"refresh\""));
    }

    #[test]
    fn full_chart_is_aggregated_to_a_bounded_number_of_candles() {
        let feed = CsvReplayFeed::load("tests/fixtures/sample.csv", "600000.SH").unwrap();
        let bars: Vec<_> = (0..100).flat_map(|_| feed.bars().iter().cloned()).collect();
        assert!(chart_bars(&bars, Some("all")).len() <= 480);
    }

    #[test]
    fn step_replay_resumes_from_the_event_ledger_and_finishes() {
        let temporary = tempfile::TempDir::new().unwrap();
        let mut config = Config::load("configs/default.yaml").unwrap();
        config.database = temporary
            .path()
            .join("step.db")
            .to_string_lossy()
            .to_string();
        let data_path = PathBuf::from("tests/fixtures/sample.csv");
        let dataset = DatasetOption {
            id: "sample.csv".to_owned(),
            label: "sample".to_owned(),
            sha256: sha256_file(&data_path).unwrap(),
            bars: Arc::new(
                CsvReplayFeed::load(&data_path, &config.symbol)
                    .unwrap()
                    .bars()
                    .to_vec(),
            ),
        };

        step_start_sync(
            config.clone(),
            dataset.clone(),
            "step-test".to_owned(),
            None,
        )
        .unwrap();
        let mut store = SqliteStore::open(&config.database).unwrap();
        store.migrate().unwrap();
        assert_eq!(
            store
                .event_count_by_type("step-test", crate::event::EventType::MarketBarProcessed)
                .unwrap(),
            0
        );

        assert!(!step_once_sync(
            config.clone(),
            vec![dataset.clone()],
            "step-test".to_owned(),
            None,
        )
        .unwrap());
        assert!(!step_once_sync(
            config.clone(),
            vec![dataset.clone()],
            "step-test".to_owned(),
            None,
        )
        .unwrap());
        let store = SqliteStore::open(&config.database).unwrap();
        assert_eq!(
            store
                .event_count_by_type("step-test", crate::event::EventType::MarketBarProcessed)
                .unwrap(),
            2
        );

        step_finish_sync(config.clone(), vec![dataset], "step-test".to_owned(), None).unwrap();
        let store = SqliteStore::open(&config.database).unwrap();
        assert_eq!(
            store
                .event_count_by_type("step-test", crate::event::EventType::MarketBarProcessed)
                .unwrap(),
            21
        );
        let initial = initial_state(&config, "step-test");
        let (snapshot, snapshot_sequence) = store.rebuild(initial.clone()).unwrap();
        let (full, full_sequence) = store.rebuild_full(initial).unwrap();
        assert_eq!(snapshot_sequence, full_sequence);
        assert_eq!(snapshot.mode, ServiceMode::Stopped);
        assert_eq!(
            serde_json::to_value(snapshot).unwrap(),
            serde_json::to_value(full).unwrap()
        );
    }

    #[test]
    fn step_dashboard_does_not_render_future_bars() {
        let config = Config::load("configs/default.yaml").unwrap();
        let feed = CsvReplayFeed::load("tests/fixtures/sample.csv", &config.symbol).unwrap();
        let state = initial_state(&config, "hidden-future");
        let dataset = DatasetOption {
            id: "sample.csv".to_owned(),
            label: "sample".to_owned(),
            sha256: sha256_file(Path::new("tests/fixtures/sample.csv")).unwrap(),
            bars: Arc::new(feed.bars().to_vec()),
        };
        let progress = ReplayProgress {
            descriptor: ReplayDescriptor {
                dataset_id: dataset.id.clone(),
                data_sha256: dataset.sha256.clone(),
                symbol: config.symbol.clone(),
                total_bars: feed.bars().len(),
                first_timestamp: feed.bars().first().unwrap().timestamp,
                last_timestamp: feed.bars().last().unwrap().timestamp,
            },
            processed_bars: 1,
        };
        let page = render_dashboard(
            &config,
            &["hidden-future".to_owned()],
            Some(&state),
            &[],
            feed.bars(),
            &DashboardQuery::default(),
            &[dataset],
            "sample.csv",
            Some(&progress),
            PlaybackView {
                active: true,
                interval_ms: 1_000,
                command_version: 1,
            },
            &[],
            "csrf-test",
        );
        assert!(page.contains("2026-01-05 09:30"));
        assert!(!page.contains("2026-01-05 09:31"));
        assert!(!page.contains("http-equiv=\"refresh\""));
        assert!(page.contains("data-playback-active=\"true\""));
        assert!(page.contains("<script src=\"/assets/dashboard.js\" defer></script>"));
        assert!(page.contains("data-dynamic-step method=\"post\""));
        assert!(page.contains("自动播放中"));
        assert!(page.contains("网格权利与递延结果"));
        assert!(page.contains("保本拦截"));
        assert!(page.contains("尚未产生网格权利"));
        assert!(page.contains("订单与成交结果"));
        assert!(page.contains("策略批次与持仓"));
        assert!(page.contains("事件账本"));
    }

    #[test]
    fn automatic_playback_can_pause_without_consuming_a_future_bar() {
        let temporary = tempfile::TempDir::new().unwrap();
        let mut config = Config::load("configs/default.yaml").unwrap();
        config.database = temporary
            .path()
            .join("auto-step.db")
            .to_string_lossy()
            .to_string();
        let data_path = PathBuf::from("tests/fixtures/sample.csv");
        let dataset = DatasetOption {
            id: "sample.csv".to_owned(),
            label: "sample".to_owned(),
            sha256: sha256_file(&data_path).unwrap(),
            bars: Arc::new(
                CsvReplayFeed::load(&data_path, &config.symbol)
                    .unwrap()
                    .bars()
                    .to_vec(),
            ),
        };
        step_start_sync(
            config.clone(),
            dataset.clone(),
            "auto-pause".to_owned(),
            None,
        )
        .unwrap();
        let cancelled = AtomicBool::new(true);
        let result = step_play_sync(
            config.clone(),
            vec![dataset],
            "auto-pause".to_owned(),
            0,
            None,
            &cancelled,
            None,
        )
        .unwrap();
        assert_eq!(result, PlaybackResult::Paused);
        let store = SqliteStore::open(&config.database).unwrap();
        assert_eq!(
            store
                .event_count_by_type("auto-pause", crate::event::EventType::MarketBarProcessed,)
                .unwrap(),
            0
        );
    }

    #[test]
    fn automatic_playback_runs_each_bar_and_stops_at_the_end() {
        let temporary = tempfile::TempDir::new().unwrap();
        let mut config = Config::load("configs/default.yaml").unwrap();
        config.database = temporary
            .path()
            .join("auto-complete.db")
            .to_string_lossy()
            .to_string();
        let data_path = PathBuf::from("tests/fixtures/sample.csv");
        let dataset = DatasetOption {
            id: "sample.csv".to_owned(),
            label: "sample".to_owned(),
            sha256: sha256_file(&data_path).unwrap(),
            bars: Arc::new(
                CsvReplayFeed::load(&data_path, &config.symbol)
                    .unwrap()
                    .bars()
                    .to_vec(),
            ),
        };
        step_start_sync(
            config.clone(),
            dataset.clone(),
            "auto-complete".to_owned(),
            None,
        )
        .unwrap();
        let result = step_play_sync(
            config.clone(),
            vec![dataset],
            "auto-complete".to_owned(),
            0,
            None,
            &AtomicBool::new(false),
            None,
        )
        .unwrap();
        assert_eq!(result, PlaybackResult::Complete);
        let store = SqliteStore::open(&config.database).unwrap();
        assert_eq!(
            store
                .event_count_by_type("auto-complete", crate::event::EventType::MarketBarProcessed,)
                .unwrap(),
            21
        );
        let (state, _) = store
            .rebuild(initial_state(&config, "auto-complete"))
            .unwrap();
        assert_eq!(state.mode, ServiceMode::Stopped);
    }
}
