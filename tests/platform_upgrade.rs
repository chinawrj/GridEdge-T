use anyhow::{bail, Result};
use chrono::NaiveDateTime;
use gridedge_t::{
    data::MarketBar,
    decision::{
        AlgorithmManifest, DecisionRequest, DecisionResponse, QuantDecisionAlgorithm,
        ResourceAwareWholeQAlgorithm, DECISION_CONTRACT_VERSION,
    },
    event::{EventEnvelope, EventType},
    journal::{EventReader, SqliteStore},
    platform_upgrade::{
        platform_upgrade_id, sha256_file, state_sha256, PlatformUpgradeActivated,
        PlatformUpgradeAuthorized, PlatformUpgradeCertification,
        PLATFORM_UPGRADE_CERTIFICATION_PROFILE,
    },
    run_context::RunContext,
    service::{
        activate_ongoing_resource_policy, authorize_platform_upgrade,
        effective_ongoing_resource_policy, GridAutomationService,
    },
    ths_sim::{RemoteFilledEvidence, SimulationFillRecord, SimulationOrderRecord},
    ths_sim_outbox::{stage_from_source, ThsSimOutbox},
    Config,
};
use rusqlite::{params, Connection};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};

const OLD_PLATFORM_SHA256: &str =
    "d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5";
const ALGORITHM_ARTIFACT_SHA256: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const ALGORITHM_ENVIRONMENT_SHA256: &str =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

struct FixedPlatformAlgorithm {
    platform_sha256: String,
    artifact_sha256: String,
}

impl FixedPlatformAlgorithm {
    fn new(platform_sha256: impl Into<String>) -> Self {
        Self {
            platform_sha256: platform_sha256.into(),
            artifact_sha256: ALGORITHM_ARTIFACT_SHA256.to_owned(),
        }
    }

    fn with_artifact(
        platform_sha256: impl Into<String>,
        artifact_sha256: impl Into<String>,
    ) -> Self {
        Self {
            platform_sha256: platform_sha256.into(),
            artifact_sha256: artifact_sha256.into(),
        }
    }
}

impl QuantDecisionAlgorithm for FixedPlatformAlgorithm {
    fn manifest(&self) -> AlgorithmManifest {
        AlgorithmManifest {
            algorithm_name: "resource-aware-whole-q-v1".to_owned(),
            algorithm_version: "1".to_owned(),
            supported_contract_versions: vec![DECISION_CONTRACT_VERSION],
            deterministic: true,
            supports_checkpoint: false,
            artifact_sha256: self.artifact_sha256.clone(),
            canonical_arguments: Vec::new(),
            environment_sha256: ALGORITHM_ENVIRONMENT_SHA256.to_owned(),
            platform_sha256: self.platform_sha256.clone(),
        }
    }

    fn decide(&self, request: &DecisionRequest) -> Result<DecisionResponse> {
        ResourceAwareWholeQAlgorithm.decide(request)
    }
}

struct UpgradeFixture {
    _directory: TempDir,
    config: Config,
    run_id: String,
    target_binary: PathBuf,
    certification: PathBuf,
    outbox: PathBuf,
    target_sha256: String,
    opening_state_sha256: String,
    opening_cash: Decimal,
}

impl UpgradeFixture {
    fn new(name: &str) -> Result<Self> {
        let directory = tempdir()?;
        let mut config = Config::load("configs/ths_002256_sim.yaml")?;
        config.database = directory.path().join("run.db").display().to_string();
        config.config_version = format!("platform-upgrade-{name}");
        config.validate()?;
        let run_id = format!("platform-upgrade-{name}");
        let mut service = GridAutomationService::start_new_with_algorithm(
            config.clone(),
            SqliteStore::open(&config.database)?,
            Box::new(FixedPlatformAlgorithm::new(OLD_PLATFORM_SHA256)),
            Some(run_id.clone()),
        )?;
        service.on_bar(&MarketBar {
            timestamp: at("2026-08-19 09:30:00"),
            symbol: config.symbol.clone(),
            open: Decimal::new(351, 2),
            high: Decimal::new(351, 2),
            low: Decimal::new(351, 2),
            close: Decimal::new(351, 2),
            volume: 0,
            amount: None,
        })?;
        assert_eq!(service.state.position.total, 27_500);
        let opening_state_sha256 = state_sha256(&service.state)?;
        let opening_cash = service.state.cash.available;
        drop(service);

        let head = SqliteStore::open(&config.database)?.latest_sequence(&run_id)?;
        let outbox = directory.path().join("outbox.db");
        let staged_status = stage_from_source(&config.database, &run_id, &outbox, Some(0))?;
        assert_eq!(staged_status.cursor, head);
        let mut durable_outbox = ThsSimOutbox::open(&outbox)?;
        let staged_intents = durable_outbox.staged_intents()?;
        let [intent] = staged_intents.as_slice() else {
            bail!("opening allocation must stage exactly one durable intent")
        };
        let intent = intent.clone();
        assert_eq!(intent.order.quantity, 27_500);
        durable_outbox.begin_submission(&intent.intent_id, &intent.request_sha256, &[])?;
        let remote_contract_id = "upgrade-opening-contract";
        let remote_order = SimulationOrderRecord {
            order_date: "2026-08-19".to_owned(),
            order_time: "09:30:01".to_owned(),
            symbol: intent.order.symbol.clone(),
            security_name: "兆新股份".to_owned(),
            direction: intent.order.direction,
            remark: "全部成交".to_owned(),
            quantity: intent.order.quantity,
            cancelled_quantity: 0,
            order_price: intent.order.limit_price,
            fill_price: Some(intent.order.limit_price.normalize().to_string()),
            contract_id: remote_contract_id.to_owned(),
            order_attribute: "限价委托".to_owned(),
        };
        durable_outbox.complete_submission(
            &intent.intent_id,
            &intent.request_sha256,
            remote_contract_id,
            &serde_json::to_string(&remote_order)?,
        )?;
        let fill = SimulationFillRecord {
            fill_date: "2026-08-19".to_owned(),
            fill_time: "09:30:02".to_owned(),
            symbol: intent.order.symbol.clone(),
            security_name: "兆新股份".to_owned(),
            direction: intent.order.direction,
            quantity: intent.order.quantity,
            price: intent.order.limit_price,
            amount: intent
                .order
                .limit_price
                .checked_mul(Decimal::from(intent.order.quantity))
                .expect("opening fill notional"),
            contract_id: remote_contract_id.to_owned(),
            fill_id: "upgrade-opening-fill".to_owned(),
            fill_category: "普通成交".to_owned(),
        };
        let evidence = RemoteFilledEvidence {
            evidence_contract_version: 1,
            order: remote_order,
            fills: vec![fill.clone()],
        };
        durable_outbox.record_remote_filled(
            &intent.intent_id,
            &intent.request_sha256,
            remote_contract_id,
            "2026-08-19 09:30:03.000",
            1,
            intent.order.quantity,
            &fill.amount.normalize().to_string(),
            &serde_json::to_string(&evidence)?,
        )?;
        assert_eq!(durable_outbox.status()?.cursor, head);

        let target_binary = directory.path().join("gridedge-target");
        std::fs::write(&target_binary, b"reviewed target platform f234")?;
        let target_sha256 = sha256_file(&target_binary)?;
        let certification = directory.path().join("certification.json");
        write_certification(&certification, &run_id, &target_sha256, true)?;
        Ok(Self {
            _directory: directory,
            config,
            run_id,
            target_binary,
            certification,
            outbox,
            target_sha256,
            opening_state_sha256,
            opening_cash,
        })
    }

    fn authorize(&self) -> Result<()> {
        self.authorize_using(&self.outbox)
    }

    fn authorize_using(&self, outbox: &Path) -> Result<()> {
        authorize_platform_upgrade(
            self.config.clone(),
            SqliteStore::open(&self.config.database)?,
            &self.run_id,
            &self.target_binary,
            &self.certification,
            outbox,
            "reviewed-platform-upgrade",
            "test-operator",
        )?;
        Ok(())
    }
}

#[test]
fn authorization_requires_same_run_head_and_fully_filled_outbox() -> Result<()> {
    let unresolved = UpgradeFixture::new("unresolved-outbox")?;
    let head_before =
        SqliteStore::open(&unresolved.config.database)?.latest_sequence(&unresolved.run_id)?;
    Connection::open(&unresolved.outbox)?.execute("DELETE FROM remote_execution_facts", [])?;
    assert!(unresolved.authorize().is_err());
    assert_eq!(
        SqliteStore::open(&unresolved.config.database)?.latest_sequence(&unresolved.run_id)?,
        head_before
    );

    let expected = UpgradeFixture::new("expected-outbox")?;
    let different = UpgradeFixture::new("different-outbox")?;
    let head_before =
        SqliteStore::open(&expected.config.database)?.latest_sequence(&expected.run_id)?;
    assert!(expected.authorize_using(&different.outbox).is_err());
    assert_eq!(
        SqliteStore::open(&expected.config.database)?.latest_sequence(&expected.run_id)?,
        head_before
    );
    Ok(())
}

#[test]
fn new_platform_is_rejected_without_authorization_and_old_platform_is_rejected_after_it(
) -> Result<()> {
    let fixture = UpgradeFixture::new("authorization-boundary")?;
    let head_before =
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?;
    assert!(GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )
    .is_err());
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        head_before
    );

    fixture.authorize()?;
    let authorized_head =
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?;
    assert_eq!(authorized_head, head_before + 1);
    assert!(GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(OLD_PLATFORM_SHA256)),
        fixture.run_id.clone(),
    )
    .is_err());
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        authorized_head
    );
    Ok(())
}

#[test]
fn target_platform_activates_once_after_crash_and_preserves_opening_paper_state() -> Result<()> {
    let fixture = UpgradeFixture::new("activation")?;
    fixture.authorize()?;
    let store = SqliteStore::open(&fixture.config.database)?;
    let authorization_sequence = store.latest_sequence(&fixture.run_id)?;
    drop(store);

    let service = GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )?;
    assert_eq!(service.state.position.total, 27_500);
    assert_eq!(service.state.cash.available, fixture.opening_cash);
    assert_eq!(state_sha256(&service.state)?, fixture.opening_state_sha256);
    drop(service);

    let store = SqliteStore::open(&fixture.config.database)?;
    let events = store.load_after(&fixture.run_id, 0)?;
    let authorizations = events
        .iter()
        .filter(|event| event.event_type == EventType::PlatformUpgradeAuthorized)
        .collect::<Vec<_>>();
    let activations = events
        .iter()
        .filter(|event| event.event_type == EventType::PlatformUpgradeActivated)
        .collect::<Vec<_>>();
    let ([authorization], [activation]) = (authorizations.as_slice(), activations.as_slice())
    else {
        bail!("one authorization and one activation expected")
    };
    assert_eq!(authorization.sequence_number, authorization_sequence);
    assert_eq!(
        activation.sequence_number,
        authorization.sequence_number + 1
    );
    assert_eq!(
        activation.causation_id.as_deref(),
        Some(authorization.event_id.as_str())
    );
    let activation_payload: PlatformUpgradeActivated =
        serde_json::from_value(activation.payload.clone())?;
    assert!(activation_payload.paper_reconciled);
    assert_eq!(activation_payload.to_platform_sha256, fixture.target_sha256);
    assert_eq!(
        activation_payload.observed_platform_sha256,
        fixture.target_sha256
    );
    assert_eq!(
        activation_payload.validated_through_sequence,
        authorization.sequence_number
    );
    let context = RunContext::load(&store, &fixture.run_id)?.expect("run context");
    assert!(context.pending_platform_upgrade.is_none());
    assert_eq!(
        context
            .algorithm_manifest
            .as_ref()
            .expect("effective manifest")
            .platform_sha256,
        fixture.target_sha256
    );
    drop(store);

    let recovered = GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )?;
    assert_eq!(
        state_sha256(&recovered.state)?,
        fixture.opening_state_sha256
    );
    drop(recovered);
    let events = SqliteStore::open(&fixture.config.database)?.load_after(&fixture.run_id, 0)?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::PlatformUpgradeAuthorized)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::PlatformUpgradeActivated)
            .count(),
        1
    );
    Ok(())
}

#[test]
fn ongoing_policy_is_one_bound_fact_and_uses_all_post_opening_cash() -> Result<()> {
    let fixture = UpgradeFixture::new("ongoing-resource-policy")?;
    fixture.authorize()?;
    drop(GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )?);
    stage_from_source(
        &fixture.config.database,
        &fixture.run_id,
        &fixture.outbox,
        None,
    )?;

    let store = SqliteStore::open(&fixture.config.database)?;
    let context = RunContext::load(&store, &fixture.run_id)?.expect("run context");
    let (pre_activation_state, expected_head) = store.rebuild_full(context.initial_state())?;
    let legacy = effective_ongoing_resource_policy(&fixture.config, &pre_activation_state)?;
    assert_eq!(legacy.minimum_free_cash, Decimal::new(100_000, 0));
    assert_eq!(legacy.policy_version, "LEGACY_CONFIG_V4");
    assert_eq!(pre_activation_state.position.total, 27_500);
    assert_eq!(
        pre_activation_state.cash.available,
        Decimal::new(10_341_853, 2)
    );
    let opening = pre_activation_state
        .initial_deployment_evaluation
        .as_ref()
        .expect("durable opening evaluation");
    assert_eq!(opening.selected_units, 5);
    let expected_initial_sha = hex::encode(Sha256::digest(serde_json::to_vec(opening)?));
    let expected_paper_sha = hex::encode(Sha256::digest(serde_json::to_vec(
        &pre_activation_state.snapshot()?,
    )?));
    let expected_outbox_sha = hex::encode(Sha256::digest(serde_json::to_vec(
        &ThsSimOutbox::open(&fixture.outbox)?.status()?,
    )?));
    drop(store);

    let decoy = fixture._directory.path().join("wrong-platform");
    std::fs::write(&decoy, b"not the effective platform")?;
    assert!(activate_ongoing_resource_policy(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        &fixture.run_id,
        &decoy,
        &fixture.outbox,
        "ONGOING_CASH_IS_FULL_FREE_BALANCE",
        "test-operator",
    )
    .is_err());
    assert!(activate_ongoing_resource_policy(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        &fixture.run_id,
        &fixture.target_binary,
        &fixture.outbox,
        "",
        "test-operator",
    )
    .is_err());
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        expected_head
    );

    let activation = activate_ongoing_resource_policy(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        &fixture.run_id,
        &fixture.target_binary,
        &fixture.outbox,
        "ONGOING_CASH_IS_FULL_FREE_BALANCE",
        "test-operator",
    )?;
    assert_eq!(activation.minimum_free_cash, Decimal::ZERO);
    assert_eq!(activation.position_limit_mode, "NUMERIC_SAFETY_ONLY");
    assert_eq!(
        activation.effective_max_position,
        fixture.config.max_position
    );
    assert_eq!(
        activation.config_content_sha256,
        fixture.config.content_sha256()?
    );
    assert_eq!(activation.effective_platform_sha256, fixture.target_sha256);
    assert_eq!(activation.initial_deployment_id, opening.deployment_id);
    assert_eq!(activation.initial_deployment_sha256, expected_initial_sha);
    assert_eq!(activation.expected_head_sequence, expected_head);
    assert_eq!(activation.paper_snapshot_sha256, expected_paper_sha);
    assert_eq!(activation.outbox_snapshot_sha256, expected_outbox_sha);

    let activated_head = expected_head + 1;
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        activated_head
    );
    assert!(activate_ongoing_resource_policy(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        &fixture.run_id,
        &fixture.target_binary,
        &fixture.outbox,
        "ONGOING_CASH_IS_FULL_FREE_BALANCE",
        "test-operator",
    )
    .is_err());
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        activated_head
    );

    let mut service = GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )?;
    let effective = effective_ongoing_resource_policy(&fixture.config, &service.state)?;
    assert_eq!(effective.minimum_free_cash, Decimal::ZERO);
    assert_eq!(
        effective.effective_max_position,
        fixture.config.max_position
    );
    for offset in 1_i64..=19 {
        let price = match offset {
            1..=15 => Decimal::new(3_540, 3),
            16 => Decimal::new(3_530, 3),
            17 => Decimal::new(3_520, 3),
            18 => Decimal::new(3_510, 3),
            19 => Decimal::new(3_500, 3),
            _ => unreachable!(),
        };
        service.on_bar(&MarketBar {
            timestamp: at("2026-08-19 09:30:00") + chrono::Duration::minutes(5 * offset),
            symbol: fixture.config.symbol.clone(),
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0,
            amount: None,
        })?;
    }
    let trigger = MarketBar {
        timestamp: at("2026-08-19 11:10:00"),
        symbol: fixture.config.symbol.clone(),
        open: Decimal::new(3_504, 3),
        high: Decimal::new(3_504, 3),
        low: Decimal::new(3_360, 3),
        close: Decimal::new(3_360, 3),
        volume: 0,
        amount: None,
    };
    let head_before_trigger = service.store.latest_sequence(&fixture.run_id)?;
    service.on_bar(&trigger)?;
    let decisions = service
        .store
        .load_after(&fixture.run_id, head_before_trigger)?
        .into_iter()
        .filter(|event| event.event_type == EventType::GateDecisionMade)
        .map(|event| serde_json::from_value::<DecisionRequest>(event.payload["request"].clone()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let request = decisions
        .into_iter()
        .find(|request| {
            request
                .context
                .funds_inventory
                .as_ref()
                .is_some_and(|resource| resource.market_signal_passed)
        })
        .expect("market-passing ongoing BUY decision");
    let resource = request
        .context
        .funds_inventory
        .expect("ongoing v4 resource evidence");
    assert!(resource.market_signal_passed);
    // The bar closes at 3.36, while the opportunity is evaluated at the
    // first canonical crossing price. Resource evidence must nevertheless
    // use the entire post-opening cash balance under the activated policy.
    assert_eq!(trigger.close, Decimal::new(3_360, 3));
    assert_eq!(resource.minimum_free_cash, Decimal::ZERO);
    assert_eq!(resource.cash_available, Decimal::new(10_341_853, 2));
    assert_eq!(resource.spendable_cash, resource.cash_available);
    assert_eq!(resource.cash_affordable_units, 5);
    assert_eq!(resource.resource_units, 5);
    assert_eq!(resource.target_position, fixture.config.max_position);
    assert_eq!(resource.feature_bar_ids.len(), 20);
    let events = service.store.load_after(&fixture.run_id, 0)?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == EventType::InitialDeploymentEvaluated)
            .count(),
        1
    );
    let activation_events = events
        .iter()
        .filter(|event| event.event_type == EventType::OngoingResourcePolicyActivated)
        .collect::<Vec<_>>();
    assert_eq!(activation_events.len(), 1);
    assert_eq!(
        serde_json::from_value::<gridedge_t::domain::OngoingResourcePolicyActivation>(
            activation_events[0].payload.clone(),
        )?,
        activation
    );
    Ok(())
}

#[test]
fn authorization_rejects_certification_or_target_tampering_before_any_event() -> Result<()> {
    for attack in ["report", "target"] {
        let fixture = UpgradeFixture::new(attack)?;
        let head_before =
            SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?;
        match attack {
            "report" => write_certification(
                &fixture.certification,
                &fixture.run_id,
                &fixture.target_sha256,
                false,
            )?,
            "target" => std::fs::write(&fixture.target_binary, b"tampered after certification")?,
            _ => unreachable!(),
        }
        assert!(fixture.authorize().is_err(), "attack={attack}");
        assert_eq!(
            SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
            head_before,
            "attack={attack}"
        );
    }
    Ok(())
}

#[test]
fn pending_upgrade_rejects_nonplatform_manifest_drift_without_activation() -> Result<()> {
    let fixture = UpgradeFixture::new("manifest-drift")?;
    fixture.authorize()?;
    let head_before =
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?;
    assert!(GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::with_artifact(
            &fixture.target_sha256,
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        )),
        fixture.run_id.clone(),
    )
    .is_err());
    let store = SqliteStore::open(&fixture.config.database)?;
    assert_eq!(store.latest_sequence(&fixture.run_id)?, head_before);
    assert_eq!(
        store
            .load_after(&fixture.run_id, 0)?
            .iter()
            .filter(|event| event.event_type == EventType::PlatformUpgradeActivated)
            .count(),
        0
    );
    Ok(())
}

#[test]
fn duplicate_pending_authorization_is_fail_closed_and_cannot_activate() -> Result<()> {
    let fixture = UpgradeFixture::new("duplicate-pending")?;
    fixture.authorize()?;
    let store = SqliteStore::open(&fixture.config.database)?;
    let authorization = only_upgrade_event(
        &store.load_after(&fixture.run_id, 0)?,
        EventType::PlatformUpgradeAuthorized,
    )?;
    append_raw_clone(
        &fixture.config.database,
        &authorization,
        authorization.sequence_number + 1,
        authorization.payload.clone(),
        "duplicate",
    )?;
    let attacked_head = authorization.sequence_number + 1;
    assert!(RunContext::load(
        &SqliteStore::open(&fixture.config.database)?,
        &fixture.run_id
    )
    .is_err());
    assert!(GridAutomationService::recover_with_algorithm(
        fixture.config.clone(),
        SqliteStore::open(&fixture.config.database)?,
        Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
        fixture.run_id.clone(),
    )
    .is_err());
    assert_eq!(
        SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
        attacked_head
    );
    Ok(())
}

#[test]
fn activated_chain_rejects_fork_and_downgrade_without_writing() -> Result<()> {
    for attack in ["fork", "downgrade"] {
        let fixture = UpgradeFixture::new(attack)?;
        fixture.authorize()?;
        drop(GridAutomationService::recover_with_algorithm(
            fixture.config.clone(),
            SqliteStore::open(&fixture.config.database)?,
            Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
            fixture.run_id.clone(),
        )?);
        let store = SqliteStore::open(&fixture.config.database)?;
        let events = store.load_after(&fixture.run_id, 0)?;
        let authorization = only_upgrade_event(&events, EventType::PlatformUpgradeAuthorized)?;
        let _activation = only_upgrade_event(&events, EventType::PlatformUpgradeActivated)?;
        let current_head = store.latest_sequence(&fixture.run_id)?;
        let mut forged: PlatformUpgradeAuthorized =
            serde_json::from_value(authorization.payload.clone())?;
        let next_target = match attack {
            "fork" => "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
            "downgrade" => OLD_PLATFORM_SHA256,
            _ => unreachable!(),
        };
        forged.from_platform_sha256 = if attack == "fork" {
            OLD_PLATFORM_SHA256.to_owned()
        } else {
            fixture.target_sha256.clone()
        };
        forged.to_platform_sha256 = next_target.to_owned();
        forged.target_binary_sha256 = next_target.to_owned();
        forged.expected_head_sequence = current_head;
        forged.upgrade_id = platform_upgrade_id(
            &fixture.run_id,
            &forged.from_platform_sha256,
            &forged.to_platform_sha256,
            forged.expected_head_sequence,
            &forged.certification_evidence_sha256,
        );
        append_raw_clone(
            &fixture.config.database,
            &authorization,
            current_head + 1,
            serde_json::to_value(forged)?,
            attack,
        )?;
        let attacked_head = current_head + 1;
        assert!(RunContext::load(
            &SqliteStore::open(&fixture.config.database)?,
            &fixture.run_id
        )
        .is_err());
        assert!(GridAutomationService::recover_with_algorithm(
            fixture.config.clone(),
            SqliteStore::open(&fixture.config.database)?,
            Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
            fixture.run_id.clone(),
        )
        .is_err());
        assert_eq!(
            SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
            attacked_head,
            "attack={attack}"
        );
    }
    Ok(())
}

#[test]
fn persisted_authorization_report_or_target_hash_tamper_is_rejected() -> Result<()> {
    for field in ["certification_evidence_sha256", "target_binary_sha256"] {
        let fixture = UpgradeFixture::new(field)?;
        fixture.authorize()?;
        let store = SqliteStore::open(&fixture.config.database)?;
        let authorization = only_upgrade_event(
            &store.load_after(&fixture.run_id, 0)?,
            EventType::PlatformUpgradeAuthorized,
        )?;
        let head = authorization.sequence_number;
        let mut payload = authorization.payload.clone();
        payload[field] = Value::String(
            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
        );
        let connection = Connection::open(&fixture.config.database)?;
        connection.execute_batch("DROP TRIGGER events_are_append_only_update")?;
        connection.execute(
            "UPDATE events SET payload=?1 WHERE run_id=?2 AND sequence_number=?3",
            params![payload.to_string(), fixture.run_id, head],
        )?;
        drop(connection);
        assert!(RunContext::load(
            &SqliteStore::open(&fixture.config.database)?,
            &fixture.run_id
        )
        .is_err());
        assert!(GridAutomationService::recover_with_algorithm(
            fixture.config.clone(),
            SqliteStore::open(&fixture.config.database)?,
            Box::new(FixedPlatformAlgorithm::new(&fixture.target_sha256)),
            fixture.run_id.clone(),
        )
        .is_err());
        assert_eq!(
            SqliteStore::open(&fixture.config.database)?.latest_sequence(&fixture.run_id)?,
            head,
            "field={field}"
        );
    }
    Ok(())
}

fn only_upgrade_event(events: &[EventEnvelope], event_type: EventType) -> Result<EventEnvelope> {
    let matches = events
        .iter()
        .filter(|event| event.event_type == event_type)
        .cloned()
        .collect::<Vec<_>>();
    let [event] = matches.as_slice() else {
        bail!("exactly one {event_type} expected")
    };
    Ok(event.clone())
}

fn append_raw_clone(
    database: &str,
    source: &EventEnvelope,
    sequence_number: i64,
    payload: Value,
    suffix: &str,
) -> Result<()> {
    let connection = Connection::open(database)?;
    connection.execute(
        "INSERT INTO events(
           event_id,event_type,schema_version,run_id,cycle_id,symbol,event_time,recorded_at,
           sequence_number,correlation_id,causation_id,idempotency_key,payload,config_version
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            format!("{}-{suffix}", source.event_id),
            source.event_type.to_string(),
            source.schema_version,
            source.run_id,
            source.cycle_id,
            source.symbol,
            source.event_time.to_string(),
            source.recorded_at.to_string(),
            sequence_number,
            source.correlation_id,
            source.event_id,
            format!("{}-{suffix}", source.idempotency_key),
            payload.to_string(),
            source.config_version,
        ],
    )?;
    Ok(())
}

fn write_certification(
    path: &Path,
    run_id: &str,
    target_binary_sha256: &str,
    passed: bool,
) -> Result<()> {
    let report = PlatformUpgradeCertification {
        certification_profile_version: PLATFORM_UPGRADE_CERTIFICATION_PROFILE.to_owned(),
        run_id: run_id.to_owned(),
        target_binary_sha256: target_binary_sha256.to_owned(),
        full_rebuild_passed: passed,
        paper_reconciliation_passed: passed,
        outbox_v3_to_v4_passed: passed,
        ambiguous_fill_recovery_passed: passed,
        duplicate_money_action_count: 0,
        full_gate_passed: passed,
        generated_at: at("2026-08-19 10:00:00"),
    };
    std::fs::write(path, serde_json::to_vec(&report)?)?;
    Ok(())
}

fn at(value: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap()
}
