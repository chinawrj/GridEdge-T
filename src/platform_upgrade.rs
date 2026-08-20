//! Append-only authorization and activation facts for replacing the binary
//! that hosts an otherwise unchanged algorithm contract.

use crate::{decision::AlgorithmManifest, domain::StrategyState};
use anyhow::{bail, Context, Result};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
use uuid::Uuid;

pub const PLATFORM_UPGRADE_CERTIFICATION_PROFILE: &str =
    "GRIDEDGE_PLATFORM_UPGRADE_CERTIFICATION_V1";
pub const PLATFORM_UPGRADE_AUTHORIZATION_KIND: &str = "LOCAL_OPERATOR_EXPLICIT";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformUpgradeAuthorized {
    pub upgrade_id: String,
    pub from_platform_sha256: String,
    pub to_platform_sha256: String,
    pub algorithm_contract_sha256: String,
    pub config_content_sha256: String,
    pub reason_code: String,
    pub operator: String,
    pub authorization_kind: String,
    pub expected_head_sequence: i64,
    pub certification_profile_version: String,
    pub certification_evidence_sha256: String,
    pub target_binary_sha256: String,
    pub authorized_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformUpgradeActivated {
    pub upgrade_id: String,
    pub authorization_event_id: String,
    pub authorization_sequence: i64,
    pub from_platform_sha256: String,
    pub to_platform_sha256: String,
    pub observed_platform_sha256: String,
    pub validated_through_sequence: i64,
    pub full_rebuild_state_sha256: String,
    pub paper_snapshot_sha256: String,
    pub paper_reconciled: bool,
    pub activated_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformUpgradeCertification {
    pub certification_profile_version: String,
    pub run_id: String,
    pub target_binary_sha256: String,
    pub full_rebuild_passed: bool,
    pub paper_reconciliation_passed: bool,
    pub outbox_v3_to_v4_passed: bool,
    pub ambiguous_fill_recovery_passed: bool,
    pub duplicate_money_action_count: u64,
    pub full_gate_passed: bool,
    pub generated_at: NaiveDateTime,
}

impl PlatformUpgradeCertification {
    pub fn parse_and_validate(bytes: &[u8], run_id: &str, target_sha256: &str) -> Result<Self> {
        let report: Self = serde_json::from_slice(bytes)
            .context("platform-upgrade certification report is invalid")?;
        if report.certification_profile_version != PLATFORM_UPGRADE_CERTIFICATION_PROFILE
            || report.run_id != run_id
            || report.target_binary_sha256 != target_sha256
            || !report.full_rebuild_passed
            || !report.paper_reconciliation_passed
            || !report.outbox_v3_to_v4_passed
            || !report.ambiguous_fill_recovery_passed
            || report.duplicate_money_action_count != 0
            || !report.full_gate_passed
        {
            bail!("platform-upgrade certification report does not authorize this artifact")
        }
        validate_sha256(&report.target_binary_sha256, "certified target binary")?;
        Ok(report)
    }
}

pub fn validate_sha256(value: &str, field: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{field} is not a canonical lowercase SHA-256")
    }
    Ok(())
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read target binary {}", path.display()))?;
    Ok(sha256_bytes(&bytes))
}

pub fn algorithm_contract_sha256(manifest: &AlgorithmManifest) -> Result<String> {
    let value = serde_json::json!({
        "algorithm_name": manifest.algorithm_name,
        "algorithm_version": manifest.algorithm_version,
        "supported_contract_versions": manifest.supported_contract_versions,
        "deterministic": manifest.deterministic,
        "supports_checkpoint": manifest.supports_checkpoint,
        "artifact_sha256": manifest.artifact_sha256,
        "canonical_arguments": manifest.canonical_arguments,
        "environment_sha256": manifest.environment_sha256,
    });
    Ok(sha256_bytes(&serde_json::to_vec(&value)?))
}

pub fn state_sha256(state: &StrategyState) -> Result<String> {
    let mut value = serde_json::to_value(state)?;
    let object = value
        .as_object_mut()
        .context("strategy state must serialize as an object")?;
    // Identity and recovery receipts are audit metadata.  Excluding their
    // counters/last label makes this digest a stable business projection that
    // proves an upgrade did not alter cash, positions, lots, rights or orders.
    object.remove("event_count");
    object.remove("duplicate_events");
    object.remove("last_recovery");
    Ok(sha256_bytes(&serde_json::to_vec(&value)?))
}

pub fn platform_upgrade_id(
    run_id: &str,
    from_platform_sha256: &str,
    to_platform_sha256: &str,
    expected_head_sequence: i64,
    certification_evidence_sha256: &str,
) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!(
            "platform-upgrade:{run_id}:{from_platform_sha256}:{to_platform_sha256}:{expected_head_sequence}:{certification_evidence_sha256}"
        )
        .as_bytes(),
    )
    .to_string()
}

pub fn manifests_match_except_platform(
    expected: &AlgorithmManifest,
    actual: &AlgorithmManifest,
) -> bool {
    expected.algorithm_name == actual.algorithm_name
        && expected.algorithm_version == actual.algorithm_version
        && expected.supported_contract_versions == actual.supported_contract_versions
        && expected.deterministic == actual.deterministic
        && expected.supports_checkpoint == actual.supports_checkpoint
        && expected.artifact_sha256 == actual.artifact_sha256
        && expected.canonical_arguments == actual.canonical_arguments
        && expected.environment_sha256 == actual.environment_sha256
}
