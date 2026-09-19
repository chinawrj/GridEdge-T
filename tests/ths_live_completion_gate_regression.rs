//! Independent production-mapping regression. No network, UI, or formal state.
use anyhow::Result;
use chrono::{Duration, NaiveDateTime};
use gridedge_t::web_market::{SourceCompletion, SourceCompletionKind, SourceObservation};
use serde_json::json;
use sha2::{Digest, Sha256};

#[allow(dead_code, clippy::items_after_test_module)]
mod subject {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/bin/gridedge_ths_live.rs"
    ));

    pub(super) fn decisions(
        completion: gridedge_t::web_market::SourceCompletion,
        observation: Option<gridedge_t::web_market::SourceObservation>,
        now: NaiveDateTime,
    ) -> Result<(bool, bool)> {
        // Compute observation inputs from clocks, then invoke the SAME mapping
        // used by run(); do not reimplement its observation/completion selection.
        let observation_gate = observation
            .map(|value| {
                Ok::<_, anyhow::Error>((
                    value,
                    market_watermark_is_current(Some(value.observed_at_us), now, 60)?,
                    market_watermarks_are_contiguous(
                        value.previous_observed_at_us,
                        value.observed_at_us,
                        60,
                    )?,
                ))
            })
            .transpose()?;
        let (current, contiguous, observation_today, bars_current) =
            completion_gate_evidence(completion, observation_gate, &[], now, 60)?;
        let live = completion.kind == SourceCompletionKind::LiveContiguous;
        let today = completion.session_date == now.date() && observation_today;
        Ok((
            completion_requires_recovery(
                gridedge_t::domain::ServiceMode::Running,
                live,
                current,
                contiguous,
                today,
                0,
                bars_current,
            ),
            completion_allows_resume(
                gridedge_t::domain::ServiceMode::ReadOnly,
                live,
                current,
                contiguous,
                today,
                0,
                bars_current,
            ),
        ))
    }
}

fn at(time: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(&format!("2026-09-14 {time}"), "%Y-%m-%d %H:%M:%S").unwrap()
}

fn micros(time: &str) -> u64 {
    u64::try_from((at(time) - Duration::hours(8)).and_utc().timestamp_micros()).unwrap()
}

fn completion(previous: &str, covered: &str) -> SourceCompletion {
    SourceCompletion {
        source_sequence: 104,
        session_date: at(covered).date(),
        kind: SourceCompletionKind::LiveContiguous,
        previous_covered_through_us: Some(micros(previous)),
        covered_through_us: micros(covered),
    }
}

fn observation(previous: &str, observed: &str, covered: &str) -> SourceObservation {
    SourceObservation {
        source_sequence: 103,
        session_date: at(observed).date(),
        previous_observed_at_us: Some(micros(previous)),
        observed_at_us: micros(observed),
        covered_through_us: micros(covered),
    }
}

#[test]
fn completion_mapping_fresh_observation_cannot_mask_stale_advancing_coverage() -> Result<()> {
    let actual = subject::decisions(
        completion("13:08:00", "13:08:03"),
        Some(observation("13:09:56", "13:09:59", "13:08:00")),
        at("13:10:00"),
    )?;
    assert_eq!(actual, (true, false), "117-second-old coverage must enter/stay READ_ONLY, even with fresh observation and zero released bars");
    Ok(())
}

#[test]
fn completion_mapping_fresh_observation_cannot_mask_real_coverage_gap() -> Result<()> {
    let actual = subject::decisions(
        completion("13:08:00", "13:09:58"),
        Some(observation("13:09:56", "13:09:59", "13:08:00")),
        at("13:10:00"),
    )?;
    assert_eq!(
        actual,
        (true, false),
        "118-second coverage gap must not inherit the 3-second observation continuity"
    );
    Ok(())
}

#[test]
fn completion_mapping_fresh_completion_cannot_mask_stale_observation() -> Result<()> {
    assert_eq!(
        subject::decisions(
            completion("13:08:50", "13:09:40"),
            Some(observation("13:08:50", "13:08:55", "13:08:50")),
            at("13:10:00"),
        )?,
        (true, false),
    );
    Ok(())
}

#[test]
fn completion_mapping_fresh_contiguous_evidence_retains_resume_eligibility() -> Result<()> {
    assert_eq!(
        subject::decisions(
            completion("13:09:30", "13:09:58"),
            Some(observation("13:09:56", "13:09:59", "13:09:30")),
            at("13:10:00"),
        )?,
        (false, true),
    );
    Ok(())
}

#[test]
fn completion_mapping_absent_observation_retains_reviewed_fallback() -> Result<()> {
    assert_eq!(
        subject::decisions(completion("13:09:30", "13:09:58"), None, at("13:10:00"))?,
        (false, true)
    );
    assert_eq!(
        subject::decisions(completion("13:08:00", "13:08:03"), None, at("13:10:00"))?,
        (true, false)
    );
    assert_eq!(
        subject::decisions(completion("13:08:00", "13:09:58"), None, at("13:10:00"))?,
        (true, false)
    );
    Ok(())
}

#[test]
fn completion_mapping_exact_sixty_second_boundary_remains_inclusive() -> Result<()> {
    assert_eq!(
        subject::decisions(
            completion("13:08:00", "13:09:00"),
            Some(observation("13:09:56", "13:09:59", "13:08:00")),
            at("13:10:00"),
        )?,
        (false, true),
    );
    Ok(())
}

#[test]
fn completion_mapping_future_or_reversed_coverage_is_rejected() {
    let observed = Some(observation("13:09:56", "13:09:59", "13:09:30"));
    assert!(
        subject::decisions(completion("13:09:30", "13:10:01"), observed, at("13:10:00")).is_err()
    );
    assert!(
        subject::decisions(completion("13:09:59", "13:09:58"), observed, at("13:10:00")).is_err()
    );
}

#[test]
fn completion_mapping_missing_previous_coverage_never_resumes() -> Result<()> {
    let mut value = completion("13:09:30", "13:09:58");
    value.previous_covered_through_us = None;
    assert_eq!(
        subject::decisions(
            value,
            Some(observation("13:09:56", "13:09:59", "13:09:30")),
            at("13:10:00")
        )?,
        (true, false)
    );
    Ok(())
}

#[test]
fn completion_mapping_both_session_dates_are_required() -> Result<()> {
    let value = completion("13:09:30", "13:09:58");
    let observed = observation("13:09:56", "13:09:59", "13:09:30");
    let mut wrong_completion_day = value;
    wrong_completion_day.session_date = value.session_date.pred_opt().unwrap();
    assert_eq!(
        subject::decisions(wrong_completion_day, Some(observed), at("13:10:00"))?,
        (true, false)
    );
    let mut wrong_observation_day = observed;
    wrong_observation_day.session_date = observed.session_date.pred_opt().unwrap();
    assert_eq!(
        subject::decisions(value, Some(wrong_observation_day), at("13:10:00"))?,
        (true, false)
    );
    Ok(())
}

#[test]
fn completion_mapping_sixty_one_seconds_fails_both_independent_gates() -> Result<()> {
    let observed = Some(observation("13:09:56", "13:09:59", "13:08:58"));
    // Fresh coverage, but a 61-second predecessor gap.
    assert_eq!(
        subject::decisions(completion("13:08:58", "13:09:59"), observed, at("13:10:00"))?,
        (true, false)
    );
    // A three-second predecessor gap, but coverage is 61 seconds old.
    assert_eq!(
        subject::decisions(completion("13:08:56", "13:08:59"), observed, at("13:10:00"))?,
        (true, false)
    );
    Ok(())
}

fn status_bytes(sequence: u64, time: &str, mut payload: serde_json::Value) -> Vec<u8> {
    payload["session_date"] = json!("2026-09-14");
    payload["source_captured_at_us"] = json!(micros(time));
    payload["capture_sha256"] = json!("c".repeat(64));
    let mut event = json!({
        "spec": "gridedge.market", "schema_version": 1, "event_type": "SOURCE_STATUS",
        "source": {"source_id": "eastmoney-web-time-sales",
            "source_instance_id": "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
            "source_type": "WEB_UI", "provider": "eastmoney",
            "provider_version": "eastmoney-time-sales-dom-v6"},
        "instrument": {"venue": "XSHE", "symbol": "002256", "asset_class": "EQUITY", "currency": "CNY"},
        "source_sequence": sequence, "ts_us": micros(time), "recv_us": micros(time),
        "payload": payload, "evidence_sha256": "e".repeat(64)
    });
    let mut identity = event.clone();
    identity.as_object_mut().unwrap().remove("recv_us");
    event["event_id"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&identity).unwrap())
    ));
    serde_json::to_vec(&event).unwrap()
}

fn observation_bytes(sequence: u64, time: &str, previous: Option<&str>) -> Vec<u8> {
    let mut payload = json!({
        "status": "SOURCE_OBSERVED_CURRENT", "observed_at_us": micros(time),
        "covered_through_us": micros("13:08:00"), "latest_displayed_trade_us": micros("13:08:00"),
        "page_index": 1, "page_count": 1, "row_count": 4,
        "policy": "REVIEWED_EASTMONEY_HTTPS_DATE_LATEST_FIRST_V3",
        "source_server_observed_at_us": micros(time),
        "source_clock_origin": "EASTMONEY_HTTPS_DATE_HEADER"
    });
    if let Some(previous) = previous {
        payload["previous_observed_at_us"] = json!(micros(previous));
    }
    status_bytes(sequence, time, payload)
}

fn real_builder_decision(covered: &str) -> Result<(bool, bool)> {
    use gridedge_t::web_market::WebTradeBarBuilder;
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let topic = "gridedge/market/v1/XSHE/002256/status";
    let now = at("13:10:00");
    let history = status_bytes(
        1,
        "13:08:00",
        json!({
            "status": "SESSION_HISTORY_COMPLETE", "covered_through_us": micros("13:08:00")
        }),
    );
    builder.ingest_with_receipt_at(topic, &history, now)?;
    builder.ingest_with_receipt_at(topic, &observation_bytes(2, "13:09:56", None), now)?;
    builder.ingest_with_receipt_at(
        topic,
        &observation_bytes(3, "13:09:59", Some("13:09:56")),
        now,
    )?;
    let live = status_bytes(
        4,
        covered,
        json!({
            "status": "LIVE_CONTIGUOUS", "previous_covered_through_us": micros("13:08:00"),
            "covered_through_us": micros(covered)
        }),
    );
    let receipt = builder.ingest_with_receipt_at(topic, &live, now)?;
    assert!(builder.has_complete_session(now.date()));
    assert!(receipt.bars.is_empty());
    assert_eq!(builder.covered_through_us(), Some(micros(covered)));
    assert!(receipt.source_observation.is_none());
    subject::decisions(
        receipt.completion.unwrap(),
        builder.latest_source_observation(),
        now,
    )
}

#[test]
fn completion_mapping_real_builder_stale_coverage_never_resumes() -> Result<()> {
    assert_eq!(real_builder_decision("13:08:03")?, (true, false));
    Ok(())
}

#[test]
fn completion_mapping_real_builder_coverage_gap_never_resumes() -> Result<()> {
    assert_eq!(real_builder_decision("13:09:58")?, (true, false));
    Ok(())
}
