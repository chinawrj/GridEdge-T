use anyhow::Result;
use gridedge_t::{
    data::MarketBar,
    decision::algorithm_from_config,
    domain::ServiceMode,
    journal::SqliteStore,
    service::GridAutomationService,
    web_market::{SourceCompletionKind, WebTradeBarBuilder},
    Config,
};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{io::Write, process::Command, str::FromStr};

const TRADE_TOPIC: &str = "gridedge/market/v1/XSHE/002256/trade";
const STATUS_TOPIC: &str = "gridedge/market/v1/XSHE/002256/status";

#[test]
fn complete_trade_ticks_wait_for_a_source_watermark_before_forming_exact_ohlcv() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert!(builder
        .ingest(TRADE_TOPIC, &tick(1, "09:30:01", 355, 2, 100))?
        .is_empty());
    assert!(builder
        .ingest(TRADE_TOPIC, &tick(2, "09:32:00", 350, 2, 200))?
        .is_empty());
    assert!(builder
        .ingest(TRADE_TOPIC, &tick(3, "09:34:59", 357, 2, 300))?
        .is_empty());
    assert!(builder
        .ingest(STATUS_TOPIC, &status(4, "09:34:59", "LIVE_CONTIGUOUS"))?
        .is_empty());

    let bars = builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(5, "09:35:01", "09:34:59"),
    )?;
    assert_eq!(bars.len(), 1);
    let bar = &bars[0];
    assert_eq!(bar.timestamp.to_string(), "2026-08-21 09:35:00");
    assert_eq!(bar.open, Decimal::from_str("3.55")?);
    assert_eq!(bar.high, Decimal::from_str("3.57")?);
    assert_eq!(bar.low, Decimal::from_str("3.50")?);
    assert_eq!(bar.close, Decimal::from_str("3.57")?);
    assert_eq!(bar.volume, 600);
    assert_eq!(bar.amount, Some(Decimal::from_str("2126.00")?));
    Ok(())
}

#[test]
fn a_complete_history_watermark_releases_multiple_bars_in_time_order() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:25:00", 351, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(2, "09:34:00", 352, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(3, "09:36:00", 353, 2, 200))?;
    let bars = builder.ingest(
        STATUS_TOPIC,
        &status(4, "09:40:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    assert_eq!(
        bars.iter()
            .map(|bar| bar.timestamp.to_string())
            .collect::<Vec<_>>(),
        ["2026-08-21 09:35:00", "2026-08-21 09:40:00",]
    );
    assert_eq!(bars[0].open, Decimal::from_str("3.51")?);
    assert_eq!(bars[0].close, Decimal::from_str("3.52")?);
    assert!(builder.has_complete_session(chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap()));
    Ok(())
}

#[test]
fn an_explicit_partial_session_boundary_is_auditable_but_not_complete_history() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "13:40:01", 336, 2, 100))?;
    let boundary = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status(2, "13:40:20", "SESSION_RESUME_BOUNDARY"),
    )?;
    assert_eq!(
        boundary.completion.expect("resume boundary receipt").kind,
        SourceCompletionKind::SessionResumeBoundary
    );
    let date = chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap();
    assert!(!builder.has_complete_session(date));
    assert!(builder.has_resume_boundary(date));

    let live = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(3, "13:40:40", "13:40:20"),
    )?;
    assert_eq!(
        live.completion
            .expect("live receipt after boundary")
            .previous_covered_through_us,
        Some(timestamp_us("13:40:20"))
    );
    assert!(!builder.partial_session_resume_is_safe(date));
    builder.ingest(TRADE_TOPIC, &tick(4, "13:44:00", 335, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(5, "13:45:00", 337, 2, 200))?;
    assert!(builder
        .ingest(
            STATUS_TOPIC,
            &status_with_previous(6, "13:45:01", "13:40:40"),
        )?
        .is_empty());
    assert!(!builder.partial_session_resume_is_safe(date));
    builder.ingest(TRADE_TOPIC, &tick(7, "13:46:00", 336, 2, 100))?;
    let safe = builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(8, "13:50:01", "13:45:01"),
    )?;
    assert_eq!(safe.len(), 1);
    assert_eq!(safe[0].timestamp.to_string(), "2026-08-21 13:50:00");
    assert_eq!(safe[0].open, Decimal::from_str("3.37")?);
    assert_eq!(safe[0].volume, 300);
    assert!(builder.partial_session_resume_is_safe(date));
    Ok(())
}

#[test]
fn an_explicit_same_day_discontinuity_replaces_complete_state_and_requires_a_new_full_bucket(
) -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "10:40:01", 336, 2, 100))?;
    builder.ingest(
        STATUS_TOPIC,
        &status(2, "10:45:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    let date = chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap();
    assert!(builder.has_complete_session(date));
    builder.ingest(
        STATUS_TOPIC,
        &source_observation_v3(3, "10:45:05", "10:45:04", None, "10:45:01"),
    )?;
    assert!(builder.latest_source_observation().is_some());

    builder.ingest(TRADE_TOPIC, &tick(4, "10:50:00", 399, 2, 777))?;
    let mut boundary: Value =
        serde_json::from_slice(&status(5, "10:50:20", "SESSION_RESUME_BOUNDARY"))?;
    boundary["payload"]["policy"] = json!("SAME_DAY_DISCONTINUITY_BOUNDARY_V2");
    boundary["payload"]["covered_from_us"] = json!(timestamp_us("10:50:00"));
    boundary["payload"]["previous_covered_through_us"] = json!(timestamp_us("10:45:01"));
    canonicalize_event_id(&mut boundary);
    builder.ingest(STATUS_TOPIC, &serde_json::to_vec(&boundary)?)?;
    assert!(!builder.has_complete_session(date));
    assert!(builder.has_resume_boundary(date));
    assert!(builder.latest_source_observation().is_none());

    builder.ingest(TRADE_TOPIC, &tick(6, "10:54:00", 337, 2, 100))?;
    assert!(builder
        .ingest(
            STATUS_TOPIC,
            &status_with_previous(7, "10:55:01", "10:50:20"),
        )?
        .is_empty());
    assert!(!builder.partial_session_resume_is_safe(date));
    builder.ingest(TRADE_TOPIC, &tick(8, "10:56:00", 338, 2, 100))?;
    let bars = builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(9, "11:00:01", "10:55:01"),
    )?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].timestamp.to_string(), "2026-08-21 11:00:00");
    assert!(builder.partial_session_resume_is_safe(date));
    Ok(())
}

#[test]
fn same_day_discontinuity_requires_the_exact_complete_watermark_atomically() -> Result<()> {
    let mut unattached = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let mut unattached_boundary: Value =
        serde_json::from_slice(&status(1, "10:50:20", "SESSION_RESUME_BOUNDARY"))?;
    unattached_boundary["payload"]["policy"] = json!("SAME_DAY_DISCONTINUITY_BOUNDARY_V2");
    unattached_boundary["payload"]["covered_from_us"] = json!(timestamp_us("10:50:00"));
    unattached_boundary["payload"]["previous_covered_through_us"] = json!(timestamp_us("10:45:01"));
    canonicalize_event_id(&mut unattached_boundary);
    assert!(unattached
        .ingest(STATUS_TOPIC, &serde_json::to_vec(&unattached_boundary)?)
        .is_err());

    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "10:40:01", 336, 2, 100))?;
    builder.ingest(
        STATUS_TOPIC,
        &status(2, "10:45:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    let before = format!("{builder:?}");
    let mut boundary: Value =
        serde_json::from_slice(&status(3, "10:50:20", "SESSION_RESUME_BOUNDARY"))?;
    boundary["payload"]["policy"] = json!("SAME_DAY_DISCONTINUITY_BOUNDARY_V2");
    boundary["payload"]["covered_from_us"] = json!(timestamp_us("10:50:00"));
    boundary["payload"]["previous_covered_through_us"] = json!(timestamp_us("10:45:00"));
    canonicalize_event_id(&mut boundary);
    assert!(builder
        .ingest(STATUS_TOPIC, &serde_json::to_vec(&boundary)?)
        .is_err());
    assert_eq!(format!("{builder:?}"), before);
    Ok(())
}

#[test]
fn partial_boundary_schema_and_live_predecessor_are_atomic_fail_closed_gates() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "13:40:01", 336, 2, 100))?;

    let mut forged_boundary: Value =
        serde_json::from_slice(&status(2, "13:40:20", "SESSION_RESUME_BOUNDARY"))?;
    forged_boundary["payload"]["policy"] = json!("UNREVIEWED");
    canonicalize_event_id(&mut forged_boundary);
    assert!(builder
        .ingest(STATUS_TOPIC, &serde_json::to_vec(&forged_boundary)?)
        .is_err());
    builder.ingest(
        STATUS_TOPIC,
        &status(2, "13:40:20", "SESSION_RESUME_BOUNDARY"),
    )?;

    assert!(builder
        .ingest(
            STATUS_TOPIC,
            &status(3, "13:40:20", "SESSION_RESUME_BOUNDARY"),
        )
        .is_err());

    let mismatched = status_with_previous(3, "13:40:40", "13:40:19");
    let before_mismatch = format!("{builder:?}");
    assert!(builder.ingest(STATUS_TOPIC, &mismatched).is_err());
    assert_eq!(format!("{builder:?}"), before_mismatch);
    builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(3, "13:40:40", "13:40:20"),
    )?;
    Ok(())
}

#[test]
fn partial_boundary_discards_auction_through_partial_bucket_and_keeps_next_bucket_open(
) -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:25:00", 331, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(2, "09:31:00", 332, 2, 100))?;
    builder.ingest(
        STATUS_TOPIC,
        &resume_boundary_with_from(3, "09:31:00", "09:25:00"),
    )?;
    builder.ingest(TRADE_TOPIC, &tick(4, "09:34:59", 333, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(5, "09:35:00", 334, 2, 200))?;
    let bars = builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(6, "09:40:01", "09:31:00"),
    )?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].timestamp.to_string(), "2026-08-21 09:40:00");
    assert_eq!(bars[0].open, Decimal::from_str("3.34")?);
    assert_eq!(bars[0].volume, 200);
    Ok(())
}

#[test]
fn partial_boundary_discards_late_rows_in_exact_lunch_and_close_buckets() -> Result<()> {
    for (boundary_time, next_tick, next_watermark) in [
        ("11:30:00", "13:00:00", "13:05:01"),
        ("15:00:00", "15:00:00", "15:00:00"),
    ] {
        let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
        builder.ingest(
            STATUS_TOPIC,
            &resume_boundary_with_from(1, boundary_time, boundary_time),
        )?;
        builder.ingest(TRADE_TOPIC, &tick(2, boundary_time, 399, 2, 900))?;
        if boundary_time == "11:30:00" {
            builder.ingest(TRADE_TOPIC, &tick(3, next_tick, 335, 2, 100))?;
            let bars = builder.ingest(
                STATUS_TOPIC,
                &status_with_previous(4, next_watermark, boundary_time),
            )?;
            assert_eq!(bars.len(), 1);
            assert_eq!(bars[0].timestamp.to_string(), "2026-08-21 13:05:00");
            assert_eq!(bars[0].open, Decimal::from_str("3.35")?);
            assert_eq!(bars[0].volume, 100);
        } else {
            assert!(!format!("{builder:?}").contains("volume: 900"));
        }
    }
    Ok(())
}

#[test]
fn a_complete_session_cannot_be_replaced_by_partial_state_and_failure_is_atomic() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 331, 2, 100))?;
    builder.ingest(
        STATUS_TOPIC,
        &status(2, "09:35:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    builder.ingest(TRADE_TOPIC, &tick(3, "09:36:00", 332, 2, 200))?;
    let before = format!("{builder:?}");
    assert!(builder
        .ingest(
            STATUS_TOPIC,
            &resume_boundary_with_from(4, "09:40:00", "09:36:00"),
        )
        .is_err());
    assert_eq!(format!("{builder:?}"), before);
    let bars = builder.ingest(
        STATUS_TOPIC,
        &status_with_previous(4, "09:40:01", "09:35:01"),
    )?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].open, Decimal::from_str("3.32")?);
    Ok(())
}

#[test]
fn source_sequence_preserves_same_second_open_and_close_at_a_bucket_boundary() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:35:00", 334, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(2, "09:35:00", 336, 2, 200))?;
    let bars = builder.ingest(STATUS_TOPIC, &status(3, "09:40:01", "LIVE_CONTIGUOUS"))?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].timestamp.to_string(), "2026-08-21 09:40:00");
    assert_eq!(bars[0].open, Decimal::from_str("3.34")?);
    assert_eq!(bars[0].close, Decimal::from_str("3.36")?);
    assert_eq!(bars[0].volume, 300);
    Ok(())
}

#[test]
fn ingest_receipt_binds_released_bars_to_the_exact_completion_status() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
    let receipt =
        builder.ingest_with_receipt(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
    assert_eq!(receipt.bars.len(), 1);
    let completion = receipt.completion.expect("completion receipt");
    assert_eq!(completion.source_sequence, 2);
    assert_eq!(completion.kind, SourceCompletionKind::LiveContiguous);
    assert_eq!(completion.previous_covered_through_us, None);
    assert_eq!(completion.covered_through_us, timestamp_us("09:35:01"));

    let next = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(3, "09:35:31", "09:35:01"),
    )?;
    assert_eq!(
        next.completion
            .expect("second completion receipt")
            .previous_covered_through_us,
        Some(timestamp_us("09:35:01"))
    );
    Ok(())
}

#[test]
fn retained_stream_restart_adopts_first_declared_predecessors_without_proving_continuity(
) -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;

    builder.ingest(TRADE_TOPIC, &tick(12_000, "13:39:30", 336, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(12_001, "13:40:30", 337, 2, 200))?;

    let first_completion = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(12_002, "13:40:40", "13:40:20"),
    )?;
    assert!(
        first_completion.bars.is_empty(),
        "the retained suffix cannot prove either partial bucket"
    );
    assert_eq!(
        first_completion
            .completion
            .expect("first retained completion")
            .previous_covered_through_us,
        None,
        "a predecessor declared before this subscriber attached is not local continuity proof"
    );

    let first_observation = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(12_003, "13:40:45", Some("13:40:30"), "13:40:40"),
    )?;
    assert_eq!(
        first_observation
            .source_observation
            .expect("first retained source observation")
            .previous_observed_at_us,
        None,
        "a predecessor declared before this subscriber attached is not local continuity proof"
    );

    let next_observation = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(12_004, "13:41:00", Some("13:40:45"), "13:40:40"),
    )?;
    assert_eq!(
        next_observation
            .source_observation
            .expect("locally contiguous source observation")
            .previous_observed_at_us,
        Some(timestamp_us("13:40:45"))
    );

    builder.ingest(TRADE_TOPIC, &tick(12_005, "13:45:00", 338, 2, 300))?;
    let safe = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(12_006, "13:50:01", "13:40:40"),
    )?;
    assert_eq!(safe.bars.len(), 1);
    assert_eq!(
        safe.completion
            .as_ref()
            .expect("locally contiguous completion")
            .previous_covered_through_us,
        Some(timestamp_us("13:40:40")),
    );
    assert_eq!(safe.bars[0].timestamp.to_string(), "2026-08-21 13:50:00");
    assert_eq!(safe.bars[0].open, Decimal::from_str("3.38")?);

    for invalid_status in [
        status_with_previous(1, "13:40:40", "13:40:20"),
        status_with_previous(12_000, "13:40:40", "13:40:40"),
        status_with_previous(12_000, "13:40:40", "13:40:41"),
        status_with_previous_at(12_000, "2026-08-21 13:40:40", "2026-08-20 13:40:20"),
        status_with_previous(12_000, "13:40:40", "12:00:00"),
    ] {
        let mut invalid = WebTradeBarBuilder::new("002256.SZ", 5)?;
        let before = format!("{invalid:?}");
        assert!(invalid.ingest(STATUS_TOPIC, &invalid_status).is_err());
        assert_eq!(format!("{invalid:?}"), before);
        invalid.ingest(
            STATUS_TOPIC,
            &status_with_previous(12_000, "13:40:40", "13:40:20"),
        )?;
    }

    let mut observation = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let retained_observation = observation.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(12_000, "13:40:45", Some("13:40:30"), "13:40:40"),
    )?;
    assert!(retained_observation.bars.is_empty());
    assert!(retained_observation.completion.is_none());
    assert_eq!(observation.covered_through_us(), None);
    assert_eq!(
        retained_observation
            .source_observation
            .expect("retained observation anchor")
            .previous_observed_at_us,
        None
    );
    let first_coverage = observation.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(12_001, "13:40:50", "13:40:40"),
    )?;
    assert!(first_coverage.bars.is_empty());
    assert_eq!(
        first_coverage
            .completion
            .expect("retained coverage anchor")
            .previous_covered_through_us,
        None
    );
    assert_eq!(
        observation.covered_through_us(),
        Some(timestamp_us("13:40:50"))
    );
    let before = format!("{observation:?}");
    assert!(observation
        .ingest_with_receipt(
            STATUS_TOPIC,
            &source_observation(12_002, "13:41:00", Some("13:41:00"), "13:40:50"),
        )
        .is_err());
    assert_eq!(format!("{observation:?}"), before);
    let adopted = observation.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(12_002, "13:41:00", Some("13:40:45"), "13:40:50"),
    )?;
    assert_eq!(
        adopted.source_observation.unwrap().previous_observed_at_us,
        Some(timestamp_us("13:40:45"))
    );

    let mut fresh = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let before_fresh = format!("{fresh:?}");
    assert!(fresh
        .ingest_with_receipt(
            STATUS_TOPIC,
            &source_observation(1, "13:40:45", None, "13:40:40"),
        )
        .is_err());
    assert_eq!(format!("{fresh:?}"), before_fresh);
    Ok(())
}

#[test]
fn retained_observation_first_is_non_executable_until_local_coverage_and_one_full_bucket(
) -> Result<()> {
    let session_date = chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap();
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;

    builder.ingest(TRADE_TOPIC, &tick(8_010, "13:29:50", 336, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(8_011, "13:30:10", 337, 2, 200))?;
    let first = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(8_012, "13:31:00", Some("13:30:45"), "13:30:10"),
    )?;
    assert!(first.bars.is_empty());
    assert!(first.completion.is_none());
    assert_eq!(builder.covered_through_us(), None);
    assert!(!builder.has_complete_session(session_date));
    assert!(!builder.partial_session_resume_is_safe(session_date));
    assert_eq!(
        first
            .source_observation
            .expect("retained source-observation anchor")
            .previous_observed_at_us,
        None
    );

    let second = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(8_013, "13:31:15", Some("13:31:00"), "13:30:10"),
    )?;
    assert!(second.bars.is_empty());
    assert_eq!(builder.covered_through_us(), None);
    assert_eq!(
        second
            .source_observation
            .expect("locally chained pre-coverage observation")
            .previous_observed_at_us,
        Some(timestamp_us("13:31:00"))
    );

    let coverage_anchor = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(8_014, "13:31:20", "13:30:10"),
    )?;
    assert!(coverage_anchor.bars.is_empty());
    assert_eq!(
        coverage_anchor
            .completion
            .expect("retained completion anchor")
            .previous_covered_through_us,
        None
    );
    assert_eq!(builder.covered_through_us(), Some(timestamp_us("13:31:20")));
    assert!(!builder.partial_session_resume_is_safe(session_date));

    let before_mismatch = format!("{builder:?}");
    assert!(builder
        .ingest_with_receipt(
            STATUS_TOPIC,
            &source_observation(8_015, "13:31:30", Some("13:31:15"), "13:31:19"),
        )
        .is_err());
    assert_eq!(format!("{builder:?}"), before_mismatch);
    let exact = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(8_015, "13:31:30", Some("13:31:15"), "13:31:20"),
    )?;
    assert!(exact.bars.is_empty());

    builder.ingest(TRADE_TOPIC, &tick(8_016, "13:35:01", 338, 2, 300))?;
    builder.ingest(TRADE_TOPIC, &tick(8_017, "13:39:59", 339, 2, 400))?;
    let first_local_bucket = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &status_with_previous(8_018, "13:40:01", "13:31:20"),
    )?;
    assert_eq!(first_local_bucket.bars.len(), 1);
    assert_eq!(
        first_local_bucket.bars[0].timestamp.to_string(),
        "2026-08-21 13:40:00"
    );
    assert_eq!(first_local_bucket.bars[0].open, Decimal::from_str("3.38")?);
    assert_eq!(first_local_bucket.bars[0].close, Decimal::from_str("3.39")?);
    assert!(builder.partial_session_resume_is_safe(session_date));
    Ok(())
}

#[test]
fn source_observations_are_separate_from_trade_coverage_and_never_release_bars() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
    builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
    let covered = builder.covered_through_us();

    let first = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(3, "09:35:30", None, "09:35:01"),
    )?;
    assert!(first.bars.is_empty());
    assert!(first.completion.is_none());
    let first_observation = first
        .source_observation
        .expect("source observation receipt");
    assert_eq!(first_observation.previous_observed_at_us, None);
    assert_eq!(first_observation.observed_at_us, timestamp_us("09:35:30"));
    assert_eq!(
        first_observation.covered_through_us,
        timestamp_us("09:35:01")
    );
    assert_eq!(builder.covered_through_us(), covered);

    let second = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(4, "09:36:00", Some("09:35:30"), "09:35:01"),
    )?;
    assert!(second.bars.is_empty());
    assert!(second.completion.is_none());
    assert_eq!(
        second
            .source_observation
            .expect("second source observation")
            .previous_observed_at_us,
        Some(timestamp_us("09:35:30")),
    );
    assert_eq!(builder.covered_through_us(), covered);
    Ok(())
}

#[test]
fn source_clock_v2_is_accepted_without_changing_trade_coverage() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
    builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
    let covered = builder.covered_through_us();

    let receipt = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation_v2(3, "09:35:30", "09:35:24", None, "09:35:01"),
    )?;
    assert!(receipt.bars.is_empty());
    assert_eq!(
        receipt
            .source_observation
            .expect("clock-bound observation")
            .observed_at_us,
        timestamp_us("09:35:30")
    );
    assert_eq!(builder.covered_through_us(), covered);
    Ok(())
}

#[test]
fn source_https_clock_v3_is_accepted_and_forgery_is_atomic() -> Result<()> {
    let base = || -> Result<WebTradeBarBuilder> {
        let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
        builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
        builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
        Ok(builder)
    };
    let mut builder = base()?;
    let receipt = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation_v3(3, "09:35:30", "09:35:24", None, "09:35:01"),
    )?;
    assert_eq!(
        receipt
            .source_observation
            .expect("HTTPS clock observation")
            .covered_through_us,
        timestamp_us("09:35:01")
    );

    for event in [
        source_observation_v3(3, "09:35:30", "09:35:14", None, "09:35:01"),
        source_observation_v3(3, "09:35:30", "09:35:31", None, "09:35:01"),
        source_observation_v3_at(
            3,
            "2026-08-21 09:35:30",
            "2026-08-20 09:35:24",
            None,
            "2026-08-21 09:35:01",
        ),
    ] {
        let mut candidate = base()?;
        let before = format!("{candidate:?}");
        assert!(candidate.ingest_with_receipt(STATUS_TOPIC, &event).is_err());
        assert_eq!(format!("{candidate:?}"), before);
    }
    Ok(())
}

#[test]
fn forged_source_clock_v2_is_rejected_atomically() -> Result<()> {
    let base = || -> Result<WebTradeBarBuilder> {
        let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
        builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
        builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
        Ok(builder)
    };
    for event in [
        source_observation_v2(3, "09:35:30", "09:35:14", None, "09:35:01"),
        source_observation_v2(3, "09:35:30", "09:35:31", None, "09:35:01"),
        source_observation_v2_at(
            3,
            "2026-08-21 09:35:30",
            "2026-08-20 09:35:24",
            None,
            "2026-08-21 09:35:01",
        ),
    ] {
        let mut builder = base()?;
        let before = format!("{builder:?}");
        assert!(builder.ingest_with_receipt(STATUS_TOPIC, &event).is_err());
        assert_eq!(format!("{builder:?}"), before);
    }

    let mut missing_clock: Value = serde_json::from_slice(&source_observation_v2(
        3, "09:35:30", "09:35:24", None, "09:35:01",
    ))?;
    missing_clock["payload"]
        .as_object_mut()
        .expect("payload object")
        .remove("source_page_observed_at_us");
    canonicalize_event_id(&mut missing_clock);
    let mut builder = base()?;
    assert!(builder
        .ingest_with_receipt(STATUS_TOPIC, &serde_json::to_vec(&missing_clock)?)
        .is_err());

    let mut legacy_with_clock: Value =
        serde_json::from_slice(&source_observation(3, "09:35:30", None, "09:35:01"))?;
    legacy_with_clock["payload"]["source_page_observed_at_us"] = json!(timestamp_us("09:35:24"));
    canonicalize_event_id(&mut legacy_with_clock);
    let mut builder = base()?;
    assert!(builder
        .ingest_with_receipt(STATUS_TOPIC, &serde_json::to_vec(&legacy_with_clock)?)
        .is_err());
    Ok(())
}

#[test]
fn forged_source_observation_is_rejected_atomically() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:31:00", 352, 2, 100))?;
    builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
    builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(3, "09:35:30", None, "09:35:01"),
    )?;
    let before = format!("{builder:?}");

    assert!(builder
        .ingest_with_receipt(
            STATUS_TOPIC,
            &source_observation(4, "09:36:00", Some("09:35:29"), "09:35:01"),
        )
        .is_err());
    assert_eq!(format!("{builder:?}"), before);
    Ok(())
}

#[test]
fn source_observation_chain_resets_only_after_new_day_coverage_is_established() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(
        STATUS_TOPIC,
        &status(1, "09:35:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(2, "09:35:30", None, "09:35:01"),
    )?;

    builder.ingest(
        STATUS_TOPIC,
        &event_at(
            3,
            "SOURCE_STATUS",
            "2026-08-24 09:35:01",
            json!({
                "status": "SESSION_HISTORY_COMPLETE",
                "session_date": "2026-08-24",
                "covered_through_us": timestamp_us_at("2026-08-24 09:35:01"),
                "capture_sha256": "d".repeat(64),
            }),
        ),
    )?;
    let first_today = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation_at(4, "2026-08-24 09:35:30", None, "2026-08-24 09:35:01"),
    )?;
    assert_eq!(
        first_today
            .source_observation
            .expect("first observation today")
            .previous_observed_at_us,
        None,
    );
    let second_today = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation_at(
            5,
            "2026-08-24 09:36:00",
            Some("2026-08-24 09:35:30"),
            "2026-08-24 09:35:01",
        ),
    )?;
    assert_eq!(
        second_today
            .source_observation
            .expect("second observation today")
            .previous_observed_at_us,
        Some(timestamp_us_at("2026-08-24 09:35:30")),
    );
    Ok(())
}

#[test]
fn source_observation_chain_also_resets_after_a_new_day_partial_boundary() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(
        STATUS_TOPIC,
        &status(1, "09:35:01", "SESSION_HISTORY_COMPLETE"),
    )?;
    builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation(2, "09:35:30", None, "09:35:01"),
    )?;
    builder.ingest(
        STATUS_TOPIC,
        &event_at(
            3,
            "SOURCE_STATUS",
            "2026-08-24 13:40:20",
            json!({
                "status": "SESSION_RESUME_BOUNDARY",
                "session_date": "2026-08-24",
                "covered_from_us": timestamp_us_at("2026-08-24 13:00:01"),
                "covered_through_us": timestamp_us_at("2026-08-24 13:40:20"),
                "page_index": 1,
                "page_count": 13,
                "row_count": 144,
                "capture_sha256": "d".repeat(64),
                "policy": "INCOMPLETE_EASTMONEY_HISTORY_EXPLICIT_POLICY_V1",
            }),
        ),
    )?;
    let first_today = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &source_observation_at(4, "2026-08-24 13:40:30", None, "2026-08-24 13:40:20"),
    )?;
    assert_eq!(
        first_today
            .source_observation
            .expect("first partial-session observation today")
            .previous_observed_at_us,
        None,
    );
    Ok(())
}

#[test]
fn javascript_durable_source_observation_bytes_are_accepted_by_the_rust_builder() -> Result<()> {
    let output = Command::new("node")
        .arg("extensions/gridedge-web-market/scripts/render_source_observation_stream.js")
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let mut observation_count = 0;
    let mut released_bars = 0;
    for line in String::from_utf8(output.stdout)?.lines() {
        let record: Value = serde_json::from_str(line)?;
        let topic = record["topic"].as_str().expect("fixture topic");
        let payload = hex::decode(record["payload_hex"].as_str().expect("fixture bytes"))?;
        let receipt = builder.ingest_with_receipt(topic, &payload)?;
        observation_count += usize::from(receipt.source_observation.is_some());
        released_bars += receipt.bars.len();
    }
    assert_eq!(observation_count, 2);
    assert_eq!(released_bars, 0);
    assert!(builder.latest_source_observation().is_some());
    Ok(())
}

#[test]
fn source_sequence_gaps_conflicts_and_forged_event_ids_fail_closed_atomically() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let first = tick(1, "09:30:01", 355, 2, 100);
    builder.ingest(TRADE_TOPIC, &first)?;
    assert!(
        builder.ingest(TRADE_TOPIC, &first)?.is_empty(),
        "exact MQTT redelivery is idempotent"
    );
    assert!(builder
        .ingest(TRADE_TOPIC, &tick(3, "09:31:00", 354, 2, 100))
        .is_err());

    let mut forged: Value = serde_json::from_slice(&tick(2, "09:31:00", 354, 2, 100))?;
    forged["event_id"] = Value::String("0".repeat(64));
    assert!(builder
        .ingest(TRADE_TOPIC, &serde_json::to_vec(&forged)?)
        .is_err());

    let bars = builder.ingest(STATUS_TOPIC, &status(2, "09:35:01", "LIVE_CONTIGUOUS"))?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].open, Decimal::from_str("3.55")?);
    assert_eq!(bars[0].close, Decimal::from_str("3.55")?);
    Ok(())
}

#[test]
fn pre_open_noise_and_lunch_ticks_never_become_strategy_bars() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(TRADE_TOPIC, &tick(1, "09:15:00", 349, 2, 100))?;
    builder.ingest(TRADE_TOPIC, &tick(2, "09:25:00", 351, 2, 100))?;
    let bars = builder.ingest(STATUS_TOPIC, &status(3, "09:35:01", "LIVE_CONTIGUOUS"))?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].open, Decimal::from_str("3.51")?);

    let lunch = builder.ingest_with_receipt(TRADE_TOPIC, &tick(4, "12:00:00", 352, 2, 100))?;
    assert_eq!(
        lunch.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    Ok(())
}

#[test]
fn persisted_closed_session_and_weekend_events_are_quarantined_without_breaking_sequence(
) -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;

    let after_close = builder.ingest_with_receipt(
        TRADE_TOPIC,
        &event_with_recv(
            1,
            "TRADE_TICK",
            "2026-08-21 15:00:00",
            "2026-08-21 15:05:00",
            json!({
                "price": {"mantissa": 335, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": "after-close",
                "source_page": 1,
            }),
        ),
    )?;
    assert_eq!(
        after_close.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    assert_eq!(
        builder
            .ingest_with_receipt(
                TRADE_TOPIC,
                &event_with_recv(
                    1,
                    "TRADE_TICK",
                    "2026-08-21 15:00:00",
                    "2026-08-21 15:05:00",
                    json!({
                        "price": {"mantissa": 335, "scale": 2},
                        "quantity": 100,
                        "unit": "SHARE",
                        "side": "UNKNOWN",
                        "source_row_key": "after-close",
                        "source_page": 1,
                    }),
                ),
            )?
            .quarantine_reason
            .as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    assert!(after_close.bars.is_empty());
    assert!(after_close.completion.is_none());

    let weekend = builder.ingest_with_receipt(
        TRADE_TOPIC,
        &event_at(
            2,
            "TRADE_TICK",
            "2026-08-22 09:30:00",
            json!({
                "price": {"mantissa": 336, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": "weekend",
                "source_page": 1,
            }),
        ),
    )?;
    assert_eq!(
        weekend.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );

    let invalid_status = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &event_at(
            3,
            "SOURCE_STATUS",
            "2026-08-22 09:31:00",
            json!({
                "status": "LIVE_CONTIGUOUS",
                "session_date": "2026-08-22",
                "covered_through_us": timestamp_us_at("2026-08-22 09:31:00"),
                "capture_sha256": "c".repeat(64),
            }),
        ),
    )?;
    assert_eq!(
        invalid_status.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    assert!(!builder.has_complete_session(chrono::NaiveDate::from_ymd_opt(2026, 8, 22).unwrap()));

    builder.ingest(
        TRADE_TOPIC,
        &event_at(
            4,
            "TRADE_TICK",
            "2026-08-24 09:31:00",
            json!({
                "price": {"mantissa": 337, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": "monday-live",
                "source_page": 1,
            }),
        ),
    )?;
    let resumed = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &event_at(
            5,
            "SOURCE_STATUS",
            "2026-08-24 09:35:01",
            json!({
                "status": "LIVE_CONTIGUOUS",
                "session_date": "2026-08-24",
                "covered_through_us": timestamp_us_at("2026-08-24 09:35:01"),
                "capture_sha256": "c".repeat(64),
            }),
        ),
    )?;
    assert!(resumed.quarantine_reason.is_none());
    assert_eq!(resumed.bars.len(), 1);
    assert_eq!(resumed.bars[0].timestamp.to_string(), "2026-08-24 09:35:00");

    let mut holiday_builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let holiday = holiday_builder.ingest_with_receipt(
        TRADE_TOPIC,
        &event_at(
            1,
            "TRADE_TICK",
            "2026-01-02 09:30:00",
            json!({
                "price": {"mantissa": 337, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": "official-holiday",
                "source_page": 1,
            }),
        ),
    )?;
    assert_eq!(
        holiday.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    Ok(())
}

#[test]
fn quarantined_status_cannot_flush_an_existing_pending_bar_or_advance_watermark() -> Result<()> {
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    builder.ingest(
        TRADE_TOPIC,
        &event_at(
            1,
            "TRADE_TICK",
            "2026-08-21 14:56:00",
            json!({
                "price": {"mantissa": 335, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": "pending-before-close",
                "source_page": 1,
            }),
        ),
    )?;
    let quarantined = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &event_with_recv(
            2,
            "SOURCE_STATUS",
            "2026-08-21 15:00:00",
            "2026-08-21 15:05:00",
            json!({
                "status": "LIVE_CONTIGUOUS",
                "session_date": "2026-08-21",
                "covered_through_us": timestamp_us_at("2026-08-21 15:00:00"),
                "capture_sha256": "c".repeat(64),
            }),
        ),
    )?;
    assert!(quarantined.bars.is_empty());
    assert!(quarantined.completion.is_none());
    assert_eq!(builder.covered_through_us(), None);

    let valid = builder.ingest_with_receipt(
        STATUS_TOPIC,
        &event_at(
            3,
            "SOURCE_STATUS",
            "2026-08-21 15:00:00",
            json!({
                "status": "LIVE_CONTIGUOUS",
                "session_date": "2026-08-21",
                "covered_through_us": timestamp_us_at("2026-08-21 15:00:00"),
                "capture_sha256": "c".repeat(64),
            }),
        ),
    )?;
    assert_eq!(valid.bars.len(), 1);
    assert_eq!(valid.bars[0].timestamp.to_string(), "2026-08-21 15:00:00");
    assert_eq!(valid.bars[0].open, Decimal::from_str("3.35")?);
    assert_eq!(valid.bars[0].volume, 100);
    Ok(())
}

#[test]
fn signed_legacy_boundary_allows_exact_v5_to_v6_sequence_transition_only() -> Result<()> {
    let legacy = legacy_event(2_763, "2026-08-21 15:00:00");
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert!(builder.ingest(TRADE_TOPIC, &legacy)?.is_empty());
    let current = event_with_source(
        2_764,
        "TRADE_TICK",
        "2026-08-24 09:31:00",
        "2026-08-24 09:31:01",
        "8101d65c-bdba-4de3-83e0-8983506f159e",
        "eastmoney-time-sales-dom-v6",
        true,
        json!({
            "price": {"mantissa": 337, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "v6-transition",
            "source_page": 1,
        }),
    );
    assert!(builder.ingest(TRADE_TOPIC, &current)?.is_empty());

    let mut beyond = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert!(beyond
        .ingest(TRADE_TOPIC, &legacy_event(2_764, "2026-08-21 15:00:00"))
        .is_err());
    let mut wrong = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let wrong_instance = event_with_source(
        1,
        "TRADE_TICK",
        "2026-08-21 15:00:00",
        "2026-08-21 15:00:01",
        "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
        "eastmoney-time-sales-dom-v5",
        false,
        json!({
            "price": {"mantissa": 335, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "wrong-legacy-instance",
            "source_page": 1,
        }),
    );
    assert!(wrong.ingest(TRADE_TOPIC, &wrong_instance).is_err());
    Ok(())
}

#[test]
fn quarantined_legacy_tail_transitions_atomically_to_v6_partial_then_safe_live() -> Result<()> {
    let fixed_instance = "8101d65c-bdba-4de3-83e0-8983506f159e";
    let quarantined_legacy = event_with_source(
        2_763,
        "TRADE_TICK",
        "2026-08-22 10:00:00",
        "2026-08-21 18:00:00",
        fixed_instance,
        "eastmoney-time-sales-dom-v5",
        false,
        json!({
            "price": {"mantissa": 335, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "polluted-weekend-tail",
            "source_page": 1,
        }),
    );
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let tail = builder.ingest_with_receipt(TRADE_TOPIC, &quarantined_legacy)?;
    assert_eq!(
        tail.quarantine_reason.as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    assert!(tail.bars.is_empty());
    assert!(tail.completion.is_none());

    let unreviewed_current_clock_skew = event_with_source(
        1,
        "TRADE_TICK",
        "2026-08-22 10:00:00",
        "2026-08-21 18:00:00",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        json!({
            "price": {"mantissa": 335, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "unreviewed-current-weekend-clock-skew",
            "source_page": 1,
        }),
    );
    let mut current_builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert!(current_builder
        .ingest(TRADE_TOPIC, &unreviewed_current_clock_skew)
        .is_err());

    let boundary_payload = json!({
        "status": "SESSION_RESUME_BOUNDARY",
        "session_date": "2026-08-24",
        "covered_from_us": timestamp_us_at("2026-08-24 13:40:01"),
        "covered_through_us": timestamp_us_at("2026-08-24 13:40:20"),
        "page_index": 1,
        "page_count": 13,
        "row_count": 144,
        "policy": "INCOMPLETE_EASTMONEY_HISTORY_EXPLICIT_POLICY_V1",
        "capture_sha256": "c".repeat(64),
    });
    let wrong_instance = event_with_source(
        2_764,
        "SOURCE_STATUS",
        "2026-08-24 13:40:20",
        "2026-08-24 13:40:21",
        "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
        "eastmoney-time-sales-dom-v6",
        true,
        boundary_payload.clone(),
    );
    assert!(builder.ingest(STATUS_TOPIC, &wrong_instance).is_err());
    let wrong_sequence = event_with_source(
        2_765,
        "SOURCE_STATUS",
        "2026-08-24 13:40:20",
        "2026-08-24 13:40:21",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        boundary_payload.clone(),
    );
    assert!(builder.ingest(STATUS_TOPIC, &wrong_sequence).is_err());

    let boundary = event_with_source(
        2_764,
        "SOURCE_STATUS",
        "2026-08-24 13:40:20",
        "2026-08-24 13:40:21",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        boundary_payload,
    );
    builder.ingest(STATUS_TOPIC, &boundary)?;
    let date = chrono::NaiveDate::from_ymd_opt(2026, 8, 24).unwrap();
    assert!(builder.has_resume_boundary(date));
    assert!(!builder.partial_session_resume_is_safe(date));

    let first_live = event_with_source(
        2_765,
        "SOURCE_STATUS",
        "2026-08-24 13:40:40",
        "2026-08-24 13:40:41",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        json!({
            "status": "LIVE_CONTIGUOUS",
            "session_date": "2026-08-24",
            "previous_covered_through_us": timestamp_us_at("2026-08-24 13:40:20"),
            "covered_through_us": timestamp_us_at("2026-08-24 13:40:40"),
            "capture_sha256": "d".repeat(64),
        }),
    );
    builder.ingest(STATUS_TOPIC, &first_live)?;
    assert!(!builder.partial_session_resume_is_safe(date));

    let trade = event_with_source(
        2_766,
        "TRADE_TICK",
        "2026-08-24 13:45:00",
        "2026-08-24 13:45:01",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        json!({
            "price": {"mantissa": 337, "scale": 2},
            "quantity": 200,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "2026-08-24|13:45:00|3.37|2|UNKNOWN|1",
            "source_page": 1,
        }),
    );
    builder.ingest(TRADE_TOPIC, &trade)?;
    let terminal_live = event_with_source(
        2_767,
        "SOURCE_STATUS",
        "2026-08-24 13:50:01",
        "2026-08-24 13:50:02",
        fixed_instance,
        "eastmoney-time-sales-dom-v6",
        true,
        json!({
            "status": "LIVE_CONTIGUOUS",
            "session_date": "2026-08-24",
            "previous_covered_through_us": timestamp_us_at("2026-08-24 13:40:40"),
            "covered_through_us": timestamp_us_at("2026-08-24 13:50:01"),
            "capture_sha256": "e".repeat(64),
        }),
    );
    let bars = builder.ingest(STATUS_TOPIC, &terminal_live)?;
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].timestamp.to_string(), "2026-08-24 13:50:00");
    assert!(builder.partial_session_resume_is_safe(date));
    Ok(())
}

#[test]
fn signed_legacy_same_day_history_backfill_is_not_misclassified_as_weekend_pollution() -> Result<()>
{
    let mut builder = WebTradeBarBuilder::new("002256.SZ", 5)?;
    let tick = event_with_source(
        1,
        "TRADE_TICK",
        "2026-08-21 09:34:00",
        "2026-08-21 11:55:00",
        "8101d65c-bdba-4de3-83e0-8983506f159e",
        "eastmoney-time-sales-dom-v5",
        false,
        json!({
            "price": {"mantissa": 335, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": "legacy-same-day-backfill",
            "source_page": 1,
        }),
    );
    let status = event_with_source(
        2,
        "SOURCE_STATUS",
        "2026-08-21 09:35:01",
        "2026-08-21 11:55:01",
        "8101d65c-bdba-4de3-83e0-8983506f159e",
        "eastmoney-time-sales-dom-v5",
        false,
        json!({
            "status": "SESSION_HISTORY_COMPLETE",
            "session_date": "2026-08-21",
            "covered_through_us": timestamp_us_at("2026-08-21 09:35:01"),
            "capture_sha256": "c".repeat(64),
        }),
    );

    assert!(builder.ingest(TRADE_TOPIC, &tick)?.is_empty());
    let receipt = builder.ingest_with_receipt(STATUS_TOPIC, &status)?;
    assert_eq!(receipt.bars.len(), 1);
    assert!(builder.has_complete_session(chrono::NaiveDate::from_ymd_opt(2026, 8, 21).unwrap()));
    Ok(())
}

#[test]
fn signed_legacy_backfill_preserves_same_day_1505_boundary_but_quarantines_cross_day_data(
) -> Result<()> {
    let legacy = |sequence, received: &str| {
        event_with_source(
            sequence,
            "TRADE_TICK",
            "2026-08-21 15:00:00",
            received,
            "8101d65c-bdba-4de3-83e0-8983506f159e",
            "eastmoney-time-sales-dom-v5",
            false,
            json!({
                "price": {"mantissa": 335, "scale": 2},
                "quantity": 100,
                "unit": "SHARE",
                "side": "UNKNOWN",
                "source_row_key": format!("legacy-boundary-{sequence}"),
                "source_page": 1,
            }),
        )
    };

    let mut exact = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert!(exact
        .ingest_with_receipt(TRADE_TOPIC, &legacy(1, "2026-08-21 15:05:00"))?
        .quarantine_reason
        .is_none());

    let mut late = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert_eq!(
        late.ingest_with_receipt(TRADE_TOPIC, &legacy(1, "2026-08-21 15:05:01"))?
            .quarantine_reason
            .as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );

    let mut weekend = WebTradeBarBuilder::new("002256.SZ", 5)?;
    assert_eq!(
        weekend
            .ingest_with_receipt(TRADE_TOPIC, &legacy(1, "2026-08-22 09:30:00"))?
            .quarantine_reason
            .as_deref(),
        Some("OUTSIDE_REVIEWED_A_SHARE_SESSION")
    );
    Ok(())
}

#[test]
fn audited_market_backfill_runs_read_only_and_only_resumes_after_reconciliation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut config = Config::load("configs/ths_002256_sim.yaml")?;
    config.database = directory
        .path()
        .join("market-recovery.db")
        .display()
        .to_string();
    config.config_version = "market-recovery-test".to_owned();
    config.validate()?;
    let mut service = GridAutomationService::start_new_with_algorithm(
        config.clone(),
        SqliteStore::open(&config.database)?,
        algorithm_from_config(&config)?,
        Some("market-recovery".to_owned()),
    )?;

    service.enter_market_data_recovery("EASTMONEY_SESSION_HISTORY_BACKFILL")?;
    assert_eq!(service.state.mode, ServiceMode::ReadOnly);
    service.enter_market_data_recovery("idempotent restart")?;
    assert!(service.resume_after_reconciliation("too early").is_err());
    assert!(service.reconcile()?.matched);
    service.resume_after_reconciliation("complete source watermark and remote account matched")?;
    assert_eq!(service.state.mode, ServiceMode::Running);
    Ok(())
}

#[test]
fn market_replay_cli_requires_the_terminal_watermark_and_writes_a_hashed_audit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let input_path = directory.path().join("market.jsonl");
    let output_path = directory.path().join("audit.json");
    let bars_path = directory.path().join("bars.csv");
    let mut input = std::fs::File::create(&input_path)?;
    for (index, (topic, payload)) in [
        (TRADE_TOPIC, tick(1, "09:25:00", 351, 2, 100)),
        (TRADE_TOPIC, tick(2, "09:34:00", 352, 2, 200)),
        (
            STATUS_TOPIC,
            status(3, "09:35:01", "SESSION_HISTORY_COMPLETE"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let wire = if index == 2 {
            serde_json::json!({"topic": topic, "payload": payload})
        } else {
            serde_json::json!({"topic": topic, "payload_hex": hex::encode(payload)})
        };
        writeln!(input, "{}", wire)?;
    }
    input.sync_all()?;

    let result = Command::new(env!("CARGO_BIN_EXE_gridedge_market_replay"))
        .args([
            "--input",
            input_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
            "--symbol",
            "002256.SZ",
            "--session-date",
            "2026-08-21",
            "--bars-csv-output",
            bars_path.to_str().unwrap(),
        ])
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let audit: Value = serde_json::from_slice(&std::fs::read(output_path)?)?;
    assert_eq!(audit["schema"], "gridedge.market-replay-audit.v1");
    assert_eq!(audit["input_event_count"], 3);
    assert_eq!(audit["bar_count"], 1);
    assert_eq!(audit["bars"][0]["open"], "3.51");
    assert_eq!(audit["bars"][0]["close"], "3.52");
    assert_eq!(audit["bars_sha256"].as_str().unwrap().len(), 64);
    let replayed = gridedge_t::data::CsvReplayFeed::load(&bars_path, "002256.SZ")?;
    assert_eq!(replayed.bars().len(), 1);
    Ok(())
}

#[test]
fn market_replay_cli_requires_explicit_authority_for_a_safe_partial_session() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let input_path = directory.path().join("partial-market.jsonl");
    let output_path = directory.path().join("partial-audit.json");
    let mut input = std::fs::File::create(&input_path)?;
    for (topic, payload) in [
        (
            STATUS_TOPIC,
            resume_boundary_with_from(1, "13:40:20", "13:40:01"),
        ),
        (TRADE_TOPIC, tick(2, "13:46:00", 352, 2, 200)),
        (
            STATUS_TOPIC,
            status_with_previous(3, "13:50:01", "13:40:20"),
        ),
    ] {
        writeln!(
            input,
            "{}",
            serde_json::json!({"topic": topic, "payload_hex": hex::encode(payload)})
        )?;
    }
    input.sync_all()?;

    let denied = Command::new(env!("CARGO_BIN_EXE_gridedge_market_replay"))
        .args([
            "--input",
            input_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
            "--symbol",
            "002256.SZ",
            "--session-date",
            "2026-08-21",
        ])
        .output()?;
    assert!(!denied.status.success());
    assert!(!output_path.exists());

    let allowed = Command::new(env!("CARGO_BIN_EXE_gridedge_market_replay"))
        .args([
            "--input",
            input_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
            "--symbol",
            "002256.SZ",
            "--session-date",
            "2026-08-21",
            "--allow-partial-session-resume-boundary",
        ])
        .output()?;
    assert!(
        allowed.status.success(),
        "{}",
        String::from_utf8_lossy(&allowed.stderr)
    );
    let audit: Value = serde_json::from_slice(&std::fs::read(output_path)?)?;
    assert_eq!(audit["history_mode"], "PARTIAL_SESSION_SAFE_AFTER_BOUNDARY");
    assert_eq!(audit["bar_count"], 1);
    assert_eq!(audit["bars"][0]["timestamp"], "2026-08-21 13:50:00");
    Ok(())
}

#[test]
fn market_replay_cli_never_exports_bars_released_before_a_partial_boundary() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let input_path = directory.path().join("mixed-boundary-market.jsonl");
    let output_path = directory.path().join("mixed-boundary-audit.json");
    let bars_path = directory.path().join("mixed-boundary-bars.csv");
    let mut input = std::fs::File::create(&input_path)?;
    for (topic, payload) in [
        (TRADE_TOPIC, tick(1, "09:31:00", 351, 2, 100)),
        (STATUS_TOPIC, status(2, "09:35:01", "LIVE_CONTIGUOUS")),
        (
            STATUS_TOPIC,
            resume_boundary_with_from(3, "13:40:20", "13:40:01"),
        ),
        (TRADE_TOPIC, tick(4, "13:46:00", 352, 2, 200)),
        (
            STATUS_TOPIC,
            status_with_previous(5, "13:50:01", "13:40:20"),
        ),
    ] {
        writeln!(
            input,
            "{}",
            serde_json::json!({"topic": topic, "payload_hex": hex::encode(payload)})
        )?;
    }
    input.sync_all()?;

    let result = Command::new(env!("CARGO_BIN_EXE_gridedge_market_replay"))
        .args([
            "--input",
            input_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
            "--symbol",
            "002256.SZ",
            "--session-date",
            "2026-08-21",
            "--allow-partial-session-resume-boundary",
            "--bars-csv-output",
            bars_path.to_str().unwrap(),
        ])
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let audit: Value = serde_json::from_slice(&std::fs::read(output_path)?)?;
    assert_eq!(audit["history_mode"], "PARTIAL_SESSION_SAFE_AFTER_BOUNDARY");
    assert_eq!(audit["bar_count"], 1);
    assert_eq!(audit["bars"][0]["timestamp"], "2026-08-21 13:50:00");
    let replayed = gridedge_t::data::CsvReplayFeed::load(&bars_path, "002256.SZ")?;
    assert_eq!(replayed.bars().len(), 1);
    assert_eq!(
        replayed.bars()[0].timestamp.to_string(),
        "2026-08-21 13:50:00"
    );
    Ok(())
}

#[test]
fn market_replay_cli_never_exports_bars_outside_requested_complete_session() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let input_path = directory.path().join("cross-session-market.jsonl");
    let output_path = directory.path().join("cross-session-audit.json");
    let bars_path = directory.path().join("cross-session-bars.csv");
    let mut input = std::fs::File::create(&input_path)?;
    for (topic, payload) in [
        (TRADE_TOPIC, tick(1, "09:31:00", 351, 2, 100)),
        (
            STATUS_TOPIC,
            status(2, "09:35:01", "SESSION_HISTORY_COMPLETE"),
        ),
        (
            TRADE_TOPIC,
            event_at(
                3,
                "TRADE_TICK",
                "2026-08-24 09:31:00",
                json!({
                    "price": {"mantissa": 352, "scale": 2},
                    "quantity": 200,
                    "unit": "SHARE",
                    "side": "UNKNOWN",
                    "source_row_key": "2026-08-24|09:31:00|352|200|UNKNOWN|1",
                    "source_page": 1,
                }),
            ),
        ),
        (
            STATUS_TOPIC,
            event_at(
                4,
                "SOURCE_STATUS",
                "2026-08-24 09:35:01",
                json!({
                    "status": "LIVE_CONTIGUOUS",
                    "session_date": "2026-08-24",
                    "covered_through_us": timestamp_us_at("2026-08-24 09:35:01"),
                    "previous_covered_through_us": timestamp_us("09:35:01"),
                    "capture_sha256": "c".repeat(64),
                }),
            ),
        ),
    ] {
        writeln!(
            input,
            "{}",
            serde_json::json!({"topic": topic, "payload_hex": hex::encode(payload)})
        )?;
    }
    input.sync_all()?;

    let result = Command::new(env!("CARGO_BIN_EXE_gridedge_market_replay"))
        .args([
            "--input",
            input_path.to_str().unwrap(),
            "--output",
            output_path.to_str().unwrap(),
            "--symbol",
            "002256.SZ",
            "--session-date",
            "2026-08-21",
            "--bars-csv-output",
            bars_path.to_str().unwrap(),
        ])
        .output()?;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("outside requested session"));
    assert!(!output_path.exists());
    assert!(!bars_path.exists());
    Ok(())
}

fn tick(sequence: u64, time: &str, mantissa: i64, scale: u32, quantity: i64) -> Vec<u8> {
    event(
        sequence,
        "TRADE_TICK",
        time,
        json!({
            "price": {"mantissa": mantissa, "scale": scale},
            "quantity": quantity,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": format!("2026-08-21|{time}|{mantissa}|{quantity}|UNKNOWN|1"),
            "source_page": 1,
        }),
    )
}

fn status(sequence: u64, time: &str, status: &str) -> Vec<u8> {
    let ts_us = timestamp_us(time);
    let mut payload = json!({
        "status": status,
        "session_date": "2026-08-21",
        "covered_through_us": ts_us,
        "capture_sha256": "c".repeat(64),
    });
    if status == "SESSION_RESUME_BOUNDARY" {
        payload["covered_from_us"] = json!(timestamp_us("13:40:01"));
        payload["page_index"] = json!(1);
        payload["page_count"] = json!(13);
        payload["row_count"] = json!(144);
        payload["policy"] = json!("INCOMPLETE_EASTMONEY_HISTORY_EXPLICIT_POLICY_V1");
    }
    event(sequence, "SOURCE_STATUS", time, payload)
}

fn status_with_previous(sequence: u64, time: &str, previous: &str) -> Vec<u8> {
    let mut value: Value =
        serde_json::from_slice(&status(sequence, time, "LIVE_CONTIGUOUS")).expect("status JSON");
    value["payload"]["previous_covered_through_us"] = json!(timestamp_us(previous));
    canonicalize_event_id(&mut value);
    serde_json::to_vec(&value).expect("status bytes")
}

fn status_with_previous_at(sequence: u64, time: &str, previous: &str) -> Vec<u8> {
    let session_date = &time[..10];
    event_at(
        sequence,
        "SOURCE_STATUS",
        time,
        json!({
            "status": "LIVE_CONTIGUOUS",
            "session_date": session_date,
            "covered_through_us": timestamp_us_at(time),
            "previous_covered_through_us": timestamp_us_at(previous),
            "capture_sha256": "c".repeat(64),
        }),
    )
}

fn source_observation(
    sequence: u64,
    observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    let mut payload = json!({
        "status": "SOURCE_OBSERVED_CURRENT",
        "session_date": "2026-08-21",
        "observed_at_us": timestamp_us(observed),
        "covered_through_us": timestamp_us(covered_through),
        "latest_displayed_trade_us": timestamp_us(covered_through),
        "page_index": 1,
        "page_count": 1,
        "row_count": 4,
        "capture_sha256": "c".repeat(64),
        "policy": "ACTIVE_REVIEWED_LATEST_FIRST_CYCLE_V1",
    });
    if let Some(previous) = previous_observed {
        payload["previous_observed_at_us"] = json!(timestamp_us(previous));
    }
    event_with_recv(
        sequence,
        "SOURCE_STATUS",
        &format!("2026-08-21 {observed}"),
        &format!("2026-08-21 {observed}"),
        payload,
    )
}

fn source_observation_at(
    sequence: u64,
    observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    let session_date = &observed[..10];
    let mut payload = json!({
        "status": "SOURCE_OBSERVED_CURRENT",
        "session_date": session_date,
        "observed_at_us": timestamp_us_at(observed),
        "covered_through_us": timestamp_us_at(covered_through),
        "latest_displayed_trade_us": timestamp_us_at(covered_through),
        "page_index": 1,
        "page_count": 1,
        "row_count": 4,
        "capture_sha256": "c".repeat(64),
        "policy": "ACTIVE_REVIEWED_LATEST_FIRST_CYCLE_V1",
    });
    if let Some(previous) = previous_observed {
        payload["previous_observed_at_us"] = json!(timestamp_us_at(previous));
    }
    event_with_recv(sequence, "SOURCE_STATUS", observed, observed, payload)
}

fn source_observation_v2(
    sequence: u64,
    observed: &str,
    source_page_observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    source_observation_v2_at(
        sequence,
        &format!("2026-08-21 {observed}"),
        &format!("2026-08-21 {source_page_observed}"),
        previous_observed
            .map(|previous| format!("2026-08-21 {previous}"))
            .as_deref(),
        &format!("2026-08-21 {covered_through}"),
    )
}

fn source_observation_v2_at(
    sequence: u64,
    observed: &str,
    source_page_observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    let session_date = &observed[..10];
    let mut payload = json!({
        "status": "SOURCE_OBSERVED_CURRENT",
        "session_date": session_date,
        "observed_at_us": timestamp_us_at(observed),
        "source_page_observed_at_us": timestamp_us_at(source_page_observed),
        "covered_through_us": timestamp_us_at(covered_through),
        "latest_displayed_trade_us": timestamp_us_at(covered_through),
        "page_index": 1,
        "page_count": 1,
        "row_count": 4,
        "capture_sha256": "c".repeat(64),
        "policy": "REVIEWED_SOURCE_CLOCK_LATEST_FIRST_V2",
    });
    if let Some(previous) = previous_observed {
        payload["previous_observed_at_us"] = json!(timestamp_us_at(previous));
    }
    event_with_recv(sequence, "SOURCE_STATUS", observed, observed, payload)
}

fn source_observation_v3(
    sequence: u64,
    observed: &str,
    source_server_observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    source_observation_v3_at(
        sequence,
        &format!("2026-08-21 {observed}"),
        &format!("2026-08-21 {source_server_observed}"),
        previous_observed
            .map(|previous| format!("2026-08-21 {previous}"))
            .as_deref(),
        &format!("2026-08-21 {covered_through}"),
    )
}

fn source_observation_v3_at(
    sequence: u64,
    observed: &str,
    source_server_observed: &str,
    previous_observed: Option<&str>,
    covered_through: &str,
) -> Vec<u8> {
    let session_date = &observed[..10];
    let mut payload = json!({
        "status": "SOURCE_OBSERVED_CURRENT",
        "session_date": session_date,
        "observed_at_us": timestamp_us_at(observed),
        "source_server_observed_at_us": timestamp_us_at(source_server_observed),
        "source_clock_origin": "EASTMONEY_HTTPS_DATE_HEADER",
        "covered_through_us": timestamp_us_at(covered_through),
        "latest_displayed_trade_us": timestamp_us_at(covered_through),
        "page_index": 1,
        "page_count": 1,
        "row_count": 4,
        "capture_sha256": "c".repeat(64),
        "policy": "REVIEWED_EASTMONEY_HTTPS_DATE_LATEST_FIRST_V3",
    });
    if let Some(previous) = previous_observed {
        payload["previous_observed_at_us"] = json!(timestamp_us_at(previous));
    }
    event_with_recv(sequence, "SOURCE_STATUS", observed, observed, payload)
}

fn resume_boundary_with_from(sequence: u64, time: &str, from: &str) -> Vec<u8> {
    let mut value: Value =
        serde_json::from_slice(&status(sequence, time, "SESSION_RESUME_BOUNDARY"))
            .expect("boundary JSON");
    value["payload"]["covered_from_us"] = json!(timestamp_us(from));
    canonicalize_event_id(&mut value);
    serde_json::to_vec(&value).expect("boundary bytes")
}

fn canonicalize_event_id(value: &mut Value) {
    value.as_object_mut().unwrap().remove("event_id");
    let mut identity = value.clone();
    identity.as_object_mut().unwrap().remove("recv_us");
    value["event_id"] = Value::String(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&identity).unwrap())
    ));
}

fn event(sequence: u64, event_type: &str, time: &str, payload: Value) -> Vec<u8> {
    event_at(sequence, event_type, &format!("2026-08-21 {time}"), payload)
}

fn event_at(sequence: u64, event_type: &str, datetime: &str, payload: Value) -> Vec<u8> {
    let received = chrono::NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S").unwrap()
        + chrono::Duration::seconds(1);
    event_with_recv(
        sequence,
        event_type,
        datetime,
        &received.format("%Y-%m-%d %H:%M:%S").to_string(),
        payload,
    )
}

fn event_with_recv(
    sequence: u64,
    event_type: &str,
    datetime: &str,
    received: &str,
    payload: Value,
) -> Vec<u8> {
    event_with_source(
        sequence,
        event_type,
        datetime,
        received,
        "0198d8f2-6a70-7f3d-a3fc-8a53a460d599",
        "eastmoney-time-sales-dom-v6",
        true,
        payload,
    )
}

fn legacy_event(sequence: u64, datetime: &str) -> Vec<u8> {
    event_with_source(
        sequence,
        "TRADE_TICK",
        datetime,
        datetime,
        "8101d65c-bdba-4de3-83e0-8983506f159e",
        "eastmoney-time-sales-dom-v5",
        false,
        json!({
            "price": {"mantissa": 335, "scale": 2},
            "quantity": 100,
            "unit": "SHARE",
            "side": "UNKNOWN",
            "source_row_key": format!("legacy-{sequence}"),
            "source_page": 1,
        }),
    )
}

#[allow(clippy::too_many_arguments)]
fn event_with_source(
    sequence: u64,
    event_type: &str,
    datetime: &str,
    received: &str,
    source_instance_id: &str,
    provider_version: &str,
    bind_capture: bool,
    payload: Value,
) -> Vec<u8> {
    let mut event = json!({
        "spec": "gridedge.market",
        "schema_version": 1,
        "event_type": event_type,
        "source": {
            "source_id": "eastmoney-web-time-sales",
            "source_instance_id": source_instance_id,
            "source_type": "WEB_UI",
            "provider": "eastmoney",
            "provider_version": provider_version
        },
        "instrument": {
            "venue": "XSHE",
            "symbol": "002256",
            "asset_class": "EQUITY",
            "currency": "CNY"
        },
        "source_sequence": sequence,
        "ts_us": timestamp_us_at(datetime),
        "recv_us": timestamp_us_at(received),
        "payload": payload,
        "evidence_sha256": "e".repeat(64),
    });
    if bind_capture {
        event["payload"]["source_captured_at_us"] = json!(timestamp_us_at(received));
    }
    let mut identity = event.clone();
    identity.as_object_mut().unwrap().remove("recv_us");
    let event_id = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&identity).unwrap())
    );
    event["event_id"] = Value::String(event_id);
    serde_json::to_vec(&event).unwrap()
}

fn timestamp_us(time: &str) -> u64 {
    timestamp_us_at(&format!("2026-08-21 {time}"))
}

fn timestamp_us_at(value: &str) -> u64 {
    let datetime = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").unwrap();
    datetime.and_utc().timestamp_micros() as u64 - 8 * 60 * 60 * 1_000_000
}

#[allow(dead_code)]
fn _type_check(_: MarketBar) {}
