(function initGridEdgeDurable(root, factory) {
  const api = factory(root.GridEdgeMarket);
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.GridEdgeDurable = api;
})(typeof globalThis === "object" ? globalThis : this, function buildDurable(core) {
  "use strict";

  const DB_NAME = "gridedge-web-market-v6";
  const DB_VERSION = 1;
  const SOURCE_ID = "eastmoney-web-time-sales";
  const AUDITED_LEGACY_SOURCE_KEY = `${SOURCE_ID}|XSHE|002256`;
  const AUDITED_LEGACY_SOURCE_INSTANCE = "8101d65c-bdba-4de3-83e0-8983506f159e";
  const AUDITED_LEGACY_NEXT_SEQUENCE = 2764;
  const SOURCE_OBSERVATION_POLICY = "ACTIVE_REVIEWED_LATEST_FIRST_CYCLE_V1";
  let writerTail = Promise.resolve();

  function request(requestObject) {
    return new Promise((resolve, reject) => {
      requestObject.onsuccess = () => resolve(requestObject.result);
      requestObject.onerror = () => reject(requestObject.error);
    });
  }

  function transactionDone(transaction) {
    return new Promise((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
      transaction.onerror = () => reject(transaction.error);
    });
  }

  async function openDatabase(indexedDb = indexedDB, name = DB_NAME) {
    const opening = indexedDb.open(name, DB_VERSION);
    opening.onupgradeneeded = () => {
      const database = opening.result;
      const sourceState = database.createObjectStore("source_state", { keyPath: "key" });
      if (name === DB_NAME) {
        sourceState.add({
          key: AUDITED_LEGACY_SOURCE_KEY,
          source_instance_id: AUDITED_LEGACY_SOURCE_INSTANCE,
          next_sequence: AUDITED_LEGACY_NEXT_SEQUENCE,
          bootstrap: "DATABASE_COMMITTED_PREFIX_V1",
        });
      }
      database.createObjectStore("capture_batches", { keyPath: "capture_sha256" });
      database.createObjectStore("web_rows", { keyPath: "identity" });
      database.createObjectStore("capture_conflicts", { keyPath: "id", autoIncrement: true });
      const outbox = database.createObjectStore("market_event_outbox", { keyPath: "event_id" });
      outbox.createIndex("state", "state", { unique: false });
    };
    return await request(opening);
  }

  function validateCapture(capture, { allowStaleLatest = false } = {}) {
    if (!capture || typeof capture !== "object" || Array.isArray(capture)) {
      throw new Error("capture must be an object");
    }
    if (Object.hasOwn(capture, "account_marker")) {
      throw new Error("account_marker is not market data");
    }
    if (capture.capture_spec !== core.CAPTURE_SPEC || capture.schema_version !== 1 ||
        capture.provider !== "eastmoney" ||
        capture.provider_version !== "eastmoney-time-sales-dom-v6" ||
        !["TIME_SALES", "TIME_SALES_SESSION"].includes(capture.page_kind)) {
      throw new Error("unsupported capture contract");
    }
    const url = new URL(capture.source_url);
    if (url.protocol !== "https:" || url.hostname !== "quote.eastmoney.com" || url.pathname !== "/f1.html") {
      throw new Error("capture URL is outside the reviewed Eastmoney time-sales page");
    }
    const match = /^([01])\.(\d{6})$/.exec(url.searchParams.get("newcode") ?? "");
    const expected = match && {
      venue: match[1] === "0" ? "XSHE" : "XSHG",
      symbol: match[2],
      asset_class: "EQUITY",
      currency: "CNY",
    };
    if (!expected || core.canonicalJson(capture.instrument) !== core.canonicalJson(expected)) {
      throw new Error("capture instrument disagrees with source URL");
    }
    if (!Number.isSafeInteger(capture.captured_at_us) || capture.captured_at_us < 0 ||
        !/^\d{4}-\d{2}-\d{2}$/.test(capture.session_date) ||
        ![true, false].includes(capture.completeness?.session_complete) ||
        capture.completeness?.identity_policy !== "DOM_CHRONOLOGICAL_ORDER_V2" ||
        capture.completeness?.row_count !== capture.rows?.length ||
        !Array.isArray(capture.rows) || capture.rows.length < 1 || capture.rows.length > 5000) {
      throw new Error("capture completeness or timestamp is invalid");
    }
    if (capture.completeness.session_complete) {
      const pages = capture.completeness.pages_captured;
      const hashes = capture.completeness.history_page_sha256;
      if (capture.page_kind !== "TIME_SALES_SESSION" ||
          !Array.isArray(pages) || pages.length !== capture.completeness.page_count ||
          pages.some((page, index) => page !== index + 1) ||
          !Array.isArray(hashes) || hashes.length !== pages.length ||
          !hashes.every((hash) => typeof hash === "string" && /^[0-9a-f]{64}$/.test(hash)) ||
          typeof capture.completeness.final_live_page_sha256 !== "string" ||
          !/^[0-9a-f]{64}$/.test(capture.completeness.final_live_page_sha256) ||
          !Number.isSafeInteger(capture.completeness.live_page_overlap) ||
          capture.completeness.live_page_overlap < 1 ||
          !Number.isSafeInteger(capture.completeness.covered_from_us) ||
          !Number.isSafeInteger(capture.completeness.covered_through_us) ||
          capture.completeness.covered_from_us > capture.completeness.covered_through_us) {
        throw new Error("complete history capture lacks a bounded page and live-overlap proof");
      }
    } else if (capture.page_kind !== "TIME_SALES") {
      throw new Error("partial capture must be one TIME_SALES page");
    }
    const keys = new Set();
    const rowTimes = [];
    for (const row of capture.rows) {
      if (typeof row.source_row_key !== "string" || !row.source_row_key || keys.has(row.source_row_key)) {
        throw new Error("source row identity must be unique within a capture");
      }
      keys.add(row.source_row_key);
      rowTimes.push(core.eventTimeUs(capture.session_date, row.source_trade_time));
      core.priceParts(row.price);
      if (!Number.isSafeInteger(row.quantity) || row.quantity <= 0 ||
          !Number.isSafeInteger(row.quantity_hands) || row.quantity_hands <= 0 ||
          !Number.isSafeInteger(row.source_same_second_ordinal) ||
          row.source_same_second_ordinal < 1 ||
          row.quantity !== row.quantity_hands * 100 || row.unit !== "SHARE" ||
          !["BUY", "SELL", "UNKNOWN"].includes(row.side) ||
          !Array.isArray(row.raw_cells) || !row.raw_cells.every((cell) => typeof cell === "string")) {
        throw new Error("capture row quantity, side, or evidence is invalid");
      }
    }
    if (capture.completeness.session_complete &&
        (capture.completeness.covered_from_us !== Math.min(...rowTimes) ||
         capture.completeness.covered_through_us !== Math.max(...rowTimes))) {
      throw new Error("complete history watermark disagrees with exact row bounds");
    }
    if (allowStaleLatest) core.validateSourceObservationTiming(capture);
    else core.validateCaptureTiming(capture);
    return capture;
  }

  function shanghaiSecondOfDay(timestampUs) {
    if (!Number.isSafeInteger(timestampUs) || timestampUs < 0) return null;
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Shanghai",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date(timestampUs / 1000));
    const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return Number(value.hour) * 3600 + Number(value.minute) * 60 + Number(value.second);
  }

  function isReviewedAshareSaleTimestamp(timestampUs) {
    const secondOfDay = shanghaiSecondOfDay(timestampUs);
    return secondOfDay !== null &&
      ((secondOfDay >= 9 * 3600 + 25 * 60 && secondOfDay <= 11 * 3600 + 30 * 60) ||
       (secondOfDay >= 13 * 3600 && secondOfDay <= 15 * 3600));
  }

  function stableRowEvidence(row) {
    const {
      source_table_ordinal: _sourceTableOrdinal,
      source_row_ordinal: _sourceRowOrdinal,
      source_same_second_ordinal: _sourceSameSecondOrdinal,
      raw_cells: _rawCells,
      ...evidence
    } = row;
    return evidence;
  }

  async function canonicalEvent(capture, row, sourceInstanceId, sourceSequence, sourceId = SOURCE_ID) {
    const evidence = {
      provider: capture.provider,
      provider_version: capture.provider_version,
      source_url: capture.source_url,
      session_date: capture.session_date,
      captured_at_us: capture.captured_at_us,
      row: stableRowEvidence(row),
    };
    const document = {
      spec: "gridedge.market",
      schema_version: 1,
      event_type: "TRADE_TICK",
      source: {
        source_id: sourceId,
        source_instance_id: sourceInstanceId,
        source_type: "WEB_UI",
        provider: capture.provider,
        provider_version: capture.provider_version,
      },
      instrument: capture.instrument,
      source_sequence: sourceSequence,
      ts_us: core.eventTimeUs(capture.session_date, row.source_trade_time),
      recv_us: capture.captured_at_us,
      payload: {
        price: core.priceParts(row.price),
        quantity: row.quantity,
        unit: "SHARE",
        side: row.side,
        source_row_key: row.source_row_key,
        source_page: capture.completeness.page_index,
        source_captured_at_us: capture.captured_at_us,
      },
      evidence_sha256: await core.sha256Hex(core.canonicalJson(evidence)),
    };
    const { recv_us: _receivedAt, ...identity } = document;
    const eventId = await core.sha256Hex(core.canonicalJson(identity));
    document.event_id = eventId;
    return {
      event_id: eventId,
      source_key: `${sourceId}|${capture.instrument.venue}|${capture.instrument.symbol}`,
      source_sequence: sourceSequence,
      mqtt_topic: `gridedge/market/v1/${capture.instrument.venue}/${capture.instrument.symbol}/trade`,
      payload: core.canonicalJson(document),
      state: "PENDING",
      attempts: 0,
      created_at_us: core.unixMicrosNow(),
    };
  }

  async function canonicalStatusEvent(capture, captureSha256, sourceInstanceId, sourceSequence, sourceId = SOURCE_ID) {
    const proof = capture.completeness;
    const payload = {
      status: "SESSION_HISTORY_COMPLETE",
      session_date: capture.session_date,
      covered_from_us: proof.covered_from_us,
      covered_through_us: proof.covered_through_us,
      page_count: proof.page_count,
      pages_captured: proof.pages_captured,
      history_page_sha256: proof.history_page_sha256,
      final_live_page_sha256: proof.final_live_page_sha256,
      live_page_overlap: proof.live_page_overlap,
      row_count: capture.rows.length,
      capture_sha256: captureSha256,
      source_captured_at_us: capture.captured_at_us,
    };
    const document = {
      spec: "gridedge.market",
      schema_version: 1,
      event_type: "SOURCE_STATUS",
      source: {
        source_id: sourceId,
        source_instance_id: sourceInstanceId,
        source_type: "WEB_UI",
        provider: capture.provider,
        provider_version: capture.provider_version,
      },
      instrument: capture.instrument,
      source_sequence: sourceSequence,
      ts_us: proof.covered_through_us,
      recv_us: capture.captured_at_us,
      payload,
      evidence_sha256: await core.sha256Hex(core.canonicalJson({
        provider: capture.provider,
        provider_version: capture.provider_version,
        source_url: capture.source_url,
        payload,
      })),
    };
    const { recv_us: _receivedAt, ...identity } = document;
    const eventId = await core.sha256Hex(core.canonicalJson(identity));
    document.event_id = eventId;
    return {
      event_id: eventId,
      source_key: `${sourceId}|${capture.instrument.venue}|${capture.instrument.symbol}`,
      source_sequence: sourceSequence,
      mqtt_topic: `gridedge/market/v1/${capture.instrument.venue}/${capture.instrument.symbol}/status`,
      payload: core.canonicalJson(document),
      state: "PENDING",
      attempts: 0,
      created_at_us: core.unixMicrosNow(),
    };
  }

  async function canonicalLiveStatusEvent(
    capture,
    captureSha256,
    sourceInstanceId,
    sourceSequence,
    previousCoveredThroughUs,
    coveredThroughUs,
    livePageOverlap,
    sourceId = SOURCE_ID,
  ) {
    const payload = {
      status: "LIVE_CONTIGUOUS",
      session_date: capture.session_date,
      previous_covered_through_us: previousCoveredThroughUs,
      covered_through_us: coveredThroughUs,
      live_page_overlap: livePageOverlap,
      page_index: capture.completeness.page_index,
      page_count: capture.completeness.page_count,
      row_count: capture.rows.length,
      capture_sha256: captureSha256,
      source_captured_at_us: capture.captured_at_us,
    };
    const document = {
      spec: "gridedge.market",
      schema_version: 1,
      event_type: "SOURCE_STATUS",
      source: {
        source_id: sourceId,
        source_instance_id: sourceInstanceId,
        source_type: "WEB_UI",
        provider: capture.provider,
        provider_version: capture.provider_version,
      },
      instrument: capture.instrument,
      source_sequence: sourceSequence,
      ts_us: coveredThroughUs,
      recv_us: capture.captured_at_us,
      payload,
      evidence_sha256: await core.sha256Hex(core.canonicalJson({
        provider: capture.provider,
        provider_version: capture.provider_version,
        source_url: capture.source_url,
        payload,
      })),
    };
    const { recv_us: _receivedAt, ...identity } = document;
    const eventId = await core.sha256Hex(core.canonicalJson(identity));
    document.event_id = eventId;
    return {
      event_id: eventId,
      source_key: `${sourceId}|${capture.instrument.venue}|${capture.instrument.symbol}`,
      source_sequence: sourceSequence,
      mqtt_topic: `gridedge/market/v1/${capture.instrument.venue}/${capture.instrument.symbol}/status`,
      payload: core.canonicalJson(document),
      state: "PENDING",
      attempts: 0,
      created_at_us: core.unixMicrosNow(),
    };
  }

  async function canonicalSourceObservationEvent(
    capture,
    captureSha256,
    sourceInstanceId,
    sourceSequence,
    previousObservedAtUs,
    coveredThroughUs,
    latestDisplayedTradeUs,
    sourceId = SOURCE_ID,
  ) {
    const payload = {
      status: "SOURCE_OBSERVED_CURRENT",
      session_date: capture.session_date,
      observed_at_us: capture.captured_at_us,
      covered_through_us: coveredThroughUs,
      latest_displayed_trade_us: latestDisplayedTradeUs,
      page_index: capture.completeness.page_index,
      page_count: capture.completeness.page_count,
      row_count: capture.rows.length,
      capture_sha256: captureSha256,
      source_captured_at_us: capture.captured_at_us,
      policy: SOURCE_OBSERVATION_POLICY,
    };
    if (previousObservedAtUs !== undefined) {
      payload.previous_observed_at_us = previousObservedAtUs;
    }
    const document = {
      spec: "gridedge.market",
      schema_version: 1,
      event_type: "SOURCE_STATUS",
      source: {
        source_id: sourceId,
        source_instance_id: sourceInstanceId,
        source_type: "WEB_UI",
        provider: capture.provider,
        provider_version: capture.provider_version,
      },
      instrument: capture.instrument,
      source_sequence: sourceSequence,
      ts_us: capture.captured_at_us,
      recv_us: capture.captured_at_us,
      payload,
      evidence_sha256: await core.sha256Hex(core.canonicalJson({
        provider: capture.provider,
        provider_version: capture.provider_version,
        source_url: capture.source_url,
        payload,
      })),
    };
    const { recv_us: _receivedAt, ...identity } = document;
    const eventId = await core.sha256Hex(core.canonicalJson(identity));
    document.event_id = eventId;
    return {
      event_id: eventId,
      source_key: `${sourceId}|${capture.instrument.venue}|${capture.instrument.symbol}`,
      source_sequence: sourceSequence,
      mqtt_topic: `gridedge/market/v1/${capture.instrument.venue}/${capture.instrument.symbol}/status`,
      payload: core.canonicalJson(document),
      state: "PENDING",
      attempts: 0,
      created_at_us: core.unixMicrosNow(),
    };
  }

  async function canonicalResumeBoundaryEvent(
    captureValue,
    claimedCaptureSha256,
    sourceInstanceId,
    sourceSequence,
    sourceId = SOURCE_ID,
  ) {
    const capture = validateCapture(captureValue);
    if (capture.completeness.session_complete || capture.completeness.page_index !== 1) {
      throw new Error("session resume boundary requires one partial Eastmoney page-one capture");
    }
    const captureSha256 = await core.sha256Hex(core.canonicalJson(capture));
    if (captureSha256 !== claimedCaptureSha256) {
      throw new Error("session resume boundary capture SHA-256 is invalid");
    }
    const timestamps = capture.rows.map((row) =>
      core.eventTimeUs(capture.session_date, row.source_trade_time));
    const coveredFromUs = Math.min(...timestamps);
    const coveredThroughUs = Math.max(...timestamps);
    if (!isReviewedAshareSaleTimestamp(coveredFromUs) ||
        !isReviewedAshareSaleTimestamp(coveredThroughUs)) {
      throw new Error("session resume boundary rows are outside the reviewed time-sales bucket window");
    }
    const payload = {
      status: "SESSION_RESUME_BOUNDARY",
      session_date: capture.session_date,
      covered_from_us: coveredFromUs,
      covered_through_us: coveredThroughUs,
      page_index: capture.completeness.page_index,
      page_count: capture.completeness.page_count,
      row_count: capture.rows.length,
      capture_sha256: captureSha256,
      source_captured_at_us: capture.captured_at_us,
      policy: "INCOMPLETE_EASTMONEY_HISTORY_EXPLICIT_POLICY_V1",
    };
    const document = {
      spec: "gridedge.market",
      schema_version: 1,
      event_type: "SOURCE_STATUS",
      source: {
        source_id: sourceId,
        source_instance_id: sourceInstanceId,
        source_type: "WEB_UI",
        provider: capture.provider,
        provider_version: capture.provider_version,
      },
      instrument: capture.instrument,
      source_sequence: sourceSequence,
      ts_us: payload.covered_through_us,
      recv_us: capture.captured_at_us,
      payload,
      evidence_sha256: await core.sha256Hex(core.canonicalJson({
        provider: capture.provider,
        provider_version: capture.provider_version,
        source_url: capture.source_url,
        payload,
      })),
    };
    const { recv_us: _receivedAt, ...identity } = document;
    const eventId = await core.sha256Hex(core.canonicalJson(identity));
    document.event_id = eventId;
    return {
      event_id: eventId,
      source_key: `${sourceId}|${capture.instrument.venue}|${capture.instrument.symbol}`,
      source_sequence: sourceSequence,
      mqtt_topic: `gridedge/market/v1/${capture.instrument.venue}/${capture.instrument.symbol}/status`,
      payload: core.canonicalJson(document),
      state: "PENDING",
      attempts: 0,
      created_at_us: core.unixMicrosNow(),
    };
  }

  async function ingestCaptureImpl(
    database,
    captureValue,
    claimedCaptureSha256,
    { createResumeBoundary = false, sourceObservationPolicy = null } = {},
  ) {
    if (sourceObservationPolicy !== null && sourceObservationPolicy !== SOURCE_OBSERVATION_POLICY) {
      throw new Error("source observation policy is not reviewed");
    }
    const capture = validateCapture(captureValue, {
      allowStaleLatest: sourceObservationPolicy === SOURCE_OBSERVATION_POLICY,
    });
    const canonicalCapture = core.canonicalJson(capture);
    const captureSha256 = await core.sha256Hex(canonicalCapture);
    if (captureSha256 !== claimedCaptureSha256) {
      throw new Error("capture SHA-256 disagrees with canonical capture");
    }
    const sourceKey = `${SOURCE_ID}|${capture.instrument.venue}|${capture.instrument.symbol}`;
    const preparedRows = [];
    for (const row of capture.rows) {
      const identity = `${capture.provider}|${capture.instrument.venue}|${capture.instrument.symbol}|${capture.session_date}|${row.source_row_key}`;
      const rowJson = core.canonicalJson(stableRowEvidence(row));
      preparedRows.push({ identity, row, rowJson, evidenceSha256: await core.sha256Hex(rowJson) });
    }

    const readTx = database.transaction(["source_state", "web_rows"], "readonly");
    const readDone = transactionDone(readTx);
    const observedState = await request(readTx.objectStore("source_state").get(sourceKey));
    const observedRows = new Map();
    for (const prepared of preparedRows) {
      observedRows.set(prepared.identity, await request(readTx.objectStore("web_rows").get(prepared.identity)));
    }
    await readDone;

    const state = observedState
      ? { ...observedState }
      : { key: sourceKey, source_instance_id: crypto.randomUUID(), next_sequence: 1 };
    const conflicts = [];
    const duplicates = [];
    const pendingRows = [];
    for (const prepared of preparedRows) {
      const existing = observedRows.get(prepared.identity);
      if (!existing) {
        pendingRows.push(prepared);
      } else if (existing.evidence_sha256 === prepared.evidenceSha256 && existing.raw_json === prepared.rowJson) {
        duplicates.push({ ...existing, delivery_count: existing.delivery_count + 1 });
      } else {
        conflicts.push({ identity: prepared.identity, existing_evidence_sha256: existing.evidence_sha256, conflicting_evidence_sha256: prepared.evidenceSha256, conflicting_raw_json: prepared.rowJson, capture_sha256: captureSha256, created_at_us: core.unixMicrosNow() });
      }
    }

    const events = [];
    const statusEvents = [];
    if (conflicts.length === 0) {
      const previousCoveredThroughUs = state.covered_through_us;
      let livePageOverlap = 0;
      let liveCoveredThroughUs = null;
      const activeSessionDate = state.complete_session_date ?? state.resume_boundary_session_date;
      if (capture.completeness.session_complete && activeSessionDate) {
        if (capture.session_date < activeSessionDate ||
            (capture.session_date === activeSessionDate &&
             previousCoveredThroughUs !== undefined &&
             capture.completeness.covered_through_us < previousCoveredThroughUs)) {
          throw new Error("complete history capture moved behind the durable market watermark");
        }
      }
      if (activeSessionDate && !capture.completeness.session_complete && !createResumeBoundary) {
        if (capture.session_date !== activeSessionDate) {
          throw new Error("a new session requires a complete history capture before live ingestion");
        }
        if (capture.completeness.page_index !== 1) {
          throw new Error("live continuity can only advance from Eastmoney page one");
        }
        if (pendingRows.length > 0) {
          livePageOverlap = preparedRows.filter((prepared) => {
            const existing = observedRows.get(prepared.identity);
            return existing && core.eventTimeUs(capture.session_date, prepared.row.source_trade_time) <= previousCoveredThroughUs;
          }).length;
          if (livePageOverlap === 0) {
            throw new Error("live capture has no overlap with the prior durable watermark");
          }
          const pendingTimes = pendingRows.map((pending) =>
            core.eventTimeUs(capture.session_date, pending.row.source_trade_time));
          if (pendingTimes.some((timestamp) => timestamp <= previousCoveredThroughUs)) {
            throw new Error("live capture introduced an unseen trade behind the prior durable watermark");
          }
          liveCoveredThroughUs = Math.max(...pendingTimes);
        }
      }
      for (const pending of pendingRows) {
        events.push(await canonicalEvent(capture, pending.row, state.source_instance_id, state.next_sequence));
        state.next_sequence += 1;
      }
      if (capture.completeness.session_complete && state.complete_capture_sha256 !== captureSha256) {
        statusEvents.push(await canonicalStatusEvent(
          capture,
          captureSha256,
          state.source_instance_id,
          state.next_sequence,
        ));
        state.next_sequence += 1;
        state.complete_capture_sha256 = captureSha256;
        state.complete_session_date = capture.session_date;
        delete state.resume_boundary_capture_sha256;
        delete state.resume_boundary_session_date;
        state.covered_through_us = capture.completeness.covered_through_us;
      } else if (createResumeBoundary) {
        if (capture.completeness.session_complete || capture.completeness.page_index !== 1) {
          throw new Error("session resume boundary requires a partial Eastmoney page-one capture");
        }
        if (state.complete_session_date === capture.session_date) {
          throw new Error("a complete session cannot be replaced by a partial resume boundary");
        }
        if (state.resume_boundary_capture_sha256 !== captureSha256) {
          const boundaryCoveredThroughUs = Math.max(...capture.rows.map((row) =>
            core.eventTimeUs(capture.session_date, row.source_trade_time)));
          if (previousCoveredThroughUs !== undefined &&
              boundaryCoveredThroughUs <= previousCoveredThroughUs) {
            throw new Error("session resume boundary must strictly advance its durable watermark");
          }
          statusEvents.push(await canonicalResumeBoundaryEvent(
            capture,
            captureSha256,
            state.source_instance_id,
            state.next_sequence,
          ));
          state.next_sequence += 1;
          state.resume_boundary_capture_sha256 = captureSha256;
          state.resume_boundary_session_date = capture.session_date;
          delete state.complete_capture_sha256;
          delete state.complete_session_date;
          state.covered_through_us = boundaryCoveredThroughUs;
        }
      } else if (liveCoveredThroughUs !== null) {
        statusEvents.push(await canonicalLiveStatusEvent(
          capture,
          captureSha256,
          state.source_instance_id,
          state.next_sequence,
          previousCoveredThroughUs,
          liveCoveredThroughUs,
          livePageOverlap,
        ));
        state.next_sequence += 1;
        state.covered_through_us = liveCoveredThroughUs;
      }
      if (sourceObservationPolicy === SOURCE_OBSERVATION_POLICY) {
        const observationSession = state.complete_session_date ?? state.resume_boundary_session_date;
        if (capture.completeness.session_complete || capture.completeness.page_index !== 1 ||
            observationSession !== capture.session_date || state.covered_through_us === undefined) {
          throw new Error("source observation lacks an active reviewed page-one session watermark");
        }
        const previousObservedAtUs = state.source_observed_session_date === capture.session_date
          ? state.source_observed_at_us
          : undefined;
        if (previousObservedAtUs !== undefined && capture.captured_at_us <= previousObservedAtUs) {
          throw new Error("source observation clock did not strictly advance");
        }
        const latestDisplayedTradeUs = Math.max(...capture.rows.map((row) =>
          core.eventTimeUs(capture.session_date, row.source_trade_time)));
        if (latestDisplayedTradeUs !== state.covered_through_us) {
          throw new Error("source observation latest displayed trade disagrees with durable coverage");
        }
        statusEvents.push(await canonicalSourceObservationEvent(
          capture,
          captureSha256,
          state.source_instance_id,
          state.next_sequence,
          previousObservedAtUs,
          state.covered_through_us,
          latestDisplayedTradeUs,
        ));
        state.next_sequence += 1;
        state.source_observed_at_us = capture.captured_at_us;
        state.source_observed_session_date = capture.session_date;
      }
    }

    const names = ["source_state", "capture_batches", "web_rows", "capture_conflicts", "market_event_outbox"];
    const writeTx = database.transaction(names, "readwrite", { durability: "strict" });
    const writeDone = transactionDone(writeTx);
    const sourceStore = writeTx.objectStore("source_state");
    const batchStore = writeTx.objectStore("capture_batches");
    const rowStore = writeTx.objectStore("web_rows");
    const conflictStore = writeTx.objectStore("capture_conflicts");
    const outboxStore = writeTx.objectStore("market_event_outbox");
    const currentState = await request(sourceStore.get(sourceKey));
    if (core.canonicalJson(currentState ?? null) !== core.canonicalJson(observedState ?? null)) {
      writeTx.abort();
      await writeDone.catch(() => undefined);
      throw new Error("source state changed during capture preparation");
    }
    for (const prepared of preparedRows) {
      const current = await request(rowStore.get(prepared.identity));
      if (core.canonicalJson(current ?? null) !== core.canonicalJson(observedRows.get(prepared.identity) ?? null)) {
        writeTx.abort();
        await writeDone.catch(() => undefined);
        throw new Error("source row changed during capture preparation");
      }
    }
    if (conflicts.length > 0) {
      for (const conflict of conflicts) conflictStore.add(conflict);
      batchStore.put({ capture_sha256: captureSha256, canonical_json: canonicalCapture, outcome: "CONFLICT", created_at_us: core.unixMicrosNow() });
      await writeDone;
      return { accepted: 0, duplicates: duplicates.length, conflicts: conflicts.length, event_ids: [] };
    }
    for (const duplicate of duplicates) rowStore.put(duplicate);
    const eventIds = [];
    for (let index = 0; index < pendingRows.length; index += 1) {
      const pending = pendingRows[index];
      const event = events[index];
      rowStore.add({ identity: pending.identity, evidence_sha256: pending.evidenceSha256, raw_json: pending.rowJson, event_id: event.event_id, source_sequence: event.source_sequence, delivery_count: 1 });
      outboxStore.add(event);
      eventIds.push(event.event_id);
    }
    for (const event of statusEvents) {
      outboxStore.add(event);
      eventIds.push(event.event_id);
    }
    sourceStore.put(state);
    batchStore.put({ capture_sha256: captureSha256, canonical_json: canonicalCapture, outcome: "ACCEPTED", created_at_us: core.unixMicrosNow() });
    await writeDone;
    return {
      accepted: pendingRows.length,
      duplicates: duplicates.length,
      conflicts: 0,
      status_events: statusEvents.length,
      event_ids: eventIds,
    };
  }

  function ingestCapture(database, captureValue, claimedCaptureSha256, options = {}) {
    const operation = writerTail.then(() =>
      ingestCaptureImpl(database, captureValue, claimedCaptureSha256, options));
    writerTail = operation.catch(() => undefined);
    return operation;
  }

  function ingestResumeBoundary(database, captureValue, claimedCaptureSha256) {
    const operation = writerTail.then(() =>
      ingestCaptureImpl(database, captureValue, claimedCaptureSha256, {
        createResumeBoundary: true,
      }));
    writerTail = operation.catch(() => undefined);
    return operation;
  }

  async function sourceState(database, instrument) {
    if (!instrument || !["XSHE", "XSHG"].includes(instrument.venue) ||
        !/^\d{6}$/.test(instrument.symbol ?? "") || instrument.asset_class !== "EQUITY" ||
        instrument.currency !== "CNY") {
      throw new Error("source state instrument is invalid");
    }
    const sourceKey = `${SOURCE_ID}|${instrument.venue}|${instrument.symbol}`;
    const tx = database.transaction("source_state", "readonly");
    const state = await request(tx.objectStore("source_state").get(sourceKey));
    await transactionDone(tx);
    if (!state) throw new Error("source state does not exist");
    return state;
  }

  async function pendingEvents(database, limit = 100) {
    const tx = database.transaction("market_event_outbox", "readonly");
    const values = await request(tx.objectStore("market_event_outbox").index("state").getAll("PENDING"));
    await transactionDone(tx);
    return values.sort((a, b) =>
      a.source_key.localeCompare(b.source_key) || a.source_sequence - b.source_sequence,
    ).slice(0, limit);
  }

  async function acknowledge(database, eventId, authority) {
    if (!["DB_COMMIT_ACK", "TEST_ONLY"].includes(authority)) {
      throw new Error("outbox acknowledgement lacks database commit authority");
    }
    const tx = database.transaction("market_event_outbox", "readwrite", { durability: "strict" });
    const store = tx.objectStore("market_event_outbox");
    const event = await request(store.get(eventId));
    if (!event || event.state !== "PENDING") throw new Error("database ACK does not match one pending event");
    event.state = "ACKNOWLEDGED";
    event.attempts += 1;
    event.acknowledged_at_us = core.unixMicrosNow();
    event.acknowledgement_authority = authority;
    store.put(event);
    await transactionDone(tx);
  }

  async function status(database) {
    const tx = database.transaction(["web_rows", "capture_conflicts", "market_event_outbox"], "readonly");
    const rows = await request(tx.objectStore("web_rows").count());
    const conflicts = await request(tx.objectStore("capture_conflicts").count());
    const pending = await request(tx.objectStore("market_event_outbox").index("state").count("PENDING"));
    const acknowledged = await request(tx.objectStore("market_event_outbox").index("state").count("ACKNOWLEDGED"));
    await transactionDone(tx);
    return { accepted_rows: rows, conflicts, pending_events: pending, acknowledged_events: acknowledged };
  }

  function utf8Hex(value) {
    return Array.from(new TextEncoder().encode(value), (byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  function shanghaiSessionDate(timestampUs) {
    if (!Number.isSafeInteger(timestampUs) || timestampUs < 0) {
      throw new Error("stored replay timestamp is invalid");
    }
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(new Date(timestampUs / 1000));
    const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${value.year}-${value.month}-${value.day}`;
  }

  async function validateReplayEvent(event) {
    if (!event || typeof event !== "object" || Array.isArray(event) ||
        !["PENDING", "ACKNOWLEDGED"].includes(event.state)) {
      throw new Error("stored replay event state is invalid");
    }
    if (typeof event.payload !== "string") throw new Error("stored replay payload is invalid");
    let document;
    try {
      document = JSON.parse(event.payload);
    } catch (_error) {
      throw new Error("stored replay payload is not JSON");
    }
    if (event.payload !== core.canonicalJson(document)) {
      throw new Error("stored replay payload is not canonical JSON");
    }
    if (document.spec !== "gridedge.market" || document.schema_version !== 1 ||
        !["TRADE_TICK", "SOURCE_STATUS"].includes(document.event_type)) {
      throw new Error("stored replay event contract is unsupported");
    }
    const source = document.source;
    if (source?.source_id !== SOURCE_ID || source.source_type !== "WEB_UI" ||
        source.provider !== "eastmoney" ||
        !["eastmoney-time-sales-dom-v5", "eastmoney-time-sales-dom-v6"].includes(source.provider_version) ||
        typeof source.source_instance_id !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(source.source_instance_id)) {
      throw new Error("stored replay source identity is invalid");
    }
    const instrument = document.instrument;
    if (!instrument || !["XSHE", "XSHG"].includes(instrument.venue) ||
        !/^\d{6}$/.test(instrument.symbol ?? "") || instrument.asset_class !== "EQUITY" ||
        instrument.currency !== "CNY") {
      throw new Error("stored replay instrument is invalid");
    }
    if (!Number.isSafeInteger(document.source_sequence) || document.source_sequence < 1 ||
        !Number.isSafeInteger(document.recv_us) || document.recv_us < 0 ||
        !Number.isSafeInteger(document.ts_us) || document.ts_us < 0) {
      throw new Error("stored replay sequence or timestamp is invalid");
    }
    const expectedSourceKey = `${SOURCE_ID}|${instrument.venue}|${instrument.symbol}`;
    const expectedTopic = `gridedge/market/v1/${instrument.venue}/${instrument.symbol}/${
      document.event_type === "TRADE_TICK" ? "trade" : "status"
    }`;
    if (event.source_key !== expectedSourceKey) throw new Error("stored replay source key is invalid");
    if (event.mqtt_topic !== expectedTopic) throw new Error("stored replay topic is invalid");
    if (event.source_sequence !== document.source_sequence || event.event_id !== document.event_id) {
      throw new Error("stored replay durable identity disagrees with payload");
    }
    const identity = structuredClone(document);
    delete identity.event_id;
    delete identity.recv_us;
    const computedEventId = await core.sha256Hex(core.canonicalJson(identity));
    if (computedEventId !== document.event_id) throw new Error("stored replay event id is invalid");

    const sessionDate = shanghaiSessionDate(document.ts_us);
    if (document.event_type === "TRADE_TICK") {
      if (document.payload?.source_row_key?.split("|", 1)[0] !== sessionDate) {
        throw new Error("stored replay trade session date disagrees with timestamp");
      }
      if (!Number.isSafeInteger(document.payload?.quantity) || document.payload.quantity <= 0 ||
          document.payload.unit !== "SHARE" ||
          !["BUY", "SELL", "UNKNOWN"].includes(document.payload.side) ||
          !Number.isSafeInteger(document.payload?.price?.mantissa) ||
          !Number.isSafeInteger(document.payload?.price?.scale)) {
        throw new Error("stored replay trade payload is invalid");
      }
    } else {
      const sourceObserved = document.payload?.status === "SOURCE_OBSERVED_CURRENT";
      if (document.payload?.session_date !== sessionDate ||
          !["SESSION_HISTORY_COMPLETE", "SESSION_RESUME_BOUNDARY", "LIVE_CONTIGUOUS", "SOURCE_OBSERVED_CURRENT"]
            .includes(document.payload?.status) ||
          (!sourceObserved && document.payload?.covered_through_us !== document.ts_us)) {
        throw new Error("stored replay status session date or watermark is invalid");
      }
      if (sourceObserved &&
          (source.provider_version !== "eastmoney-time-sales-dom-v6" ||
           document.payload.observed_at_us !== document.ts_us ||
           document.recv_us !== document.ts_us ||
           !Number.isSafeInteger(document.payload.covered_through_us) ||
           document.payload.covered_through_us > document.payload.observed_at_us ||
           document.payload.latest_displayed_trade_us !== document.payload.covered_through_us ||
           document.payload.page_index !== 1 ||
           !Number.isSafeInteger(document.payload.page_count) || document.payload.page_count < 1 ||
           !Number.isSafeInteger(document.payload.row_count) || document.payload.row_count < 1 ||
           document.payload.policy !== SOURCE_OBSERVATION_POLICY ||
           (document.payload.previous_observed_at_us !== undefined &&
            (!Number.isSafeInteger(document.payload.previous_observed_at_us) ||
             document.payload.previous_observed_at_us >= document.payload.observed_at_us)))) {
        throw new Error("stored replay source observation proof is invalid");
      }
      if (document.payload.status === "SESSION_RESUME_BOUNDARY" &&
          (source.provider_version !== "eastmoney-time-sales-dom-v6" ||
           !Number.isSafeInteger(document.payload.covered_from_us) ||
           document.payload.covered_from_us > document.payload.covered_through_us ||
           shanghaiSessionDate(document.payload.covered_from_us) !== sessionDate ||
           !isReviewedAshareSaleTimestamp(document.payload.covered_from_us) ||
           document.payload.page_index !== 1 ||
           !Number.isSafeInteger(document.payload.page_count) || document.payload.page_count < 1 ||
           !Number.isSafeInteger(document.payload.row_count) || document.payload.row_count < 1 ||
           document.payload.policy !== "INCOMPLETE_EASTMONEY_HISTORY_EXPLICIT_POLICY_V1")) {
        throw new Error("stored replay partial boundary proof is invalid");
      }
      if (document.payload.status === "LIVE_CONTIGUOUS" &&
          (!Number.isSafeInteger(document.payload.previous_covered_through_us) ||
           document.payload.previous_covered_through_us >= document.payload.covered_through_us)) {
        throw new Error("stored replay live predecessor is invalid");
      }
    }
    if (source.provider_version === "eastmoney-time-sales-dom-v6" &&
        document.payload?.source_captured_at_us !== document.recv_us) {
      throw new Error("stored replay v6 capture timestamp is not bound to receipt time");
    }
    return { event, document, sessionDate, sourceIdentity: core.canonicalJson({
      source_id: source.source_id,
      source_instance_id: source.source_instance_id,
      venue: instrument.venue,
      symbol: instrument.symbol,
    }) };
  }

  async function replayExport(database, sessionDate) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(sessionDate)) {
      throw new Error("replay export session date must be YYYY-MM-DD");
    }
    const tx = database.transaction("market_event_outbox", "readonly");
    const stored = await request(tx.objectStore("market_event_outbox").getAll());
    await transactionDone(tx);

    const validated = [];
    for (const event of stored) validated.push(await validateReplayEvent(event));
    const selected = validated.filter((entry) => entry.sessionDate === sessionDate);
    selected.sort((left, right) => left.event.source_sequence - right.event.source_sequence);
    if (new Set(selected.map((entry) => entry.sourceIdentity)).size > 1) {
      throw new Error("stored replay selection does not belong to a single source identity");
    }
    if (selected.some((entry, index) => index > 0 &&
        entry.event.source_sequence !== selected[index - 1].event.source_sequence + 1)) {
      throw new Error("stored replay source sequence is not contiguous");
    }
    const records = selected.map(({ event }) => core.canonicalJson({
      topic: event.mqtt_topic,
      payload_hex: utf8Hex(event.payload),
    }));
    return {
      session_date: sessionDate,
      record_count: records.length,
      first_source_sequence: selected[0]?.event.source_sequence ?? null,
      last_source_sequence: selected.at(-1)?.event.source_sequence ?? null,
      pending_count: selected.filter(({ event }) => event.state === "PENDING").length,
      acknowledged_count: selected.filter(({ event }) => event.state === "ACKNOWLEDGED").length,
      provider_versions: [...new Set(selected.map(({ document }) => document.source?.provider_version))].sort(),
      integrity_scope: "LOCAL_BROWSER_FORENSIC",
      trusted_for_live: false,
      records,
    };
  }

  return { DATABASE_NAME: DB_NAME, SOURCE_ID, acknowledge, canonicalEvent, canonicalResumeBoundaryEvent, ingestCapture, ingestResumeBoundary, openDatabase, pendingEvents, replayExport, sourceState, status, validateCapture };
});
