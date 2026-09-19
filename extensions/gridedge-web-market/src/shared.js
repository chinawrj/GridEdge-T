(function initGridEdgeMarket(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) {
    module.exports = api;
  }
  root.GridEdgeMarket = api;
})(typeof globalThis === "object" ? globalThis : this, function buildApi() {
  "use strict";

  const CAPTURE_SPEC = "gridedge.web-market.capture";
  const CAPTURE_SCHEMA_VERSION = 1;
  const MAX_SAFE_U64 = Number.MAX_SAFE_INTEGER;
  const A_SHARE_CALENDAR_VERSION = "SSE_2026_NOTICE_45";
  const A_SHARE_2026_CLOSED_WEEKDAYS = new Set([
    "2026-01-01", "2026-01-02",
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-23",
    "2026-04-06", "2026-05-01", "2026-05-04", "2026-05-05", "2026-06-19",
    "2026-09-25", "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07",
  ]);

  function normalizeText(value) {
    return String(value ?? "")
      .replace(/[\u00a0\u3000]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function canonicalize(value) {
    if (Array.isArray(value)) {
      return value.map(canonicalize);
    }
    if (value && typeof value === "object") {
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [key, canonicalize(value[key])]),
      );
    }
    return value;
  }

  function canonicalJson(value) {
    return JSON.stringify(canonicalize(value));
  }

  function stableMarketRowEvidence(row) {
    const {
      source_table_ordinal: _sourceTableOrdinal,
      source_row_ordinal: _sourceRowOrdinal,
      source_same_second_ordinal: _sourceSameSecondOrdinal,
      raw_cells: _rawCells,
      ...evidence
    } = row;
    return evidence;
  }

  async function sha256Hex(value) {
    const bytes =
      typeof value === "string" ? new TextEncoder().encode(value) : value;
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    return Array.from(new Uint8Array(digest), (byte) =>
      byte.toString(16).padStart(2, "0"),
    ).join("");
  }

  function unixMicrosNow() {
    const value = Date.now() * 1000;
    if (!Number.isSafeInteger(value) || value < 0 || value > MAX_SAFE_U64) {
      throw new Error("collector clock is outside the exact JavaScript u64 range");
    }
    return value;
  }

  function shanghaiDate(now = new Date()) {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(now);
    const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${values.year}-${values.month}-${values.day}`;
  }

  function strictPositiveDecimal(value, label = "price") {
    const text = normalizeText(value).replace(/,/g, "");
    if (!/^(?:0|[1-9]\d*)(?:\.\d{1,6})?$/.test(text)) {
      throw new Error(`${label} is not a supported decimal`);
    }
    if (/^0(?:\.0+)?$/.test(text)) {
      throw new Error(`${label} must be positive`);
    }
    const [whole, fractional = ""] = text.split(".");
    const normalizedFractional = fractional.replace(/0+$/, "");
    return normalizedFractional ? `${whole}.${normalizedFractional}` : whole;
  }

  function strictNonNegativeInteger(value, label) {
    const text = normalizeText(value).replace(/,/g, "");
    if (!/^\d+$/.test(text)) {
      throw new Error(`${label} must be a non-negative integer`);
    }
    const parsed = Number(text);
    if (!Number.isSafeInteger(parsed)) {
      throw new Error(`${label} exceeds the exact JavaScript integer range`);
    }
    return parsed;
  }

  function priceParts(value) {
    const text = strictPositiveDecimal(value, "trade price");
    const [whole, fraction = ""] = text.split(".");
    const mantissa = Number(`${whole}${fraction}`);
    if (!Number.isSafeInteger(mantissa)) {
      throw new Error("trade price mantissa exceeds the exact JavaScript integer range");
    }
    return { mantissa, scale: fraction.length };
  }

  function eventTimeUs(sessionDate, tradeTime) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(sessionDate) ||
        !/^(?:0\d|1\d|2[0-3]):[0-5]\d:[0-5]\d$/.test(tradeTime)) {
      throw new Error("trade row lacks a valid source date/time");
    }
    const milliseconds = Date.parse(`${sessionDate}T${tradeTime}+08:00`);
    const value = milliseconds * 1000;
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error("trade row timestamp is outside the exact JavaScript range");
    }
    return value;
  }

  function isReviewedAshareSaleTimestamp(timestampUs) {
    if (!Number.isSafeInteger(timestampUs) || timestampUs < 0) return false;
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Shanghai",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    }).formatToParts(new Date(timestampUs / 1000));
    const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    const secondOfDay = Number(value.hour) * 3600 + Number(value.minute) * 60 +
      Number(value.second);
    return (secondOfDay >= 9 * 3600 + 25 * 60 && secondOfDay <= 11 * 3600 + 30 * 60) ||
      (secondOfDay >= 13 * 3600 && secondOfDay <= 15 * 3600);
  }

  function isAshareTradingDate(sessionDate) {
    if (!/^2026-\d{2}-\d{2}$/.test(sessionDate)) {
      throw new Error(`session date is outside calendar ${A_SHARE_CALENDAR_VERSION}`);
    }
    const [year, month, day] = sessionDate.split("-").map(Number);
    const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
    return weekday !== 0 && weekday !== 6 && !A_SHARE_2026_CLOSED_WEEKDAYS.has(sessionDate);
  }

  function validateSourceObservationTiming(capture, nowUs = unixMicrosNow()) {
    if (!capture || !Number.isSafeInteger(capture.captured_at_us) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(capture.session_date) ||
        !Array.isArray(capture.rows) || capture.rows.length === 0) {
      throw new Error("capture timing evidence is incomplete");
    }
    const capturedDate = shanghaiDate(new Date(capture.captured_at_us / 1000));
    if (capturedDate !== capture.session_date) {
      throw new Error("capture session date disagrees with its Shanghai clock");
    }
    if (!isAshareTradingDate(capture.session_date)) {
      throw new Error("capture is on a non-trading day");
    }
    if (!Number.isSafeInteger(nowUs) || capture.captured_at_us > nowUs) {
      throw new Error("capture clock is in the future");
    }
    const midnightUs = eventTimeUs(capture.session_date, "00:00:00");
    const clockSeconds = Math.floor((capture.captured_at_us - midnightUs) / 1_000_000);
    const morningOpen = 9 * 3600 + 15 * 60;
    const morningStop = 11 * 3600 + 31 * 60;
    const afternoonOpen = 12 * 3600 + 59 * 60;
    const afternoonStop = 15 * 3600 + 60;
    if (!((clockSeconds >= morningOpen && clockSeconds <= morningStop) ||
          (clockSeconds >= afternoonOpen && clockSeconds <= afternoonStop))) {
      throw new Error("capture is outside the reviewed collection window");
    }
    const latestTradeUs = Math.max(...capture.rows.map((row) =>
      eventTimeUs(capture.session_date, row.source_trade_time)));
    const ageUs = capture.captured_at_us - latestTradeUs;
    if (ageUs < 0) {
      throw new Error("capture latest row is later than its capture clock");
    }
    return capture;
  }

  function validateCaptureTiming(capture, nowUs = unixMicrosNow()) {
    validateSourceObservationTiming(capture, nowUs);
    const latestTradeUs = Math.max(...capture.rows.map((row) =>
      eventTimeUs(capture.session_date, row.source_trade_time)));
    if (capture.captured_at_us - latestTradeUs > 60_000_000) {
      throw new Error("capture latest row is stale");
    }
    return capture;
  }

  function validateClockBoundSourceObservationTiming(capture, nowUs = unixMicrosNow()) {
    validateSourceObservationTiming(capture, nowUs);
    if (!Number.isSafeInteger(capture.source_page_observed_at_us) ||
        capture.source_page_observed_at_us < 0 ||
        capture.source_row_order !== "LATEST_FIRST") {
      throw new Error("source observation lacks its reviewed page clock or order proof");
    }
    const ageUs = capture.captured_at_us - capture.source_page_observed_at_us;
    if (ageUs < 0) throw new Error("source page clock is in the future");
    if (ageUs > 15_000_000) throw new Error("source page clock is stale");
    if (shanghaiDate(new Date(capture.source_page_observed_at_us / 1000)) !== capture.session_date) {
      throw new Error("source page clock session date disagrees with capture");
    }
    return capture;
  }

  function validateServerClockBoundSourceObservationTiming(capture, nowUs = unixMicrosNow()) {
    validateSourceObservationTiming(capture, nowUs);
    if (!Number.isSafeInteger(capture.source_server_observed_at_us) ||
        capture.source_server_observed_at_us < 0 ||
        capture.source_clock_origin !== "EASTMONEY_HTTPS_DATE_HEADER" ||
        capture.source_row_order !== "LATEST_FIRST") {
      throw new Error("source observation lacks its reviewed HTTPS clock or order proof");
    }
    const ageUs = capture.captured_at_us - capture.source_server_observed_at_us;
    if (ageUs < 0) throw new Error("source HTTPS clock is in the future");
    if (ageUs > 15_000_000) throw new Error("source HTTPS clock is stale");
    if (shanghaiDate(new Date(capture.source_server_observed_at_us / 1000)) !== capture.session_date) {
      throw new Error("source HTTPS clock session date disagrees with capture");
    }
    return capture;
  }

  return {
    CAPTURE_SCHEMA_VERSION,
    CAPTURE_SPEC,
    A_SHARE_CALENDAR_VERSION,
    canonicalJson,
    normalizeText,
    eventTimeUs,
    isReviewedAshareSaleTimestamp,
    isAshareTradingDate,
    priceParts,
    providers: {},
    sha256Hex,
    shanghaiDate,
    stableMarketRowEvidence,
    strictNonNegativeInteger,
    strictPositiveDecimal,
    unixMicrosNow,
    validateCaptureTiming,
    validateClockBoundSourceObservationTiming,
    validateServerClockBoundSourceObservationTiming,
    validateSourceObservationTiming,
  };
});
