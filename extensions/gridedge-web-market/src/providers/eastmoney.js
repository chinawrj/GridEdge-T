(function registerEastmoney(root) {
  "use strict";

  const core = root.GridEdgeMarket;
  if (!core) {
    throw new Error("GridEdge shared market helpers were not loaded");
  }

  const PROVIDER = "eastmoney";
  const PROVIDER_VERSION = "eastmoney-time-sales-dom-v6";
  const TRADE_TIME = /^(?:0\d|1\d|2[0-3]):[0-5]\d:[0-5]\d$/;

  function tradePrice(cellText) {
    const text = core.normalizeText(cellText);
    const match = /^(.*?)(?:[↑↓])?$/.exec(text);
    return core.strictPositiveDecimal(match[1], "trade price");
  }

  function matches(url) {
    const parsed = new URL(url);
    return (
      parsed.protocol === "https:" &&
      parsed.hostname === "quote.eastmoney.com" &&
      (parsed.pathname === "/f1.html" || /^\/(?:sh|sz)\d{6}\.html$/.test(parsed.pathname))
    );
  }

  function instrumentFromUrl(url) {
    const parsed = new URL(url);
    const newCode = parsed.searchParams.get("newcode");
    if (newCode) {
      const match = /^([01])\.(\d{6})$/.exec(newCode);
      if (!match) {
        throw new Error("Eastmoney newcode is not a reviewed A-share identity");
      }
      return {
        venue: match[1] === "0" ? "XSHE" : "XSHG",
        symbol: match[2],
        asset_class: "EQUITY",
        currency: "CNY",
      };
    }
    const pathMatch = /^\/(sh|sz)(\d{6})\.html$/.exec(parsed.pathname);
    if (!pathMatch) {
      throw new Error("Eastmoney URL does not identify one reviewed A-share instrument");
    }
    return {
      venue: pathMatch[1] === "sz" ? "XSHE" : "XSHG",
      symbol: pathMatch[2],
      asset_class: "EQUITY",
      currency: "CNY",
    };
  }

  function pageNumber(bodyText) {
    const normalized = core.normalizeText(bodyText);
    const matches = [...normalized.matchAll(/(?:^|\D)(\d+)\s*\/\s*(\d+)\s*页(?=\D|$)/g)];
    const pages = new Map(matches.map((match) => [
      `${Number(match[1])}/${Number(match[2])}`,
      { page_index: Number(match[1]), page_count: Number(match[2]) },
    ]));
    if (pages.size > 1) {
      throw new Error("Eastmoney page exposes conflicting pagination tokens");
    }
    return pages.values().next().value ?? { page_index: null, page_count: null };
  }

  function locateTimeSalesTables(tables) {
    const matches = [];
    for (let tableIndex = 0; tableIndex < tables.length; tableIndex += 1) {
      const table = tables[tableIndex];
      for (let index = 0; index < table.length; index += 1) {
        const header = table[index].map((cell) => core.normalizeText(cell.text));
        const time = header.indexOf("时间");
        const price = header.indexOf("成交价");
        const hands = header.indexOf("手数");
        if (time >= 0 && price >= 0 && hands >= 0) {
          matches.push({ table, tableIndex, headerIndex: index, columns: { time, price, hands } });
          break;
        }
      }
    }
    return matches;
  }

  function parseRows(tableMatches, sessionDate) {
    const candidates = [];
    for (const tableMatch of tableMatches) {
      const { table, tableIndex, headerIndex, columns } = tableMatch;
      const requiredIndex = Math.max(columns.time, columns.price, columns.hands);
      const sourceRows = table.slice(headerIndex + 1);
      for (let rowIndex = 0; rowIndex < sourceRows.length; rowIndex += 1) {
        const cells = sourceRows[rowIndex];
        if (cells.length <= requiredIndex) {
          if (cells.some((cell) => core.normalizeText(cell.text))) {
            throw new Error("Eastmoney time-sales row has fewer cells than its reviewed header");
          }
          continue;
        }
        const time = core.normalizeText(cells[columns.time].text);
        const priceText = core.normalizeText(cells[columns.price].text);
        const handsText = core.normalizeText(cells[columns.hands].text);
        if (!time && !priceText && !handsText) {
          continue;
        }
        if (!TRADE_TIME.test(time)) {
          throw new Error("Eastmoney time-sales row has an unrecognized source time");
        }
        let price;
        let hands;
        try {
          price = tradePrice(priceText);
          hands = core.strictNonNegativeInteger(handsText, "trade hands");
        } catch (error) {
          throw new Error(`Eastmoney time-sales row is incomplete: ${error.message}`);
        }
        if (hands === 0) {
          continue;
        }
        const quantity = hands * 100;
        if (!Number.isSafeInteger(quantity)) {
          throw new Error("Eastmoney trade quantity exceeds the exact JavaScript range");
        }
        candidates.push({
          source_trade_time: time,
          price,
          quantity,
          quantity_hands: hands,
          unit: "SHARE",
          side: "UNKNOWN",
          source_table_ordinal: tableIndex,
          source_row_ordinal: rowIndex,
          raw_cells: cells.map((cell) => core.normalizeText(cell.text)),
        });
      }
    }
    candidates.sort((left, right) =>
      left.source_trade_time.localeCompare(right.source_trade_time) ||
      left.price.localeCompare(right.price) ||
      left.quantity_hands - right.quantity_hands ||
      left.side.localeCompare(right.side),
    );
    const occurrences = new Map();
    return candidates.map((candidate) => {
      const identity = `${sessionDate}|${candidate.source_trade_time}|${candidate.price}|${candidate.quantity_hands}|${candidate.side}`;
      const occurrence = (occurrences.get(identity) ?? 0) + 1;
      occurrences.set(identity, occurrence);
      return {
        ...candidate,
        source_row_key: `${identity}|${occurrence}`,
        occurrence,
      };
    });
  }

  function parseSnapshot(snapshot) {
    if (!matches(snapshot.url)) {
      throw new Error("page is outside the Eastmoney adapter allowlist");
    }
    const instrument = instrumentFromUrl(snapshot.url);
    const sessionDate = snapshot.sessionDate;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(sessionDate)) {
      throw new Error("capture lacks a valid Asia/Shanghai session date");
    }
    const pagination = pageNumber(snapshot.bodyText);
    const rows = parseRows(locateTimeSalesTables(snapshot.tables), sessionDate);
    return {
      capture_spec: core.CAPTURE_SPEC,
      schema_version: core.CAPTURE_SCHEMA_VERSION,
      provider: PROVIDER,
      provider_version: PROVIDER_VERSION,
      page_kind: "TIME_SALES",
      source_url: snapshot.url,
      source_title: core.normalizeText(snapshot.title),
      captured_at_us: snapshot.capturedAtUs,
      session_date: sessionDate,
      instrument,
      completeness: {
        ...pagination,
        row_count: rows.length,
        session_complete: false,
        session_date_basis: "COLLECTOR_ASIA_SHANGHAI_DATE",
        identity_policy: "DOM_VALUE_OCCURRENCE_V1",
      },
      rows,
    };
  }

  function stableFact(row) {
    return {
      source_row_key: row.source_row_key,
      source_trade_time: row.source_trade_time,
      price: row.price,
      quantity: row.quantity,
      quantity_hands: row.quantity_hands,
      unit: row.unit,
      side: row.side,
      occurrence: row.occurrence,
    };
  }

  function assembleSessionHistory(pageCaptures, finalFirstPage, pageHashes, finalFirstPageHash) {
    if (!Array.isArray(pageCaptures) || pageCaptures.length === 0 ||
        !Array.isArray(pageHashes) || pageHashes.length !== pageCaptures.length ||
        !/^[0-9a-f]{64}$/.test(finalFirstPageHash) ||
        !pageHashes.every((hash) => /^[0-9a-f]{64}$/.test(hash))) {
      throw new Error("history assembly requires one reviewed hash per page capture");
    }
    const all = [...pageCaptures, finalFirstPage];
    const reference = pageCaptures[0];
    if (reference.completeness.page_index !== 1 || finalFirstPage.completeness.page_index !== 1) {
      throw new Error("history assembly must start and finish on page one");
    }
    for (const capture of all) {
      if (capture.provider !== PROVIDER || capture.provider_version !== PROVIDER_VERSION ||
          capture.page_kind !== "TIME_SALES" || capture.session_date !== reference.session_date ||
          core.canonicalJson(capture.instrument) !== core.canonicalJson(reference.instrument)) {
        throw new Error("history pages do not share one reviewed source session");
      }
    }
    const pageCount = finalFirstPage.completeness.page_count;
    const pageIndexes = pageCaptures.map((capture) => capture.completeness.page_index);
    if (!Number.isSafeInteger(pageCount) || pageCount < 1 || pageCaptures.length !== pageCount ||
        pageIndexes.some((page, index) => page !== index + 1) ||
        pageCaptures.some((capture) => capture.completeness.page_count > pageCount)) {
      throw new Error("history assembly must capture every history page exactly once");
    }
    const initialKeys = new Set(reference.rows.map((row) => row.source_row_key));
    const livePageOverlap = finalFirstPage.rows.filter((row) => initialKeys.has(row.source_row_key)).length;
    if (livePageOverlap === 0) {
      throw new Error("final live page has no overlap with the initial live page");
    }
    const merged = new Map();
    for (const capture of all) {
      for (const row of capture.rows) {
        const fact = core.canonicalJson(stableFact(row));
        const existing = merged.get(row.source_row_key);
        if (existing && existing.fact !== fact) {
          throw new Error("history pages reuse one source row identity with different facts");
        }
        if (!existing) merged.set(row.source_row_key, { fact, row });
      }
    }
    const rows = [...merged.values()].map((entry) => entry.row).sort((left, right) =>
      left.source_trade_time.localeCompare(right.source_trade_time) ||
      left.price.localeCompare(right.price) ||
      left.quantity_hands - right.quantity_hands ||
      left.source_row_key.localeCompare(right.source_row_key),
    );
    if (rows.length === 0) throw new Error("history assembly produced no market rows");
    return {
      ...finalFirstPage,
      page_kind: "TIME_SALES_SESSION",
      completeness: {
        ...finalFirstPage.completeness,
        pages_captured: pageIndexes,
        history_page_sha256: pageHashes,
        final_live_page_sha256: finalFirstPageHash,
        live_page_overlap: livePageOverlap,
        row_count: rows.length,
        session_complete: true,
        covered_from_us: core.eventTimeUs(reference.session_date, rows[0].source_trade_time),
        covered_through_us: core.eventTimeUs(reference.session_date, rows.at(-1).source_trade_time),
      },
      rows,
    };
  }

  core.providers[PROVIDER] = {
    PROVIDER,
    PROVIDER_VERSION,
    instrumentFromUrl,
    matches,
    assembleSessionHistory,
    parseSnapshot,
  };

  if (typeof module === "object" && module.exports) {
    module.exports = core.providers[PROVIDER];
  }
})(typeof globalThis === "object" ? globalThis : this);
