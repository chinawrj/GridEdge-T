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

  function parseRows(tableMatches, sessionDate, rowOrder) {
    if (!["LATEST_FIRST", "EARLIEST_FIRST"].includes(rowOrder)) {
      throw new Error("Eastmoney capture lacks an explicit reviewed DOM row order");
    }
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
    let direction = 0;
    for (let index = 1; index < candidates.length; index += 1) {
      const compared = candidates[index].source_trade_time.localeCompare(
        candidates[index - 1].source_trade_time,
      );
      if (compared === 0) continue;
      const nextDirection = compared > 0 ? 1 : -1;
      if (direction !== 0 && direction !== nextDirection) {
        throw new Error("Eastmoney time-sales DOM order is not monotonic");
      }
      direction = nextDirection;
    }
    const expectedDirection = rowOrder === "LATEST_FIRST" ? -1 : 1;
    if (direction !== 0 && direction !== expectedDirection) {
      throw new Error("Eastmoney time-sales DOM order disagrees with its reviewed control");
    }
    if (rowOrder === "LATEST_FIRST") candidates.reverse();
    const occurrences = new Map();
    const secondOrdinals = new Map();
    return candidates.map((candidate) => {
      const identity = `${sessionDate}|${candidate.source_trade_time}|${candidate.price}|${candidate.quantity_hands}|${candidate.side}`;
      const occurrence = (occurrences.get(identity) ?? 0) + 1;
      occurrences.set(identity, occurrence);
      const sourceSameSecondOrdinal =
        (secondOrdinals.get(candidate.source_trade_time) ?? 0) + 1;
      secondOrdinals.set(candidate.source_trade_time, sourceSameSecondOrdinal);
      return {
        ...candidate,
        source_row_key: `${identity}|${occurrence}`,
        occurrence,
        source_same_second_ordinal: sourceSameSecondOrdinal,
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
    const rows = parseRows(locateTimeSalesTables(snapshot.tables), sessionDate, snapshot.rowOrder);
    return {
      capture_spec: core.CAPTURE_SPEC,
      schema_version: core.CAPTURE_SCHEMA_VERSION,
      provider: PROVIDER,
      provider_version: PROVIDER_VERSION,
      page_kind: "TIME_SALES",
      source_url: snapshot.url,
      source_title: core.normalizeText(snapshot.title),
      source_row_order: snapshot.rowOrder,
      captured_at_us: snapshot.capturedAtUs,
      session_date: sessionDate,
      instrument,
      completeness: {
        ...pagination,
        row_count: rows.length,
        session_complete: false,
        session_date_basis: "COLLECTOR_ASIA_SHANGHAI_DATE",
        identity_policy: "DOM_CHRONOLOGICAL_ORDER_V2",
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
    const pageRowsets = pageCaptures.map((capture) =>
      core.canonicalJson(capture.rows.map(stableFact)));
    if (new Set(pageRowsets).size !== pageRowsets.length) {
      throw new Error("history pages expose a duplicate time-sales rowset after pagination");
    }
    for (let index = 1; index < pageCaptures.length; index += 1) {
      const newer = pageCaptures[index - 1].rows;
      const older = pageCaptures[index].rows;
      const newerFirst = newer[0].source_trade_time;
      const newerLast = newer.at(-1).source_trade_time;
      const olderFirst = older[0].source_trade_time;
      const olderLast = older.at(-1).source_trade_time;
      if (olderFirst > newerFirst || olderLast > newerLast ||
          (olderFirst === newerFirst && olderLast === newerLast)) {
        throw new Error("history page time range does not move backward after pagination");
      }
    }
    const historicalKeys = new Set();
    for (const capture of pageCaptures) {
      for (const row of capture.rows) {
        if (historicalKeys.has(row.source_row_key)) {
          throw new Error("history pages overlap at an unprovable source row identity");
        }
        historicalKeys.add(row.source_row_key);
      }
    }
    const initialKeys = new Set(reference.rows.map((row) => row.source_row_key));
    const livePageOverlap = finalFirstPage.rows.filter((row) => initialKeys.has(row.source_row_key)).length;
    if (livePageOverlap === 0) {
      throw new Error("final live page has no overlap with the initial live page");
    }
    for (const row of finalFirstPage.rows) {
      if (initialKeys.has(row.source_row_key)) {
        const initial = reference.rows.find((candidate) =>
          candidate.source_row_key === row.source_row_key);
        if (core.canonicalJson(stableFact(initial)) !== core.canonicalJson(stableFact(row))) {
          throw new Error("final live page changed an overlapping source fact");
        }
      } else if (historicalKeys.has(row.source_row_key)) {
        throw new Error("final live page overlaps an older history page ambiguously");
      }
    }
    const rows = pageCaptures.slice().reverse().flatMap((capture) => capture.rows);
    const newestHistoricalTime = rows.at(-1)?.source_trade_time;
    const appendedLiveRows = finalFirstPage.rows.filter((row) => !initialKeys.has(row.source_row_key));
    if (appendedLiveRows.some((row) => row.source_trade_time < newestHistoricalTime)) {
      throw new Error("final live page introduced an unseen row behind captured history");
    }
    rows.push(...appendedLiveRows);
    for (let index = 1; index < rows.length; index += 1) {
      if (rows[index].source_trade_time < rows[index - 1].source_trade_time) {
        throw new Error("assembled history is not chronological in reviewed DOM order");
      }
    }
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
