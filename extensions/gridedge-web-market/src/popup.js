"use strict";

const state = document.querySelector("#state");
const detail = document.querySelector("#detail");
const exportButton = document.querySelector("#export");

function shanghaiDate() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const value = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
}

async function message(type) {
  const response = await chrome.runtime.sendMessage({ type });
  if (response?.ok === false) {
    throw new Error(response.error ?? response.reason ?? "操作失败");
  }
  return response;
}

async function refresh() {
  const response = await message("GRIDEDGE_GET_STATUS");
  state.textContent = response.settings.enabled ? "采集已启用" : "采集已暂停";
  detail.textContent = JSON.stringify({ store: response.store, last_status: response.last_status }, null, 2);
}

document.querySelector("#scan").addEventListener("click", async () => {
  try {
    detail.textContent = JSON.stringify(await message("GRIDEDGE_SCAN_ACTIVE_TAB"), null, 2);
    await refresh();
  } catch (error) {
    detail.textContent = String(error.message ?? error);
  }
});
document.querySelector("#flush").addEventListener("click", async () => {
  try {
    detail.textContent = JSON.stringify(await message("GRIDEDGE_FLUSH_OUTBOX"), null, 2);
    await refresh();
  } catch (error) {
    detail.textContent = String(error.message ?? error);
  }
});
exportButton.addEventListener("click", async () => {
  let database;
  exportButton.disabled = true;
  try {
    const sessionDate = shanghaiDate();
    database = await GridEdgeDurable.openDatabase();
    let exported = await GridEdgeDurable.replayExport(database, sessionDate);
    let storeGeneration = GridEdgeDurable.DATABASE_NAME;
    if (exported.record_count === 0) {
      database.close();
      database = undefined;
      const legacyName = "gridedge-web-market-v5";
      const databases = await indexedDB.databases();
      if (databases.some(({ name }) => name === legacyName)) {
        database = await GridEdgeDurable.openDatabase(indexedDB, legacyName);
        exported = await GridEdgeDurable.replayExport(database, sessionDate);
        storeGeneration = `${legacyName}:LOCAL_BROWSER_FORENSIC`;
      }
    }
    if (exported.record_count === 0) throw new Error(`${sessionDate} 没有可导出的持久行情`);
    const blob = new Blob([`${exported.records.join("\n")}\n`], { type: "application/x-ndjson" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `gridedge-002256-${sessionDate}-market-replay-${storeGeneration}.jsonl`;
    anchor.hidden = true;
    document.body.append(anchor);
    anchor.click();
    setTimeout(() => {
      URL.revokeObjectURL(url);
      anchor.remove();
    }, 1000);
    detail.textContent = JSON.stringify({ ...exported, store_generation: storeGeneration, records: undefined }, null, 2);
  } catch (error) {
    detail.textContent = String(error.message ?? error);
  } finally {
    database?.close();
    exportButton.disabled = false;
  }
});
document.querySelector("#options").addEventListener("click", () => chrome.runtime.openOptionsPage());
document.querySelector("#reload").addEventListener("click", () => void message("GRIDEDGE_RELOAD_EXTENSION"));
void refresh().catch((error) => { detail.textContent = String(error.message ?? error); });
