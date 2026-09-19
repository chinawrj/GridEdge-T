// Read-only regression for the persisted automation ownership contract.
// It does not import/start the app or mutate the scheduler database.
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');

const args = process.argv.slice(2);
const dbIndex = args.indexOf('--db');
const ownerIndex = args.indexOf('--owner-thread');
assert.ok(dbIndex >= 0 && args[dbIndex + 1] && ownerIndex >= 0 && args[ownerIndex + 1],
  'usage: node verify-automation-ownership.cjs --db PATH --owner-thread THREAD_ID');
const database = args[dbIndex + 1];
const ownerThread = args[ownerIndex + 1];
const query = [
  "SELECT id,status,next_run_at,kind,target_thread_id,prompt",
  "FROM automations",
  "WHERE id IN ('automation-2','goal','gridedge')",
  'ORDER BY id;',
].join(' ');
const rows = JSON.parse(execFileSync('/usr/bin/sqlite3', [
  '-readonly', '-json', database, query,
], { encoding: 'utf8' }));
const byId = Object.fromEntries(rows.map(row => [row.id, row]));

assert.deepEqual(Object.keys(byId).sort(), ['automation-2', 'goal', 'gridedge'],
  'one or more required automations are missing');
assert.equal(byId['automation-2'].status, 'ACTIVE',
  'production operations owner must remain ACTIVE');
assert.equal(byId.gridedge.status, 'ACTIVE',
  'bounded delivery owner must remain ACTIVE through delivery closeout');
assert.equal(byId.goal.status, 'PAUSED',
  'standalone post-close goal must remain PAUSED while delivery owner is ACTIVE');
assert.equal(byId.goal.next_run_at, null,
  'a paused standalone post-close goal must not retain a next run');
assert.equal(byId.gridedge.target_thread_id, ownerThread,
  'delivery heartbeat must target the current active Goal owner');
assert.match(byId.gridedge.prompt, /verify-automation-ownership\.cjs/,
  'delivery owner must execute the persisted ownership watchdog every turn');
assert.match(byId.gridedge.prompt, /唯一写者自动恢复/,
  'delivery owner must carry the reviewed duplicate-writer recovery contract');
assert.match(byId.gridedge.prompt, /2026-09-08 18:10收盘交付基线/,
  'delivery owner must carry the current post-close delivery baseline');
assert.doesNotMatch(byId.gridedge.prompt,
  /现有独立recorder PID92463\/父92462持续到今日11:32/,
  'delivery owner must not retain an executable instruction for the completed recorder');
assert.doesNotMatch(byId.gridedge.prompt,
  /下一轮11:10须接住11:32终态分析/,
  'delivery owner must not retain an obsolete analysis handoff instruction');
assert.doesNotMatch(byId.gridedge.prompt, /active Goal须持续到真实恢复/,
  'delivery owner must not override the Goal blocked audit with stale prompt text');

const publicRows = rows.map(({ prompt, ...row }) => ({
  ...row,
  prompt_sha256: require('node:crypto').createHash('sha256').update(prompt).digest('hex'),
  ownership_watchdog: prompt.includes('verify-automation-ownership.cjs'),
  duplicate_writer_recovery: prompt.includes('唯一写者自动恢复'),
  current_postclose_baseline: prompt.includes('2026-09-08 18:10收盘交付基线'),
  stale_research_instruction: prompt.includes('持续到今日11:32') ||
    prompt.includes('下一轮11:10须接住11:32终态分析'),
  stale_goal_status_instruction: prompt.includes('active Goal须持续到真实恢复'),
}));

console.log(JSON.stringify({
  kind: 'READ_ONLY_PERSISTED_AUTOMATION_OWNERSHIP_REGRESSION',
  checked_at: new Date().toISOString(),
  database,
  expected_owner_thread_id: ownerThread,
  automations: publicRows,
}, null, 2));
