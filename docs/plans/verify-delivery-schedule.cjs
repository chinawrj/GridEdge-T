// Read-only regression against the installed app's PURE recurrence functions.
// Does not import/start the app, mutate its DB, or create/update a timer.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const app = '/Applications/ChatGPT.app/Contents/Resources/app.asar';
const archive = fs.readFileSync(app);
const header = JSON.parse(archive.subarray(16, 16 + archive.readUInt32LE(12)));
const base = 8 + archive.readUInt32LE(4);
const entries = header.files['.vite'].files.build.files;
const candidates = Object.entries(entries).filter(([name, item]) =>
  /^src-.*\.js$/.test(name) && !item.unpacked && item.size < 10_000_000);
const selected = candidates.map(([name, item]) => ({ name, text: archive.subarray(
  base + Number(item.offset), base + Number(item.offset) + item.size).toString() }))
  .filter(({ text }) => text.includes('function oI({rrule:e,now:t})'));
assert.equal(selected.length, 1, 'installed source shape changed; inspect before reuse');
const { name, text } = selected[0];
function section(start, stop) {
  const a = text.indexOf(start), b = text.indexOf(stop, a);
  assert.ok(a >= 0 && b > a, `missing pure section: ${start}`);
  return text.slice(a, b);
}
const pure = [
  'var ' + section('$M=[`MO`', 'function uF('),
  section('var fF=10080*60;', 'var _F='),
  section('var _F=`automations`', 'function AF('),
  section('function Gg(', 'function Kg('),
  section('function oI({rrule:e,now:t})', 'function sI('),
  section('var EI={MO:1', 'function II('),
  'globalThis.next = (rrule, now) => oI({rrule, now});',
].join('\n');
const tslib = header.files.node_modules.files.tslib.files['tslib.js'];
assert.ok(!tslib.unpacked, 'tslib extraction shape changed');
const tslibText = archive.subarray(base + Number(tslib.offset),
  base + Number(tslib.offset) + tslib.size).toString();
const sandbox = vm.createContext({ module: { exports: {} } });
vm.runInContext(tslibText, sandbox, { timeout: 2000 });
sandbox.u = sandbox.module.exports;
try { vm.runInContext(pure, sandbox, { timeout: 2000 }); }
catch (error) { throw new Error(`Pure scheduler initialization: ${error.message}`); }
const rule = 'RRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;BYMINUTE=10;UNTIL=20260911T091500Z';
// Shanghai has UTC+08:00 throughout this bounded Sep 8-11 window. Explicit UTC
// DTSTART avoids both implicit-zone branches, while preserving the exact cutoff.
const corrected = 'DTSTART:20260908T001000Z\nRRULE:FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR;BYHOUR=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14;BYMINUTE=10;UNTIL=20260911T091500Z';
function next(rrule, at) {
  sandbox.rule = rrule;
  sandbox.now = Date.parse(at);
  const value = vm.runInContext('next(rule, now)', sandbox, { timeout: 2000 });
  return value == null ? null : new Date(value).toISOString();
}
const cases = [
  ['2026-09-08T00:11:17+08:00', '2026-09-08T00:10:00.000Z'],
  ['2026-09-08T08:10:01+08:00', '2026-09-08T01:10:00.000Z'],
  ['2026-09-08T22:11:00+08:00', '2026-09-09T00:10:00.000Z'],
  ['2026-09-11T16:11:00+08:00', '2026-09-11T09:10:00.000Z'],
  ['2026-09-11T17:11:00+08:00', null],
];
const results = cases.map(([at, expected]) => {
  const actual = next(corrected, at);
  assert.equal(actual, expected, `incorrect next time at ${at}`);
  return { at, expected, actual };
});
const original = next(rule, cases[0][0]);
assert.equal(original, '2026-09-07T17:10:00.000Z', 'original UTC-path repro changed');
console.log(JSON.stringify({
  kind: 'READ_ONLY_INSTALLED_SCHEDULER_REGRESSION_NOT_TIMER_ACTIVATION',
  module: name,
  module_sha256: crypto.createHash('sha256').update(text).digest('hex'),
  pure_sha256: crypto.createHash('sha256').update(pure).digest('hex'),
  system_timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  original_next_utc: original, corrected_rule: corrected, cases: results,
  excludes_app_jitter: true,
}, null, 2));
