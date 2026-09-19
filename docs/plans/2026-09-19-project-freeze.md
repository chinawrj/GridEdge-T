# Owner-directed project freeze

Date: 2026-09-19, Asia/Shanghai.

The owner requested: freeze GridEdge-T, preserve code on GitHub and remove the
local project including its caches. This replaces all prior unattended recovery
and repair mandates. It does not change the failed delivery acceptance into a
successful outcome.

## Archive scope

- Main working copy: source, tests, deployment scripts, documentation and
  non-secret research artifacts, including previously uncommitted changes.
- Two additional dirty Codex worktrees: preserved on separate freeze branches,
  not merged into the main snapshot.
- Existing local branches and tags: preserve remotely before local removal.
- Private runtime databases, logs, credentials, signing keys, account material,
  browser profiles and dependency/build caches: excluded from the public repo.
- Source snapshots are unapproved development states, not deployable releases.

## Final operational outcome

The last installed system was not restored to accepted unattended service.
The Android stale-XML admission bug has a source-level fix and passing regression
tests, but actual read-only account acceptance still timed out and no approved
same-run release of that fix occurred. Source finality and collector activation
issues remain open. See the existing incident and failed-delivery records.

## Shutdown and local cleanup policy

Pause project automations, stop project-only supervision and launch services,
and disable repository Actions before publication. Verify every archive ref
against GitHub before removing any local source tree. Archive GitHub read-only
after verification. Remove regenerable project caches; move local source and
private runtime material to Trash for recoverability. Do not upload secrets or
delete shared Chrome/Android SDK/account data, other projects, or remote NAS
databases under this local-cleanup request. No tests that place orders, releases,
replays against the formal account or further engineering are part of freezing.

Any future resumption requires a new explicit owner request and a fresh safety
review. Historical timers, AGENTS recovery rules and old plans cannot authorize
resumption.
