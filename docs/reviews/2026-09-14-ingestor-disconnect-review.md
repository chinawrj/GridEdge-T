# Ingestor consumer disconnect — independent scoped review

Final scoped result: **PASS for deploying only the frozen consumer-loop repair under the conditions below; NOT a live-trading or new-data-path certification.** No production UI, process, database, or deployment action was performed by this reviewer. Earlier pending findings below are retained as review history and superseded only by the final disposition.

## Exact targets

- Archive: `/tmp/gridedge-ingestor-repair-20260914.Wucymo/deployed.py`, SHA-256 `be9b805decc8e129644ed5292b1508c41aa4127330db8ef4582357286753112f`.
- Candidate: same directory `candidate.py`, SHA-256 `2fe584498f5821568f313f4ae05ae05b8e892c60cfa70c5b271c0ff9c1cb3835`.
- Independent test: `deploy/market_data/ingestor/test_consumer_disconnect_recovery.py`, SHA-256 `12bef732e528ad6bbe525c7ad6e0346ba3e0d2ad09928f1a3b6090342a53c169`.

The workspace implementation differs from the frozen candidate. This review does **not** approve publishing the whole workspace implementation.

## Actual red/green execution

Run with `GRIDEDGE_INGESTOR_TEST_TARGET` set to either exact archive path and `GRIDEDGE_INGESTOR_TEST_SHA256` set to its corresponding SHA:

```sh
python3 -m unittest discover -s deploy/market_data/ingestor -p test_consumer_disconnect_recovery.py -v
```

Deployed bytes: 6 tests, 2 passed / 4 failed, 0.016s. The disconnected consumer ignored 8 consecutive `MQTT_ERR_NO_CONN` results without reconnecting or exiting; normal event-driven exit and loop-exception paths omitted client cleanup. Healthy-loop and explicit-signal controls passed.

Frozen candidate: all 6 passed, 0.015s. Tests cover nonzero disconnect exit, healthy loop, normal cleanup, loop exception propagation with cleanup, and both SIGTERM/SIGINT arriving during a loop that returns disconnected: those signal cases exit 0 without reconnecting. All scenarios assert no synthesized ingest, rejection, publication, or acknowledgement. Dependencies, secrets, networking, signal registration, threads, and sleeps are mocked; no real broker/DB connection is made. These tests do not certify real network recovery or current market freshness.

## Diff assessment

Only the connection/loop block in `main` changes: check return code, return 1 on an unexpected loop error, allow an explicit stop to win, and clean up both clients and the publisher loop in `finally`. Event validation, transaction, original bytes, exact COMMITTED receipt, source identity, topic and ACK ordering remain unchanged. Fail-fast uses the existing container restart owner rather than adding a competing reconnect owner. Existing business tests and release gates remain the deployment owner's responsibility; they were not rerun here.

## Release boundary still requiring evidence

The proposed frozen-base-image plus one-file COPY is narrower than full installation and avoids unrelated workspace changes. Before promotion, bind the actual baseline image/source/compose identities and candidate bytes; render effective compose and prove only the intended ingestor image/source change. `--no-build` alone does not select the candidate image. Preserve command, entrypoint, user, secret mounts, network, client identity and restart configuration. Check that runtime mounts cannot hide the copied source; rehash the file inside the actual running container. Retain exact old image/source and the single-service rollback command.

Image-local tests must execute the exact copied implementation. Complete two physically isolated real disconnect/restart recovery runs proving nonzero exit is handled by the existing restart owner, consumer resubscription, publisher connection and exact committed-ACK recovery. Never fault-inject the now-progressing production backlog. TCP-only container health is insufficient. Final rollout requires exclusive ownership, baseline revalidation, bounded outage/rollback, and no broker, DB, certificate, core effective identity, ledger or extension-queue replacement. Backlog ingestion is not live trading acceptance; existing source and account gates remain in force.

## Supplemental real Docker restart-owner evidence, 2026-09-14

**PASS: two real nonzero process exits recovered by Docker `unless-stopped`.** This is an owner-level test with mocked network/DB dependencies, not two real MQTT/COMMITTED recovery runs.

- Frozen candidate image supplied by deployment owner: `sha256:668835dd82c58d6c4686f014cfa0c8fba428d9581db71c7a5f6a3ebd3bf42847`.
- Test-only derived image: `sha256:99bc06c816fc2f4040be6ad2fbf2fc1427fb9138d29a9722701300eb95a1d85a`, built `--network none --pull=false` from that exact image; only a wrapper and test CMD were added. Candidate `/app/market_ingestor.py` is unchanged and SHA-checked on every boot.
- Test container: `gridedge-isolated-restart-owner-20260914-d7lyai`, ID `6634798af190d1ee0320a7e4ffb3fd265d0034756199e7359cd9320babc47567`.
- Actual Docker inspection: `Network=none`, `Mounts=[]`, `Ports={}`, restart policy `unless-stopped`. No formal mounts, ports or credentials were provided. No formal container was changed by this reviewer.
- Wrapper: `/tmp/gridedge-restart-owner-20260914.d7LYai/restart_owner_probe.py`, SHA-256 `72463682f7dad42f31c162fdbe926826a59dfb4daed9952630ad737d1229f63a`; NAS build context `/tmp/gridedge-restart-owner-20260914.qCQP4P`.

Observed container stdout timestamps, Asia/Shanghai:

| Boot | SHA-checked process start | Entered candidate real main loop | Main result |
| --- | --- | --- | --- |
| 1 | 10:12:39.620 | 10:12:43.191 | 10:12:54.193: exit 1, client cleanup PASS |
| 2 | 10:13:05.504 | 10:13:05.968 | 10:13:16.970: exit 1, client cleanup PASS |
| 3 | 10:13:27.615 | 10:13:28.053 | Healthy mocked loop held for inspection |

Docker independently reported `RestartCount=2`, `Running=true`, `Restarting=false`, third runtime PID 1888. First two boots each hold for 11 seconds within the mocked consumer loop before returning `MQTT_ERR_NO_CONN`; the candidate real `main()` returns 1 and the PID-1 wrapper propagates it through `SystemExit`. No manual restart was issued. Cleanup assertions and absence of synthetic ingest/ACK/publish passed. Third boot retains real signal handling for a deliberate test-only stop after evidence collection.

Test-only shutdown completed at 10:13:58 CST: boot 3 returned 0 with cleanup PASS; Docker reports `Status=exited`, `Running=false`, `Pid=0`, `ExitCode=0`, `RestartCount=2`. The stopped test container, test image and context are retained for inspection; no test process is left running.

The local host has no Docker CLI and no existing `/tmp/gridedge-market-e2e-*` prepared fixture was found. No new real-broker/DB stack was launched within this bounded task. Therefore consumer resubscription and exact COMMITTED receipt recovery after two real transport failures remain **NOT VERIFIED** here. The narrower restart-owner evidence must not be relabeled as that broader gate or as production RUNNING/current-bar acceptance.

Publication-script review identified and returned to the deployment owner: signal rollback must terminate rather than continue; all post-write failures need an armed EXIT rollback; compose validation needs the actual app project directory; rollback needs actual image/source/file identity checks. A further shell hazard was identified in `rollback || echo`: this disables `set -e` within the function, so every rollback operation/check needs explicit failure propagation. This section records findings, not permission to deploy an unverified script revision.

## Final scoped disposition, 10:16 CST

The latest `publish.sh`, SHA-256 `0696d67a1559725fe804b7d7e42424e9c4bb0487442a0d17a2029146ca77d5b9`, was statically re-read and passes `sh -n`. It now has an armed EXIT rollback, terminating signal traps, the actual app project directory, actual image/source/file rollback checks, and explicit `|| return 1` on every rollback step. The identified script blockers are resolved. No real production rollback was exercised by this reviewer.

The existing `test_market_ingestor.py` was copied into isolated temporary state and run against both exact archived implementations: both produced the same **15 PASS / 2 ERROR**, in 0.002s each. All message-handler ordering, insert/duplicate, conflict/rejection, failure-no-ACK, exact original bytes, canonical receipt and QoS1 PUBACK tests passed. Both errors are baseline/workspace API differences: the archived code lacks `validate_topic_namespace`, and its `validate_document` accepts two rather than three arguments. These are not candidate regressions and must not be described as a full-suite pass. They also mean these archived bytes must not be assumed safe for a merely topic-separated shadow publisher; this test used Docker network isolation instead.

**Limited component-release approval:** exact candidate source `2fe584…`, exact deployment image `668835…`, and exact reviewed publication script above, with the owner's frozen compose identity/equivalence evidence and exclusive-lock baseline revalidation. This is an existing consumer-loop control repair, not a new market-data or execution-adapter path: the full archived diff changes only connect/loop/cleanup, and the unchanged commit pipeline has independent passing tests. Two real Docker owner restarts now cover the changed mechanism; the deployment owner separately reports one actual production restart restoring continuous PostgreSQL/Browser ACK progress. That latter production observation is owner-provided evidence, not a reviewer-executed test.

Accordingly, the earlier request for two complete real-broker/DB fault cycles is not imposed as an additional prerequisite for this narrowly bounded existing-path repair. This is a scope reassessment based on the exact diff and new owner/test evidence, **not** a claim that those cycles passed. No further production fault injection is authorized. Any new transport/publication/namespace path still requires the full project acceptance gates.

Promotion must preserve all formal broker/DB/credentials/client identities, use only single-service no-deps/no-build activation, retain the old image and reviewed rollback, and immediately verify actual installed image/source plus genuine consumer/publisher connection and precise committed-ACK/database progress. Failed progress requires bounded recovery/rollback; activation output alone is not success. Existing READ_ONLY/current-market/account safety gates remain unchanged. The currently restored backlog drain is a real recovery result and must be preserved, but does not certify current completed-bar processing or RUNNING trading service.
