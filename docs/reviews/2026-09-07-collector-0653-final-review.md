# Collector 0.6.53 isolated acceptance: scoped PASS

Independent reviewer: task 01a00079-b837-7750-9e60-1dbe3f803685, 2026-09-07.
Scope: frozen candidate in two isolated READ_ONLY live recovery rounds. Production activation and final-segment completion are excluded and remain pending/open.

| Evidence | R1 | R2 |
|---|---:|---:|
| Elapsed seconds | 900.469 | 900.163 |
| Messages / observations | 405 / 177 | 409 / 153 |
| Bars / journal stages | 2 / 6 | 1 / 3 |
| First committed observation, seconds | 16.924449 | 19.036686 |
| Maximum source observation gap, seconds | 38.001 | 21.870 |
| Maximum committed receipt gap, seconds | 37.967789 | 22.006824 |
| Maximum observation age, seconds | 11.632602 | 11.796816 |

The unchanged executable acceptance contract requires bar_count != 0 per round, not two bars per round. R2 therefore meets the reviewed contract. R2's isolated page was closed at 13:02:09.346 and a new target for the exact reviewed URL was observed at 13:02:30.152, about 20.806 seconds later.

The reviewer verified frozen bundle and output hashes, unique event IDs, exact source sequences 1..405 and 1..409, independent source identities, V3 observations with provider v6, no future timestamps, SQLite chain/order/payload integrity, money_actions_enabled=false and strategy_evaluated=false. Runtime endpoints were loopback only, namespaces/clients/paths were fresh, and ACL denied formal gridedge/#. Candidate processes and loopback listeners were absent after acceptance. The manifest retains declared formal-host permissions; the actual isolated runtime bootstrap, namespace validator and broker ACL constrained these runs to loopback.

No contemporaneously named formal DB tripwire snapshot was retained. No such historical claim is made. A separate POSTHOC read-only query at 13:22:07 found zero formal PostgreSQL rows for the two candidate source instances; this is explicitly labelled posthoc-formal-db-source-absence.json.

Formal worker PID 50053 and installed/effective core identity were preserved. Formal 13:20 bar stages 1921/1922/1923 were all RUNNING, head/cursor 1923/1923, cash 103418.530, position/sellable 27500 and unresolved/open orders zero.

Frozen extension tree: 719726ca656d8e5ed8ab50dbd905226748ebc6c8151c04dc3d3210cf932afd0f.
Formal 0.6.51 tree remains: 4881a04687535be542b99906a9314e4d5d057b10c932496e49c9592011f92a6b.

Browser URL policy refused access to Chrome extension management and forbids alternative-surface/CDP/command circumvention. No bypass was attempted. The frozen candidate and manual instructions are supplied separately, without modifying the registered build. User manual activation must be followed by actual runtime identity, fresh V3 receipt, exact PG committed ACK, current coverage, terminal account reconciliation and a current complete bar under the existing recovery gates. Do not force RUNNING or replay historical orders.

Source-native completion for the 11:30/15:00 final bucket remains OPEN. This scoped PASS does not complete the overall Goal.
