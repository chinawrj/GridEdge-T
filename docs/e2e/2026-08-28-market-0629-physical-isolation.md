# 0.6.29 physical-isolation preparation — 2026-08-28

This is deterministic infrastructure evidence only. The market was closed, so
it is not one of the two required live READ_ONLY browser E2E rounds.

- Nonce: `e2e-0629-b4a83e91-73f7-4c7f-9bb9-7c92d3e1a640`
- Broker: ephemeral Mosquitto, loopback listeners `127.0.0.1:18883` and
  `127.0.0.1:19001`; nonce-scoped ACL; formal `gridedge/#` denied.
- Database: ephemeral PostgreSQL 17 on `127.0.0.1:15432`.
- Namespace:
  `gridedge-e2e/e2e-0629-b4a83e91-73f7-4c7f-9bb9-7c92d3e1a640`.
- Ingestor TCP peers observed only on loopback MQTT and PostgreSQL ports.
- Synthetic canonical event result: PostgreSQL row count 1, exact committed
  payload match, exact application ACK `COMMITTED`, delivery count 1,
  conflicts 0, rejections 0.
- The same credential attempted a local formal-topic publication; the broker
  denied it and the local PostgreSQL formal-topic tripwire remained zero.
- The stack was stopped and removed through the nonce-root teardown guard.

After the first review, a second fresh stack used nonce
`e2e-0629-304baed6-9bd7-4e8f-a591-f470644f3002` to validate the hardened
runtime contract:

- start refused pre-existing listener ownership and bound all three listeners
  to processes whose command lines contained the exact nonce root;
- a separate `gridedge-e2e-worker` credential could read only the nonce-scoped
  committed topic; the Rust MQTT client accepted that endpoint/client/topic
  tuple atomically and normalized the committed event to the internal logical
  market topic;
- the contract assigned independent raw-event, bar, quote, ledger and outbox
  paths under the nonce root and fixed `money_actions_enabled=false`;
- PostgreSQL again contained one exact payload with delivery count 1, no
  conflicts/rejections and no formal-topic row; and
- guarded teardown removed the exact root and left no listeners on 18883,
  19001 or 15432.

Production NAS credentials, account identity and paper execution were not used.
No Android or order path was instantiated.

This still is not the required live two-round browser/worker acceptance. It
proves the physical transport, topic, credential and state-path isolation that
must be in place before those market-hours rounds may start.
