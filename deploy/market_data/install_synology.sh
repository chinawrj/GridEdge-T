#!/bin/sh
set -eu

HOST=${GRIDEDGE_MARKET_HOST:-192.168.1.201}
MARKET_ROOT=${GRIDEDGE_MARKET_ROOT:-/volume1/Projects/GridEdge-Market}
REMOTE_DOCKER=/usr/local/bin/docker
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
LOCAL_CLIENT_DIR=${GRIDEDGE_MARKET_CLIENT_DIR:-"$HOME/Library/Application Support/GridEdge-T/market-mqtt"}

tar -C "$SCRIPT_DIR" \
  --no-xattrs \
  --exclude='.env' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  -cf - . | ssh "$HOST" "umask 077; mkdir -p '$MARKET_ROOT/app'; tar -xf - -C '$MARKET_ROOT/app'"

ssh "$HOST" MARKET_ROOT="$MARKET_ROOT" REMOTE_DOCKER="$REMOTE_DOCKER" 'sh -s' <<'REMOTE'
set -eu

# Synology applies the caller's restrictive umask to tar extraction. Application
# files are public configuration inside containers, not secrets, so normalize
# them before any non-host UID attempts to read the bind mounts.
find "$MARKET_ROOT/app" -type d -exec chmod 755 {} \;
find "$MARKET_ROOT/app" -type f -exec chmod 644 {} \;
chmod 755 "$MARKET_ROOT/app/install_synology.sh"

mkdir -p \
  "$MARKET_ROOT/data/mosquitto" \
  "$MARKET_ROOT/data/postgres" \
  "$MARKET_ROOT/logs/mosquitto" \
  "$MARKET_ROOT/secrets/tls"
chmod 700 "$MARKET_ROOT/secrets"

"$REMOTE_DOCKER" pull eclipse-mosquitto:2.1.2-alpine
"$REMOTE_DOCKER" pull postgres:17-bookworm

# Recover ownership left by an interrupted installation before host-side
# certificate rotation logic needs to inspect the directory.
host_uid=$(id -u)
host_gid=$(id -g)
"$REMOTE_DOCKER" run --rm --user 0 \
  -e HOST_UID="$host_uid" \
  -e HOST_GID="$host_gid" \
  -v "$MARKET_ROOT/secrets/tls:/tls" \
  eclipse-mosquitto:2.1.2-alpine \
  sh -c 'chown -R "$HOST_UID:$HOST_GID" /tls; chmod 700 /tls'

create_secret() {
  path=$1
  if [ ! -s "$path" ]; then
    openssl rand -hex 32 > "$path"
    chmod 600 "$path"
  fi
}

create_secret "$MARKET_ROOT/secrets/mqtt-publisher.password"
create_secret "$MARKET_ROOT/secrets/mqtt-ingestor.password"
create_secret "$MARKET_ROOT/secrets/postgres.password"

if [ ! -s "$MARKET_ROOT/secrets/tls/ca.crt" ] || [ ! -s "$MARKET_ROOT/secrets/tls/ca.key" ]; then
  openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 3650 \
    -subj '/CN=GridEdge Market Local CA' \
    -keyout "$MARKET_ROOT/secrets/tls/ca.key" \
    -out "$MARKET_ROOT/secrets/tls/ca.crt"
fi

# The CA remains stable, while the leaf can be safely renewed on install. Its
# SAN covers both LAN clients and the private Compose service name.
openssl req -newkey rsa:3072 -sha256 -nodes \
  -subj '/CN=192.168.1.201' \
  -keyout "$MARKET_ROOT/secrets/tls/server.key" \
  -out "$MARKET_ROOT/secrets/tls/server.csr"
printf '%s\n' \
  'subjectAltName=IP:192.168.1.201,IP:127.0.0.1,DNS:ds1621plus,DNS:mqtt' \
  'extendedKeyUsage=serverAuth' \
  'keyUsage=digitalSignature,keyEncipherment' \
  > "$MARKET_ROOT/secrets/tls/server.ext"
openssl x509 -req -sha256 -days 1825 \
  -in "$MARKET_ROOT/secrets/tls/server.csr" \
  -CA "$MARKET_ROOT/secrets/tls/ca.crt" \
  -CAkey "$MARKET_ROOT/secrets/tls/ca.key" \
  -CAcreateserial \
  -extfile "$MARKET_ROOT/secrets/tls/server.ext" \
  -out "$MARKET_ROOT/secrets/tls/server.crt"
rm -f "$MARKET_ROOT/secrets/tls/server.csr" "$MARKET_ROOT/secrets/tls/server.ext"
chmod 600 "$MARKET_ROOT/secrets/tls/ca.key"

if [ ! -s "$MARKET_ROOT/secrets/mosquitto.passwd" ]; then
  printf 'gridedge-publisher:%s\ngridedge-ingestor:%s\n' \
    "$(cat "$MARKET_ROOT/secrets/mqtt-publisher.password")" \
    "$(cat "$MARKET_ROOT/secrets/mqtt-ingestor.password")" \
    > "$MARKET_ROOT/secrets/.mosquitto.passwd.plain"
  "$REMOTE_DOCKER" run --rm --user 0 \
    -v "$MARKET_ROOT/secrets:/work" \
    eclipse-mosquitto:2.1.2-alpine \
    mosquitto_passwd -U /work/.mosquitto.passwd.plain
  mv "$MARKET_ROOT/secrets/.mosquitto.passwd.plain" "$MARKET_ROOT/secrets/mosquitto.passwd"
  chmod 600 "$MARKET_ROOT/secrets/mosquitto.passwd"
fi

"$REMOTE_DOCKER" run --rm --user 0 \
  -e HOST_UID="$host_uid" \
  -e HOST_GID="$host_gid" \
  -v "$MARKET_ROOT/data/mosquitto:/data" \
  -v "$MARKET_ROOT/logs/mosquitto:/log" \
  -v "$MARKET_ROOT/secrets:/secrets" \
  eclipse-mosquitto:2.1.2-alpine \
  sh -c 'chown -R 1883:1883 /data /log && chmod 750 /data /log && chown "$HOST_UID:$HOST_GID" /secrets/tls /secrets/tls/ca.key && chmod 700 /secrets/tls && chmod 600 /secrets/tls/ca.key && chown 1883:1883 /secrets/mosquitto.passwd /secrets/tls/server.key && chmod 600 /secrets/mosquitto.passwd /secrets/tls/server.key && chown "$HOST_UID:10001" /secrets/mqtt-ingestor.password /secrets/postgres.password && chmod 640 /secrets/mqtt-ingestor.password /secrets/postgres.password && chmod 644 /secrets/tls/ca.crt /secrets/tls/server.crt'

printf 'MARKET_ROOT=%s\n' "$MARKET_ROOT" > "$MARKET_ROOT/app/.env"
chmod 600 "$MARKET_ROOT/app/.env"

cd "$MARKET_ROOT/app"
"$REMOTE_DOCKER" compose config --quiet
"$REMOTE_DOCKER" compose up -d postgres
"$REMOTE_DOCKER" compose up -d --force-recreate mqtt

attempt=0
while [ "$attempt" -lt 60 ]; do
  mqtt_health=$("$REMOTE_DOCKER" inspect --format '{{.State.Health.Status}}' gridedge-market-mqtt 2>/dev/null || true)
  db_health=$("$REMOTE_DOCKER" inspect --format '{{.State.Health.Status}}' gridedge-market-postgres 2>/dev/null || true)
  if [ "$mqtt_health" = healthy ] && [ "$db_health" = healthy ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done

if [ "$mqtt_health" != healthy ] || [ "$db_health" != healthy ]; then
  "$REMOTE_DOCKER" compose ps
  "$REMOTE_DOCKER" compose logs --tail=100 mqtt postgres
  exit 1
fi

# PostgreSQL may already contain a durable cluster from an interrupted first
# installation. Init scripts only run for an empty cluster, so apply this
# idempotent schema explicitly when the authoritative table is absent.
schema=$($REMOTE_DOCKER exec gridedge-market-postgres \
  psql -U gridedge_market -d gridedge_market -Atqc \
  "SELECT COALESCE(to_regclass('public.market_events')::text, '')")
if [ "$schema" != market_events ]; then
  "$REMOTE_DOCKER" exec -i gridedge-market-postgres \
    psql -v ON_ERROR_STOP=1 -U gridedge_market -d gridedge_market \
    < "$MARKET_ROOT/app/postgres/init/001_market_schema.sql"
fi

# An interrupted pre-migration install may contain v1 tables without the
# migration ledger. Record that known physical state, then apply every later
# migration exactly once in its own transaction.
"$REMOTE_DOCKER" exec gridedge-market-postgres \
  psql -v ON_ERROR_STOP=1 -U gridedge_market -d gridedge_market -c \
  "CREATE TABLE IF NOT EXISTS market_schema_migrations (version INTEGER PRIMARY KEY CHECK (version > 0), applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()); INSERT INTO market_schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING;" \
  >/dev/null
for migration in "$MARKET_ROOT"/app/postgres/migrations/*.sql; do
  [ -f "$migration" ] || continue
  version=$(basename "$migration" | sed 's/^0*\([0-9][0-9]*\)_.*/\1/')
  applied=$("$REMOTE_DOCKER" exec gridedge-market-postgres \
    psql -U gridedge_market -d gridedge_market -Atqc \
    "SELECT count(*) FROM market_schema_migrations WHERE version=$version")
  if [ "$applied" = 0 ]; then
    "$REMOTE_DOCKER" exec -i gridedge-market-postgres \
      psql -v ON_ERROR_STOP=1 -U gridedge_market -d gridedge_market < "$migration"
  fi
done

# The ingestor uses a deliberately explicit paho loop and does not silently
# reconnect after a broker process replacement. Recreate it after MQTT so a
# healthy-but-unsubscribed old container cannot acknowledge no market data.
"$REMOTE_DOCKER" compose up -d --build --force-recreate ingestor

attempt=0
while [ "$attempt" -lt 30 ]; do
  ingestor_health=$("$REMOTE_DOCKER" inspect --format '{{.State.Health.Status}}' gridedge-market-ingestor 2>/dev/null || true)
  [ "$ingestor_health" = healthy ] && break
  attempt=$((attempt + 1))
  sleep 2
done
if [ "$ingestor_health" != healthy ]; then
  "$REMOTE_DOCKER" compose ps
  "$REMOTE_DOCKER" compose logs --tail=100 ingestor
  exit 1
fi

"$REMOTE_DOCKER" compose ps
"$REMOTE_DOCKER" image inspect eclipse-mosquitto:2.1.2-alpine --format 'mosquitto={{index .RepoDigests 0}}'
"$REMOTE_DOCKER" image inspect postgres:17-bookworm --format 'postgres={{index .RepoDigests 0}}'
REMOTE

mkdir -p "$LOCAL_CLIENT_DIR"
chmod 700 "$LOCAL_CLIENT_DIR"
ssh "$HOST" "cat '$MARKET_ROOT/secrets/tls/ca.crt'" > "$LOCAL_CLIENT_DIR/ca.crt"
ssh "$HOST" "cat '$MARKET_ROOT/secrets/mqtt-publisher.password'" > "$LOCAL_CLIENT_DIR/publisher.password"
chmod 600 "$LOCAL_CLIENT_DIR/ca.crt" "$LOCAL_CLIENT_DIR/publisher.password"

echo "Installed MQTT 5 market plane on $HOST; publisher material is in $LOCAL_CLIENT_DIR"
