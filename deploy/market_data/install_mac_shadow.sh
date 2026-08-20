#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CLIENT_DIR=${GRIDEDGE_MARKET_CLIENT_DIR:-"$HOME/Library/Application Support/GridEdge-T/market-mqtt"}
PLIST_SOURCE="$SCRIPT_DIR/../com.gridedge.market-shadow.plist"
PLIST_TARGET="$HOME/Library/LaunchAgents/com.gridedge.market-shadow.plist"

test -s "$CLIENT_DIR/ca.crt"
test -s "$CLIENT_DIR/publisher.password"
mkdir -p "$CLIENT_DIR/bin" "$CLIENT_DIR/state" "$CLIENT_DIR/logs" "$HOME/Library/LaunchAgents"
chmod 700 "$CLIENT_DIR" "$CLIENT_DIR/bin" "$CLIENT_DIR/state" "$CLIENT_DIR/logs"

if [ ! -x "$CLIENT_DIR/venv/bin/python" ]; then
  python3 -m venv "$CLIENT_DIR/venv"
fi
"$CLIENT_DIR/venv/bin/python" -m pip install --disable-pip-version-check \
  --requirement "$SCRIPT_DIR/publisher/requirements.txt"
install -m 700 "$SCRIPT_DIR/publisher/shadow_publisher.py" "$CLIENT_DIR/bin/shadow_publisher.py"
plutil -lint "$PLIST_SOURCE" >/dev/null
install -m 600 "$PLIST_SOURCE" "$PLIST_TARGET"

echo "Installed the read-only market shadow publisher at $PLIST_TARGET"
