#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cd "$ROOT"

for source in \
  extensions/gridedge-web-market/src/*.js \
  extensions/gridedge-web-market/src/providers/*.js
do
  node --check "$source"
done
node --test extensions/gridedge-web-market/tests/*.test.js
python3 -m unittest \
  deploy/market_data/test_deployment_contract.py \
  deploy/market_data/ingestor/test_market_ingestor.py

BUILD=$($ROOT/extensions/gridedge-web-market/scripts/build.sh)
diff -qr "$ROOT/extensions/gridedge-web-market/src" "$BUILD/src"
diff -q "$ROOT/extensions/gridedge-web-market/manifest.json" "$BUILD/manifest.json"
