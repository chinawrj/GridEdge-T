#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
SOURCE="$ROOT/extensions/gridedge-web-market"
DESTINATION="$ROOT/build/gridedge-web-market-extension"

rm -rf "$DESTINATION"
mkdir -p "$DESTINATION/src/providers"
mkdir -p "$DESTINATION/vendor"
cp "$SOURCE/manifest.json" "$DESTINATION/manifest.json"
cp "$SOURCE/src/"*.js "$SOURCE/src/"*.html "$SOURCE/src/"*.css "$DESTINATION/src/"
cp "$SOURCE/src/providers/"*.js "$DESTINATION/src/providers/"
cp "$SOURCE/vendor/"* "$DESTINATION/vendor/"
printf '%s\n' "$DESTINATION"
