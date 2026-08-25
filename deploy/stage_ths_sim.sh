#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
unsigned_binary="$project_root/target/release/gridedge_ths_live"
staging_root=${GRIDEDGE_SIGNED_STAGING_ROOT:-"$project_root/target/release/signed"}
codesign_identity=${GRIDEDGE_CODESIGN_IDENTITY:?GRIDEDGE_CODESIGN_IDENTITY is required}
codesign_team_id=${GRIDEDGE_CODESIGN_TEAM_ID:-WM4JXVE5GV}

cd "$project_root"
cargo build --locked --release --bin gridedge_ths_live
security find-identity -v -p codesigning | grep -F "\"$codesign_identity\"" >/dev/null

install -d "$staging_root"
candidate=$(mktemp "$staging_root/.gridedge_ths_live.XXXXXX")
trap 'rm -f "$candidate"' EXIT HUP INT TERM
cp "$unsigned_binary" "$candidate"
chmod 755 "$candidate"
codesign --force --sign "$codesign_identity" --identifier com.gridedge.ths-live \
  --timestamp=none "$candidate"
codesign --verify --strict --verbose=2 "$candidate"
codesign_metadata=$(codesign -dvvv "$candidate" 2>&1)
printf '%s\n' "$codesign_metadata" | grep -Fx 'Identifier=com.gridedge.ths-live' >/dev/null
printf '%s\n' "$codesign_metadata" | grep -Fx "TeamIdentifier=$codesign_team_id" >/dev/null
if printf '%s\n' "$codesign_metadata" | grep -Fx 'Signature=adhoc' >/dev/null; then
  echo "staged worker must not use an ad-hoc signature" >&2
  exit 1
fi

candidate_sha256=$(shasum -a 256 "$candidate" | awk '{print $1}')
frozen="$staging_root/gridedge_ths_live-$candidate_sha256"
if [ -e "$frozen" ]; then
  cmp "$candidate" "$frozen"
  rm -f "$candidate"
else
  mv "$candidate" "$frozen"
fi
chmod 555 "$frozen"
trap - EXIT HUP INT TERM
printf 'GRIDEDGE_SIGNED_BINARY=%s\nGRIDEDGE_SIGNED_SHA256=%s\n' \
  "$frozen" "$candidate_sha256"
