#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
deployment_root="/Users/ACCOUNT/Library/Application Support/GridEdge-T"
release_binary=${GRIDEDGE_SIGNED_BINARY:?run deploy/stage_ths_sim.sh and authorize its exact SHA first}
expected_sha256=${GRIDEDGE_SIGNED_SHA256:?set the frozen signed artifact SHA}
installed_binary="$deployment_root/bin/gridedge_ths_live"
codesign_team_id=${GRIDEDGE_CODESIGN_TEAM_ID:-WM4JXVE5GV}

cd "$project_root"
actual_sha256=$(shasum -a 256 "$release_binary" | awk '{print $1}')
if [ "$actual_sha256" != "$expected_sha256" ]; then
  echo "frozen signed worker differs from its authorized SHA-256" >&2
  exit 1
fi
codesign --verify --strict --verbose=2 "$release_binary"
codesign_metadata=$(codesign -dvvv "$release_binary" 2>&1)
printf '%s\n' "$codesign_metadata" | grep -Fx 'Identifier=com.gridedge.ths-live' >/dev/null
printf '%s\n' "$codesign_metadata" | grep -Fx "TeamIdentifier=$codesign_team_id" >/dev/null
if printf '%s\n' "$codesign_metadata" | grep -Fx 'Signature=adhoc' >/dev/null; then
  echo "frozen signed worker must not use an ad-hoc signature" >&2
  exit 1
fi

install -d "$deployment_root/bin" "$deployment_root/config" \
  "$deployment_root/runtime" "$deployment_root/logs"
installed_candidate=$(mktemp "$deployment_root/bin/.gridedge_ths_live.XXXXXX")
trap 'rm -f "$installed_candidate"' EXIT HUP INT TERM
install -m 755 "$release_binary" "$installed_candidate"
cmp "$release_binary" "$installed_candidate"
candidate_sha256=$(shasum -a 256 "$installed_candidate" | awk '{print $1}')
if [ "$candidate_sha256" != "$expected_sha256" ]; then
  echo "copied worker differs from its authorized SHA-256" >&2
  exit 1
fi
codesign --verify --strict --verbose=2 "$installed_candidate"
candidate_codesign_metadata=$(codesign -dvvv "$installed_candidate" 2>&1)
printf '%s\n' "$candidate_codesign_metadata" | grep -Fx 'Identifier=com.gridedge.ths-live' >/dev/null
printf '%s\n' "$candidate_codesign_metadata" | grep -Fx "TeamIdentifier=$codesign_team_id" >/dev/null
install -m 600 deploy/ths_002256_sim.yaml \
  "$deployment_root/config/ths_002256_sim.yaml"
install -m 644 deploy/com.gridedge.ths-sim.plist \
  "/Users/ACCOUNT/Library/LaunchAgents/com.gridedge.ths-sim.plist"

target/release/gridedge validate-config --config deploy/ths_002256_sim.yaml
plutil -lint deploy/com.gridedge.ths-sim.plist \
  "/Users/ACCOUNT/Library/LaunchAgents/com.gridedge.ths-sim.plist"

artifact_strings=$(strings "$installed_candidate")
if printf '%s\n' "$artifact_strings" | grep -F 'tell process "Dock"' >/dev/null; then
  echo "installed worker still contains the forbidden Dock accessibility activation" >&2
  exit 1
fi
printf '%s\n' "$artifact_strings" | grep -F '/usr/bin/open' >/dev/null
printf '%s\n' "$artifact_strings" | grep -F 'cn.com.10jqka.macstockPro' >/dev/null

mv -f "$installed_candidate" "$installed_binary"
trap - EXIT HUP INT TERM
installed_sha256=$(shasum -a 256 "$installed_binary" | awk '{print $1}')
if [ "$installed_sha256" != "$expected_sha256" ]; then
  echo "installed worker differs from its authorized SHA-256" >&2
  exit 1
fi
codesign --verify --strict --verbose=2 "$installed_binary"
installed_codesign_metadata=$(codesign -dvvv "$installed_binary" 2>&1)
printf '%s\n' "$installed_codesign_metadata" | grep -Fx 'Identifier=com.gridedge.ths-live' >/dev/null
printf '%s\n' "$installed_codesign_metadata" | grep -Fx "TeamIdentifier=$codesign_team_id" >/dev/null
printf '%s  %s\n' "$installed_sha256" "$installed_binary"
