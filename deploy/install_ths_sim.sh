#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
user_home=${GRIDEDGE_USER_HOME:?GRIDEDGE_USER_HOME is required}
android_sdk_root=${GRIDEDGE_ANDROID_SDK_ROOT:?GRIDEDGE_ANDROID_SDK_ROOT is required}
android_masked_account=${GRIDEDGE_ANDROID_MASKED_ACCOUNT:?GRIDEDGE_ANDROID_MASKED_ACCOUNT is required}
market_host=${GRIDEDGE_MARKET_HOST:?GRIDEDGE_MARKET_HOST is required}
release_binary=${GRIDEDGE_SIGNED_BINARY:?run deploy/stage_ths_sim.sh and authorize its exact SHA first}
expected_sha256=${GRIDEDGE_SIGNED_SHA256:?set the frozen signed artifact SHA}
validator_binary=${GRIDEDGE_VALIDATOR_BINARY:?run deploy/stage_ths_sim.sh and freeze its validator}
expected_validator_sha256=${GRIDEDGE_VALIDATOR_SHA256:?set the frozen validator SHA}
installed_binary="$deployment_root/bin/gridedge_ths_live"
installed_launch_agent="$user_home/Library/LaunchAgents/com.gridedge.ths-sim.plist"
codesign_team_id=${GRIDEDGE_CODESIGN_TEAM_ID:-WM4JXVE5GV}

cd "$project_root"
if [ ! -f "$validator_binary" ] || [ -L "$validator_binary" ] || [ ! -x "$validator_binary" ]; then
  echo "frozen validator is not one regular executable file" >&2
  exit 1
fi
actual_validator_sha256=$(shasum -a 256 "$validator_binary" | awk '{print $1}')
if [ "$actual_validator_sha256" != "$expected_validator_sha256" ]; then
  echo "frozen validator differs from its staged SHA-256" >&2
  exit 1
fi
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
install -d "$user_home/Library/LaunchAgents"
installed_candidate=$(mktemp "$deployment_root/bin/.gridedge_ths_live.XXXXXX")
config_candidate=$(mktemp "$deployment_root/config/.ths_002256_sim.XXXXXX")
runner_candidate=$(mktemp "$deployment_root/bin/.run_ths_android_sim.XXXXXX")
guard_candidate=$(mktemp "$deployment_root/bin/.run_ths_trusted_session_guard.XXXXXX")
plist_candidate=$(mktemp "$user_home/Library/LaunchAgents/.com.gridedge.ths-sim.XXXXXX")
trap 'rm -f "$installed_candidate" "$config_candidate" "$runner_candidate" "$guard_candidate" "$plist_candidate"' EXIT HUP INT TERM
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
escape_sed_replacement() {
  printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}
sed \
  -e "s|__GRIDEDGE_DEPLOYMENT_ROOT__|$(escape_sed_replacement "$deployment_root")|g" \
  deploy/ths_002256_sim.yaml >"$config_candidate"
chmod 600 "$config_candidate"
sed \
  -e "s|__GRIDEDGE_DEPLOYMENT_ROOT__|$(escape_sed_replacement "$deployment_root")|g" \
  -e "s|__GRIDEDGE_USER_HOME__|$(escape_sed_replacement "$user_home")|g" \
  -e "s|__GRIDEDGE_ANDROID_SDK_ROOT__|$(escape_sed_replacement "$android_sdk_root")|g" \
  -e "s|__GRIDEDGE_MARKET_HOST__|$(escape_sed_replacement "$market_host")|g" \
  -e "s|\*\*0000|$(escape_sed_replacement "$android_masked_account")|g" \
  deploy/com.gridedge.ths-sim.plist >"$plist_candidate"
chmod 644 "$plist_candidate"
install -m 755 deploy/run_ths_android_sim.sh "$runner_candidate"
install -m 755 deploy/run_ths_trusted_session_guard.sh "$guard_candidate"
cmp deploy/run_ths_android_sim.sh "$runner_candidate"
cmp deploy/run_ths_trusted_session_guard.sh "$guard_candidate"
sh -n "$runner_candidate" "$guard_candidate"
"$validator_binary" validate-config --config "$config_candidate"
plutil -lint deploy/com.gridedge.ths-sim.plist "$plist_candidate"
plutil -convert json -o - "$plist_candidate" | /usr/bin/python3 -c '
import json, sys
allowed = {
    "EnvironmentVariables", "Label", "ProcessType", "ProgramArguments",
    "StandardErrorPath", "StandardOutPath", "WorkingDirectory",
}
actual = set(json.load(sys.stdin))
if actual != allowed:
    raise SystemExit(f"rendered LaunchAgent keys differ from identity-only allowlist: {sorted(actual)}")
'

artifact_strings=$(strings "$installed_candidate")
if printf '%s\n' "$artifact_strings" | grep -F 'tell process "Dock"' >/dev/null; then
  echo "installed worker still contains the forbidden Dock accessibility activation" >&2
  exit 1
fi
printf '%s\n' "$artifact_strings" | grep -F '/usr/bin/open' >/dev/null
printf '%s\n' "$artifact_strings" | grep -F 'cn.com.10jqka.macstockPro' >/dev/null

# The publisher holds one coordination lock across launchd bootout, old-owner
# quiescence, the final absence proof, and atomic target replacements.
GRIDEDGE_DEPLOYMENT_ROOT="$deployment_root" \
GRIDEDGE_LAUNCHD_DOMAIN="gui/$(id -u)" \
  sh deploy/publish_ths_sim_staged.sh \
    "$installed_candidate" "$installed_binary" \
    "$config_candidate" "$deployment_root/config/ths_002256_sim.yaml" \
    "$runner_candidate" "$deployment_root/bin/run_ths_android_sim.sh" \
    "$guard_candidate" "$deployment_root/bin/run_ths_trusted_session_guard.sh" \
    "$plist_candidate" "$installed_launch_agent"
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
