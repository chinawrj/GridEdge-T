#!/bin/sh
set -eu

# Publish already-validated adjacent temporary files only after proving that no
# legacy launchd job can still start a competing worker.
launchctl_bin=${GRIDEDGE_LAUNCHCTL_BIN:-/bin/launchctl}
domain=${GRIDEDGE_LAUNCHD_DOMAIN:?GRIDEDGE_LAUNCHD_DOMAIN is required}
label=${GRIDEDGE_LAUNCHD_LABEL:-com.gridedge.ths-sim}
deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
coordination_lock="$deployment_root/runtime/ths-deployment-coordination.lock"
maintenance="$deployment_root/runtime/ths-deployment-maintenance"

if [ "$#" -ne 10 ]; then
  echo "expected five staged/target path pairs" >&2
  exit 64
fi
staged_binary=$1; target_binary=$2
staged_config=$3; target_config=$4
staged_runner=$5; target_runner=$6
staged_guard=$7; target_guard=$8
staged_plist=$9; shift 9; target_plist=$1

validate_pair() {
  staged=$1
  target=$2
  if [ ! -f "$staged" ]; then
    echo "missing staged deployment file: $staged" >&2
    exit 1
  fi
  if [ "$(dirname "$staged")" != "$(dirname "$target")" ]; then
    echo "staged deployment file is not adjacent to its target" >&2
    exit 1
  fi
}
validate_pair "$staged_binary" "$target_binary"
validate_pair "$staged_config" "$target_config"
validate_pair "$staged_runner" "$target_runner"
validate_pair "$staged_guard" "$target_guard"
validate_pair "$staged_plist" "$target_plist"

if ! mkdir "$coordination_lock" 2>/dev/null; then
  echo "another trusted-session start or deployment owns the coordination lock" >&2
  exit 1
fi
if ! mkdir "$maintenance" 2>/dev/null; then
  rmdir "$coordination_lock"
  echo "trusted-session deployment maintenance is already active" >&2
  exit 1
fi

is_absent_result() {
  grep -Eq 'Could not find service|Could not find specified service|service not found|No such process' "$1"
}

launch_state=$(mktemp "${TMPDIR:-/tmp}/gridedge-launch-state.XXXXXX")
cleanup_publish() {
  rm -f "$launch_state"
  rmdir "$maintenance" 2>/dev/null || true
  rmdir "$coordination_lock" 2>/dev/null || true
}
trap cleanup_publish EXIT HUP INT TERM
if "$launchctl_bin" print "$domain/$label" >"$launch_state" 2>&1; then
  if ! "$launchctl_bin" bootout "$domain/$label" >"$launch_state" 2>&1; then
    echo "failed to unload legacy Tonghuashun LaunchAgent" >&2
    exit 1
  fi
  if "$launchctl_bin" print "$domain/$label" >"$launch_state" 2>&1; then
    echo "legacy Tonghuashun LaunchAgent remained loaded after bootout" >&2
    exit 1
  fi
  if ! is_absent_result "$launch_state"; then
    echo "could not prove legacy Tonghuashun LaunchAgent is absent" >&2
    exit 1
  fi
else
  if ! is_absent_result "$launch_state"; then
    echo "could not determine legacy Tonghuashun LaunchAgent state" >&2
    exit 1
  fi
fi

# launchd is absent and compliant starters cannot cross the coordination lock.
# Stop any old guard inode, then assert once more immediately before publish.
sh deploy/quiesce_ths_runtime.sh
if [ "${GRIDEDGE_DEPLOY_TEST_MODE:-0}" = 1 ] && [ -n "${GRIDEDGE_DEPLOY_TEST_AFTER_QUIESCE_HOOK:-}" ]; then
  sh "$GRIDEDGE_DEPLOY_TEST_AFTER_QUIESCE_HOOK"
fi
GRIDEDGE_QUIESCE_ASSERT_ONLY=1 sh deploy/quiesce_ths_runtime.sh

# All possible launchd failures and candidate validation failures have now
# completed. Each rename below is atomic within its target directory.
mv -f "$staged_binary" "$target_binary"
mv -f "$staged_config" "$target_config"
mv -f "$staged_runner" "$target_runner"
mv -f "$staged_guard" "$target_guard"
mv -f "$staged_plist" "$target_plist"

rm -f "$launch_state"
rmdir "$maintenance"
rmdir "$coordination_lock"
trap - EXIT HUP INT TERM
