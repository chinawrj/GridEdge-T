#!/bin/sh
set -eu

deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
android_sdk_root=${GRIDEDGE_ANDROID_SDK_ROOT:?GRIDEDGE_ANDROID_SDK_ROOT is required}
market_host=${GRIDEDGE_MARKET_HOST:?GRIDEDGE_MARKET_HOST is required}
masked_account=${GRIDEDGE_ANDROID_MASKED_ACCOUNT:?GRIDEDGE_ANDROID_MASKED_ACCOUNT is required}
reviewed_session_date=${GRIDEDGE_REVIEWED_SESSION_DATE:?GRIDEDGE_REVIEWED_SESSION_DATE is required}
user_home=${GRIDEDGE_USER_HOME:?GRIDEDGE_USER_HOME is required}
tmux_bin=${GRIDEDGE_TMUX_BIN:-/opt/homebrew/bin/tmux}
tmux_socket=${GRIDEDGE_TMUX_SOCKET:-gridedge_codex}
tmux_session=${GRIDEDGE_TMUX_SESSION:-ths_worker}
coordination_lock="$deployment_root/runtime/ths-deployment-coordination.lock"
maintenance="$deployment_root/runtime/ths-deployment-maintenance"
guard="$deployment_root/bin/run_ths_trusted_session_guard.sh"
case "$tmux_socket:$tmux_session" in
  *[!A-Za-z0-9_.:-]*) echo "tmux identity is outside the reviewed alphabet" >&2; exit 1 ;;
esac

if ! mkdir "$coordination_lock" 2>/dev/null; then
  echo "deployment or trusted-session startup is already active" >&2
  exit 1
fi
cleanup_start_lock() { rmdir "$coordination_lock" 2>/dev/null || true; }
trap cleanup_start_lock EXIT HUP INT TERM
[ ! -e "$maintenance" ] || { echo "deployment maintenance is active" >&2; exit 1; }
[ -x "$guard" ] || { echo "installed trusted-session guard is unavailable" >&2; exit 1; }
if "$tmux_bin" -L "$tmux_socket" has-session -t "$tmux_session" 2>/dev/null; then
  echo "trusted-session tmux owner already exists" >&2
  exit 1
fi
"$tmux_bin" -L "$tmux_socket" new-session -d -s "$tmux_session" \
  -e "GRIDEDGE_DEPLOYMENT_ROOT=$deployment_root" \
  -e "GRIDEDGE_ANDROID_SDK_ROOT=$android_sdk_root" \
  -e "GRIDEDGE_MARKET_HOST=$market_host" \
  -e "GRIDEDGE_ANDROID_MASKED_ACCOUNT=$masked_account" \
  -e "GRIDEDGE_REVIEWED_SESSION_DATE=$reviewed_session_date" \
  -e "GRIDEDGE_USER_HOME=$user_home" \
  -e "GRIDEDGE_GUARD_EXEC=$guard" \
  'exec "$GRIDEDGE_GUARD_EXEC"'

expected_guard_sha256=$(shasum -a 256 "$guard" | awk '{print $1}')
ready="$deployment_root/runtime/ths-trusted-session-guard.lock/ready"
attempt=0
while [ "$attempt" -lt 40 ]; do
  if [ -f "$ready" ]; then
    read -r ready_pid ready_sha256 <"$ready" || true
    case "${ready_pid:-}" in ''|*[!0-9]*) ready_pid=0 ;; esac
    if [ "$ready_pid" -gt 1 ] && kill -0 "$ready_pid" 2>/dev/null &&
       [ "${ready_sha256:-}" = "$expected_guard_sha256" ] &&
       "$tmux_bin" -L "$tmux_socket" has-session -t "$tmux_session" 2>/dev/null
    then
      cleanup_start_lock
      trap - EXIT HUP INT TERM
      exit 0
    fi
  fi
  if ! "$tmux_bin" -L "$tmux_socket" has-session -t "$tmux_session" 2>/dev/null; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.25
done
"$tmux_bin" -L "$tmux_socket" kill-session -t "$tmux_session" 2>/dev/null || true
echo "trusted-session guard did not publish a matching ready handshake" >&2
exit 1
