#!/bin/sh
set -eu

deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
tmux_bin=${GRIDEDGE_TMUX_BIN:-/opt/homebrew/bin/tmux}
pgrep_bin=${GRIDEDGE_PGREP_BIN:-/usr/bin/pgrep}
sleep_bin=${GRIDEDGE_SLEEP_BIN:-/bin/sleep}
breaker="$deployment_root/runtime/android-runner-failures"
# Match an executing shell script, not an unrelated process whose later argv
# merely contains the guard path (the tmux server retains its original
# new-session command line after the ths_worker session has exited).
escaped_deployment_root=$(printf '%s' "$deployment_root" | sed 's/[][(){}.^$*+?|\\]/\\&/g')
guard_process_pattern="^([^[:space:]]*/)?(sh|bash|zsh)[[:space:]]+$escaped_deployment_root/bin/run_ths_trusted_session_guard\\.sh([[:space:]]|$)"
breaker_snapshot=$(mktemp "$deployment_root/runtime/.android-runner-failures.XXXXXX")
breaker_existed=false
if [ -e "$breaker" ]; then
  [ -f "$breaker" ] || { echo "Android circuit breaker is not a regular file" >&2; exit 1; }
  cp "$breaker" "$breaker_snapshot"
  breaker_existed=true
fi
trap 'rm -f "$breaker_snapshot"' EXIT HUP INT TERM

sessions=$($tmux_bin -L gridedge_codex list-sessions -F '#{session_name}' 2>/dev/null || true)
if printf '%s\n' "$sessions" | grep -Fx ths_worker >/dev/null; then
  if [ "${GRIDEDGE_QUIESCE_ASSERT_ONLY:-0}" = 1 ]; then
    echo "formal tmux owner reappeared inside the deployment critical section" >&2
    exit 1
  fi
  $tmux_bin -L gridedge_codex kill-session -t ths_worker
fi

attempt=0
while [ "$attempt" -lt 20 ]; do
  worker_pids=$($pgrep_bin -f "^$deployment_root/bin/gridedge_ths_live( |$)" 2>/dev/null || true)
  guard_pids=$($pgrep_bin -f "$guard_process_pattern" 2>/dev/null || true)
  if [ -z "$worker_pids" ] && [ -z "$guard_pids" ]; then
    break
  fi
  attempt=$((attempt + 1))
  $sleep_bin 0.25
done
if [ -n "${worker_pids:-}" ] || [ -n "${guard_pids:-}" ]; then
  echo "formal worker or trusted guard remained alive after controlled handoff stop" >&2
  exit 1
fi

if [ "$breaker_existed" = true ]; then
  if [ ! -f "$breaker" ] || ! cmp -s "$breaker_snapshot" "$breaker"; then
    echo "Android circuit breaker changed while quiescing the trusted owner" >&2
    exit 1
  fi
elif [ -e "$breaker" ]; then
  echo "Android circuit breaker changed while quiescing the trusted owner" >&2
  exit 1
fi
rm -f "$breaker_snapshot"
trap - EXIT HUP INT TERM
