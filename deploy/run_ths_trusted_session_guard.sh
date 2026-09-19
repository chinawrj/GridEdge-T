#!/bin/sh
set -eu

# This guard is launched inside the reviewed `tmux -L gridedge_codex` worker
# session during the 09:00 preflight. It deliberately has no dependency on the
# Codex desktop process or its heartbeat scheduler.
deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
android_sdk_root=${GRIDEDGE_ANDROID_SDK_ROOT:?GRIDEDGE_ANDROID_SDK_ROOT is required}
market_host=${GRIDEDGE_MARKET_HOST:?GRIDEDGE_MARKET_HOST is required}
masked_account=${GRIDEDGE_ANDROID_MASKED_ACCOUNT:?GRIDEDGE_ANDROID_MASKED_ACCOUNT is required}
reviewed_session_date=${GRIDEDGE_REVIEWED_SESSION_DATE:?GRIDEDGE_REVIEWED_SESSION_DATE is required}
user_home=${GRIDEDGE_USER_HOME:?GRIDEDGE_USER_HOME is required}
guard_test_mode=${GRIDEDGE_GUARD_DEPENDENCY_TEST_MODE:-0}
pgrep_bin=/usr/bin/pgrep
nc_bin=/usr/bin/nc
if [ "$guard_test_mode" = 1 ]; then
  case "$deployment_root" in
    /tmp/gridedge-guard-test-*|/private/tmp/gridedge-guard-test-*) ;;
    *) echo "guard dependency test mode requires an isolated temporary root" >&2; exit 1 ;;
  esac
  if [ "${GRIDEDGE_DEPLOY_TEST_MODE:-0}" != 1 ]; then
    echo "guard dependency test mode requires deployment test mode" >&2
    exit 1
  fi
  pgrep_bin=${GRIDEDGE_GUARD_TEST_PGREP_BIN:?test pgrep is required}
  nc_bin=${GRIDEDGE_GUARD_TEST_NC_BIN:?test nc is required}
fi

runner="$deployment_root/bin/run_ths_android_sim.sh"
failure_state="$deployment_root/runtime/android-runner-failures"
guard_lock="$deployment_root/runtime/ths-trusted-session-guard.lock"
guard_log="$deployment_root/logs/trusted-session-guard.log"
maintenance="$deployment_root/runtime/ths-deployment-maintenance"

test -x "$runner"
test -d "$deployment_root/runtime"
test -d "$deployment_root/logs"
if [ -e "$maintenance" ]; then
  echo "trusted-session deployment maintenance is active" >&2
  exit 1
fi

if ! mkdir "$guard_lock" 2>/dev/null; then
  old_pid=$(sed -n '1p' "$guard_lock/pid" 2>/dev/null || true)
  case "$old_pid" in
    ''|*[!0-9]*) old_pid=0 ;;
  esac
  if [ "$old_pid" -gt 1 ] && kill -0 "$old_pid" 2>/dev/null; then
    echo "reviewed trusted-session guard is already running" >&2
    exit 1
  fi
  # A SIGKILL can leave both the owner record and a previously published
  # readiness record behind.  They belong to the same dead owner and must be
  # removed together before taking the lock; any other unexpected entry keeps
  # rmdir fail-closed.
  rm -f "$guard_lock/ready" "$guard_lock/ready.tmp.$old_pid" "$guard_lock/pid"
  rmdir "$guard_lock"
  mkdir "$guard_lock"
fi
printf '%s\n' "$$" >"$guard_lock/pid"
guard_sha256=$(shasum -a 256 "$0" | awk '{print $1}')
cleanup_guard_lock() {
  rm -f "$guard_lock/ready" "$guard_lock/ready.tmp.$$"
  rm -f "$guard_lock/pid"
  rmdir "$guard_lock" 2>/dev/null || true
}
stop_guard() {
  cleanup_guard_lock
  exit 0
}
trap cleanup_guard_lock EXIT
trap stop_guard HUP INT TERM

current_date=$(/bin/date +%Y-%m-%d)
if [ "$current_date" != "$reviewed_session_date" ]; then
  echo "trusted-session guard date differs from the reviewed session date" >&2
  exit 1
fi

# Publish readiness only after every one-time startup gate has passed.  The
# starter may treat this record as proof that the installed guard entered its
# stable supervision loop, so a partial record must never be observable.
ready_tmp="$guard_lock/ready.tmp.$$"
printf '%s %s\n' "$$" "$guard_sha256" >"$ready_tmp"
mv "$ready_tmp" "$guard_lock/ready"

# Reviewed launch windows: 09:00–11:30 and 12:55–15:05. A worker that remains
# alive through lunch is left alone; only a missing child is deferred to 12:55.
while [ "$(/bin/date +%Y-%m-%d)" = "$reviewed_session_date" ]; do
  hour=$(/bin/date +%H)
  minute=$(/bin/date +%M)
  # Avoid `expr` here: a numerically valid zero has exit status 1, which makes
  # this `set -e` guard terminate at every top of hour (for example 10:00).
  hour=${hour#0}
  minute=${minute#0}
  hour=${hour:-0}
  minute=${minute:-0}
  minute_of_day=$((hour * 60 + minute))
  if [ "$guard_test_mode" = 1 ]; then
    minute_of_day=540
  fi

  if [ "$minute_of_day" -ge 905 ]; then
    break
  fi
  if [ -e "$maintenance" ]; then
    echo "trusted-session deployment maintenance became active" >&2
    exit 0
  fi
  if ! { [ "$minute_of_day" -ge 540 ] && [ "$minute_of_day" -lt 690 ]; } &&
     ! { [ "$minute_of_day" -ge 775 ] && [ "$minute_of_day" -lt 905 ]; }; then
    sleep 15
    continue
  fi

  worker_pids=$($pgrep_bin -f "^$deployment_root/bin/gridedge_ths_live( |$)" || true)
  worker_count=$(printf '%s\n' "$worker_pids" | awk 'NF { count += 1 } END { print count + 0 }')
  if [ "$worker_count" -gt 1 ]; then
    echo "trusted-session guard found multiple formal workers" >&2
    exit 1
  fi
  if [ "$worker_count" -eq 1 ]; then
    sleep 15
    continue
  fi

  # Do not hand a routine dependency outage to the money-enabled wrapper: its
  # daily breaker is reserved for failures reached after these app-independent
  # prerequisites are present.  This gate is deliberately read-only and does
  # not claim market continuity, account reconciliation, or RUNNING mode.
  adb="$android_sdk_root/platform-tools/adb"
  adb_inventory=$($adb devices 2>/dev/null || true)
  attached_devices=$(printf '%s\n' "$adb_inventory" | awk 'NR > 1 && NF { count += 1 } END { print count + 0 }')
  exact_ready_devices=$(printf '%s\n' "$adb_inventory" | awk 'NR > 1 && $1 == "emulator-5554" && $2 == "device" && NF == 2 { count += 1 } END { print count + 0 }')
  android_state=$($adb -s emulator-5554 get-state 2>/dev/null || true)
  android_boot=$($adb -s emulator-5554 shell getprop sys.boot_completed 2>/dev/null | tr -d '\r' || true)
  android_avd=$($adb -s emulator-5554 shell getprop ro.boot.qemu.avd_name 2>/dev/null | tr -d '\r' || true)
  dependency_reason=
  if [ "$attached_devices" != 1 ] || [ "$exact_ready_devices" != 1 ] ||
     [ "$android_state" != device ] ||
     [ "$android_boot" != 1 ] || [ "$android_avd" != THSP_API_32 ]; then
    dependency_reason=ANDROID_NOT_READY
  elif ! $pgrep_bin -x "Google Chrome" >/dev/null 2>&1; then
    dependency_reason=CHROME_NOT_RUNNING
  elif ! $nc_bin -z -w 3 "$market_host" 8883 >/dev/null 2>&1; then
    dependency_reason=MARKET_MQTT_UNREACHABLE
  fi
  if [ -n "$dependency_reason" ]; then
    printf '%s worker_deferred dependency=%s breaker_unchanged=true\n' \
      "$(/bin/date '+%Y-%m-%dT%H:%M:%S%z')" "$dependency_reason" >>"$guard_log"
    if [ "$guard_test_mode" = 1 ]; then
      exit 0
    fi
    sleep 15
    continue
  fi

  failure_count=0
  if [ -f "$failure_state" ]; then
    read -r stored_date stored_count <"$failure_state" || true
    if [ "${stored_date:-}" = "$reviewed_session_date" ]; then
      failure_count=${stored_count:-0}
    fi
  fi
  case "$failure_count" in
    ''|*[!0-9]*)
      echo "trusted-session guard found an invalid Android circuit-breaker state" >&2
      exit 1
      ;;
  esac
  if [ "$failure_count" -ge 3 ]; then
    printf '%s circuit_breaker_open failures=%s\n' "$(/bin/date '+%Y-%m-%dT%H:%M:%S%z')" "$failure_count" >>"$guard_log"
    sleep 60
    continue
  fi

  printf '%s worker_start mode=READ_ONLY_FIRST\n' "$(/bin/date '+%Y-%m-%dT%H:%M:%S%z')" >>"$guard_log"
  if GRIDEDGE_DEPLOYMENT_ROOT="$deployment_root" \
     GRIDEDGE_ANDROID_SDK_ROOT="$android_sdk_root" \
     "$runner" \
       --config "$deployment_root/config/ths_002256_sim.yaml" \
       --run-id ths-002256-20260819-grid15-opening-v1 \
       --outbox "$deployment_root/runtime/002256-outbox-opening-v1.db" \
       --quote-log "$deployment_root/runtime/002256-opening-v1-quotes.jsonl" \
       --bar-log "$deployment_root/runtime/002256-opening-v1-bars.jsonl" \
       --market-event-log "$deployment_root/runtime/002256-opening-v1-market-mqtt.jsonl" \
       --market-mqtt-host "$market_host" \
       --market-mqtt-port 8883 \
       --market-mqtt-username gridedge-publisher \
       --market-mqtt-password-file "$deployment_root/market-mqtt/publisher.password" \
       --market-mqtt-ca-file "$deployment_root/market-mqtt/ca.crt" \
       --interval-minutes 5 \
       --poll-seconds 5 \
       --maximum-unchanged-seconds 60 \
       --maximum-order-age-seconds 300 \
       --allow-partial-session-resume-boundary \
       --simulation-adapter android \
       --android-serial emulator-5554 \
       --android-avd-marker THSP_API_32 \
       --android-masked-account "$masked_account" \
       --android-security-name 兆新股份 \
       --android-adb-path "$android_sdk_root/platform-tools/adb" \
       --android-confirmation-account-sha256-file "$deployment_root/android-ths/confirmation-account.sha256" \
       --android-money-actions-enabled \
       --execution-runner-file "$runner" \
       --execution-launch-plist-file "$user_home/Library/LaunchAgents/com.gridedge.ths-sim.plist" \
       --execution-guard-file "$deployment_root/bin/run_ths_trusted_session_guard.sh"
  then
    worker_status=0
  else
    worker_status=$?
  fi
  printf '%s worker_exit status=%s\n' "$(/bin/date '+%Y-%m-%dT%H:%M:%S%z')" "$worker_status" >>"$guard_log"
  sleep 60
done
