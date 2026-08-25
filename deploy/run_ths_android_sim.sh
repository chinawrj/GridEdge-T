#!/bin/sh
set -eu

deployment_root=${GRIDEDGE_DEPLOYMENT_ROOT:?GRIDEDGE_DEPLOYMENT_ROOT is required}
android_sdk_root=${GRIDEDGE_ANDROID_SDK_ROOT:?GRIDEDGE_ANDROID_SDK_ROOT is required}
adb="$android_sdk_root/platform-tools/adb"
emulator="$android_sdk_root/emulator/emulator"
serial=emulator-5554
avd=THSP_API_32
package=com.hexin.plat.android.supremacy
activity=com.hexin.plat.android.Hexin
emulator_log="$deployment_root/logs/android-emulator.log"
failure_state="$deployment_root/runtime/android-runner-failures"
today=$(/bin/date +%Y-%m-%d)
failure_count=0
if [ -f "$failure_state" ]; then
  read -r stored_date stored_count <"$failure_state" || true
  if [ "${stored_date:-}" = "$today" ]; then
    failure_count=${stored_count:-0}
  fi
fi
case "$failure_count" in
  ''|*[!0-9]*) echo "Android runner failure state is invalid" >&2; exit 0 ;;
esac
if [ "$failure_count" -ge 3 ]; then
  echo "Android runner daily circuit breaker is open" >&2
  exit 0
fi
record_failure() {
  code=$?
  if [ "$code" -ne 0 ]; then
    next=$((failure_count + 1))
    temporary="$failure_state.$$"
    printf '%s %s\n' "$today" "$next" >"$temporary"
    mv -f "$temporary" "$failure_state"
  fi
  exit "$code"
}
trap record_failure EXIT

test -x "$adb"
test -x "$emulator"
test -s "$deployment_root/android-ths/confirmation-account.sha256"

connected=$($adb devices | awk 'NR > 1 && NF {count += 1} END {print count + 0}')
if [ "$connected" -eq 0 ]; then
  "$emulator" -avd "$avd" -memory 4096 -no-snapshot-save -no-boot-anim \
    >>"$emulator_log" 2>&1 &
elif [ "$connected" -ne 1 ]; then
  echo "Android simulation requires exactly one ADB device" >&2
  exit 1
fi

attempt=0
while [ "$attempt" -lt 60 ]; do
  state=$($adb -s "$serial" get-state 2>/dev/null || true)
  boot=$($adb -s "$serial" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r' || true)
  if [ "$state" = device ] && [ "$boot" = 1 ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 2
done
if [ "$attempt" -eq 60 ]; then
  echo "reviewed Android simulation emulator did not finish booting" >&2
  exit 1
fi

connected=$($adb devices | awk 'NR > 1 && NF {count += 1} END {print count + 0}')
if [ "$connected" -ne 1 ]; then
  echo "Android simulation requires exactly one ADB device after boot" >&2
  exit 1
fi
if [ "$($adb -s "$serial" shell getprop ro.boot.qemu.avd_name | tr -d '\r')" != "$avd" ]; then
  echo "connected emulator is not the reviewed Android simulation AVD" >&2
  exit 1
fi

$adb -s "$serial" shell settings put global window_animation_scale 0
$adb -s "$serial" shell settings put global transition_animation_scale 0
$adb -s "$serial" shell settings put global animator_duration_scale 0
$adb -s "$serial" shell am start -n "$package/$activity" >/dev/null
sleep 2

"$deployment_root/bin/gridedge_ths_live" "$@"
printf '%s 0\n' "$today" >"$failure_state"
trap - EXIT
