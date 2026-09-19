/* Read-only macOS kernel process state, compiled against the installed SDK.
 * No signals, process launch, filesystem write, or inferred PID absence.
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/proc.h>

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    errno = 0;
    char *end = NULL;
    long value = strtol(argv[1], &end, 10);
    if (errno || end == argv[1] || *end || value <= 1 || value > INT_MAX) return 2;
    int mib[] = { CTL_KERN, KERN_PROC, KERN_PROC_PID, (int)value };
    struct kinfo_proc info;
    size_t size = sizeof(info);
    if (sysctl(mib, 4, &info, &size, NULL, 0) != 0 || size != sizeof(info)) return 3;
    if (info.kp_proc.p_pid != value) return 4;
    printf("{\"pid\":%d,\"uid\":%u,\"status\":%d,\"start_sec\":%lld,\"start_usec\":%d}\n",
        info.kp_proc.p_pid, info.kp_eproc.e_ucred.cr_uid, info.kp_proc.p_stat,
        (long long)info.kp_proc.p_starttime.tv_sec, info.kp_proc.p_starttime.tv_usec);
    return 0;
}
