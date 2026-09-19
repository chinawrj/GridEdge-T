#!/usr/bin/env python3
"""Own one fixed isolated shadow run and persist its terminal result atomically."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def main() -> int:
    if len(sys.argv) != 2:
        raise RuntimeError("shadow runner requires one owned E2E root")
    root = Path(sys.argv[1]).resolve()
    contract = root / "contract.json"
    binary = root / "bin/gridedge_market_e2e_shadow"
    completed = subprocess.run([
        str(binary), "--contract", str(contract),
        "--duration-seconds", "900", "--interval-minutes", "5",
    ], capture_output=True, text=True, check=False)
    (root / "logs/shadow.stdout.log").write_text(completed.stdout, encoding="utf-8")
    (root / "logs/shadow.stderr.log").write_text(completed.stderr, encoding="utf-8")
    parsed = None
    if completed.returncode == 0:
        lines = [line for line in completed.stdout.splitlines() if line.strip()]
        if len(lines) != 1:
            completed = subprocess.CompletedProcess(completed.args, 125,
                                                    completed.stdout, completed.stderr)
        else:
            parsed = json.loads(lines[0])
            if not isinstance(parsed, dict) or parsed.get("money_actions_enabled") is not False:
                completed = subprocess.CompletedProcess(completed.args, 125,
                                                        completed.stdout, completed.stderr)
    result = {
        "exit_code": completed.returncode,
        "output": parsed,
        "stderr_sha256": hashlib.sha256(completed.stderr.encode()).hexdigest(),
        "stdout_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
    }
    result_path = root / "state/shadow-result.json"
    temporary = result_path.with_suffix(".tmp")
    temporary.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, result_path)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
