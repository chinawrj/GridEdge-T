#!/usr/bin/env python3
"""Manage a loopback-only, nonce-scoped market-data E2E stack.

This stack is deliberately unable to address the production broker or topic
namespace. It owns an ephemeral PostgreSQL cluster, Mosquitto broker, ingestor,
credentials, logs, and browser settings under one validated /tmp root.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

NONCE = re.compile(r"^e2e-0629-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
ROOT_NAME = re.compile(r"^gridedge-market-e2e-e2e-0629-[0-9a-f-]{36}$")
MQTT_PORT = 18883
MQTT_WS_PORT = 19001
POSTGRES_PORT = 15432
BROWSER_DEBUG_PORT = 19222
REPO = Path(__file__).resolve().parents[3]
POSTGRES_BIN = Path("/opt/homebrew/opt/postgresql@17/bin")
MOSQUITTO = Path("/opt/homebrew/sbin/mosquitto")
MOSQUITTO_PASSWD = Path("/opt/homebrew/bin/mosquitto_passwd")
CHROME = Path.home() / (
    "Library/Caches/ms-playwright/chromium-1234/chrome-mac-arm64/"
    "Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"
)
EASTMONEY_REVIEWED_URL = "https://quote.eastmoney.com/f1.html?newcode=0.002256"
BUNDLE_NAME = re.compile(r"^gridedge-market-e2e-bundle-e2e-0629-[0-9a-f-]{36}$")


def reviewed_chrome_identity() -> tuple[str, str]:
    if not CHROME.is_file() or not os.access(CHROME, os.X_OK):
        raise RuntimeError("reviewed Chrome for Testing binary is unavailable")
    version = subprocess.run(
        [str(CHROME), "--version"], capture_output=True, text=True, check=True
    ).stdout.strip()
    if re.fullmatch(r"Google Chrome for Testing 151\.\d+\.\d+\.\d+", version) is None:
        raise RuntimeError("isolated E2E requires the reviewed Chrome for Testing major 151")
    return version, hashlib.sha256(CHROME.read_bytes()).hexdigest()


def validated_root(value: str) -> Path:
    root = Path(value).resolve()
    allowed_parent = Path("/tmp").resolve()
    if root.parent != allowed_parent or ROOT_NAME.fullmatch(root.name) is None:
        raise ValueError("E2E root must be one exact /tmp/gridedge-market-e2e-e2e-0629-<uuid> path")
    return root


def validated_nonce(value: str) -> str:
    if NONCE.fullmatch(value) is None:
        raise ValueError("E2E nonce is invalid")
    return value


def validated_bundle_root(value: str) -> Path:
    root = Path(value).resolve()
    if root.parent != Path("/tmp").resolve() or BUNDLE_NAME.fullmatch(root.name) is None:
        raise ValueError(
            "candidate bundle must be one exact /tmp/gridedge-market-e2e-bundle-e2e-0629-<uuid> path"
        )
    return root


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree_sha256(root: Path) -> str:
    if not root.is_dir() or root.is_symlink():
        raise RuntimeError("candidate tree must be one real directory")
    digest = hashlib.sha256()
    files = sorted(path for path in root.rglob("*") if path.is_file())
    if not files:
        raise RuntimeError("candidate tree must not be empty")
    for path in files:
        if path.is_symlink():
            raise RuntimeError("candidate tree must not contain symlinks")
        relative = path.relative_to(root).as_posix().encode()
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def source_snapshot_sha256() -> str:
    digest = hashlib.sha256()
    roots = [REPO / "src", REPO / "deploy", REPO / "extensions/gridedge-web-market"]
    files = [REPO / "Cargo.toml", REPO / "Cargo.lock", REPO / "GOAL.md", REPO / "AGENTS.md"]
    for root in roots:
        files.extend(path for path in root.rglob("*") if path.is_file()
                     and "node_modules" not in path.parts)
    for path in sorted(set(files)):
        if path.is_symlink():
            raise RuntimeError("source snapshot must not contain symlinks")
        relative = path.relative_to(REPO).as_posix().encode()
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def validate_candidate_bundle(bundle_root: Path) -> dict[str, object]:
    bundle_root = validated_bundle_root(str(bundle_root))
    manifest_path = bundle_root / "bundle-manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected = {
        "schema_version": 1,
        "source_revision": str(manifest.get("source_revision", "")),
        "source_snapshot_sha256": str(manifest.get("source_snapshot_sha256", "")),
        "extension_tree_sha256": tree_sha256(bundle_root / "extension"),
        "shadow_binary_sha256": sha256_file(bundle_root / "gridedge_market_e2e_shadow"),
        "release_binary_sha256": sha256_file(bundle_root / "gridedge_ths_live"),
        "ingestor_sha256": sha256_file(bundle_root / "market_ingestor.py"),
        "shadow_runner_sha256": sha256_file(bundle_root / "shadow_runner.py"),
    }
    if not re.fullmatch(r"[0-9a-f]{40}", expected["source_revision"]):
        raise RuntimeError("candidate bundle lacks one exact Git source revision")
    if not re.fullmatch(r"[0-9a-f]{64}", expected["source_snapshot_sha256"]):
        raise RuntimeError("candidate bundle lacks one exact source snapshot SHA-256")
    if manifest != expected:
        raise RuntimeError("candidate bundle identity or bytes changed")
    return {**manifest, "manifest_sha256": sha256_file(manifest_path)}


def freeze_candidate_bundle(bundle_root: Path) -> dict[str, object]:
    bundle_root = validated_bundle_root(str(bundle_root))
    if bundle_root.exists():
        raise RuntimeError("candidate bundle already exists")
    temporary = Path(f"{bundle_root}.tmp-{os.getpid()}")
    if temporary.exists():
        raise RuntimeError("candidate bundle temporary path already exists")
    try:
        temporary.mkdir(mode=0o700)
        shutil.copytree(REPO / "build/gridedge-web-market-extension", temporary / "extension")
        artifacts = {
            "gridedge_market_e2e_shadow": REPO / "target/debug/gridedge_market_e2e_shadow",
            "gridedge_ths_live": REPO / "target/release/gridedge_ths_live",
            "market_ingestor.py": REPO / "deploy/market_data/ingestor/market_ingestor.py",
            "shadow_runner.py": REPO / "deploy/market_data/local_e2e/shadow_runner.py",
        }
        for name, source in artifacts.items():
            if not source.is_file():
                raise RuntimeError(f"candidate artifact is missing: {source}")
            shutil.copy2(source, temporary / name)
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
        ).stdout.strip()
        manifest = {
            "schema_version": 1,
            "source_revision": revision,
            "source_snapshot_sha256": source_snapshot_sha256(),
            "extension_tree_sha256": tree_sha256(temporary / "extension"),
            "shadow_binary_sha256": sha256_file(temporary / "gridedge_market_e2e_shadow"),
            "release_binary_sha256": sha256_file(temporary / "gridedge_ths_live"),
            "ingestor_sha256": sha256_file(temporary / "market_ingestor.py"),
            "shadow_runner_sha256": sha256_file(temporary / "shadow_runner.py"),
        }
        write_private(temporary / "bundle-manifest.json", json.dumps(manifest, sort_keys=True) + "\n")
        for path in temporary.rglob("*"):
            if path.is_file():
                path.chmod(0o500 if os.access(path, os.X_OK) else 0o400)
        os.replace(temporary, bundle_root)
        return validate_candidate_bundle(bundle_root)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def topic_namespace(nonce: str) -> str:
    return f"gridedge-e2e/{validated_nonce(nonce)}"


def generated_contract(root: Path, nonce: str,
                       bundle_root: Path | None = None) -> dict[str, object]:
    root = root.resolve()
    if bundle_root is not None:
        bundle_root = bundle_root.resolve()
    namespace = topic_namespace(nonce)
    browser_version, browser_sha256 = reviewed_chrome_identity()
    contract = {
        "mode": "ISOLATED_READ_ONLY_E2E",
        "nonce": nonce,
        "root": str(root),
        "mqtt_host": "127.0.0.1",
        "mqtt_port": MQTT_PORT,
        "mqtt_ws_url": f"ws://127.0.0.1:{MQTT_WS_PORT}/mqtt",
        "mqtt_namespace": namespace,
        "mqtt_input": f"{namespace}/market/v1/#",
        "mqtt_ack": f"{namespace}/market-ack/v1/#",
        "mqtt_committed": f"{namespace}/market-committed/v1/#",
        "worker_mqtt_username": "gridedge-e2e-worker",
        "worker_mqtt_client_id": f"{nonce}-worker",
        "postgres_host": "127.0.0.1",
        "postgres_port": POSTGRES_PORT,
        "run_id": f"{nonce}-read-only",
        "market_event_log": str(root / "state/market-events.ndjson"),
        "bar_log": str(root / "state/bars.ndjson"),
        "quote_log": str(root / "state/quotes.ndjson"),
        "ledger": str(root / "state/ledger.sqlite3"),
        "outbox": str(root / "state/outbox.sqlite3"),
        "chrome_profile": str(root / "chrome-profile"),
        "browser_extension": str(root / "browser-extension"),
        "browser_extension_installation": "UNPACKED_REVIEWED",
        "browser_binary": str(CHROME),
        "browser_version": browser_version,
        "browser_binary_sha256": browser_sha256,
        "money_actions_enabled": False,
    }
    if bundle_root is not None:
        identity = validate_candidate_bundle(bundle_root)
        contract.update({
            "candidate_bundle": str(bundle_root),
            "candidate_bundle_manifest_sha256": identity["manifest_sha256"],
            "candidate_extension_tree_sha256": identity["extension_tree_sha256"],
            "candidate_shadow_binary_sha256": identity["shadow_binary_sha256"],
            "candidate_release_binary_sha256": identity["release_binary_sha256"],
            "candidate_source_revision": identity["source_revision"],
            "candidate_source_snapshot_sha256": identity["source_snapshot_sha256"],
        })
    return contract


def load_contract(root: Path) -> dict[str, object]:
    contract = json.loads((root / "contract.json").read_text(encoding="utf-8"))
    nonce = validated_nonce(str(contract.get("nonce", "")))
    expected = validated_root(f"/tmp/gridedge-market-e2e-{nonce}")
    if root != expected or Path(str(contract.get("root", ""))).resolve() != root:
        raise RuntimeError("E2E contract root and nonce identity do not match")
    bundle_root = validated_bundle_root(str(contract.get("candidate_bundle", "")))
    if generated_contract(root, nonce, bundle_root) != contract:
        raise RuntimeError("E2E runtime contract was modified")
    return contract


def listening_pids(port: int) -> set[int]:
    result = subprocess.run(["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
                            capture_output=True, text=True)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"cannot inspect listener ownership for port {port}")
    return {int(line) for line in result.stdout.splitlines() if line.strip()}


def process_command(pid: int) -> str:
    result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True,
                            text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else ""


def require_owned_process(root: Path, pid: int, label: str) -> None:
    command = process_command(pid)
    if not command or str(root) not in command:
        raise RuntimeError(f"{label} PID is stale or is not owned by this E2E root")


def wait_gone(pid: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_command(pid):
            return
        time.sleep(0.05)
    raise RuntimeError(f"E2E process {pid} did not stop within the bounded deadline")


def write_private(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)


def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, **kwargs)


def prepare(root: Path, nonce: str, bundle_root: Path) -> None:
    if root.exists():
        raise RuntimeError("E2E root already exists; use a fresh nonce")
    for name in ("bin", "certs", "logs", "mqtt-data", "pgdata", "run", "secrets", "socket", "state"):
        (root / name).mkdir(parents=True, mode=0o700, exist_ok=True)
    bundle_root = validated_bundle_root(str(bundle_root))
    bundle = validate_candidate_bundle(bundle_root)
    contract = generated_contract(root, nonce, bundle_root)
    namespace = str(contract["mqtt_namespace"])
    publisher_password = secrets.token_urlsafe(32)
    ingestor_password = secrets.token_urlsafe(32)
    worker_password = secrets.token_urlsafe(32)
    postgres_password = secrets.token_urlsafe(32)
    write_private(root / "secrets/publisher.password", publisher_password + "\n")
    write_private(root / "secrets/ingestor.password", ingestor_password + "\n")
    write_private(root / "secrets/worker.password", worker_password + "\n")
    write_private(root / "secrets/postgres.password", postgres_password + "\n")

    run([str(MOSQUITTO_PASSWD), "-b", "-c", str(root / "mosquitto.passwd"),
         "gridedge-e2e-publisher", publisher_password])
    run([str(MOSQUITTO_PASSWD), "-b", str(root / "mosquitto.passwd"),
         "gridedge-e2e-ingestor", ingestor_password])
    run([str(MOSQUITTO_PASSWD), "-b", str(root / "mosquitto.passwd"),
         "gridedge-e2e-worker", worker_password])
    (root / "mosquitto.passwd").chmod(0o600)
    acl = f"""user gridedge-e2e-publisher
topic deny readwrite gridedge/#
topic write {namespace}/market/v1/#
topic read {namespace}/market-ack/v1/#
topic read {namespace}/market-committed/v1/#

user gridedge-e2e-ingestor
topic deny readwrite gridedge/#
topic read {namespace}/market/v1/#
topic write {namespace}/market-ack/v1/#
topic write {namespace}/market-committed/v1/#

user gridedge-e2e-worker
topic deny readwrite gridedge/#
topic read {namespace}/market-committed/v1/#
"""
    write_private(root / "acl", acl)

    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-subj", "/CN=GridEdge E2E CA", "-days", "2",
         "-keyout", str(root / "certs/ca.key"), "-out", str(root / "certs/ca.crt")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run(["openssl", "req", "-newkey", "rsa:2048", "-nodes",
         "-subj", "/CN=localhost", "-keyout", str(root / "certs/server.key"),
         "-out", str(root / "certs/server.csr")], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    (root / "certs/ext.cnf").write_text(
        "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n",
        encoding="utf-8")
    run(["openssl", "x509", "-req", "-in", str(root / "certs/server.csr"),
         "-CA", str(root / "certs/ca.crt"), "-CAkey", str(root / "certs/ca.key"),
         "-CAcreateserial", "-days", "2", "-extfile", str(root / "certs/ext.cnf"),
         "-out", str(root / "certs/server.crt")], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    (root / "certs/server.key").chmod(0o600)

    conf = f"""pid_file {root / 'run/mosquitto.pid'}
persistence true
persistence_location {root / 'mqtt-data'}/
allow_anonymous false
password_file {root / 'mosquitto.passwd'}
acl_file {root / 'acl'}
listener {MQTT_PORT} 127.0.0.1
protocol mqtt
cafile {root / 'certs/ca.crt'}
certfile {root / 'certs/server.crt'}
keyfile {root / 'certs/server.key'}
tls_version tlsv1.2
listener {MQTT_WS_PORT} 127.0.0.1
protocol websockets
log_dest file {root / 'logs/mosquitto.log'}
log_type all
"""
    (root / "mosquitto.conf").write_text(conf, encoding="utf-8")
    write_private(root / "contract.json", json.dumps(contract, indent=2) + "\n")
    browser = {
        "enabled": True,
        "deployment_mode": "ISOLATED_READ_ONLY_E2E",
        "mqtt_url": contract["mqtt_ws_url"],
        "mqtt_username": "gridedge-e2e-publisher",
        "mqtt_password_file": str(root / "secrets/publisher.password"),
        "mqtt_namespace": namespace,
        "mqtt_client_id": f"{nonce}-publisher",
    }
    write_private(root / "browser-settings.json", json.dumps(browser, indent=2) + "\n")
    # The frozen bundle is intentionally read-only. Runtime nonce/credential
    # derivation happens only in this owned copy and must never mutate it.
    shutil.copytree(bundle_root / "extension", root / "browser-extension",
                    copy_function=shutil.copyfile)
    background_path = root / "browser-extension/src/background.js"
    background = background_path.read_text(encoding="utf-8")
    replacements = {
        "enabled: false,": "enabled: true,",
        'mqtt_url: "ws://192.168.1.201:9001/mqtt",':
            f'mqtt_url: {json.dumps(contract["mqtt_ws_url"])},',
        'mqtt_username: "gridedge-publisher",':
            'mqtt_username: "gridedge-e2e-publisher",',
        'mqtt_password: "",': f"mqtt_password: {json.dumps(publisher_password)},",
        'deployment_mode: "PRODUCTION",':
            'deployment_mode: "ISOLATED_READ_ONLY_E2E",',
        'mqtt_namespace: "gridedge",': f'mqtt_namespace: {json.dumps(namespace)},',
        'mqtt_client_id: "",': f'mqtt_client_id: {json.dumps(f"{nonce}-publisher")},',
        'initialization_policy: "FULL_HISTORY_OR_REVIEWED_FALLBACK_V1",':
            'initialization_policy: "ISOLATED_EMPTY_STATE_RESUME_BOUNDARY_V1",',
    }
    for old, new in replacements.items():
        if background.count(old) != 1:
            raise RuntimeError(f"candidate browser bootstrap contract changed at {old}")
        background = background.replace(old, new)
    write_private(background_path, background)
    worker = {
        "market_mqtt_host": contract["mqtt_host"],
        "market_mqtt_port": contract["mqtt_port"],
        "market_mqtt_username": contract["worker_mqtt_username"],
        "market_mqtt_password_file": str(root / "secrets/worker.password"),
        "market_mqtt_ca_file": str(root / "certs/ca.crt"),
        "market_mqtt_client_id": contract["worker_mqtt_client_id"],
        "market_mqtt_topic_namespace": namespace,
        "run_id": contract["run_id"],
        "market_event_log": contract["market_event_log"],
        "bar_log": contract["bar_log"],
        "quote_log": contract["quote_log"],
        "ledger": contract["ledger"],
        "outbox": contract["outbox"],
        "money_actions_enabled": False,
    }
    write_private(root / "worker-settings.json", json.dumps(worker, indent=2) + "\n")
    shutil.copy2(bundle_root / "market_ingestor.py",
                 root / "bin/market_ingestor.py")
    shutil.copy2(bundle_root / "shadow_runner.py",
                 root / "bin/shadow_runner.py")
    shadow_binary = bundle_root / "gridedge_market_e2e_shadow"
    installed_shadow = root / "bin/gridedge_market_e2e_shadow"
    shutil.copy2(shadow_binary, installed_shadow)
    write_private(root / "shadow-binary.sha256",
                  hashlib.sha256(installed_shadow.read_bytes()).hexdigest() + "\n")
    if (root / "shadow-binary.sha256").read_text().strip() != bundle["shadow_binary_sha256"]:
        raise RuntimeError("prepared shadow binary differs from the frozen candidate bundle")

    run([str(POSTGRES_BIN / "initdb"), "-D", str(root / "pgdata"),
         "-U", "gridedge_e2e", "--auth-local=trust", "--auth-host=scram-sha-256",
         "--pwfile", str(root / "secrets/postgres.password"), "--no-locale", "--encoding=UTF8"],
        stdout=subprocess.DEVNULL)
    run([sys.executable, "-m", "venv", str(root / "venv")])
    run([str(root / "venv/bin/python"), "-m", "pip", "install", "--disable-pip-version-check",
         "-r", str(REPO / "deploy/market_data/ingestor/requirements.txt")],
        stdout=subprocess.DEVNULL)


def start(root: Path) -> None:
    try:
        start_services(root)
    except Exception as start_error:
        try:
            stop(root)
        except Exception as rollback_error:
            raise RuntimeError(
                f"isolated E2E start failed and bounded rollback was incomplete: {rollback_error}"
            ) from start_error
        raise


def start_services(root: Path) -> None:
    contract = load_contract(root)
    occupied = {port: listening_pids(port) for port in (MQTT_PORT, MQTT_WS_PORT, POSTGRES_PORT)}
    if any(occupied.values()):
        raise RuntimeError(f"isolated E2E ports are already owned: {occupied}")
    pg_password = (root / "secrets/postgres.password").read_text().strip()
    run([str(POSTGRES_BIN / "pg_ctl"), "-D", str(root / "pgdata"), "-l",
         str(root / "logs/postgres.log"), "-o",
         f"-h 127.0.0.1 -p {POSTGRES_PORT} -k {root / 'socket'}", "start"])
    env = {**os.environ, "PGPASSWORD": pg_password}
    for _ in range(30):
        status = subprocess.run([str(POSTGRES_BIN / "pg_isready"), "-h", "127.0.0.1",
                                 "-p", str(POSTGRES_PORT)], env=env, capture_output=True)
        if status.returncode == 0:
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("isolated PostgreSQL did not become ready")
    exists = subprocess.run([str(POSTGRES_BIN / "psql"), "-h", "127.0.0.1", "-p",
                             str(POSTGRES_PORT), "-U", "gridedge_e2e", "-d", "postgres",
                             "-Atc", "SELECT 1 FROM pg_database WHERE datname='gridedge_market_e2e'"],
                            env=env, capture_output=True, text=True, check=True).stdout.strip()
    if exists != "1":
        run([str(POSTGRES_BIN / "createdb"), "-h", "127.0.0.1", "-p", str(POSTGRES_PORT),
             "-U", "gridedge_e2e", "gridedge_market_e2e"], env=env)
        run([str(POSTGRES_BIN / "psql"), "-h", "127.0.0.1", "-p", str(POSTGRES_PORT),
             "-U", "gridedge_e2e", "-d", "gridedge_market_e2e", "-f",
             str(REPO / "deploy/market_data/postgres/init/001_market_schema.sql")], env=env,
            stdout=subprocess.DEVNULL)
    subprocess.Popen([str(MOSQUITTO), "-c", str(root / "mosquitto.conf"), "-d"])
    time.sleep(0.5)
    mqtt_password = (root / "secrets/ingestor.password").read_text().strip()
    write_private(root / "secrets/ingestor-runtime.password", mqtt_password + "\n")
    ingestor_env = {
        **os.environ,
        "POSTGRES_HOST": "127.0.0.1", "POSTGRES_PORT": str(POSTGRES_PORT),
        "POSTGRES_DB": "gridedge_market_e2e", "POSTGRES_USER": "gridedge_e2e",
        "POSTGRES_PASSWORD_FILE": str(root / "secrets/postgres.password"),
        "MQTT_HOST": "127.0.0.1", "MQTT_PORT": str(MQTT_PORT),
        "MQTT_CLIENT_ID": f"{contract['nonce']}-ingestor",
        "MQTT_USERNAME": "gridedge-e2e-ingestor",
        "MQTT_PASSWORD_FILE": str(root / "secrets/ingestor-runtime.password"),
        "MQTT_CA_FILE": str(root / "certs/ca.crt"),
        "MQTT_TOPIC_NAMESPACE": str(contract["mqtt_namespace"]),
        "MQTT_TOPIC": str(contract["mqtt_input"]),
    }
    log = (root / "logs/ingestor.log").open("ab")
    process = subprocess.Popen([str(root / "venv/bin/python"),
        str(root / "bin/market_ingestor.py")],
        env=ingestor_env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (root / "run/ingestor.pid").write_text(f"{process.pid}\n", encoding="utf-8")
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError("isolated ingestor exited during startup")
    postgres_pid = int((root / "pgdata/postmaster.pid").read_text().splitlines()[0])
    broker_pid = int((root / "run/mosquitto.pid").read_text().strip())
    for pid, label in ((postgres_pid, "PostgreSQL"), (broker_pid, "Mosquitto"),
                       (process.pid, "ingestor")):
        require_owned_process(root, pid, label)
    expected = {POSTGRES_PORT: {postgres_pid}, MQTT_PORT: {broker_pid}, MQTT_WS_PORT: {broker_pid}}
    actual = {port: listening_pids(port) for port in expected}
    if actual != expected:
        raise RuntimeError(f"isolated E2E listener ownership is invalid: {actual}")


def stop(root: Path) -> None:
    load_contract(root)
    shadow_pid_file = root / "run/shadow.pid"
    if shadow_pid_file.exists():
        pid = int(shadow_pid_file.read_text().strip())
        if process_command(pid):
            require_owned_process(root, pid, "shadow worker")
            os.killpg(pid, signal.SIGTERM)
            wait_gone(pid, 10.0)
        shadow_pid_file.unlink(missing_ok=True)
    pid_file = root / "run/ingestor.pid"
    if pid_file.exists():
        pid = int(pid_file.read_text().strip())
        require_owned_process(root, pid, "ingestor")
        os.kill(pid, signal.SIGTERM)
        wait_gone(pid)
        pid_file.unlink(missing_ok=True)
    stop_browser(root)
    broker_pid = root / "run/mosquitto.pid"
    if broker_pid.exists():
        pid = int(broker_pid.read_text().strip())
        require_owned_process(root, pid, "Mosquitto")
        os.kill(pid, signal.SIGTERM)
        wait_gone(pid)
        broker_pid.unlink(missing_ok=True)
    if (root / "pgdata/postmaster.pid").exists():
        pid = int((root / "pgdata/postmaster.pid").read_text().splitlines()[0])
        require_owned_process(root, pid, "PostgreSQL")
        run([str(POSTGRES_BIN / "pg_ctl"), "-D", str(root / "pgdata"), "stop", "-m", "fast"])
        wait_gone(pid)
    occupied = {port: listening_pids(port) for port in (MQTT_PORT, MQTT_WS_PORT, POSTGRES_PORT)}
    if any(occupied.values()):
        raise RuntimeError(f"isolated E2E teardown left listeners; preserving root: {occupied}")


def validated_browser_endpoint(root: Path) -> str:
    contract = load_contract(root)
    pid_file = root / "run/chrome.pid"
    if not pid_file.exists():
        raise RuntimeError("isolated Chrome has no owned PID")
    pid = int(pid_file.read_text().strip())
    require_owned_process(root, pid, "Chrome")
    if listening_pids(BROWSER_DEBUG_PORT) != {pid}:
        raise RuntimeError("isolated Chrome CDP listener is not owned by its reviewed process")
    with urllib.request.urlopen(
            f"http://127.0.0.1:{BROWSER_DEBUG_PORT}/json/version", timeout=2.0) as response:
        version = json.load(response)
    expected_version = str(contract["browser_version"]).split()[-1]
    if version.get("Browser") != f"Chrome/{expected_version}":
        raise RuntimeError("isolated Chrome CDP product identity changed")
    endpoint = version.get("webSocketDebuggerUrl")
    if not isinstance(endpoint, str) or re.fullmatch(
            rf"ws://127\.0\.0\.1:{BROWSER_DEBUG_PORT}/devtools/browser/[0-9a-f-]+",
            endpoint,
    ) is None:
        raise RuntimeError("isolated Chrome returned an unreviewed CDP endpoint")
    return endpoint


def browser_cdp_command(
        root: Path, method: str, params: dict[str, object]
) -> dict[str, object]:
    """Send one bounded command to the isolated browser-level CDP endpoint."""
    import websocket

    connection = websocket.create_connection(
        validated_browser_endpoint(root),
        origin=f"http://127.0.0.1:{BROWSER_DEBUG_PORT}",
        timeout=5.0,
    )
    try:
        connection.send(json.dumps({"id": 1, "method": method, "params": params}))
        reply = json.loads(connection.recv())
    finally:
        connection.close()
    if reply.get("id") != 1 or "error" in reply:
        raise RuntimeError(f"isolated Chrome CDP command failed: {reply}")
    result = reply.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("isolated Chrome CDP command returned no result")
    return result


def browser_targets(root: Path) -> list[dict[str, object]]:
    result = browser_cdp_command(root, "Target.getTargets", {})
    targets = result.get("targetInfos")
    if not isinstance(targets, list):
        raise RuntimeError("isolated Chrome target inventory is missing")
    return [target for target in targets if isinstance(target, dict)]


def load_browser_extension(root: Path) -> str:
    """Load the reviewed unpacked candidate after Chrome removed the CLI switch."""
    extension = (root / "browser-extension").resolve()
    if extension.parent != root or not extension.is_dir():
        raise RuntimeError("isolated browser extension escaped the nonce root")
    result = browser_cdp_command(root, "Extensions.loadUnpacked", {"path": str(extension)})
    extension_id = result.get("id")
    if not isinstance(extension_id, str) or not re.fullmatch(r"[a-p]{32}", extension_id):
        raise RuntimeError("isolated Chrome did not return a valid extension identity")
    return extension_id


def stop_browser(root: Path) -> None:
    browser_pid_file = root / "run/chrome.pid"
    if not browser_pid_file.exists():
        return
    pid = int(browser_pid_file.read_text().strip())
    if process_command(pid):
        require_owned_process(root, pid, "Chrome")
        os.killpg(pid, signal.SIGTERM)
        wait_gone(pid, 10.0)
    browser_pid_file.unlink(missing_ok=True)
    profile_arg = f"--user-data-dir={root / 'chrome-profile'}"
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        processes = subprocess.run(
            ["ps", "-axo", "pid=,command="], capture_output=True, text=True, check=True
        ).stdout.splitlines()
        if not any(profile_arg in process for process in processes):
            return
        time.sleep(0.1)
    raise RuntimeError("isolated Chrome left a helper outside its owned process lifetime")


def start_browser(root: Path) -> None:
    try:
        start_browser_owned(root)
    except Exception:
        stop_browser(root)
        raise


def create_and_activate_reviewed_target(root: Path) -> str:
    created = browser_cdp_command(
        root, "Target.createTarget", {"url": EASTMONEY_REVIEWED_URL}
    )
    target_id = created.get("targetId") if isinstance(created, dict) else None
    if not isinstance(target_id, str) or re.fullmatch(r"[0-9A-Fa-f]{32}", target_id) is None:
        raise RuntimeError("isolated reviewed page lacks one exact target identity")
    browser_cdp_command(root, "Target.activateTarget", {"targetId": target_id})
    return target_id


def start_browser_owned(root: Path) -> None:
    contract = load_contract(root)
    if not CHROME.is_file() or not os.access(CHROME, os.X_OK):
        raise RuntimeError("reviewed Chrome for Testing binary is unavailable")
    if (root / "run/chrome.pid").exists():
        raise RuntimeError("isolated Chrome PID already exists")
    occupied = listening_pids(BROWSER_DEBUG_PORT)
    if occupied:
        raise RuntimeError(f"isolated Chrome CDP port is already owned: {occupied}")
    process = subprocess.Popen([
        str(CHROME),
        f"--user-data-dir={contract['chrome_profile']}",
        "--remote-debugging-address=127.0.0.1",
        f"--remote-debugging-port={BROWSER_DEBUG_PORT}",
        f"--remote-allow-origins=http://127.0.0.1:{BROWSER_DEBUG_PORT}",
        "--enable-unsafe-extension-debugging",
        "--disable-breakpad",
        "--disable-crash-reporter",
        f"--crash-dumps-dir={root / 'logs/crash'}",
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
    ], stdout=(root / "logs/chrome.log").open("ab"), stderr=subprocess.STDOUT,
       start_new_session=True)
    (root / "run/chrome.pid").write_text(f"{process.pid}\n", encoding="utf-8")
    time.sleep(1.0)
    if process.poll() is not None:
        raise RuntimeError("isolated Chrome exited during startup")
    require_owned_process(root, process.pid, "Chrome")
    deadline = time.monotonic() + 10.0
    while True:
        try:
            targets = browser_targets(root)
            if any(target.get("type") == "page" for target in targets):
                break
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("isolated Chrome CDP or reviewed page did not become ready")
        time.sleep(0.2)
    extension_id = load_browser_extension(root)
    write_private(root / "state/browser-extension-id", extension_id + "\n")
    create_and_activate_reviewed_target(root)
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        targets = browser_targets(root)
        reviewed_pages = [target for target in targets
                          if target.get("url") == EASTMONEY_REVIEWED_URL
                          and target.get("type") == "page"]
        extension_workers = [target for target in targets
                             if target.get("url") ==
                             f"chrome-extension://{extension_id}/src/background.js"
                             and target.get("type") == "service_worker"]
        extension_targets = [target for target in targets
                             if str(target.get("url", "")).startswith(
                                 f"chrome-extension://{extension_id}/")]
        if (len(reviewed_pages) == 1 and len(extension_workers) == 1
                and extension_targets == extension_workers):
            return
        time.sleep(0.2)
    raise RuntimeError("isolated Chrome did not expose exactly one reviewed page and extension worker")


def start_shadow(root: Path) -> None:
    load_contract(root)
    if (root / "run/chrome.pid").exists():
        raise RuntimeError("isolated shadow must subscribe before the candidate browser starts")
    pg_password = (root / "secrets/postgres.password").read_text().strip()
    count = subprocess.run([
        str(POSTGRES_BIN / "psql"), "-h", "127.0.0.1", "-p", str(POSTGRES_PORT),
        "-U", "gridedge_e2e", "-d", "gridedge_market_e2e", "-Atc",
        "SELECT count(*) FROM market_events",
    ], env={**os.environ, "PGPASSWORD": pg_password}, capture_output=True, text=True,
       check=True).stdout.strip()
    if count != "0":
        raise RuntimeError("isolated shadow requires an empty committed-event database")
    pid_file = root / "run/shadow.pid"
    if pid_file.exists():
        raise RuntimeError("isolated shadow PID already exists")
    binary = root / "bin/gridedge_market_e2e_shadow"
    expected_sha = (root / "shadow-binary.sha256").read_text().strip()
    if hashlib.sha256(binary.read_bytes()).hexdigest() != expected_sha:
        raise RuntimeError("isolated shadow binary identity changed")
    process = subprocess.Popen([
        sys.executable, str(root / "bin/shadow_runner.py"), str(root),
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    pid_file.write_text(f"{process.pid}\n", encoding="utf-8")
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError("isolated shadow exited during startup")
    require_owned_process(root, process.pid, "shadow worker")


def status(root: Path) -> None:
    contract = load_contract(root)
    contract["formal_topic_present_in_acl"] = "gridedge/market/v1" in (root / "acl").read_text()
    contract["formal_host_present"] = "192.168.1.201" in "\n".join(
        path.read_text(errors="ignore") for path in root.glob("*.json"))
    result_path = root / "state/shadow-result.json"
    contract["shadow_result"] = (json.loads(result_path.read_text())
                                 if result_path.exists() else None)
    print(json.dumps(contract, sort_keys=True))


def shadow_result_succeeded(root: Path) -> bool:
    result_path = root / "state/shadow-result.json"
    if not result_path.exists():
        return False
    result = json.loads(result_path.read_text())
    output = result.get("output")
    return (result.get("exit_code") == 0 and isinstance(output, dict)
            and output.get("money_actions_enabled") is False
            and output.get("shadow_market_path_only") is True
            and output.get("strategy_evaluated") is False
            and isinstance(output.get("ledger_head_sha256"), str)
            and len(output["ledger_head_sha256"]) == 64)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=(
        "freeze-bundle", "prepare", "start", "start-browser", "start-shadow", "status",
        "stop", "teardown"))
    parser.add_argument("--root")
    parser.add_argument("--nonce")
    parser.add_argument("--bundle")
    args = parser.parse_args()
    if args.command == "freeze-bundle":
        identity = freeze_candidate_bundle(validated_bundle_root(args.bundle or ""))
        print(json.dumps(identity, sort_keys=True))
        return 0
    root = validated_root(args.root or "")
    if args.command == "prepare":
        prepare(root, validated_nonce(args.nonce or ""),
                validated_bundle_root(args.bundle or ""))
    elif args.command == "start":
        start(root)
    elif args.command == "start-browser":
        start_browser(root)
    elif args.command == "start-shadow":
        start_shadow(root)
    elif args.command == "status":
        status(root)
    elif args.command == "stop":
        stop(root)
    else:
        stop(root)
        shutil.rmtree(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
