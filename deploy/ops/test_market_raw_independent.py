"""Independent raw-repair review using temporary files only.

Nothing in this module opens the production raw log, broker, PostgreSQL, worker,
guard, browser or Android emulator.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from deploy.market_data.ingestor.test_market_ingestor import event, market_ingestor
from deploy.ops import market_raw_repair as subject
from deploy.ops import publish_session_supervisor as publisher
from deploy.ops import session_supervisor as supervisor


CUTOFF_US = 1_787_103_010_000_000


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def fixture():
    rows = []
    lines = []
    for sequence in range(1, 6):
        document = json.loads(event(
            source_id=subject.SOURCE,
            source_sequence=sequence,
            price=3530 + sequence,
        ))
        document["source"]["source_instance_id"] = subject.INSTANCE
        document["event_id"] = market_ingestor.canonical_event_identity(document).hex()
        payload = market_ingestor.canonical_json(document)
        topic = "gridedge/market/v1/XSHE/002256/trade"
        rows.append({
            "topic": topic,
            "payload_hex": payload.hex(),
            "event_id": document["event_id"],
            "payload_sha256": digest(payload),
            "source_sequence": sequence,
            "qos": 1,
        })
        # Deliberately retain non-canonical envelope whitespace. Known local
        # bytes are evidence and must not be normalized by repair.
        lines.append(json.dumps({"topic": topic, "payload": list(payload)}).encode() + b"\n")
    return rows, lines


def one_day_replay_evidence(candidate: bytes):
    first = json.loads(bytes(json.loads(candidate.splitlines()[0])["payload"]))
    day = datetime.fromtimestamp(first["ts_us"] / 1_000_000, subject.TZ).date().isoformat()
    return [{
        "day": day,
        "input_sha256": digest(candidate),
        "output_sha256": "1" * 64,
        "replay_binary_sha256": "2" * 64,
        "exit_code": 0,
    }]


class PrepareIndependentContract(unittest.TestCase):
    def test_exact_duplicates_and_noncanonical_envelope_bytes_are_preserved(self):
        rows, lines = fixture()
        original = lines[0] + lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        self.assertTrue(candidate.startswith(lines[0] + lines[0]))
        self.assertTrue(candidate.endswith(lines[4]))
        self.assertEqual(candidate.count(lines[0]), 2)
        self.assertEqual(audit["added_sequences"], [2, 3, 4])

    def test_nonadjacent_exact_old_duplicate_is_preserved_but_changed_old_sequence_is_rejected(self):
        rows, lines = fixture()
        original = lines[0] + lines[4] + lines[0]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        self.assertTrue(candidate.endswith(lines[4] + lines[0]))
        self.assertEqual(candidate.count(lines[0]), 2)
        self.assertEqual(audit["added_sequences"], [2, 3, 4])

        changed_document = json.loads(event(
            source_id=subject.SOURCE,
            source_sequence=1,
            price=9999,
        ))
        changed_document["source"]["source_instance_id"] = subject.INSTANCE
        changed_document["event_id"] = market_ingestor.canonical_event_identity(
            changed_document
        ).hex()
        changed_payload = market_ingestor.canonical_json(changed_document)
        changed_line = json.dumps({
            "topic": rows[0]["topic"],
            "payload": list(changed_payload),
        }).encode() + b"\n"
        with self.assertRaisesRegex(ValueError, "conflicting local duplicate"):
            subject.prepare(lines[0] + lines[4] + changed_line, rows, CUTOFF_US)

    def test_every_export_identity_dimension_is_enforced(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        mutations = []
        for key, value in [
            ("topic", "gridedge/market/v1/XSHE/002256/status"),
            ("event_id", "0" * 64),
            ("payload_sha256", "0" * 64),
            ("source_sequence", 99),
            ("qos", 0),
        ]:
            changed = [dict(row) for row in rows]
            changed[2][key] = value
            mutations.append(changed)
        mutations.extend([
            rows[:2] + rows[3:],
            [rows[0], rows[2], rows[1], rows[3], rows[4]],
            [rows[0], rows[1], rows[1], rows[2], rows[3], rows[4]],
        ])
        for changed in mutations:
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    subject.prepare(original, changed, CUTOFF_US)


class PublishIndependentContract(unittest.TestCase):
    def test_semantic_replay_evidence_must_be_bound_to_exact_candidate(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        audit["semantic_replays"] = [{
            "day": "2099-01-01",
            "input_sha256": "0" * 64,
            "output_sha256": "1" * 64,
        }]
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-publish-") as temporary:
            path = Path(temporary) / "market.jsonl"
            path.write_bytes(original)
            with self.assertRaises(ValueError):
                subject.publish(path, original, candidate, audit, lambda: None)
            self.assertEqual(path.read_bytes(), original)

    def test_backup_directory_entry_is_durable_before_atomic_replace(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        audit["semantic_replays"] = one_day_replay_evidence(candidate)
        events = []
        real_replace = subject.os.replace

        def replace(source, destination):
            events.append("replace")
            return real_replace(source, destination)

        def fsync_directory(_path):
            events.append("directory-fsync")

        with tempfile.TemporaryDirectory(prefix="gridedge-raw-publish-") as temporary:
            path = Path(temporary) / "market.jsonl"
            path.write_bytes(original)
            with patch.object(subject.os, "replace", side_effect=replace), patch.object(
                subject, "fsync_directory", side_effect=fsync_directory
            ):
                subject.publish(path, original, candidate, audit, lambda: None)
            self.assertEqual(path.read_bytes(), candidate)
        self.assertIn("replace", events)
        self.assertIn("directory-fsync", events)
        self.assertLess(events.index("directory-fsync"), events.index("replace"))

    def test_second_quiescence_fence_prevents_replace_after_concurrent_change(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        audit["semantic_replays"] = one_day_replay_evidence(candidate)
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-publish-") as temporary:
            path = Path(temporary) / "market.jsonl"
            path.write_bytes(original)
            calls = {"count": 0}

            def quiescence():
                calls["count"] += 1
                if calls["count"] == 2:
                    path.write_bytes(original + lines[0])

            with self.assertRaisesRegex(ValueError, "publication fence"):
                subject.publish(path, original, candidate, audit, quiescence)
            self.assertEqual(path.read_bytes(), original + lines[0])

    def test_preexisting_backup_symlink_is_not_accepted_as_immutable_evidence(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        audit["semantic_replays"] = one_day_replay_evidence(candidate)
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-publish-") as temporary:
            root = Path(temporary)
            path = root / "market.jsonl"
            path.write_bytes(original)
            redirected = root / "redirected-evidence"
            redirected.write_bytes(original)
            backup = path.with_name(path.name + ".before-repair-" + digest(original))
            backup.symlink_to(redirected)
            with self.assertRaises(ValueError):
                subject.publish(path, original, candidate, audit, lambda: None)
            self.assertTrue(backup.is_symlink())
            self.assertEqual(path.read_bytes(), original)

    def test_retry_accepts_only_the_same_existing_regular_backup(self):
        rows, lines = fixture()
        original = lines[0] + lines[4]
        candidate, audit = subject.prepare(original, rows, CUTOFF_US)
        audit["semantic_replays"] = one_day_replay_evidence(candidate)
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-publish-") as temporary:
            path = Path(temporary) / "market.jsonl"
            path.write_bytes(original)
            backup = subject.publish(path, original, candidate, audit, lambda: None)
            self.assertFalse(backup.is_symlink())
            self.assertEqual(backup.read_bytes(), original)
            path.write_bytes(original)
            same_backup = subject.publish(path, original, candidate, audit, lambda: None)
            self.assertEqual(same_backup, backup)
            self.assertEqual(path.read_bytes(), candidate)


class CoordinatorIndependentContract(unittest.TestCase):
    def test_running_worker_blocks_before_guard_signal_or_raw_access(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-coordinator-") as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            manifest = {"root": str(root)}

            def pids(pattern, exact=False):
                del exact
                return [6161] if "gridedge_ths_live" in pattern else []

            with patch.object(supervisor, "pids", side_effect=pids), patch.object(
                supervisor, "load_manifest", return_value=manifest
            ), patch.object(supervisor.os, "kill") as kill, patch.object(
                supervisor, "raw_module"
            ) as raw_module:
                with self.assertRaisesRegex(ValueError, "cannot displace a running worker"):
                    supervisor.repair_raw(manifest, "manifest", "frozen")
            kill.assert_not_called()
            raw_module.assert_not_called()
            self.assertFalse((root / "runtime/ths-deployment-coordination.lock").exists())
            self.assertFalse((root / "runtime/ths-deployment-maintenance").exists())

    def test_replay_binary_must_equal_the_frozen_manifest_before_publish(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-raw-coordinator-") as temporary:
            root = Path(temporary)
            runtime = root / "runtime"
            runtime.mkdir()
            raw_path = runtime / "002256-opening-v1-market-mqtt.jsonl"
            original = b"isolated-original\n"
            candidate = b"isolated-candidate\n"
            raw_path.write_bytes(original)
            replay = root / "bin/gridedge_market_replay"
            replay.parent.mkdir()
            replay.write_bytes(b"reviewed replay binary")
            manifest = {
                "root": str(root),
                "files": {str(replay): "a" * 64},
            }
            killed = {"value": False}
            published = {"value": False}

            def pids(pattern, exact=False):
                del exact
                if "gridedge_ths_live" in pattern:
                    return []
                if "run_ths_trusted_session_guard" in pattern:
                    return [] if killed["value"] else [4242]
                return []

            def kill(pid, signal):
                self.assertEqual(pid, 4242)
                self.assertEqual(signal, supervisor.signal.SIGTERM)
                killed["value"] = True

            fake_raw = SimpleNamespace(
                local_records=lambda _original, _cutoff: [(1,), (3,)],
                missing_intervals=lambda _original, _cutoff: [(2, 2)],
                committed_rows=lambda _low, _high: ["committed"],
                prepare=lambda _original, _rows, _cutoff: (
                    candidate,
                    {
                        "original_sha256": digest(original),
                        "candidate_sha256": digest(candidate),
                    },
                ),
                semantic_replay=lambda _candidate, _directory, _binary: [{
                    "day": "2026-09-04",
                    "input_sha256": digest(candidate),
                    "output_sha256": "1" * 64,
                    "replay_binary_sha256": "b" * 64,
                    "exit_code": 0,
                }],
                fsync_directory=lambda _directory: None,
            )

            def publish(*_args, **_kwargs):
                published["value"] = True
                return runtime / "backup"

            fake_raw.publish = publish
            state = {"identity": True, "unlocked": True, "breaker_open": False}
            with patch.object(supervisor, "pids", side_effect=pids), patch.object(
                supervisor, "command", return_value=SimpleNamespace(
                    returncode=0, stdout=b"reviewed guard fingerprint", stderr=b""
                )
            ), patch.object(supervisor.os, "kill", side_effect=kill), patch.object(
                supervisor, "load_manifest", return_value=manifest
            ), patch.object(supervisor, "snapshot", return_value=(state, {})), patch.object(
                supervisor, "raw_module", return_value=fake_raw
            ):
                with self.assertRaisesRegex(ValueError, "replay binary"):
                    supervisor.repair_raw(manifest, "manifest", "frozen")
            self.assertFalse(published["value"])
            self.assertFalse((runtime / "ths-deployment-coordination.lock").exists())
            self.assertFalse((runtime / "ths-deployment-maintenance").exists())


class PublisherIndependentContract(unittest.TestCase):
    @staticmethod
    def installation_fixture(temporary):
        root = Path(temporary) / "installed"
        (root / "runtime").mkdir(parents=True)
        (root / "bin").mkdir()
        (root / "config").mkdir()
        stage = Path(temporary) / "gridedge-supervisor-stage-fixture"
        stage.mkdir()
        for name in publisher.NAMES:
            (stage / name).write_bytes(("reviewed-" + name).encode())
        expected = {name: digest((stage / name).read_bytes()) for name in publisher.NAMES}
        return root, stage, expected

    def test_copy_time_stage_mutation_is_rejected_before_installation(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-publisher-") as temporary:
            root, stage, expected = self.installation_fixture(temporary)
            args = SimpleNamespace(stage=str(stage), stage_sha256="frozen-index")
            real_copy = publisher.shutil.copyfile

            def copy(source, destination):
                if Path(source).name == "gridedge_market_replay":
                    Path(destination).write_bytes(b"mutated-after-final-validation")
                    return destination
                return real_copy(source, destination)

            process = SimpleNamespace(returncode=1, stdout=b"", stderr=b"")
            with patch.object(publisher, "ROOT", root), patch.object(
                publisher.argparse.ArgumentParser, "parse_args", return_value=args
            ), patch.object(publisher, "validate", return_value=expected), patch.object(
                publisher.shutil, "copyfile", side_effect=copy
            ), patch.object(publisher.subprocess, "run", return_value=process), patch(
                "builtins.print"
            ):
                with self.assertRaisesRegex(ValueError, "staged bytes changed"):
                    publisher.main()
            self.assertFalse((root / "bin/gridedge_market_replay").exists())
            self.assertFalse((root / "runtime/ths-deployment-coordination.lock").exists())

    def test_late_validation_failure_rolls_back_every_installed_byte(self):
        with tempfile.TemporaryDirectory(prefix="gridedge-publisher-") as temporary:
            root, stage, expected = self.installation_fixture(temporary)
            old = {}
            for name in publisher.NAMES:
                target = root / ("config" if name == "session-supervisor-manifest.json" else "bin") / name
                value = ("old-" + name).encode()
                target.write_bytes(value)
                old[target] = value
            args = SimpleNamespace(stage=str(stage), stage_sha256="frozen-index")

            def run(argv, **_kwargs):
                if argv[0] == "/usr/bin/pgrep":
                    return SimpleNamespace(returncode=1, stdout=b"", stderr=b"")
                raise publisher.subprocess.CalledProcessError(1, argv)

            with patch.object(publisher, "ROOT", root), patch.object(
                publisher.argparse.ArgumentParser, "parse_args", return_value=args
            ), patch.object(publisher, "validate", return_value=expected), patch.object(
                publisher.subprocess, "run", side_effect=run
            ), patch("builtins.print"):
                with self.assertRaises(publisher.subprocess.CalledProcessError):
                    publisher.main()
            for target, value in old.items():
                self.assertEqual(target.read_bytes(), value)
            self.assertFalse((root / "runtime/ths-deployment-coordination.lock").exists())


if __name__ == "__main__":
    unittest.main()
