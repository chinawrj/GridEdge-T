"""Independent real-filesystem migration primitives; temporary roots only."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch


class Crash(RuntimeError):
    pass


def sha(value):
    return hashlib.sha256(value).hexdigest()


class CoreMigrationFilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="gridedge-real-fs-test-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "installation"
        (self.root / "runtime").mkdir(parents=True)
        self.index = sha(b"reviewed frozen stage")
        self.owner = {"pid": 41001, "birth": "fixture-original-birth", "executable_sha256": sha(b"reviewed owner"), "executable_inode": 77}
        self.baseline = {"head": 2448, "source_database_instance_id": "fixture-database-instance",
                         "effective": sha(b"old core"), "binding_revision": 15, "binding_sha256": sha(b"old binding"),
                         "breaker": sha(b"original breaker")}
        self.coordination = self.root / "runtime/ths-deployment-coordination.lock"
        self.maintenance = self.root / "runtime/ths-deployment-maintenance"
        spec = importlib.util.spec_from_file_location("core_migration_files_under_test", Path(__file__).with_name("core_migration_files.py"))
        self.subject = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.subject
        spec.loader.exec_module(self.subject)

    def test_atomic_write_replaces_only_target_with_exact_bytes_and_mode(self):
        target = self.root / "artifact"
        target.write_bytes(b"old")
        sibling = self.root / "unrelated"
        sibling.write_bytes(b"keep")
        self.subject.atomic_write(target, b"exact frozen replacement", mode=0o400)
        self.assertEqual(target.read_bytes(), b"exact frozen replacement")
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o400)
        self.assertEqual(sibling.read_bytes(), b"keep")

    def test_atomic_write_crash_at_each_durable_boundary_has_no_partial_target(self):
        labels = ("before_file_fsync", "after_file_fsync", "before_rename", "after_rename",
                  "before_dir_fsync", "after_dir_fsync")
        for index, label in enumerate(labels):
            with self.subTest(label=label):
                target = self.root / ("artifact-" + str(index))
                target.write_bytes(b"old complete artifact")
                reached = []
                def checkpoint(actual):
                    if actual == label:
                        reached.append(actual)
                        raise Crash(label)
                with self.assertRaises(Crash):
                    self.subject.atomic_write(target, b"new complete artifact", checkpoint=checkpoint)
                self.assertEqual(reached, [label], "checkpoint was not exercised")
                expected = b"old complete artifact" if index < 3 else b"new complete artifact"
                self.assertEqual(target.read_bytes(), expected)
                self.subject.atomic_write(target, b"new complete artifact")
                self.assertEqual(target.read_bytes(), b"new complete artifact")

    def test_atomic_write_really_syncs_file_before_rename_and_directory_after(self):
        target = self.root / "ordered-artifact"
        target.write_bytes(b"old")
        events = []
        original_sync = os.fsync
        def fsync(fd):
            events.append("dirsync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "filesync")
            return original_sync(fd)
        def checkpoint(label):
            events.append(label)
        with patch("os.fsync", side_effect=fsync):
            self.subject.atomic_write(target, b"new", checkpoint=checkpoint)
        self.assertLess(events.index("filesync"), events.index("before_rename"))
        self.assertLess(events.index("after_rename"), events.index("dirsync"))

    def test_symlink_target_and_parent_do_not_redirect_writes(self):
        outside = self.root / "untouched"
        outside.write_bytes(b"untouched")
        link = self.root / "redirect"
        link.symlink_to(outside)
        with self.assertRaises((ValueError, OSError)):
            self.subject.atomic_write(link, b"forbidden")
        self.assertEqual(outside.read_bytes(), b"untouched")
        actual_dir = self.root / "actual"
        actual_dir.mkdir()
        parent_link = self.root / "redirect-directory"
        parent_link.symlink_to(actual_dir, target_is_directory=True)
        with self.assertRaises((ValueError, OSError)):
            self.subject.atomic_write(parent_link / "new", b"forbidden")
        self.assertFalse((actual_dir / "new").exists())

    def reserve_path(self):
        return self.root / "runtime" / ("core-migration-" + self.index) / "reserve.json"

    def test_reserve_is_create_once_and_cannot_rebaseline(self):
        self.subject.reserve(self.root, self.index, self.baseline, self.owner)
        original = self.reserve_path().read_bytes()
        self.subject.reserve(self.root, self.index, self.baseline, self.owner)
        self.assertEqual(self.reserve_path().read_bytes(), original)
        for key, value in self.baseline.items():
            with self.subTest(key=key):
                changed = dict(self.baseline, **{key: value + 1 if isinstance(value, int) else "different"})
                with self.assertRaises((ValueError, OSError)):
                    self.subject.reserve(self.root, self.index, changed, self.owner)
                self.assertEqual(self.reserve_path().read_bytes(), original)

    def test_reserve_corruption_or_symlink_is_not_overwritten(self):
        self.subject.reserve(self.root, self.index, self.baseline, self.owner)
        target = self.reserve_path()
        target.chmod(0o600)
        target.write_bytes(b"corrupt reservation")
        with self.assertRaises((ValueError, OSError)):
            self.subject.reserve(self.root, self.index, self.baseline, self.owner)
        self.assertEqual(target.read_bytes(), b"corrupt reservation")

    def test_fence_retains_maintenance_until_explicit_finish(self):
        fence = self.subject.OwnedFence(self.root, self.index, self.owner, lambda _: "ABSENT")
        with fence:
            self.assertTrue(self.coordination.is_dir())
            self.assertTrue(self.maintenance.is_dir())
            self.assertTrue((self.coordination / "owner.json").is_file())
            self.assertTrue((self.maintenance / "owner.json").is_file())
        self.assertTrue(self.maintenance.is_dir(), "implicit successful context exit cleared maintenance")

    def test_explicit_finish_releases_only_own_two_fences(self):
        unrelated = self.root / "runtime/unrelated"
        unrelated.mkdir()
        fence = self.subject.OwnedFence(self.root, self.index, self.owner, lambda _: "ABSENT")
        with fence:
            fence.finish()
        self.assertFalse(self.coordination.exists())
        self.assertFalse(self.maintenance.exists())
        self.assertTrue(unrelated.exists())

    def test_missing_or_corrupt_owner_is_never_removed(self):
        for index, contents in enumerate((None, b"broken")):
            with self.subTest(contents=contents):
                root = self.root / ("bad-owner-" + str(index))
                coordination = root / "runtime/ths-deployment-coordination.lock"
                coordination.mkdir(parents=True)
                if contents is not None:
                    (coordination / "owner.json").write_bytes(contents)
                with self.assertRaises((ValueError, OSError, RuntimeError)):
                    with self.subject.OwnedFence(root, self.index, self.owner, lambda _: "ABSENT"):
                        self.fail("missing owner metadata was accepted")
                self.assertTrue(coordination.is_dir())
                if contents is not None:
                    self.assertEqual((coordination / "owner.json").read_bytes(), contents)

    def test_existing_live_or_unknown_owner_cannot_be_reclaimed(self):
        for index, alive in enumerate(("MATCH", "MISMATCH", "UNKNOWN")):
            with self.subTest(alive=alive):
                root = self.root / ("live-owner-" + str(index))
                (root / "runtime").mkdir(parents=True)
                first = self.subject.OwnedFence(root, self.index, self.owner, lambda _: "ABSENT")
                with first:
                    pass
                observed = []
                def owner_state(owner):
                    observed.append(owner)
                    return alive
                second = self.subject.OwnedFence(root, self.index, dict(self.owner, birth="different-birth"), owner_state)
                before = (root / "runtime/ths-deployment-coordination.lock/owner.json").read_bytes()
                with self.assertRaises((ValueError, OSError, RuntimeError)):
                    with second:
                        self.fail("live/unknown original identity was reclaimed")
                self.assertEqual(observed, [self.owner], "owner evidence was not checked")
                self.assertEqual((root / "runtime/ths-deployment-coordination.lock/owner.json").read_bytes(), before)

    def test_foreign_stage_lock_is_retained_even_if_owner_is_dead(self):
        foreign = sha(b"foreign transaction")
        first = self.subject.OwnedFence(self.root, foreign, self.owner, lambda _: "ABSENT")
        with first:
            pass
        before = (self.coordination / "owner.json").read_bytes()
        with self.assertRaises((ValueError, OSError, RuntimeError)):
            with self.subject.OwnedFence(self.root, self.index, dict(self.owner, pid=41002), lambda _: "ABSENT"):
                self.fail("foreign stage lock was stolen")
        self.assertEqual((self.coordination / "owner.json").read_bytes(), before)
        self.assertTrue(self.maintenance.is_dir())

    def test_reserve_crashes_preserve_original_evidence_and_retry_once(self):
        for number, label in enumerate(("reserve.before_publish", "reserve.after_publish",
                                        "reserve.before_dir_fsync", "reserve.after_dir_fsync")):
            with self.subTest(label=label):
                root = self.root / ("reserve-crash-" + str(number))
                (root / "runtime").mkdir(parents=True)
                reached = []
                def checkpoint(actual):
                    if actual == label:
                        reached.append(actual)
                        raise Crash(label)
                with self.assertRaises(Crash):
                    self.subject.reserve(root, self.index, self.baseline, self.owner, checkpoint=checkpoint)
                self.assertEqual(reached, [label])
                path = root / "runtime" / ("core-migration-" + self.index) / "reserve.json"
                original = path.read_bytes() if path.exists() else None
                if original is not None:
                    self.assertEqual(json.loads(original)["baseline"], self.baseline)
                self.subject.reserve(root, self.index, self.baseline, dict(self.owner, pid=41002))
                self.assertEqual(json.loads(path.read_bytes())["baseline"], self.baseline)
                if original is not None:
                    self.assertEqual(path.read_bytes(), original, "retry overwrote original owner/evidence")

    def test_fence_publication_crashes_never_expose_ownerless_claim(self):
        labels = [prefix + "." + suffix for prefix in ("coordination", "maintenance")
                  for suffix in ("before_publish", "after_publish", "before_dir_fsync", "after_dir_fsync")]
        for number, label in enumerate(labels):
            with self.subTest(label=label):
                root = self.root / ("fence-crash-" + str(number))
                (root / "runtime").mkdir(parents=True)
                reached = []
                def checkpoint(actual):
                    if actual == label:
                        reached.append(actual)
                        raise Crash(label)
                with self.assertRaises(Crash):
                    with self.subject.OwnedFence(root, self.index, self.owner, lambda _: "ABSENT", checkpoint=checkpoint):
                        self.fail("checkpoint did not interrupt publication")
                self.assertEqual(reached, [label])
                for name in ("ths-deployment-coordination.lock", "ths-deployment-maintenance"):
                    path = root / "runtime" / name
                    if path.exists():
                        self.assertIsInstance(json.loads((path / "owner.json").read_bytes()), dict)
                with self.subject.OwnedFence(root, self.index, dict(self.owner, pid=41002), lambda _: "ABSENT") as resumed:
                    resumed.finish()
                self.assertFalse((root / "runtime/ths-deployment-coordination.lock").exists())
                self.assertFalse((root / "runtime/ths-deployment-maintenance").exists())

    def test_finish_rename_crashes_are_recoverable_without_deleting_evidence(self):
        labels = ["finish." + name + "." + suffix for name in ("maintenance", "coordination")
                  for suffix in ("before_rename", "after_rename")]
        for number, label in enumerate(labels):
            with self.subTest(label=label):
                root = self.root / ("finish-crash-" + str(number))
                (root / "runtime").mkdir(parents=True)
                reached = []
                def checkpoint(actual):
                    if actual == label:
                        reached.append(actual)
                        raise Crash(label)
                with self.assertRaises(Crash):
                    with self.subject.OwnedFence(root, self.index, self.owner, lambda _: "ABSENT", checkpoint=checkpoint) as first:
                        first.finish()
                self.assertEqual(reached, [label])
                evidence_before = {p: p.read_bytes() for p in (root / "runtime").rglob("owner.json")
                                   if p.parent.name not in ("ths-deployment-coordination.lock", "ths-deployment-maintenance")}
                with self.subject.OwnedFence(root, self.index, dict(self.owner, pid=41002), lambda _: "ABSENT") as resumed:
                    resumed.finish()
                for path, content in evidence_before.items():
                    self.assertEqual(path.read_bytes(), content)
                self.assertFalse((root / "runtime/ths-deployment-coordination.lock").exists())
                self.assertFalse((root / "runtime/ths-deployment-maintenance").exists())

    def test_existing_reserve_retry_syncs_publication_directory_before_success(self):
        def crash(label):
            if label == "reserve.after_publish":
                raise Crash(label)
        with self.assertRaises(Crash):
            self.subject.reserve(self.root, self.index, self.baseline, self.owner, checkpoint=crash)
        original = self.reserve_path().read_bytes()
        runtime_inode = (self.root / "runtime").stat().st_ino
        synced = []
        original_sync = os.fsync
        def fsync(fd):
            item = os.fstat(fd)
            if stat.S_ISDIR(item.st_mode):
                synced.append(item.st_ino)
            return original_sync(fd)
        with patch("os.fsync", side_effect=fsync):
            self.subject.reserve(self.root, self.index, self.baseline, dict(self.owner, pid=41002))
        self.assertEqual(self.reserve_path().read_bytes(), original)
        self.assertIn(runtime_inode, synced, "existing reserve was returned without making its parent rename durable")

    def test_atomic_write_rejects_temporary_inode_replacement_at_publish_boundary(self):
        target = self.root / "artifact-replaced-temp"
        target.write_bytes(b"reviewed old artifact")
        reached = []
        def replace_temporary(label):
            if label == "before_rename":
                temporary, = self.root.glob(".artifact-replaced-temp.migration-*")
                temporary.rename(self.root / "original-staged-inode")
                temporary.write_bytes(b"unreviewed substituted artifact")
                reached.append(True)
        with self.assertRaises((ValueError, OSError)):
            self.subject.atomic_write(target, b"reviewed new artifact", checkpoint=replace_temporary)
        self.assertEqual(reached, [True])
        self.assertEqual(target.read_bytes(), b"reviewed old artifact")

    def test_atomic_write_parent_replacement_cannot_redirect_publication(self):
        parent = self.root / "publish-parent"
        parent.mkdir()
        target = parent / "artifact"
        target.write_bytes(b"reviewed old artifact")
        held = self.root / "original-parent-inode"
        reached = []
        def replace_parent(label):
            if label == "before_rename":
                temporary, = parent.glob(".artifact.migration-*")
                parent.rename(held)
                parent.mkdir()
                (parent / temporary.name).write_bytes(b"substituted staging bytes")
                target.write_bytes(b"unrelated replacement directory target")
                reached.append(True)
        with self.assertRaises((ValueError, OSError)):
            self.subject.atomic_write(target, b"reviewed new artifact", checkpoint=replace_parent)
        self.assertEqual(reached, [True])
        self.assertEqual(target.read_bytes(), b"unrelated replacement directory target")
        self.assertEqual((held / "artifact").read_bytes(), b"reviewed old artifact")

    def test_finish_does_not_archive_foreign_replacement_after_owner_check(self):
        replaced = []
        def replace_owner(label):
            if label == "finish.maintenance.before_rename":
                self.maintenance.rename(self.root / "original-maintenance-evidence")
                self.maintenance.mkdir()
                foreign = {"schema": "gridedge.core-migration-lock.v1", "index": sha(b"foreign successor"),
                           "owner": dict(self.owner, pid=41002)}
                (self.maintenance / "owner.json").write_text(json.dumps(foreign, sort_keys=True, separators=(',', ':')))
                replaced.append((self.maintenance / "owner.json").read_bytes())
        with self.subject.OwnedFence(self.root, self.index, self.owner, lambda _: "ABSENT", checkpoint=replace_owner) as fence:
            with self.assertRaises((ValueError, OSError, RuntimeError)):
                fence.finish()
        self.assertEqual(len(replaced), 1)
        self.assertEqual((self.maintenance / "owner.json").read_bytes(), replaced[0], "foreign successor was moved away")
        self.assertTrue(self.coordination.exists())

    def test_reserve_parent_replacement_cannot_publish_into_unrelated_runtime(self):
        runtime = self.root / "runtime"
        held = self.root / "original-runtime"
        reached = []
        def replace_runtime(label):
            if label == "reserve.before_publish":
                temporary, = runtime.glob(".core-migration-prepared-*")
                document = (temporary / "reserve.json").read_bytes()
                runtime.rename(held)
                runtime.mkdir()
                substitute = runtime / temporary.name
                substitute.mkdir()
                (substitute / "reserve.json").write_bytes(document)
                (runtime / "unrelated-sentinel").write_bytes(b"keep")
                reached.append(True)
        with self.assertRaises((ValueError, OSError)):
            self.subject.reserve(self.root, self.index, self.baseline, self.owner, checkpoint=replace_runtime)
        self.assertEqual(reached, [True])
        self.assertFalse(self.reserve_path().exists(), "reservation was published through replacement parent")
        self.assertEqual((runtime / "unrelated-sentinel").read_bytes(), b"keep")

    def test_fence_parent_replacement_cannot_publish_into_unrelated_runtime(self):
        for number, prefix in enumerate(("coordination", "maintenance")):
            with self.subTest(prefix=prefix):
                root = self.root / ("replace-lock-parent-" + str(number))
                runtime = root / "runtime"
                runtime.mkdir(parents=True)
                held = root / "original-runtime"
                reached = []
                def replace_runtime(label):
                    if label == prefix + ".before_publish":
                        temporary, = runtime.glob(".core-migration-prepared-*")
                        document = (temporary / "owner.json").read_bytes()
                        runtime.rename(held)
                        runtime.mkdir()
                        substitute = runtime / temporary.name
                        substitute.mkdir()
                        (substitute / "owner.json").write_bytes(document)
                        (runtime / "unrelated-sentinel").write_bytes(b"keep")
                        reached.append(True)
                with self.assertRaises((ValueError, OSError, RuntimeError)):
                    with self.subject.OwnedFence(root, self.index, self.owner, lambda _: "ABSENT", checkpoint=replace_runtime):
                        pass
                self.assertEqual(reached, [True])
                self.assertFalse((runtime / "ths-deployment-coordination.lock").exists())
                self.assertFalse((runtime / "ths-deployment-maintenance").exists())
                self.assertEqual((runtime / "unrelated-sentinel").read_bytes(), b"keep")


if __name__ == "__main__":
    unittest.main()
