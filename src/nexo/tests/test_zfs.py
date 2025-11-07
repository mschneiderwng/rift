from typing import Optional, Sequence

from attrs import Factory, define
from precisely import assert_that, equal_to

from rift.commands import NoSuchDatasetError, Runner
from rift.snapshots import Bookmark, Snapshot
from rift.zfs import ZfsBackend, ZfsStream


@define
class TestRunner(Runner):
    recorded: list[Sequence[str]] = Factory(list)
    returns: str = ""
    raises: Optional[Exception] = None

    def run(self, command: Sequence[str], *others: Sequence[str]) -> str:
        self.recorded += [command] + list(others)
        if self.raises is not None:
            raise self.raises
        return self.returns

    def __hash__(self):
        return hash(id(self))


def test_exists():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    assert_that(dataset.exists(), equal_to(True))


def test_not_exists():
    runner = TestRunner(raises=NoSuchDatasetError(1, ""))
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    assert_that(dataset.exists(), equal_to(False))


def test_snapshot():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.snapshot("s2")
    assert_that(runner.recorded, equal_to([("zfs", "snapshot", "source/A@s2")]))


def test_snapshot_remote():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote="user@host", runner=runner)
    dataset.snapshot("s2")
    assert_that(
        runner.recorded,
        equal_to([("ssh", "user@host", "--", "zfs", "snapshot", "source/A@s2")]),
    )


def test_bookmark():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.bookmark("s2")
    assert_that(runner.recorded, equal_to([("zfs", "bookmark", "source/A@s2", "source/A#s2")]))


def test_bookmark_remote():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote="user@host", runner=runner)
    dataset.bookmark("s2")
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "ssh",
                    "user@host",
                    "--",
                    "zfs",
                    "bookmark",
                    "source/A@s2",
                    "source/A#s2",
                )
            ]
        ),
    )


def test_snapshots():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.snapshots()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "zfs",
                    "list",
                    "-pHt",
                    "snapshot",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_snapshots_cache():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.snapshots()
    dataset.snapshots()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "zfs",
                    "list",
                    "-pHt",
                    "snapshot",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_snapshots_remote():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote="user@host", runner=runner)
    dataset.snapshots()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "ssh",
                    "user@host",
                    "--",
                    "zfs",
                    "list",
                    "-pHt",
                    "snapshot",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.bookmarks()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "zfs",
                    "list",
                    "-pHt",
                    "bookmark",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks_cache():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.bookmarks()
    dataset.bookmarks()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "zfs",
                    "list",
                    "-pHt",
                    "bookmark",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks_remote():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote="user@host", runner=runner)
    dataset.bookmarks()
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "ssh",
                    "user@host",
                    "--",
                    "zfs",
                    "list",
                    "-pHt",
                    "bookmark",
                    "-o",
                    "name,guid,creation",
                    "source/A",
                )
            ]
        ),
    )


def test_send_resume():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    stream = dataset.send("token")
    assert_that(stream, equal_to(ZfsStream(("zfs", "send", "-w", "-t", "token"), runner)))


def test_send_incremental_from_bookmark():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    anchor = Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", creation="")
    snapshot = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", creation="2")
    stream = dataset.send(snapshot, anchor)
    assert_that(
        stream,
        equal_to(ZfsStream(("zfs", "send", "-w", "-i", "source/A#s1", "source/A@s2"), runner)),
    )


def test_send_incremental_from_snapshot():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    anchor = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", creation="1")
    snapshot = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", creation="2")
    stream = dataset.send(snapshot, anchor)
    assert_that(
        stream,
        equal_to(ZfsStream(("zfs", "send", "-w", "-i", "source/A@s1", "source/A@s2"), runner)),
    )


def test_send_full():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    snapshot = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", creation="1")
    stream = dataset.send(snapshot)
    assert_that(stream, equal_to(ZfsStream(("zfs", "send", "-w", "source/A@s1"), runner)))


def test_recv():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.recv(ZfsStream(("zfs", "send", "..."), runner), bwlimit=None, dry_run=False)
    assert_that(
        runner.recorded,
        equal_to([("zfs", "send", "..."), ("zfs", "receive", "-s", "-u", "source/A")]),
    )


def test_recv_bwlimit():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.recv(ZfsStream(("zfs", "send", "..."), runner), bwlimit="1M", dry_run=False)
    assert_that(
        runner.recorded,
        equal_to(
            [
                ("zfs", "send", "..."),
                ("mbuffer", "-m", "1M"),
                ("zfs", "receive", "-s", "-u", "source/A"),
            ]
        ),
    )


def test_resume_token():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.resume_token()
    assert_that(
        runner.recorded,
        equal_to([("zfs", "get", "-H", "-o", "value", "receive_resume_token", "source/A")]),
    )


def test_resume_token_cache():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.resume_token()
    dataset.resume_token()
    assert_that(
        runner.recorded,
        equal_to([("zfs", "get", "-H", "-o", "value", "receive_resume_token", "source/A")]),
    )


def test_stream_size():
    runner = TestRunner(
        returns="send from source/A@rift_2025-10-11_10:42:52_weekly to source/A@rift_2025-10-11_12:40:19_weekly estimated size is 624B\ntotal estimated size is 624"
    )
    size = ZfsStream(("zfs", "send", "..."), runner).size()
    assert_that(size, equal_to(624))


def test_destroy_none():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.destroy([], dry_run=False)
    assert_that(runner.recorded, equal_to([]))


def test_destroy():
    runner = TestRunner()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.destroy(["s1", "s2"], dry_run=False)
    assert_that(runner.recorded, equal_to([("zfs", "destroy", "source/A@s1,s2")]))
