from typing import Optional, Sequence

from attrs import Factory, define
from precisely import assert_that, equal_to

from rift.commands import NoSuchDatasetError, Runner
from rift.snapshots import Bookmark, Snapshot
from rift.zfs import Remote, ZfsBackend, ZfsStream


@define
class RunnerMock(Runner):
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
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    assert_that(dataset.exists(), equal_to(True))


def test_not_exists():
    runner = RunnerMock(raises=NoSuchDatasetError(1, ""))
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    assert_that(dataset.exists(), equal_to(False))


def test_snapshot():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.snapshot("s2")
    assert_that(runner.recorded, equal_to([("zfs", "snapshot", "source/A@s2")]))


def test_snapshot_remote():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=Remote("user@host"), runner=runner)
    dataset.snapshot("s2")
    assert_that(
        runner.recorded,
        equal_to([("ssh", "user@host", "--", "zfs", "snapshot", "source/A@s2")]),
    )


def test_snapshot_remote_options():
    runner = RunnerMock()
    dataset = ZfsBackend(
        path="source/A",
        remote=Remote("user@host", options=("ServerAliveInterval=60", "Compression=yes")),
        runner=runner,
    )
    dataset.snapshot("s2")
    assert_that(
        runner.recorded,
        equal_to(
            [
                (
                    "ssh",
                    "user@host",
                    "-o",
                    "ServerAliveInterval=60",
                    "-o",
                    "Compression=yes",
                    "--",
                    "zfs",
                    "snapshot",
                    "source/A@s2",
                )
            ]
        ),
    )


def test_bookmark():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.bookmark("s2")
    assert_that(runner.recorded, equal_to([("zfs", "bookmark", "source/A@s2", "source/A#s2")]))


def test_bookmark_remote():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=Remote("user@host"), runner=runner)
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
    runner = RunnerMock()
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_snapshots_cache():
    runner = RunnerMock()
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_snapshots_remote():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=Remote("user@host"), runner=runner)
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks():
    runner = RunnerMock()
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks_cache():
    runner = RunnerMock()
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_bookmarks_remote():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=Remote("user@host"), runner=runner)
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
                    "name,guid,createtxg",
                    "source/A",
                )
            ]
        ),
    )


def test_send_resume():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    stream = dataset.send("token", send_options=("-w",))
    assert_that(stream, equal_to(ZfsStream(("zfs", "send", "-w", "-t", "token"), runner)))


def test_send_incremental_from_bookmark():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    anchor = Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", createtxg=1)
    snapshot = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    stream = dataset.send(snapshot, anchor, send_options=("-w",))
    assert_that(
        stream,
        equal_to(ZfsStream(("zfs", "send", "-w", "-i", "source/A#s1", "source/A@s2"), runner)),
    )


def test_send_incremental_from_snapshot():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    anchor = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    snapshot = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    stream = dataset.send(snapshot, anchor, send_options=("-w",))
    assert_that(
        stream,
        equal_to(ZfsStream(("zfs", "send", "-w", "-i", "source/A@s1", "source/A@s2"), runner)),
    )


def test_send_full():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    snapshot = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    stream = dataset.send(snapshot, send_options=("-w",))
    assert_that(stream, equal_to(ZfsStream(("zfs", "send", "-w", "source/A@s1"), runner)))


def test_recv():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.recv(ZfsStream(("zfs", "send", "..."), runner), recv_options=("-s", "-u", "-F"), dry_run=False)
    assert_that(
        runner.recorded,
        equal_to([("zfs", "send", "..."), ("zfs", "receive", "-s", "-u", "-F", "source/A")]),
    )


def test_resume_token():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.resume_token()
    assert_that(
        runner.recorded,
        equal_to([("zfs", "get", "-H", "-o", "value", "receive_resume_token", "source/A")]),
    )


def test_resume_token_cache():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.resume_token()
    dataset.resume_token()
    assert_that(
        runner.recorded,
        equal_to([("zfs", "get", "-H", "-o", "value", "receive_resume_token", "source/A")]),
    )


def test_stream_size():
    runner = RunnerMock(
        returns="""full    rpool@rift_2025-12-06_05:15:04_frequently       3711767360
                   size    3711767360"""
    )
    size = ZfsStream(("zfs", "send", "..."), runner).size()
    assert_that(size, equal_to(3711767360))


def test_stream_size_resume():
    runner = RunnerMock(
        returns="""resume token contents:
                nvlist version: 0
                        object = 0x72a
                        offset = 0x5f80000
                        bytes = 0x90c3f2184
                        toguid = 0xc7442ab399a28a9b
                        toname = rpool@rift_2025-12-05_07:36:58_weekly
                        compressok = 1
                        rawok = 1
                full    rpool@rift_2025-12-06_05:15:04_frequently       3711767360
                size    3711767360"""
    )
    size = ZfsStream(("zfs", "send", "..."), runner).size()
    assert_that(size, equal_to(3711767360))


def test_destroy_none():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.destroy([], dry_run=False)
    assert_that(runner.recorded, equal_to([]))


def test_destroy():
    runner = RunnerMock()
    dataset = ZfsBackend(path="source/A", remote=None, runner=runner)
    dataset.destroy(["s1", "s2"], dry_run=False)
    assert_that(runner.recorded, equal_to([("zfs", "destroy", "source/A@s1,s2")]))
