import logging

import structlog
from precisely import assert_that, contains_exactly, equal_to, includes

from rift.datasets import Dataset, Remote, Stream
from rift.snapshots import Bookmark, Snapshot
from rift.tests.mocks import InMemoryDataset, InMemoryFS

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))

"""
This file contains low level tests; checking zfs shell commands.
"""

def test_snapshot_list():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    dataset.snapshots()
    assert_that(fs.recorded, equal_to(["ssh user@remote -- zfs list -pHt snapshot -o name,guid,createtxg pool/A"]))


def test_snapshot_list_caching():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.snapshots()
    dataset.snapshots()
    assert_that(fs.recorded, equal_to(["zfs list -pHt snapshot -o name,guid,createtxg pool/A"]))


def test_ssh_options():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote", ("Compression=yes",)), runner=fs)
    dataset.snapshots()
    assert_that(
        fs.recorded,
        equal_to(["ssh user@remote -o Compression=yes -- zfs list -pHt snapshot -o name,guid,createtxg pool/A"]),
    )


def test_bookmarks_list():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    dataset.bookmarks()
    assert_that(fs.recorded, equal_to(["ssh user@remote -- zfs list -pHt bookmark -o name,guid,createtxg pool/A"]))


def test_bookmarks_list_caching():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.bookmarks()
    dataset.bookmarks()
    assert_that(fs.recorded, equal_to(["zfs list -pHt bookmark -o name,guid,createtxg pool/A"]))


def test_exists():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    dataset = Dataset(path="pool/A", runner=fs)
    assert_that(dataset.exists(), equal_to(True))


def test_exists_remote():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote", ("Compression=yes",)), runner=fs)
    assert_that(dataset.exists(), equal_to(True))


def test_not_exists():
    fs = InMemoryFS.of()
    dataset = Dataset(path="pool/AB", runner=fs)
    assert_that(dataset.exists(), equal_to(False))


def test_snapshot():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.snapshot("s1")
    assert_that(fs.recorded, equal_to(["zfs snapshot pool/A@s1"]))
    assert_that(fs.entries(), contains_exactly("pool/A@s1\tuuid:pool/A@s1\t896"))


def test_snapshot_remote():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote", ("Compression=yes",)), runner=fs)
    dataset.snapshot("s1")
    assert_that(fs.recorded, equal_to(["ssh user@remote -o Compression=yes -- zfs snapshot pool/A@s1"]))


def test_bookmark():
    poolA = InMemoryDataset("pool/A")
    fs = InMemoryFS.of(poolA.snapshot("s1"))
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.bookmark("s1")
    assert_that(fs.recorded, equal_to(["zfs bookmark pool/A@s1 pool/A#s1"]))
    assert_that(fs.entries(), contains_exactly("pool/A@s1\tuuid:pool/A@s1\t896", "pool/A#s1\tuuid:pool/A@s1\t896"))


def test_bookmark_remote():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote").snapshot("s1"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote", ("Compression=yes",)), runner=fs)
    dataset.bookmark("s1")
    assert_that(fs.recorded, equal_to(["ssh user@remote -o Compression=yes -- zfs bookmark pool/A@s1 pool/A#s1"]))


def test_send_resume():
    fs = InMemoryFS.of()
    dataset = Dataset(path="pool/A", runner=fs)
    stream = dataset.send("token", options=("-w",))
    assert_that(stream, equal_to(Stream(("zfs", "send", "-w", "-t", "token"), fs)))


def test_send_incremental_from_snapshot():
    fs = InMemoryFS.of()
    dataset = Dataset(path="pool/A", runner=fs)
    anchor = Snapshot(fqn="pool/A@s1", guid="uuid:pool/A@s1", createtxg=1)
    snapshot = Snapshot(fqn="pool/A@s2", guid="uuid:pool/A@s2", createtxg=2)
    stream = dataset.send(snapshot, anchor)
    assert_that(stream, equal_to(Stream(("zfs", "send", "-i", "pool/A@s1", "pool/A@s2"), fs)))


def test_send_incremental_from_bookmark():
    fs = InMemoryFS.of()
    dataset = Dataset(path="pool/A", runner=fs)
    anchor = Bookmark(fqn="pool/A#s1", guid="uuid:pool/A@s1", createtxg=1)
    snapshot = Snapshot(fqn="pool/A@s2", guid="uuid:pool/A@s2", createtxg=2)
    stream = dataset.send(snapshot, anchor)
    assert_that(stream, equal_to(Stream(("zfs", "send", "-i", "pool/A#s1", "pool/A@s2"), fs)))


def test_send_full():
    fs = InMemoryFS.of()
    dataset = Dataset(path="pool/A", runner=fs)
    snapshot = Snapshot(fqn="pool/A@s2", guid="uuid:pool/A@s2", createtxg=2)
    stream = dataset.send(snapshot, options=("-w",))
    assert_that(stream, equal_to(Stream(("zfs", "send", "-w", "pool/A@s2"), fs)))


def test_recv():
    poolA = InMemoryDataset("pool/A").snapshot("s1")
    poolB = InMemoryDataset("pool/B", "user@remote")
    fs = InMemoryFS.of(poolA, poolB)
    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", remote=Remote("user@remote"), runner=fs)
    snapshot = fs.find("pool/A").find("pool/A@s1")
    stream = source.send(snapshot)
    target.recv(stream, options=("-s", "-u", "-F"), dry_run=False)
    assert_that(fs.recorded, equal_to(["zfs send pool/A@s1 | ssh user@remote -- zfs receive -s -u -F pool/B"]))
    assert_that(fs.entries(), contains_exactly("pool/A@s1\tuuid:pool/A@s1\t896", "pool/B@s1\tuuid:pool/A@s1\t655"))


def test_get_resume_token():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", remote="user@remote", token="341293104"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    dataset.resume_token()
    assert_that(fs.recorded, equal_to(["ssh user@remote -- zfs get -H -o value receive_resume_token pool/A"]))


def test_get_resume_token_caching():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", remote="user@remote", token="341293104"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    dataset.resume_token()
    dataset.resume_token()
    assert_that(fs.recorded, equal_to(["ssh user@remote -- zfs get -H -o value receive_resume_token pool/A"]))


def test_stream_size():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"))
    source = Dataset(path="pool/A", runner=fs)
    snapshot = fs.find("pool/A").find("pool/A@s1")
    stream = source.send(snapshot)
    assert_that(stream.size(), equal_to(3711767360))


def test_destroy_none():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"))
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.destroy([], dry_run=False)
    assert_that(fs.recorded, equal_to([]))


def test_destroy():
    poolA = InMemoryDataset("pool/A").snapshot("s2", "s3")
    fs = InMemoryFS.of(poolA)
    dataset = Dataset(path="pool/A", runner=fs)
    dataset.destroy(["s1", "s2"], dry_run=False)
    assert_that(fs.recorded, equal_to(["zfs destroy pool/A@s1,s2"]))
    assert_that(fs.entries(), contains_exactly("pool/A@s3\tuuid:pool/A@s3\t897"))


def test_send_rev():
    poolA = InMemoryDataset("pool/A").snapshot("s1")
    poolB = InMemoryDataset("pool/B")
    fs = InMemoryFS.of(poolA, poolB)
    s1 = poolA.find("pool/A@s1")

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    stream = source.send(s1)
    target.recv(stream, dry_run=False)

    # assert that all options were passed through
    assert_that(fs.recorded, includes("zfs send pool/A@s1 | zfs receive pool/B"))


def test_send_rev_with_options():
    poolA = InMemoryDataset("pool/A", "userA@remoteA").snapshot("s1")
    poolB = InMemoryDataset("pool/B", "userB@remoteB")
    fs = InMemoryFS.of(poolA, poolB)
    s1 = poolA.find("pool/A@s1")

    source = Dataset(path="pool/A", remote=Remote("userA@remoteA", ("option=A",)), runner=fs)
    target = Dataset(path="pool/B", remote=Remote("userB@remoteB", ("option=B",)), runner=fs)

    stream = source.send(s1, options=("-w",))
    target.recv(stream, options=("-s", "-u", "-F"), dry_run=False)

    # assert that all options were passed through
    assert_that(
        fs.recorded,
        includes(
            "ssh userA@remoteA -o option=A -- zfs send -w pool/A@s1 | ssh userB@remoteB -o option=B -- zfs receive -s -u -F pool/B"
        ),
    )


def test_send_rev_dry_run():
    poolA = InMemoryDataset("pool/A").snapshot("s1")
    poolB = InMemoryDataset("pool/B")
    fs = InMemoryFS.of(poolA, poolB)
    s1 = poolA.find("pool/A@s1")

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    stream = source.send(s1)
    target.recv(stream, dry_run=True)

    # assert that all options were passed through
    assert_that(fs.recorded, includes("zfs send pool/A@s1 | zfs receive pool/B -n -v"))
