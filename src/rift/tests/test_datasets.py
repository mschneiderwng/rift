import logging

import pytest
import structlog
from precisely import assert_that, contains_exactly, equal_to, includes

from rift.datasets import Dataset, Remote
from rift.replication import ancestor, prune, send, sync
from rift.snapshots import Bookmark, Snapshot
from rift.tests.mocks import InMemoryDataset, InMemoryFS, fqn2token

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))

"""
This file contains high level tests (mostly not checking zfs shell commands).
"""

def test_path():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    assert_that(dataset.path, equal_to("pool/A"))


def test_fqn():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    dataset = Dataset(path="pool/A", runner=fs)
    assert_that(dataset.fqn, equal_to("pool/A"))


def test_fqn_remote():
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    dataset = Dataset(path="pool/A", remote=Remote("user@remote"), runner=fs)
    assert_that(dataset.fqn, equal_to("user@remote:pool/A"))


def test_snapshot():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    src = Dataset(path="pool/A", runner=fs)
    src.snapshot("s1")
    assert_that(src.snapshots(), contains_exactly(Snapshot(fqn="pool/A@s1", guid="uuid:pool/A@s1", createtxg=896)))
    src.snapshot("s2")
    assert_that(
        src.snapshots(),
        contains_exactly(
            Snapshot(fqn="pool/A@s1", guid="uuid:pool/A@s1", createtxg=896),
            Snapshot(fqn="pool/A@s2", guid="uuid:pool/A@s2", createtxg=897),
        ),
    )


def test_bookmark():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1", "s2"))
    src = Dataset(path="pool/A", runner=fs)
    src.bookmark("s2")
    assert_that(src.bookmarks(), contains_exactly(Bookmark(fqn="pool/A#s2", guid="uuid:pool/A@s2", createtxg=897)))


def test_find():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1", "s2"))
    src = Dataset(path="pool/A", runner=fs)
    assert_that(src.find("s1"), equal_to(Snapshot(fqn="pool/A@s1", guid="uuid:pool/A@s1", createtxg=896)))
    assert_that(src.find("s2"), equal_to(Snapshot(fqn="pool/A@s2", guid="uuid:pool/A@s2", createtxg=897)))
    with pytest.raises(ValueError):
        src.find("s3")


def test_send_without_source_snapshot():
    fs = InMemoryFS.of(InMemoryDataset("pool/A"), InMemoryDataset("pool/B"))

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    # try s1 from source to target without s1 being in source
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    with pytest.raises(FileNotFoundError):
        send(s1, source, target, dry_run=False)


def test_send_full():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B"))

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    # send s1 from source to target
    s1 = source.find("s1")
    send(s1, source, target, dry_run=False)
    assert_that({s.guid for s in target.snapshots()}, equal_to({s.guid for s in source.snapshots()}))

    # assert that s1 was a full sent
    assert_that(fs.recorded, includes("zfs send pool/A@s1 | zfs receive pool/B"))


def test_send_incremental():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2")
    poolB = InMemoryDataset("pool/B").recv(poolA.find("pool/A@s1"))
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    # send s2 from source to target
    s2 = source.find("s2")
    send(s2, source, target, dry_run=False)
    assert_that({s.guid for s in target.snapshots()}, equal_to({s.guid for s in source.snapshots()}))

    # assert that s1 was an incremental sent
    assert_that(fs.recorded, includes("zfs send -i pool/A@s1 pool/A@s2 | zfs receive pool/B"))


def test_send_resume():
    token = fqn2token("pool/A@s1")  # simulate a token by using the fqn
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B", token=token))

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    # send s1 from source to target
    s1 = source.find("s1")
    send(s1, source, target, dry_run=False)
    assert_that({s.guid for s in target.snapshots()}, equal_to({s.guid for s in source.snapshots()}))

    # assert that s1 was a full sent
    assert_that(fs.recorded, includes("zfs send -t 706f6f6c2f41407331 | zfs receive pool/B"))


def test_ancestor():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2", "s3", "s4", "s5")
    poolB = (
        InMemoryDataset("pool/B")
        .recv(poolA.find("pool/A@s1"))
        .recv(poolA.find("pool/A@s2"))
        .recv(poolA.find("pool/A@s5"))
    )
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    s2 = source.find("s2")
    s4 = source.find("s4")
    assert_that(ancestor(s4, source, target), equal_to(s2))


def test_ancestor_no_common():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2")
    poolB = InMemoryDataset("pool/B").snapshot("s3", "s4")
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    s2 = source.find("s2")
    assert_that(ancestor(s2, source, target), equal_to(None))


def test_ancestor_bookmark():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2", "s3", "s4", "s5")
    poolB = (
        InMemoryDataset("pool/B")
        .recv(poolA.find("pool/A@s1"))
        .recv(poolA.find("pool/A@s2"))
        .recv(poolA.find("pool/A@s5"))
    )
    poolA.bookmark("s2")
    poolA.destroy("s2")
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    s2 = poolA.find("pool/A#s2")
    s4 = source.find("s4")
    assert_that(ancestor(s4, source, target), equal_to(s2))


def test_sync_full():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2")
    poolB = InMemoryDataset("pool/B")

    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    sync(source, target, dry_run=False)
    assert_that({s.guid for s in target.snapshots()}, equal_to({s.guid for s in source.snapshots()}))


def test_sync():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2", "s3")
    poolB = InMemoryDataset("pool/B").recv(poolA.find("pool/A@s1"))
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    sync(source, target, dry_run=False)
    assert_that({s.guid for s in target.snapshots()}, equal_to({s.guid for s in source.snapshots()}))


def test_sync_filter():
    poolA = InMemoryDataset("pool/A").snapshot("s1", "s2", "s3")
    poolB = InMemoryDataset("pool/B").recv(poolA.find("pool/A@s1"))
    fs = InMemoryFS.of(poolA, poolB)

    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    sync(source, target, regex=".*3", dry_run=False)

    s1 = source.find("s1")
    s3 = source.find("s3")
    assert_that({s.guid for s in target.snapshots()}, equal_to({s1.guid, s3.guid}))


def test_sync_target_contains_wrong_snapshot():
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B").snapshot("s2"))
    source = Dataset(path="pool/A", runner=fs)
    target = Dataset(path="pool/B", runner=fs)

    with pytest.raises(RuntimeError):
        sync(source, target, dry_run=False)


def test_prune():
    poolA = InMemoryDataset("pool/A").snapshot("s1_weekly", "s2_weekly", "s3_daily", "s4_monthly")
    fs = InMemoryFS.of(poolA)
    dataset = Dataset(path="pool/A", runner=fs)

    policy = {".*_daily": 5, ".*_weekly": 1, ".*_monthly": 0}
    prune(dataset, policy, dry_run=False)

    s2 = dataset.find("s2_weekly")
    s3 = dataset.find("s3_daily")
    assert_that(dataset.snapshots(), contains_exactly(s2, s3))
