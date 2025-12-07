import logging
from collections.abc import Sequence
from functools import cache
from typing import Iterable, Optional

import pytest
import structlog
from attrs import Factory, define, frozen
from multimethod import multimethod
from precisely import assert_that, contains_exactly, equal_to, is_instance

from rift.datasets import Backend, Dataset, Remote, Stream, ancestor, prune, send, sync
from rift.snapshots import Bookmark, Snapshot

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))


@frozen
class MemoryStream(Stream):
    snapshot: Snapshot

    def size(self) -> int:
        return 0


@frozen
class ResumingInMemoryStream(MemoryStream):
    token: str


@frozen
class IncrementalInMemoryStream(MemoryStream):
    ancestor: Snapshot | Bookmark


@frozen
class FullInMemoryStream(MemoryStream):
    pass


@define
class InMemoryBackend(Backend):
    snapshots_data: list[Snapshot] = Factory(list)
    bookmarks_data: list[Bookmark] = Factory(list)
    resume_token_data: Optional[str] = None
    received_as: dict[Snapshot, Stream] = Factory(dict)
    does_exist: bool = True

    def snapshots(self) -> tuple[Snapshot, ...]:
        return tuple(self.snapshots_data)

    @cache
    def bookmarks(self) -> tuple[Bookmark, ...]:
        return tuple(self.bookmarks_data)

    def snapshot(self, name: str) -> None:
        def next_createtxg():
            if len(self.snapshots_data) == 0:
                return 1
            return int(self.snapshots_data[-1].createtxg) + 1

        snapshot = Snapshot(
            fqn=self.path + "@" + name,
            guid="uuid:" + self.path + "@" + name,
            createtxg=next_createtxg(),
        )
        self.snapshots_data.append(snapshot)

    def bookmark(self, snapshot_name: str) -> None:
        snapshot = next(s for s in self.snapshots() if s.name == snapshot_name)
        bookmark = Bookmark(snapshot.fqn.replace("@", "#"), snapshot.guid, snapshot.createtxg)
        self.bookmarks_data.append(bookmark)

    @multimethod
    def send(self, token: str, *, options: tuple[str, ...] = ("-w",)) -> Stream:
        return ResumingInMemoryStream(self.snapshots_data[int(token)], token)

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark, *, options: tuple[str, ...] = ("-w",)) -> Stream:
        return IncrementalInMemoryStream(snapshot, ancestor)

    @multimethod
    def send(self, snapshot: Snapshot, *, options: tuple[str, ...] = ("-w",)) -> Stream:
        return FullInMemoryStream(snapshot)

    def recv(
        self,
        stream: Stream,
        *,
        options: tuple[str, ...] = ("-s", "-u"),
        pipes: Sequence[tuple[str, ...]] = (),
        dry_run: bool,
    ) -> None:
        assert isinstance(
            stream,
            (FullInMemoryStream, IncrementalInMemoryStream, ResumingInMemoryStream),
        )
        self.snapshots_data.append(stream.snapshot)
        self.received_as[stream.snapshot] = stream

    def resume_token(self) -> Optional[str]:
        return self.resume_token_data

    def exists(self) -> bool:
        return self.does_exist

    def destroy(self, snapshots: Iterable[str], dry_run: bool) -> None:
        for s in self.snapshots_data:
            if s.name in snapshots:
                self.snapshots_data.remove(s)

    def __hash__(self):
        return hash(self.path)


def test_path():
    assert_that(Dataset(InMemoryBackend("source/A")).path, equal_to("source/A"))


def test_fqn():
    assert_that(Dataset(InMemoryBackend("source/A", remote=None)).fqn, equal_to("source/A"))


def test_fqn_remote():
    assert_that(
        Dataset(InMemoryBackend("source/A", remote=Remote("user@host"))).fqn,
        equal_to("user@host:source/A"),
    )


def test_snapshot():
    source = Dataset(InMemoryBackend("source/A"))
    source.snapshot("s1")
    assert_that(
        source.snapshots(),
        contains_exactly(Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)),
    )
    source.snapshot("s2")
    assert_that(
        source.snapshots(),
        contains_exactly(
            Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1),
            Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2),
        ),
    )


def test_bookmark():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2]))
    source.bookmark("s2")
    assert_that(
        source.bookmarks(),
        equal_to((Bookmark(fqn="source/A#s2", guid="uuid:source/A@s2", createtxg=2),)),
    )


def test_find():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2]))
    assert_that(source.find("s1"), equal_to(s1))
    assert_that(source.find("s2"), equal_to(s2))
    with pytest.raises(ValueError):
        source.find("s3")


def test_send_without_source():
    source = Dataset(InMemoryBackend("source/A"))
    target = Dataset(InMemoryBackend("target/backups/A"))

    # try s1 from source to target without s1 being in source
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    with pytest.raises(FileNotFoundError):
        send(s1, source, target, dry_run=False)


def test_full_send():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1]))
    target = Dataset(InMemoryBackend("target/backups/A"))

    # send s1 from source to target
    send(s1, source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(*source.snapshots()))

    # assert that s1 was a full send
    assert isinstance(target.backend, InMemoryBackend)
    assert_that(target.backend.received_as[s1], is_instance(FullInMemoryStream))


def test_incremental_send():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=5)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=6)
    t1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[t1]))

    # send s2 from source to target
    send(s2, source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(t1, s2))

    # assert that s2 was an incremental send
    assert isinstance(target.backend, InMemoryBackend)
    assert_that(target.backend.received_as[s2], is_instance(IncrementalInMemoryStream))


def test_resume_send():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[s1], resume_token_data="1"))

    # send s2 from source to target
    send(s2, source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(*source.snapshots()))

    # assert that s2 was a resume send
    assert isinstance(target.backend, InMemoryBackend)
    assert_that(target.backend.received_as[s2], is_instance(ResumingInMemoryStream))


def test_no_send():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[s1]))

    # send s1 from source to target
    send(s1, source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(*source.snapshots()))

    # assert that nothing was actually received
    assert isinstance(target.backend, InMemoryBackend)
    assert_that(target.backend.received_as, contains_exactly())


def test_no_ancestor():
    snapshot = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    source = Dataset(
        InMemoryBackend(
            "source/A",
            snapshots_data=[
                Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1),
                snapshot,
            ],
        )
    )
    target = Dataset(
        InMemoryBackend(
            "target/backups/A",
            snapshots_data=[
                Snapshot(fqn="target/backups/A@s3", guid="uuid:source/A@s3", createtxg=0),
                Snapshot(fqn="target/backups/A@s4", guid="uuid:source/A@s4", createtxg=0),
            ],
        )
    )

    assert_that(ancestor(snapshot, source, target), equal_to(None))


def test_ancestor():
    base = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    snapshot = Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4)

    source = Dataset(
        InMemoryBackend(
            "source/A",
            snapshots_data=[
                Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Snapshot(fqn="source/A@s3", guid="uuid:source/A@s3", createtxg=3),  # missing on target
                Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4),  # snapshot
                Snapshot(fqn="source/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )
    target = Dataset(
        InMemoryBackend(
            "target/backups/A",
            snapshots_data=[
                Snapshot(fqn="target/backups/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Snapshot(fqn="target/backups/A@s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Snapshot(fqn="target/backups/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )

    assert_that(ancestor(snapshot, source, target), equal_to(base))


def test_ancestor_bookmark():
    base = Bookmark(fqn="source/A#s2", guid="uuid:source/A@s2", createtxg=2)
    snapshot = Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4)

    source = Dataset(
        InMemoryBackend(
            "source/A",
            snapshots_data=[
                Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                # Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2), # ancestor / base
                Snapshot(fqn="source/A@s3", guid="uuid:source/A@s3", createtxg=3),  # missing on target
                Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4),  # snapshot
                Snapshot(fqn="source/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
            bookmarks_data=[
                Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Bookmark(fqn="source/A#s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Bookmark(fqn="source/A#s3", guid="uuid:source/A@s3", createtxg=3),  # missing on target
                Bookmark(fqn="source/A#s4", guid="uuid:source/A@s4", createtxg=4),  # snapshot
                Bookmark(fqn="source/A#s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )
    target = Dataset(
        InMemoryBackend(
            "target/backups/A",
            snapshots_data=[
                Snapshot(fqn="target/backups/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Snapshot(fqn="target/backups/A@s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Snapshot(fqn="target/backups/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )

    assert_that(ancestor(snapshot, source, target), equal_to(base))


def test_ancestor_snapshot_before_bookmark():
    base = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    snapshot = Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4)

    source = Dataset(
        InMemoryBackend(
            "source/A",
            snapshots_data=[
                Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Snapshot(fqn="source/A@s3", guid="uuid:source/A@s3", createtxg=3),  # missing on target
                Snapshot(fqn="source/A@s4", guid="uuid:source/A@s4", createtxg=4),  # snapshot
                Snapshot(fqn="source/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
            bookmarks_data=[
                Bookmark(fqn="source/A#s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Bookmark(fqn="source/A#s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Bookmark(fqn="source/A#s3", guid="uuid:source/A@s3", createtxg=3),  # missing on target
                Bookmark(fqn="source/A#s4", guid="uuid:source/A@s4", createtxg=4),  # snapshot
                Bookmark(fqn="source/A#s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )
    target = Dataset(
        InMemoryBackend(
            "target/backups/A",
            snapshots_data=[
                Snapshot(fqn="target/backups/A@s1", guid="uuid:source/A@s1", createtxg=1),  # older common
                Snapshot(fqn="target/backups/A@s2", guid="uuid:source/A@s2", createtxg=2),  # ancestor / base
                Snapshot(fqn="target/backups/A@s5", guid="uuid:source/A@s5", createtxg=5),  # newer common
            ],
        )
    )

    assert_that(ancestor(snapshot, source, target), equal_to(base))


def test_sync_initial():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    s3 = Snapshot(fqn="source/A@s3", guid="uuid:source/A@s3", createtxg=3)
    s4 = Snapshot(fqn="source/A@s3", guid="uuid:source/A@s4", createtxg=4)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2, s3, s4]))
    target = Dataset(InMemoryBackend("target/backups/A", does_exist=False, snapshots_data=[]))

    # sync newer from source to target
    sync(source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(s1, s2, s3, s4))


def test_sync():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    s3 = Snapshot(fqn="source/A@s3", guid="uuid:source/A@s3", createtxg=3)
    s4 = Snapshot(fqn="source/A@s3", guid="uuid:source/A@s4", createtxg=4)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2, s3, s4]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[s2]))

    # sync newer from source to target
    sync(source, target, dry_run=False)
    assert_that(target.snapshots(), contains_exactly(s2, s3, s4))


def test_sync_filtered():
    s1 = Snapshot(fqn="source/A@rift_s1", guid="uuid:source/A@rift_s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    s3 = Snapshot(fqn="source/A@rift_s3", guid="uuid:source/A@rift_s3", createtxg=3)
    s4 = Snapshot(fqn="source/A@s3", guid="uuid:source/A@s4", createtxg=4)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2, s3, s4]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[]))

    # sync newer from source to target
    sync(source, target, regex="rift_.*", dry_run=False)
    assert_that(target.snapshots(), contains_exactly(s1, s3))


def test_sync_target_contains_wrong_snapshot():
    s1 = Snapshot(fqn="source/A@s1", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@s2", guid="uuid:source/A@s2", createtxg=2)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1]))
    target = Dataset(InMemoryBackend("target/backups/A", snapshots_data=[s2]))

    with pytest.raises(RuntimeError):
        sync(source, target, dry_run=False)


def test_prune():
    s1 = Snapshot(fqn="source/A@rift_s1_weekly", guid="uuid:source/A@s1", createtxg=1)
    s2 = Snapshot(fqn="source/A@rift_s2_weekly", guid="uuid:source/A@s2", createtxg=2)
    s3 = Snapshot(fqn="source/A@rift_s3_daily", guid="uuid:source/A@s3", createtxg=3)
    s4 = Snapshot(fqn="source/A@rift_s4_monthly", guid="uuid:source/A@s4", createtxg=4)
    source = Dataset(InMemoryBackend("source/A", snapshots_data=[s1, s2, s3, s4]))
    policy = {"rift_.*_daily": 5, "rift_.*_weekly": 1, "rift_.*_monthly": 0}
    prune(source, policy, dry_run=False)

    assert_that(source.snapshots(), contains_exactly(s2, s3))
