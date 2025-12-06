import re
from operator import attrgetter
from typing import Collection, Optional, Sequence

import structlog
from attrs import define, frozen
from multimethod import multimethod

from rift.snapshots import Bookmark, Snapshot

guid = str


def sizeof_fmt(num: float, suffix: str = "B") -> str:
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


@frozen
class Stream:
    def to(self, target: "Dataset", *, pipes: Sequence[tuple[str, ...]] = (), dry_run: bool):
        target.recv(self, pipes=pipes, dry_run=dry_run)

    def size(self) -> int:
        """Returns the estimated size of the stream in bytes"""
        raise NotImplementedError


@frozen
class Remote:
    host: str
    options: tuple[str, ...] = ()


@define
class Backend:
    path: str
    remote: Optional[Remote] = None

    @property
    def fqn(self):
        return f"{self.remote.host}:{self.path}" if self.remote is not None else self.path

    def snapshots(self) -> tuple[Snapshot, ...]:
        """List all snapshots of this dataset"""
        raise NotImplementedError

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """List all snapshots of this dataset"""
        raise NotImplementedError

    def snapshot(self, name: str) -> None:
        """Create a snapshot with the given name"""
        raise NotImplementedError

    def bookmark(self, snapshot: str) -> None:
        """Create a bookmark from the given snapshot"""
        raise NotImplementedError

    def exists(self) -> bool:
        """Returns true if the dataset exists"""
        raise NotImplementedError

    @multimethod
    def send(self, token: str) -> Stream:
        """Create a resume stream"""
        raise NotImplementedError

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark) -> Stream:
        """Create an incremental stream"""
        raise NotImplementedError

    @multimethod
    def send(self, snapshot: Snapshot) -> Stream:
        """Create a full stream"""
        raise NotImplementedError

    def recv(self, stream: Stream, *, pipes: Sequence[tuple[str, ...]] = (), dry_run: bool) -> None:
        """Consume a stream to produce a snapshot on the target (self)"""
        raise NotImplementedError

    def resume_token(self) -> Optional[str]:
        """Returns a resume token of a previously interrupted recv"""
        raise NotImplementedError

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """Consume a stream to produce a snapshot on the target (self)"""
        raise NotImplementedError


@frozen
class Dataset:
    backend: Backend

    @property
    def fqn(self):
        return self.backend.fqn

    @property
    def path(self) -> str:
        """Return the path of the dataset"""
        return self.backend.path

    def snapshots(self) -> tuple[Snapshot, ...]:
        """List all snapshots of this dataset"""
        snapshots = snapshots if (snapshots := self.backend.snapshots()) is not None else ()
        return tuple(sorted(snapshots, key=attrgetter("createtxg")))

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """List all snapshots of this dataset"""
        bookmarks = bookmarks if (bookmarks := self.backend.bookmarks()) is not None else ()
        return tuple(sorted(bookmarks, key=attrgetter("createtxg")))

    def find(self, name: str) -> Snapshot:
        """Find snapshot with given name"""
        log = structlog.get_logger()
        log.debug(f"finding snapshot '{name}' on '{self.fqn}'")
        try:
            return next(s for s in self.snapshots() if s.name == name)
        except StopIteration:
            raise ValueError(f"No snapshot '{name}' in '{self.path}'")

    def snapshot(self, name: str) -> None:
        """Create a snapshot with the given name"""
        self.backend.snapshot(name)

    def bookmark(self, snapshot: str) -> None:
        """Create a bookmark from the given snapshot"""
        self.backend.bookmark(snapshot)

    def exists(self) -> bool:
        """Returns true if the dataset exists"""
        return self.backend.exists()

    @multimethod
    def send(self, token: str) -> Stream:
        """Create a resume stream"""
        return self.backend.send(token)

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark) -> Stream:
        """Create an incremental stream"""
        return self.backend.send(snapshot, ancestor)

    @multimethod
    def send(self, snapshot: Snapshot) -> Stream:
        """Create a full stream"""
        return self.backend.send(snapshot)

    def recv(self, stream: Stream, *, pipes: Sequence[tuple[str, ...]] = (), dry_run: bool) -> None:
        """Consume a stream to produce a snapshot on the target (self)"""
        self.backend.recv(stream=stream, pipes=pipes, dry_run=dry_run)

    def resume_token(self) -> Optional[str]:
        """Returns a resume token of a previously interrupted recv"""
        return self.backend.resume_token()

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """Consume a stream to produce a snapshot on the target (self)"""
        return self.backend.destroy(snapshots, dry_run=dry_run)


def ancestor(snapshot: Snapshot, source: Dataset, target: Dataset) -> Optional[Snapshot | Bookmark]:
    """Find common ancestor"""
    # on the source side, it can be a snapshot or a bookmark
    # on the target side we need a snapshot

    # only snapshots/bookmarks which are older than snapshot.createtxg
    candidates = filter(
        lambda s: s.createtxg < snapshot.createtxg,
        source.snapshots() + source.bookmarks(),
    )
    candidates = sorted(candidates, key=lambda s: (s.createtxg, isinstance(s, Snapshot)))

    target_guids = {snap.guid: snap for snap in target.snapshots()}
    for snapshot in reversed(candidates):
        if snapshot.guid in target_guids:
            return snapshot
    return None


def send(
    snapshot: Snapshot,
    source: Dataset,
    target: Dataset,
    *,
    pipes: Sequence[tuple[str, ...]] = (),
    dry_run: bool,
) -> None:
    """Send snapshot from source to target"""
    log = structlog.get_logger()

    if snapshot.guid not in map(attrgetter("guid"), source.snapshots()):
        raise FileNotFoundError(f"snapshot '{snapshot.fqn}' not in source '{source.fqn}'")

    if not target.exists():
        stream = source.send(snapshot)
        log.info(f"rift send (full) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        return stream.to(target, pipes=pipes, dry_run=dry_run)

    if snapshot.guid in map(attrgetter("guid"), target.snapshots()):
        log.info(f"rift send '{snapshot.fqn}' to '{target.fqn}' skipped since snapshot already on target")
        return None

    elif (token := target.resume_token()) is not None:
        stream = source.send(token)
        log.info(f"rift send (resume) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        log.debug(f"resume send with token='{token}' [{sizeof_fmt(stream.size())}]")
        return stream.to(target, pipes=pipes, dry_run=dry_run)

    elif (base := ancestor(snapshot, source, target)) is not None:
        stream = source.send(snapshot, base)
        log.info(f"rift send (incremental) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        log.debug(f"incremental send '{snapshot.fqn}' from base '{base.fqn}' [{sizeof_fmt(stream.size())}]")
        return stream.to(target, pipes=pipes, dry_run=dry_run)

    else:
        stream = source.send(snapshot)
        log.info(f"rift send (full) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        return stream.to(target, pipes=pipes, dry_run=dry_run)


def sync(
    source: Dataset,
    target: Dataset,
    *,
    pipes: Sequence[tuple[str, ...]] = (),
    regex: str = ".*",
    dry_run: bool,
) -> None:
    """Send all snapshots from source to target which match the regex"""
    log = structlog.get_logger()
    log.info(f"rift sync newer snapshots from '{source.fqn}' to '{target.fqn}'")

    if not target.exists() or len(target.snapshots()) == 0:
        # send all snapshots
        missing = source.snapshots()
        to_sync = missing
    else:
        # find all snapshots in source that are not in target
        missing = [s for s in source.snapshots() if s.guid not in map(attrgetter("guid"), target.snapshots())]

        # get latest snapshot from target
        latest_guid = target.snapshots()[-1].guid
        latest = next(s for s in source.snapshots() if s.guid == latest_guid)
        log.debug(f"latest snapshots on target is: {latest.fqn}, guid={latest.guid}, createtxg={latest.createtxg}")

        # sync only newer snapshots
        to_sync = [s for s in missing if s.createtxg > latest.createtxg]

    # only sync snapshots which match regex
    p = re.compile(regex)
    to_sync = [s for s in to_sync if p.match(s.name)]
    log.info(f"{len(to_sync)} snapshots need syncing")

    for s in source.snapshots():
        if s in to_sync:
            log.debug(f"[to be sync    ] {s.name}")
        elif not p.match(s.name):
            log.debug(f"[filtered      ] {s.name}")
        elif s in missing:
            log.debug(f"[too old       ] {s.name}")
        elif not p.match(s.name):
            log.debug(f"[excluded      ] {s.name}")
        else:
            log.debug(f"[already synced] {s.name}")

    # send missing snapshots
    for snapshot in to_sync:
        send(snapshot, source, target, pipes=pipes, dry_run=dry_run)


def prune(dataset: Dataset, policy: dict[str, int], *, dry_run: bool) -> None:
    """Prune old snapshots"""
    # retention policy is a dict, mapping from regex to the number of snapshots to keep
    # for example, {"rift_.*_hourly": 24, "rift_.*_weekly": 7}

    log = structlog.get_logger()

    # collect all snapshots to delete
    obsolete = []
    for regex, keep in policy.items():
        # get all snapshots matching regex
        p = re.compile(regex)
        snapshots = [s for s in dataset.snapshots() if p.match(s.name)]
        # retain the last count snapshots
        retain = set(snapshots[-keep:]) if keep > 0 else {}
        # delete everything that should not be retained
        destroy = [s.name for s in snapshots if s not in retain]
        obsolete += destroy

        log.info(
            f"rift prune '{dataset.fqn}' of '{regex}': {keep}/{len(retain)}/{len(snapshots)} destroy {len(destroy)}"
        )

        # create debug output
        for s in snapshots:
            log.debug(f"{'[prune]' if s.name in destroy else '[keep ]'} {s.name}")

    # destroy snapshots
    dataset.destroy(obsolete, dry_run=dry_run)
