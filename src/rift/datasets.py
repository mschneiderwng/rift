import re
from operator import attrgetter
from typing import Collection, Optional, Sequence

import structlog
from attrs import define, frozen
from multimethod import multimethod

from rift.snapshots import Bookmark, Snapshot


def sizeof_fmt(num: float, suffix: str = "B") -> str:
    """
    Convert a number of bytes into a human-readable format with appropriate suffixes.

    The function takes a numerical value representing bytes and translates it into
    a more user-friendly format by scaling it and appending the respective binary
    prefix. This is useful for displaying file sizes or memory consumption in a
    readable manner.

    :param num: The numeric value representing the size in bytes.
    :param suffix: A string suffix to append to the result (default is 'B').
    :return: A human-readable string representation of the size with an appropriate suffix.
    """
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


@frozen
class Stream:
    """
    Represents ZFS data stream i.e., the beginning of the ZFS send/recv pipe. For example,
    `zfs send -i src/data@snap1 src/data@snap2`.
    """

    def size(self) -> int:
        """Returns the estimated size of the stream in bytes"""
        raise NotImplementedError


@frozen
class Remote:
    """
    Represents a remote resource with `user@host` and an optional set of ssh options.

    :param host: The username and hostname the remote resource e.g. `user@host`.
    :param options: A tuple of ssh options e.g. `Compression=yes` which will be passed to `ssh` with the "-o" flag.
    """

    host: str
    options: tuple[str, ...] = ()


@define
class Backend:
    """
    Represents a backend for managing datasets, including functionalities for creating, sending, receiving, and
    managing snapshots and bookmarks.

    The Backend class provides an abstraction for interacting with a dataset, enabling operations such as creating
    snapshots, managing bookmarks, sending and receiving data streams, and checking for dataset existence. Each
    operation is expected to be implemented by the subclass of this base class.

    :param path: The filesystem or dataset path associated with this backend.
    :param remote: The remote instance representing the external connectivity configuration, if applicable.
    """

    path: str
    remote: Optional[Remote] = None

    @property
    def fqn(self):
        """
        Computes the fully qualified name (FQN) of a path, incorporating the remote's host
        if the path is associated with a remote location. For example,
         `Backend("source/A", remote=Remote("user@host").fqn` returns "user@host:source/A".

        :return: Fully qualified name (FQN) of the path
        """
        return f"{self.remote.host}:{self.path}" if self.remote is not None else self.path

    def snapshots(self) -> tuple[Snapshot, ...]:
        """
        List all snapshots of this dataset

        :raises RuntimeError: If the subprocess command fails during execution.
        :raises NoSuchDatasetError: If the given filesystem does not exist.

        :return: A tuple containing all parsed `Snapshot` objects from the retrieved
            snapshot data. If no snapshots exist, an empty tuple is returned.
        """
        raise NotImplementedError

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """
        Retrieves all bookmarks for the given filesystem.

        :raises RuntimeError: If the subprocess command fails during execution.
        :raises NoSuchDatasetError: If the given filesystem does not exist.

        :return: A tuple containing all parsed `Bookmark` objects from the retrieved
            bookmark data. If no bookmarks exist, an empty tuple is returned.
        """
        raise NotImplementedError

    def snapshot(self, name: str) -> None:
        """
        Create a snapshot for the given ZFS filesystem path.

        This method creates a snapshot of the ZFS filesystem using the provided name.

        :param name: The name to assign to the snapshot.
        """
        raise NotImplementedError

    def bookmark(self, snapshot: str) -> None:
        """
        Bookmark a given snapshot to save its state. This function creates a permanent
        bookmark for a specific ZFS snapshot.

        :param snapshot: The name of the snapshot to create a bookmark for.
        """
        raise NotImplementedError

    def exists(self) -> bool:
        """Returns true if the dataset exists."""
        raise NotImplementedError

    @multimethod
    def send(self, token: str, *, options: tuple[str, ...]) -> Stream:
        """Constructs a resumeable ZFS send stream."""
        raise NotImplementedError

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark, *, options: tuple[str, ...]) -> Stream:
        """Constructs an incremental ZFS send stream."""
        raise NotImplementedError

    @multimethod
    def send(self, snapshot: Snapshot, *, options: tuple[str, ...]) -> Stream:
        """Constructs a full ZFS send stream."""
        raise NotImplementedError

    def recv(
        self, stream: Stream, *, options: tuple[str, ...], pipes: Sequence[tuple[str, ...]] = (), dry_run: bool
    ) -> None:
        """Consume a stream to produce a snapshot on the target (self).
        :param stream: The `zfs send` command to be received.
        :param options: Additional ZFS receive options provided as a tuple of strings.
        :param pipes: Sequence of additional commands to pipe in between the send and receive commands.
        :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
        """
        raise NotImplementedError

    def resume_token(self) -> Optional[str]:
        """
        Retrieve the resume token for a ZFS dataset.

        The resume token can be used to resume a previously interrupted ZFS receive operation.
        If no token exists, the method returns None.

        :returns: The resume token as a string if it exists, otherwise None.
        """
        raise NotImplementedError

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """
        Destroy specified ZFS snapshots using `zfs destroy`. If the dry_run option is enabled, the method will
        simulate the destruction operation without actually performing it.

        :param snapshots: A collection of snapshot names to be destroyed. Must not be empty.
        :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
        """
        raise NotImplementedError


@frozen
class Dataset:
    """
    Represents a dataset and provides operations for managing snapshots,
    bookmarks, and data streams.

    The class mostly delegates to the backend, performing additional operations on top of the returned results,
    such as sorting and filtering.

    :param backend: Backend system responsible for dataset management.
    """

    backend: Backend

    @property
    def fqn(self):
        """See Backend.fqn"""
        return self.backend.fqn

    @property
    def path(self) -> str:
        """Return the path of the dataset"""
        return self.backend.path

    def snapshots(self) -> tuple[Snapshot, ...]:
        """
        List all snapshots of this dataset (see Backend.snapshots).
        The snapshots are sorted by `createtxg` from oldest to newest.
        This guarantees that the snapshots are in order, independent of the system time or the snapshot name.
        """
        snapshots = snapshots if (snapshots := self.backend.snapshots()) is not None else ()
        return tuple(sorted(snapshots, key=attrgetter("createtxg")))

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """
        List all bookmarks of this dataset (see Backend.bookmarks).
        The bookmarks are sorted by `createtxg` from oldest to newest.
        This guarantees that the bookmarks are in order, independent of the system time or the bookmark name.
        """
        bookmarks = bookmarks if (bookmarks := self.backend.bookmarks()) is not None else ()
        return tuple(sorted(bookmarks, key=attrgetter("createtxg")))

    def find(self, name: str) -> Snapshot:
        """
        Finds a snapshot by its name.

        This method searches through all available snapshots to locate the one with
        the specified `name`. If no snapshot with the provided name is found, a
        ValueError indicating the absence of the snapshot is raised.

        :param name: The name of the snapshot to search for.
        :raises ValueError: If a snapshot with the specified name does not exist.
        :return: The snapshot with the specified name.
        """
        log = structlog.get_logger()
        log.debug(f"finding snapshot '{name}' on '{self.fqn}'")
        try:
            return next(s for s in self.snapshots() if s.name == name)
        except StopIteration:
            raise ValueError(f"No snapshot '{name}' in '{self.path}'")

    def snapshot(self, name: str) -> None:
        """Create a snapshot with the given name (see Backend.snapshot)."""
        self.backend.snapshot(name)

    def bookmark(self, snapshot: str) -> None:
        """Create a bookmark from the given snapshot (see Backend.bookmark)."""
        self.backend.bookmark(snapshot)

    def exists(self) -> bool:
        """Returns true if the dataset exists (see Backend.exists)."""
        return self.backend.exists()

    @multimethod
    def send(self, token: str, *, options: tuple[str, ...]) -> Stream:
        """Constructs a resumeable ZFS send stream (see Backend.send)."""
        return self.backend.send(token, options=options)

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark, *, options: tuple[str, ...]) -> Stream:
        """Constructs an incremental ZFS send stream (see Backend.send)."""
        return self.backend.send(snapshot, ancestor, options=options)

    @multimethod
    def send(self, snapshot: Snapshot, *, options: tuple[str, ...]) -> Stream:
        """Constructs a full ZFS send stream (see Backend.send)."""
        return self.backend.send(snapshot, options=options)

    def recv(
        self, stream: Stream, *, options: tuple[str, ...], pipes: Sequence[tuple[str, ...]] = (), dry_run: bool
    ) -> None:
        """Consume a stream to produce a snapshot on the target (see Backend.recv)."""
        self.backend.recv(stream=stream, options=options, pipes=pipes, dry_run=dry_run)

    def resume_token(self) -> Optional[str]:
        """Retrieve the resume token for a ZFS dataset. (see Backend.resume_token)."""
        return self.backend.resume_token()

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """Destroy specified ZFS snapshots (see Backend.destroy)."""
        return self.backend.destroy(snapshots, dry_run=dry_run)


def ancestor(snapshot: Snapshot, source: Dataset, target: Dataset) -> Optional[Snapshot | Bookmark]:
    """
    Determines the common ancestor for the provided snapshot in the source and target datasets.
    On the source side, it can be a snapshot or a bookmark; on the target side we need a snapshot.

    :param snapshot: The reference `Snapshot` for which a common ancestor should be found.
    :param source: The source `Dataset`, which includes snapshots and bookmarks, to search for ancestor candidates.
    :param target: The target `Dataset`, containing snapshots, to identify overlaps with the source candidates.
    :return: A `Snapshot` or `Bookmark` instance representing the most recent common ancestor,
        or None if no ancestor exists.
    """
    # consider only source snapshots/bookmarks which are older than snapshot.createtxg
    candidates = filter(
        lambda s: s.createtxg < snapshot.createtxg,
        source.snapshots() + source.bookmarks(),
    )
    # sort by createtxg, but snapshots take precedence over bookmarks
    candidates = sorted(candidates, key=lambda s: (s.createtxg, isinstance(s, Snapshot)))

    # save target snapshot guids in a set for fast lookup
    target_guids = {snap.guid: snap for snap in target.snapshots()}

    # go from the newest to oldest source snapshot, looking for a matching guid in the set of target snapshots
    for snapshot in reversed(candidates):
        if snapshot.guid in target_guids:
            return snapshot  # common ancestor found!
    return None


def send(
    snapshot: Snapshot,
    source: Dataset,
    target: Dataset,
    *,
    send_options: tuple[str, ...] = (),
    recv_options: tuple[str, ...] = (),
    pipes: Sequence[tuple[str, ...]] = (),
    dry_run: bool,
) -> None:
    """
    Sends the provided snapshot from the source dataset to the target dataset. This function
    handles various scenarios such as full send, incremental send, resuming interrupted sends,
    or skipping sends if the snapshot already exists on the target.

    :param snapshot: The snapshot to be sent.
    :param source: The source dataset from which the snapshot originates.
    :param target: The target dataset to which the snapshot will be sent.
    :param send_options: Additional options for `zfs send`.
    :param recv_options: Additional options for `zfs recv`.
    :param pipes: Sequence of additional commands to pipe in between the send and receive commands.
    :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
    :raises FileNotFoundError: If the snapshot is not found in the source dataset.
    """
    log = structlog.get_logger()

    # check if snapshot exists in source
    if snapshot.guid not in map(attrgetter("guid"), source.snapshots()):
        raise FileNotFoundError(f"snapshot '{snapshot.fqn}' not in source '{source.fqn}'")

    # if the target dataset does not exist, send full snapshot
    if not target.exists():
        stream = source.send(snapshot, options=send_options)
        log.info(f"rift send (full) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        return target.recv(stream, options=recv_options, pipes=pipes, dry_run=dry_run)

    # if the snapshot already exists on the target, skip send
    if snapshot.guid in map(attrgetter("guid"), target.snapshots()):
        log.info(f"rift send '{snapshot.fqn}' to '{target.fqn}' skipped since snapshot already on target")
        return None

    # if the snapshot is resumable, resume send
    elif (token := target.resume_token()) is not None:
        stream = source.send(token, options=send_options)
        log.info(f"rift send (resume) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        log.debug(f"resume send with token='{token}' [{sizeof_fmt(stream.size())}]")
        return target.recv(stream, options=recv_options, pipes=pipes, dry_run=dry_run)

    # if a common ancestor exists between the snapshot and the target, send incremental snapshot
    elif (base := ancestor(snapshot, source, target)) is not None:
        stream = source.send(snapshot, base, options=send_options)
        log.info(f"rift send (incremental) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        log.debug(f"incremental send '{snapshot.fqn}' from base '{base.fqn}' [{sizeof_fmt(stream.size())}]")
        return target.recv(stream, options=recv_options, pipes=pipes, dry_run=dry_run)

    # send full snapshot otherwise
    else:
        stream = source.send(snapshot, options=send_options)
        log.info(f"rift send (full) [{sizeof_fmt(stream.size())}] '{snapshot.fqn}' to '{target.fqn}'")
        return target.recv(stream, options=recv_options, pipes=pipes, dry_run=dry_run)


def sync(
    source: Dataset,
    target: Dataset,
    *,
    send_options: tuple[str, ...] = (),
    recv_options: tuple[str, ...] = (),
    pipes: Sequence[tuple[str, ...]] = (),
    regex: str = ".*",
    dry_run: bool,
) -> None:
    """
    Sends multiple snapshots from the source dataset to the target dataset. This function identifies
    snapshots that exist in the source dataset but are missing the target dataset.
    It then filters the snapshots based on a provided regex pattern and sends only the
    matching snapshots.

    :param source: The source dataset containing the snapshots to be sent.
    :param target: The target dataset where snapshots will be sent to.
    :param send_options: Additional options for `zfs send`.
    :param recv_options: Additional options for `zfs recv`.
    :param pipes: Sequence of additional commands to pipe in between the send and receive commands.
    :param regex: A regular expression pattern to filter which snapshots to be sent.
    :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
    """
    log = structlog.get_logger()
    log.info(f"rift sync newer snapshots from '{source.fqn}' to '{target.fqn}'")

    # if the target dataset does not exist or is empty, send all snapshots
    if not target.exists() or len(target.snapshots()) == 0:
        missing = source.snapshots()  # snapshots which are missing on target
        to_sync = missing  # snapshots to sync
    else:
        # find all snapshots in source that are not in target
        missing = [s for s in source.snapshots() if s.guid not in map(attrgetter("guid"), target.snapshots())]

        # get the guid of the latest snapshot on the target
        latest_guid = target.snapshots()[-1].guid

        try:
            # find the same snapshot in source by comparing guids
            latest = next(s for s in source.snapshots() if s.guid == latest_guid)
        except StopIteration:
            # there is an unexpected snapshot in the target dataset maybe inserted by a third party.
            # it needs manual rollback on the target side.
            raise RuntimeError(f"latest snapshot on target '{latest_guid}' not found in source '{source.fqn}'")

        log.debug(f"latest snapshots on target is: {latest.fqn}, guid={latest.guid}, createtxg={latest.createtxg}")

        # sync only newer snapshots: collect all snapshots in source which are newer than latest snapshot on target.
        # we are iterating over snapshots in source since createtxg guarantees chronological order but can be
        # different on the target.
        to_sync = [s for s in missing if s.createtxg > latest.createtxg]

    # filter out snapshots that do not match the regex pattern
    p = re.compile(regex)
    to_sync = [s for s in to_sync if p.match(s.name)]
    log.info(f"{len(to_sync)} snapshots need syncing")

    # log the reason why snapshots are not being synced or not
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
        send(
            snapshot, source, target, send_options=send_options, recv_options=recv_options, pipes=pipes, dry_run=dry_run
        )


def prune(dataset: Dataset, policy: dict[str, int], *, dry_run: bool) -> None:
    """
    Prune snapshots from a dataset based on a retention policy.

    This function removes obsolete snapshots from a dataset according to the provided
    retention policy. The retention policy is a dictionary where keys are regex patterns
    matching snapshot names and values are the number of snapshots to retain. Any snapshots
    that exceed the retention limit specified in the policy are marked for destruction. If
    `dry_run` is True, no snapshots are actually deleted, and only debug information is
    logged.

    :param dataset: The dataset containing snapshots.
    :param policy: A dictionary specifying regex patterns as keys and the number of snapshots
                   to retain as values. For example, `{"rift_.*_hourly": 24, "rift_.*_weekly": 7}`.
    :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
    """
    log = structlog.get_logger()

    # collect all snapshots to delete
    obsolete = []
    for regex, keep in policy.items():
        # get all snapshots matching regex
        p = re.compile(regex)
        snapshots = [s for s in dataset.snapshots() if p.match(s.name)]
        # retain the last n snapshots
        retain = set(snapshots[-keep:]) if keep > 0 else {}
        # delete everything that should not be retained
        destroy = [s.name for s in snapshots if s not in retain]
        # collect all snapshots to then delete in a single zfs destroy command
        obsolete += destroy

        log.info(
            f"rift prune '{dataset.fqn}' of '{regex}': {keep}/{len(retain)}/{len(snapshots)} destroy {len(destroy)}"
        )

        # create debug output
        for s in snapshots:
            log.debug(f"{'[prune]' if s.name in destroy else '[keep ]'} {s.name}")

    # destroy snapshots
    dataset.destroy(obsolete, dry_run=dry_run)
