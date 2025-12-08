import re
from operator import attrgetter
from typing import Optional, Sequence

import structlog

from rift.datasets import Dataset
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
            log.debug(f"[excluded      ] {s.name}")
        elif s in missing:
            log.debug(f"[too old       ] {s.name}")
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
