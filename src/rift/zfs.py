import re
from functools import cache
from shlex import split
from typing import Collection, Optional, Sequence

import structlog
from attrs import field, frozen
from multimethod import multimethod

from rift.commands import NoSuchDatasetError, Runner
from rift.datasets import Backend, Remote, Stream
from rift.snapshots import Bookmark, Snapshot


def ssh(remote: Optional[Remote]) -> tuple[str, ...]:
    if remote is None:
        return ()
    return ("ssh", remote.host) + sum((("-o", o) for o in remote.options), ()) + ("--",)


@frozen(slots=False)
class ZfsStream(Stream):
    args: tuple[str, ...]
    runner: Runner

    def __attrs_post_init__(self):
        # instance based caches since the @cache decorator operates on classes.
        object.__setattr__(self, "size", cache(self.size))

    def size(self) -> int:
        """Returns the estimated size of the stream in bytes"""
        log = structlog.get_logger()
        log.debug("getting estimate of snapshot (stream) size")
        # get size estimate by running the command in --dry-run mode and parsing output
        output = self.runner.run(self.args + ("-P", "-n", "-v")).split("\n")[-1].strip()
        m = re.match(r"size\s*(\d+)$", output)
        if not m:
            raise RuntimeError(f"cannot parse size form output '{output.strip()}'")
        return int(m.group(1))


@frozen(slots=False)
class ZfsBackend(Backend):
    runner: Runner = field(kw_only=True)

    def __attrs_post_init__(self):
        # instance based caches since the @cache decorator operates on classes.
        object.__setattr__(self, "snapshots", cache(self.snapshots))
        object.__setattr__(self, "bookmarks", cache(self.bookmarks))
        object.__setattr__(self, "resume_token", cache(self.resume_token))

    def snapshots(self) -> tuple[Snapshot, ...]:
        """
        List all snapshots of this dataset
        @:return None if dataset does not exist and a list of datasets otherwise
        """
        log = structlog.get_logger()
        log.debug(f"retrieving snapshots for '{self.fqn}'")
        args = split(f"zfs list -pHt snapshot -o name,guid,createtxg {self.path}")
        result = self.runner.run(ssh(self.remote) + tuple(args))
        return () if len(result) == 0 else tuple(map(Snapshot.parse, result.split("\n")))

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """
        List all bookmarks of this dataset
        @:return None if dataset does not exist and a list of bookmarks otherwise
        """
        log = structlog.get_logger()
        log.debug(f"retrieving bookmarks for '{self.fqn}'")
        args = split(f"zfs list -pHt bookmark -o name,guid,createtxg {self.path}")
        result = self.runner.run(ssh(self.remote) + tuple(args))
        return () if len(result) == 0 else tuple(map(Bookmark.parse, result.split("\n")))

    def snapshot(self, name: str) -> None:
        """Create a snapshot with the given name"""
        log = structlog.get_logger()
        log.info(f"creating snapshot '{self.fqn}@{name}'")
        self.cache_clear()  # invalidate caches
        args = ("zfs", "snapshot", f"{self.path}@{name}")
        self.runner.run(ssh(self.remote) + args)

    def bookmark(self, snapshot: str) -> None:
        """Create a bookmark from the given snapshot"""
        log = structlog.get_logger()
        log.info(f"creating bookmark '{self.fqn}#{snapshot}'")
        self.cache_clear()  # invalidate caches
        args = ("zfs", "bookmark", f"{self.path}@{snapshot}", f"{self.path}#{snapshot}")
        self.runner.run(ssh(self.remote) + args)

    def exists(self) -> bool:
        """Returns true if the dataset exists"""
        try:
            self.snapshots()
            return True
        except NoSuchDatasetError:
            return False

    @multimethod
    def send(self, token: str, *, options: tuple[str, ...] = ()) -> Stream:
        """Create a resume stream"""
        return ZfsStream(ssh(self.remote) + ("zfs", "send", *options, "-t", token), self.runner)

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark, *, options: tuple[str, ...] = ()) -> Stream:
        # use -i flag since we may want to filter intermediary snapshots
        return ZfsStream(
            ssh(self.remote) + ("zfs", "send", *options, "-i", ancestor.fqn, snapshot.fqn), self.runner
        )

    @multimethod
    def send(self, snapshot: Snapshot, *, options: tuple[str, ...] = ()) -> Stream:
        """Create a full stream"""
        return ZfsStream(ssh(self.remote) + ("zfs", "send", *options, snapshot.fqn), self.runner)

    def recv(
        self,
        stream: Stream,
        *,
        options: tuple[str, ...] = (),
        pipes: Sequence[tuple[str, ...]] = (),
        dry_run: bool,
    ) -> None:
        assert isinstance(stream, ZfsStream), f"do not know how to recv {stream}"
        self.cache_clear()
        args = ssh(self.remote) + ("zfs", "receive", *options, self.path) + (("-n", "-v") if dry_run else ())
        # replace templates
        pipes = [tuple(map(lambda arg: arg.format(size=stream.size()), pipe)) for pipe in pipes]
        self.runner.run(stream.args, *pipes, args)

    def resume_token(self) -> Optional[str]:
        """Returns a resume token of a previously interrupted recv"""
        log = structlog.get_logger()
        log.debug(f"looking for resume token on {self.fqn}")
        args = ("zfs", "get", "-H", "-o", "value", "receive_resume_token", self.path)
        result = self.runner.run(ssh(self.remote) + args)
        return None if result == "-" else result

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """Consume a stream to produce a snapshot on the target (self)"""
        if len(snapshots) == 0:
            return

        self.cache_clear()
        fqns = f"{self.path}@" + ",".join(snapshots)
        args = ("zfs", "destroy") + (("-n", "-v") if dry_run else ()) + (fqns,)
        self.runner.run(ssh(self.remote) + args)

    def cache_clear(self):
        getattr(self, "snapshots").cache_clear()
        getattr(self, "bookmarks").cache_clear()
        getattr(self, "resume_token").cache_clear()
