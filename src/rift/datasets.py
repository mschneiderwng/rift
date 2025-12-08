import re
from functools import cache
from operator import attrgetter
from shlex import split
from typing import Collection, Optional, Sequence

import structlog
from attrs import field, frozen
from multimethod import multimethod

from rift.commands import NoSuchDatasetError, Runner
from rift.snapshots import Bookmark, Snapshot


@frozen
class Remote:
    """
    Represents a remote resource with `user@host` and an optional set of ssh options.

    :param host: The username and hostname the remote resource e.g. `user@host`.
    :param options: A tuple of ssh options e.g. `Compression=yes` which will be passed to `ssh` with the "-o" flag.
    """

    host: str
    options: tuple[str, ...] = ()


def ssh(remote: Optional[Remote]) -> tuple[str, ...]:
    """
    Builds an SSH command as a tuple of strings based on the provided remote details.

    This function generates a tuple representing the SSH command to connect to a
    given remote host. If the remote is not provided, it returns an empty tuple.

    :param remote: Optional remote connection configuration containing host and options.
    :return: A tuple of strings representing the constructed SSH command.
    """
    if remote is None:
        return ()
    return ("ssh", remote.host) + sum((("-o", o) for o in remote.options), ()) + ("--",)


@frozen(slots=False)
class Stream:
    """
    Represents ZFS data stream i.e., the beginning of the ZFS send/recv pipe. For example,
    `zfs send -i src/data@snap1 src/data@snap2`.

    :param args: The `zfs send` command arguments as a tuple of strings.
        For example, `("zfs", "send", "-i", "src/data@snap1", "src/data@snap2")`
    :param runner: A way to execute shell commands.
    """

    args: tuple[str, ...]
    runner: Runner

    def __attrs_post_init__(self):
        # instance-based caches since the @cache decorator operates on classes.
        object.__setattr__(self, "size", cache(self.size))

    def size(self) -> int:
        """Returns the estimated size of the stream in bytes"""
        log = structlog.get_logger()
        log.debug("getting estimate of snapshot (stream) size")
        # get a size estimate by running the command in --dry-run mode and parsing output
        output = self.runner.run(self.args + ("-P", "-n", "-v")).split("\n")[-1].strip()
        m = re.match(r"size\s*(\d+)$", output)
        if not m:
            raise RuntimeError(f"cannot parse size form output '{output.strip()}'")
        return int(m.group(1))


@frozen(slots=False)
class Dataset:
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
    runner: Runner = field(kw_only=True)

    def __attrs_post_init__(self):
        # instance-based caches since the @cache decorator operates on classes.
        object.__setattr__(self, "snapshots", cache(self.snapshots))
        object.__setattr__(self, "bookmarks", cache(self.bookmarks))
        object.__setattr__(self, "resume_token", cache(self.resume_token))

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
        Retrieves all snapshots for the given filesystem. The snapshots are obtained by
        running a `zfs list` command.

        :raises RuntimeError: If the subprocess command fails during execution.
        :raises NoSuchDatasetError: If the given filesystem does not exist.

        :return: A tuple containing all parsed `Snapshot` objects from the retrieved
            snapshot data. If no snapshots exist, an empty tuple is returned.
        """
        log = structlog.get_logger()
        log.debug(f"retrieving snapshots for '{self.fqn}'")
        args = split(f"zfs list -pHt snapshot -o name,guid,createtxg {self.path}")
        result = self.runner.run(ssh(self.remote) + tuple(args))
        snapshots = () if len(result) == 0 else tuple(map(Snapshot.parse, result.split("\n")))
        return tuple(sorted(snapshots, key=attrgetter("createtxg")))

    def bookmarks(self) -> tuple[Bookmark, ...]:
        """
        Retrieves all bookmarks for the given filesystem. The bookmarks are obtained by
        running a `zfs list` command.

        :raises RuntimeError: If the subprocess command fails during execution.
        :raises NoSuchDatasetError: If the given filesystem does not exist.

        :return: A tuple containing all parsed `Bookmark` objects from the retrieved
            bookmark data. If no bookmarks exist, an empty tuple is returned.
        """
        log = structlog.get_logger()
        log.debug(f"retrieving bookmarks for '{self.fqn}'")
        args = split(f"zfs list -pHt bookmark -o name,guid,createtxg {self.path}")
        result = self.runner.run(ssh(self.remote) + tuple(args))
        bookmarks = () if len(result) == 0 else tuple(map(Bookmark.parse, result.split("\n")))
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
        """
        Create a snapshot for the given ZFS filesystem path.

        This method creates a snapshot of the ZFS filesystem using the provided name.

        :param name: The name to assign to the snapshot.
        """
        log = structlog.get_logger()
        log.info(f"creating snapshot '{self.fqn}@{name}'")
        self.cache_clear()  # invalidate caches
        args = ("zfs", "snapshot", f"{self.path}@{name}")
        self.runner.run(ssh(self.remote) + args)

    def bookmark(self, snapshot: str) -> None:
        """
        Bookmark a given snapshot to save its state. This function creates a permanent
        bookmark for a specific ZFS snapshot.

        :param snapshot: The name of the snapshot to create a bookmark for without path@ prefix, e.g. snap1.
        """
        log = structlog.get_logger()
        log.info(f"creating bookmark '{self.fqn}#{snapshot}'")
        self.cache_clear()  # invalidate caches
        args = ("zfs", "bookmark", f"{self.path}@{snapshot}", f"{self.path}#{snapshot}")
        self.runner.run(ssh(self.remote) + args)

    def exists(self) -> bool:
        """
        Determines whether the dataset exists.

        :return: A boolean value indicating whether the dataset exists.
        """
        # This method checks for the presence of the dataset by attempting to retrieve
        # its snapshots. If the dataset does not exist, self.snapshots() raises a `NoSuchDatasetError`.
        try:
            self.snapshots()
            return True
        except NoSuchDatasetError:
            return False

    @multimethod
    def send(self, token: str, *, options: tuple[str, ...] = ()) -> Stream:
        """
        Constructs a resumeable ZFS send stream to a remote destination. It stores the first part of the pipe, e.g.
        `ssh user@remote -- zfs send -t token` along with additional ZFS options.

        :param token: The zfs resume token.
        :param options: Additional options for the ZFS send command.
        :return: A `Stream` object encapsulating the constructed ZFS send stream.
        """
        return Stream(ssh(self.remote) + ("zfs", "send", *options, "-t", token), self.runner)

    @multimethod
    def send(self, snapshot: Snapshot, ancestor: Snapshot | Bookmark, *, options: tuple[str, ...] = ()) -> Stream:
        """
        Constructs an incremental ZFS send stream to a remote destination. It stores the first part of the pipe, e.g.
        `ssh user@remote -- zfs send -i src/data@snap1 src/data@snap2` along with additional ZFS options.

        :param snapshot: The ZFS snapshot to be sent.
        :param ancestor: The ZFS snapshot or bookmark indicating the ancestor snapshot for an incremental send.
        :param options: Additional options for the ZFS send command.
        :return: A `Stream` object encapsulating the constructed ZFS send stream.
        """
        # use -i flag since we may want to filter intermediary snapshots
        return Stream(ssh(self.remote) + ("zfs", "send", *options, "-i", ancestor.fqn, snapshot.fqn), self.runner)

    @multimethod
    def send(self, snapshot: Snapshot, *, options: tuple[str, ...] = ()) -> Stream:
        """
        Constructs a full ZFS send stream to a remote destination. It stores the first part of the pipe, e.g.
        `ssh user@remote -- zfs send src/data@snap1` along with additional ZFS options.

        :param snapshot: The ZFS snapshot to be sent.
        :param options: Additional options for the ZFS send command.
        :return: A `Stream` object encapsulating the constructed ZFS send stream.
        """
        return Stream(ssh(self.remote) + ("zfs", "send", *options, snapshot.fqn), self.runner)

    def recv(
        self,
        stream: Stream,
        *,
        options: tuple[str, ...] = (),
        pipes: Sequence[tuple[str, ...]] = (),
        dry_run: bool,
    ) -> None:
        """
        Constructs the command for a ZFS send/recv pipe. The stream contains the beginning of the pipe, e.g.
        `zfs send src/data@snap1` and this method appends `zfs recv dest/data` along with additional ZFS options.

        :param stream: The `zfs send` command to be received.
        :param options: Additional ZFS receive options provided as a tuple of strings.
        :param pipes: Sequence of additional commands to pipe in between the send and receive commands.
        :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
        """
        assert isinstance(stream, Stream), f"do not know how to recv {stream}"
        self.cache_clear()  # invalidate caches
        # construct zfs recv command
        args = ssh(self.remote) + ("zfs", "receive", *options, self.path) + (("-n", "-v") if dry_run else ())
        # replace templates in piped commands
        pipes = [tuple(map(lambda arg: arg.format(size=stream.size()), pipe)) for pipe in pipes]
        # execute all commands (zfs send | pipe1 | pipe2 | zfs recv)
        self.runner.run(stream.args, *pipes, args)

    def resume_token(self) -> Optional[str]:
        """
        Retrieve the resume token for a ZFS dataset.

        The resume token can be used to resume a previously interrupted ZFS receive operation.
        If no token exists, the method returns None.

        :returns: The resume token as a string if it exists, otherwise None.
        """
        log = structlog.get_logger()
        log.debug(f"looking for resume token on {self.fqn}")
        args = ("zfs", "get", "-H", "-o", "value", "receive_resume_token", self.path)
        result = self.runner.run(ssh(self.remote) + args)
        return None if result == "-" else result

    def destroy(self, snapshots: Collection[str], dry_run: bool) -> None:
        """
        Destroy specified ZFS snapshots using `zfs destroy`. If the dry_run option is enabled, the method will
        simulate the destruction operation without actually performing it.

        :param snapshots: A collection of snapshot names to be destroyed. Must not be empty.
        :param dry_run: Boolean flag to determine if the operation should be executed as a dry run.
        """
        if len(snapshots) == 0:
            return

        self.cache_clear()  # invalidate caches
        # maps [s1,s2] to "source/A@s1,s2"
        fqns = f"{self.path}@" + ",".join(snapshots)
        # append -n and -v flags if dry_run is enabled
        args = ("zfs", "destroy") + (("-n", "-v") if dry_run else ()) + (fqns,)
        # execute destroy command (zfs destroy source/A@s1,s2)
        self.runner.run(ssh(self.remote) + args)

    def cache_clear(self):
        """
        Clears all cached properties of the object.
        """
        getattr(self, "snapshots").cache_clear()
        getattr(self, "bookmarks").cache_clear()
        getattr(self, "resume_token").cache_clear()
