import random
from typing import Optional, Sequence

from attrs import Factory, define

from rift.commands import NoSuchDatasetError, Runner
from rift.snapshots import Bookmark, Snapshot


def fqn2token(fqn: str) -> str:
    return fqn.encode("utf-8").hex()


def token2fqn(token: str) -> str:
    return bytes.fromhex(token).decode("utf-8")


@define
class InMemoryDataset:
    path: str  # e.g. pool/A
    remote: Optional[str] = None  # e.g. user@remote
    token: Optional[str] = None  # resume token for zfs send
    data: dict[str, str] = Factory(dict)  # fqn -> zfs list entry
    createtxg: int = 0  # zfs transaction id
    rng: random.Random = None

    def __attrs_post_init__(self):
        self.rng = random.Random(self.path)
        # simulate that zfs transaction ids are not unique across hosts
        self.createtxg = self.rng.randint(0, 1000)

    def find(self, fqn: str) -> Snapshot | Bookmark:
        if fqn not in self.data:
            raise RuntimeError(f"snapshot {fqn} does not exist")
        return Snapshot.parse(self.data[fqn]) if "@" in fqn else Bookmark.parse(self.data[fqn])

    def snapshots(self) -> list[str]:
        """Returns all lines in the dataset that are snapshots"""
        snapshots = list([line for line in self.data.values() if "@" in line.split()[0]])
        self.rng.shuffle(snapshots)  # make sure rift does not depend on the order of snapshots returned by zfs
        return snapshots

    def bookmarks(self) -> list[str]:
        """Returns all lines in the dataset that are bookmarks"""
        bookmarks = list([line for line in self.data.values() if "#" in line.split()[0]])
        self.rng.shuffle(bookmarks)  # make sure rift does not depend on the order of bookmarks returned by zfs
        return bookmarks

    def snapshot(self, name: str, *other: str) -> "InMemoryDataset":
        """
        Create one or multiple snapshots for the dataset.
        :param name: The name for the snapshot, e.g. snap1
        """
        for name in (name, *other):
            self.createtxg += 1
            fqn = f"{self.path}@{name}"
            self.data[fqn] = f"{fqn}\tuuid:{fqn}\t{self.createtxg}"
        return self

    def bookmark(self, snapshot_name: str, bookmark_name: str = None) -> "InMemoryDataset":
        """
        Bookmarks a bookmark from a snapshot.
        :param snapshot_name: The name of the snapshot e.g. snap1
        :param bookmark_name: Optional name for the bookmark. Defaults to the snapshot name if not provided.
        """
        bookmark_name = bookmark_name or snapshot_name
        fqn = f"{self.path}@{snapshot_name}"
        if fqn not in self.data:
            raise RuntimeError(f"snapshot {fqn} does not exist")
        fqn, uuid, createtxg = self.data[fqn].split()
        self.data[f"{self.path}#{bookmark_name}"] = f"{self.path}#{bookmark_name}\t{uuid}\t{createtxg}"
        return self

    def recv(self, snapshot: Snapshot) -> "InMemoryDataset":
        """Insert the received snapshot into the dataset."""
        self.createtxg += 1
        fqn = f"{self.path}@{snapshot.name}"
        self.data[fqn] = f"{fqn}\t{snapshot.guid}\t{self.createtxg}"
        return self

    def destroy(self, *snapshots: str) -> "InMemoryDataset":
        """
        Deletes specified snapshots from the dataset.
        :param snapshots: The names of the snapshots to be deleted e.g. snap1,snap2
        """
        to_delete = {f"{self.path}@{snap}" for snap in snapshots}
        for fqn in list(self.data.keys()):
            if fqn in to_delete:
                del self.data[fqn]
        return self

    def entries(self) -> list[str]:
        """
        Retrieves all snapshots/bookmarks.
        """
        return [entry for entry in self.data.values()]


@define
class InMemoryFS(Runner):
    """
    Simulates an in-memory zfs file system for managing and manipulating datasets, aimed at
    testing or mocking behavior without interacting with real storage systems.
    """

    datasets: dict[str, InMemoryDataset] = Factory(dict)  # mapping from path to dataset
    recorded: list[str] = Factory(list)  # track calls to self.run

    @staticmethod
    def of(*datasets: InMemoryDataset):
        # to make it easier, identity datasets by path, ignoring remotes
        # hence, paths must be unique even though zfs allows multiple datasets with the same path on remotes
        paths = [dataset.path for dataset in datasets]
        assert len(paths) == len(set(paths)), "all datasets must have unique paths"
        return InMemoryFS(dict(zip(paths, datasets)))

    def find(self, path: str, create_if_missing: bool = False) -> InMemoryDataset:
        if path not in self.datasets:
            if create_if_missing:
                self.datasets[path] = InMemoryDataset(path)
            else:
                raise NoSuchDatasetError(f"dataset {path} does not exist", None)
        return self.datasets[path]

    def run(self, command: Sequence[str], *others: Sequence[str]) -> str:
        commands = [command] + list(others)
        self.recorded.append(" | ".join(map(" ".join, commands)))

        def remove_remote(command):
            return command[1:] if command[0] == "ssh" else command

        # match zfs list snapshot
        if "zfs list" in " ".join(command) and "snapshot" in command:
            command = remove_remote(command)
            path = command[-1]
            return "\n".join(self.find(path).snapshots())

        # match zfs list bookmark
        if "zfs list" in " ".join(command) and "bookmark" in command:
            command = remove_remote(command)
            path = command[-1]
            return "\n".join(self.find(path).bookmarks())

        # match zfs snapshot
        if "zfs snapshot" in " ".join(command):
            command = remove_remote(command)
            path, snapshot_name = command[-1].split("@")
            self.find(path).snapshot(snapshot_name)
            return ""

        # match zfs snapshot
        if "zfs bookmark" in " ".join(command):
            command = remove_remote(command)
            path, snapshot_name = command[-2].split("@")
            path, bookmark_name = command[-1].split("#")
            self.find(path).bookmark(snapshot_name, bookmark_name)
            return ""

        # match zfs send -t 23479 | zfs receive pool/B
        if "zfs send" in " ".join(commands[0]) and "-t" in commands[0] and "zfs receive" in " ".join(commands[-1]):
            send_command = remove_remote(commands[0])
            fqn = token2fqn(send_command[-1])
            src_path, snapshot_name = fqn.split("@")
            dst_path = next((part for part in commands[-1] if "/" in part))  # find dataset path in commands
            snapshot = self.find(src_path).find(fqn)
            self.find(dst_path, create_if_missing=True).recv(snapshot)
            return ""

        # match zfs send ... | zfs receive
        if "zfs send" in " ".join(commands[0]) and "zfs receive" in " ".join(commands[-1]):
            fqn = commands[0][-1]
            src_path, snapshot_name = fqn.split("@")
            dst_path = next((part for part in commands[-1] if "/" in part))  # find dataset path in commands
            snapshot = self.find(src_path).find(fqn)
            self.find(dst_path, create_if_missing=True).recv(snapshot)
            return ""

        # match zfs send pool/A@s1 -P -n -v
        if "zfs send" in " ".join(command):
            return """full    pool/A@s1       3711767360\nsize    3711767360"""

        # match zfs get receive_resume_token
        if "receive_resume_token" in " ".join(command):
            command = remove_remote(command)
            path = command[-1]
            return self.find(path).token

        # match zfs destroy pool/A@s1,s2
        if "zfs destroy" in " ".join(command):
            command = remove_remote(command)
            path, snapshots = command[-1].split("@")
            self.find(path).destroy(*snapshots.split(","))
            return ""

        raise NotImplementedError("> " + " | ".join(map(" ".join, commands)))

    def entries(self) -> list[str]:
        """Retrieves all snapshots/bookmarks from all datasets."""
        return [entry for dataset in self.datasets.values() for entry in dataset.entries()]

    def __hash__(self):
        return hash(id(self))
