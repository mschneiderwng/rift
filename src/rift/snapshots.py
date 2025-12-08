from attrs import frozen

type guid = str


@frozen
class Snapshot:
    """
    Represents a ZFS snapshot.

    :param fqn: The fully qualified name of the snapshot, e.g. `datset@snap1`.
    :param guid: The zfs guid property of the snapshot.
    :param createtxg: The zfs createtxg property of the snapshot.
    """

    fqn: str
    guid: str
    createtxg: int

    @staticmethod
    def parse(line: str) -> "Snapshot":
        """Parses a snapshot line from `zfs list -pHt snapshot -o name,guid,createtxg`"""
        parts = line.split()
        return Snapshot(parts[0], parts[1], int(parts[2]))

    @property
    def name(self) -> str:
        """The name of the snapshot, e.g. `snap1`"""
        return self.fqn.split("@")[1]


@frozen
class Bookmark:
    """
    Represents a ZFS bookmark.

    :param fqn: The fully qualified name of the snapshot, e.g. `datset#snap1`.
    :param guid: The zfs guid property of the bookmark.
    :param createtxg: The zfs createtxg property of the bookmark.
    """

    fqn: str
    guid: str
    createtxg: int

    @staticmethod
    def parse(line: str) -> "Bookmark":
        """Parses a snapshot line from `zfs list -pHt bookmark -o name,guid,createtxg`"""
        parts = line.split()
        return Bookmark(parts[0], parts[1], int(parts[2]))

    @property
    def name(self) -> str:
        """The name of the snapshot, e.g. `snap1`"""
        return self.fqn.split("#")[1]
