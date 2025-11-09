from attrs import frozen

type guid = str


@frozen
class Snapshot:
    fqn: str
    guid: str
    createtxg: int

    @staticmethod
    def parse(line: str) -> "Snapshot":
        parts = line.split()
        return Snapshot(parts[0], parts[1], int(parts[2]))

    @property
    def name(self):
        return self.fqn.split("@")[1]


@frozen
class Bookmark:
    fqn: str
    guid: str
    createtxg: int

    @staticmethod
    def parse(line: str) -> "Bookmark":
        parts = line.split()
        return Bookmark(parts[0], parts[1], int(parts[2]))

    @property
    def name(self):
        return self.fqn.split("#")[1]
