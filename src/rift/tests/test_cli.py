from typing import Iterable, Sequence

import click
import pytest
from click.testing import CliRunner
from freezegun import freeze_time
from precisely import assert_that, equal_to

import rift.cli
from rift.cli import DatasetType, SnapshotType, prune, send, snapshot
from rift.commands import Runner


class RunnerMock(Runner):
    def __init__(self, returns: Iterable):
        self.recorded: list[Sequence[str]] = []
        self.returns = iter(returns)

    def run(self, command: Sequence[str], *others: Sequence[str]) -> str:
        self.recorded.append(" | ".join(map(" ".join, [command] + list(others))))
        return next(self.returns)

    def __hash__(self):
        return hash(id(self))


def test_dataset_type_no_remote():
    type = DatasetType()
    remote, dataset = type.convert("rpool", None, None)
    assert_that(remote, equal_to(None))
    assert_that(dataset, equal_to("rpool"))


def test_dataset_type_remote():
    type = DatasetType()
    remote, dataset = type.convert("user@nas:rpool", None, None)
    assert_that(remote, equal_to("user@nas"))
    assert_that(dataset, equal_to("rpool"))


def test_dataset_type_invalid():
    type = DatasetType()
    with pytest.raises(click.exceptions.BadParameter):
        type.convert("rpool@snap", None, None)


def test_snapshot_type_no_remote():
    type = SnapshotType()
    remote, dataset, snapshot = type.convert("rpool@rift_2025-12-06_05:15:03_frequently", None, None)
    assert_that(remote, equal_to(None))
    assert_that(dataset, equal_to("rpool"))
    assert_that(snapshot, equal_to("rift_2025-12-06_05:15:03_frequently"))


def test_snapshot_type_remote():
    type = SnapshotType()
    remote, dataset, snapshot = type.convert("user@nas:rpool@rift_2025-12-06_05:15:03_frequently", None, None)
    assert_that(remote, equal_to("user@nas"))
    assert_that(dataset, equal_to("rpool"))
    assert_that(snapshot, equal_to("rift_2025-12-06_05:15:03_frequently"))


def test_snapshot_type_invalid():
    type = SnapshotType()
    with pytest.raises(click.exceptions.BadParameter):
        type.convert("rpool", None, None)


@freeze_time("2012-01-14")
def test_snapshot():
    runner = CliRunner(catch_exceptions=False)

    rift.cli.runner = RunnerMock(
        returns=[],
    )

    result = runner.invoke(snapshot, ["user@remote:backup/rpool"])

    if result.stderr.strip():
        raise RuntimeError(result.stderr)

    assert_that(
        rift.cli.runner.recorded,
        equal_to(["ssh user@remote -- zfs snapshot backup/rpool@rift_2012-01-14_00:00:00"]),
    )


def test_send():
    runner = CliRunner(catch_exceptions=False)

    rift.cli.runner = RunnerMock(
        returns=[
            "rpool@rift_2025-12-06_06:15:10_frequently      372815780617067482      1337733",
            "",
            None,
            "",
            "full    rpool@rift_2025-12-06_05:15:04_frequently       3711767360\nsize    3711767360",
        ],
    )

    result = runner.invoke(send, ["rpool@rift_2025-12-06_06:15:10_frequently", "user@remote:backup/rpool"])

    if result.stderr.strip():
        raise RuntimeError(result.stderr)

    assert_that(
        rift.cli.runner.recorded,
        equal_to(
            [
                "zfs list -pHt snapshot -o name,guid,createtxg rpool",
                "ssh user@remote -- zfs list -pHt snapshot -o name,guid,createtxg backup/rpool",
                "ssh user@remote -- zfs get -H -o value receive_resume_token backup/rpool",
                "zfs list -pHt bookmark -o name,guid,createtxg rpool",
                "zfs send -w rpool@rift_2025-12-06_06:15:10_frequently -P -n -v",
                "zfs send -w rpool@rift_2025-12-06_06:15:10_frequently | ssh user@remote -- zfs receive -s -u backup/rpool",
            ]
        ),
    )


def test_send_pipes():
    runner = CliRunner(catch_exceptions=False)

    rift.cli.runner = RunnerMock(
        returns=[
            "rpool@rift_2025-12-06_06:15:10_frequently      372815780617067482      1337733",
            "",
            None,
            "",
            "full    rpool@rift_2025-12-06_05:15:04_frequently       3711767360\nsize    3711767360",
        ],
    )

    result = runner.invoke(
        send,
        [
            "rpool@rift_2025-12-06_06:15:10_frequently",
            "user@remote:backup/rpool",
            "--pipes",
            "mbuffer -r 1M",
            "--pipes",
            "pv",
        ],
    )

    if result.stderr.strip():
        raise RuntimeError(result.stderr)

    assert_that(
        rift.cli.runner.recorded,
        equal_to(
            [
                "zfs list -pHt snapshot -o name,guid,createtxg rpool",
                "ssh user@remote -- zfs list -pHt snapshot -o name,guid,createtxg backup/rpool",
                "ssh user@remote -- zfs get -H -o value receive_resume_token backup/rpool",
                "zfs list -pHt bookmark -o name,guid,createtxg rpool",
                "zfs send -w rpool@rift_2025-12-06_06:15:10_frequently -P -n -v",
                "zfs send -w rpool@rift_2025-12-06_06:15:10_frequently | mbuffer -r 1M | pv | ssh user@remote -- zfs receive -s -u backup/rpool",
            ]
        ),
    )


def test_prune():
    runner = CliRunner(catch_exceptions=False)

    rift.cli.runner = RunnerMock(
        returns=[
            """
        rpool@rift_2025-12-06_05:15:03_frequently      5163463594405973537     1336840
        rpool@rift_2025-12-06_05:30:52_frequently      8257781083657354115     1337081
        rpool@rift_2025-12-06_05:46:09_frequently      12656343660155722312    1337291
        rpool@rift_2025-12-06_06:00:20_frequently      13371907294208531207    1337492
        rpool@rift_2025-12-06_06:15:10_frequently      372815780617067482      1337733
        """.strip()
        ],
    )

    result = runner.invoke(prune, ["rpool", "--keep", 2, "rift_*"])

    if result.stderr.strip():
        raise RuntimeError(result.stderr)

    assert_that(
        rift.cli.runner.recorded,
        equal_to(
            [
                "zfs list -pHt snapshot -o name,guid,createtxg rpool",
                "zfs destroy rpool@rift_2025-12-06_05:15:03_frequently,rift_2025-12-06_05:30:52_frequently,rift_2025-12-06_05:46:09_frequently",
            ]
        ),
    )
