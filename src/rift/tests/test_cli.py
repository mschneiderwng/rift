import click
import pytest
from click.testing import CliRunner
from freezegun import freeze_time
from precisely import assert_that, contains_exactly, equal_to, includes

import rift.cli
from rift.cli import DatasetType, SnapshotType
from rift.tests.mocks import InMemoryDataset, InMemoryFS


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
    with pytest.raises(click.exceptions.BadParameter):  # ty: ignore
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
    with pytest.raises(click.exceptions.BadParameter):  # ty: ignore
        type.convert("rpool", None, None)


@freeze_time("2012-01-14")
def test_snapshot():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.snapshot, ["pool/A", "--no-bookmark", "--name", "rift_{datetime}_daily"])
    assert_that(fs.recorded, contains_exactly("zfs snapshot pool/A@rift_2012-01-14_00:00:00_daily"))


@freeze_time("2012-01-14")
def test_snapshot_remote():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.snapshot, ["user@remote:pool/A", "--no-bookmark", "--name", "rift_{datetime}_daily"])
    assert_that(fs.recorded, contains_exactly("ssh user@remote -- zfs snapshot pool/A@rift_2012-01-14_00:00:00_daily"))


@freeze_time("2012-01-14")
def test_snapshot_ssh_options():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "user@remote"))
    rift.cli.runner = fs
    runner.invoke(
        rift.cli.snapshot,
        ["user@remote:pool/A", "-s", "Compression=yes", "--no-bookmark", "--name", "rift_{datetime}_daily"],
    )
    assert_that(
        fs.recorded,
        contains_exactly("ssh user@remote -o Compression=yes -- zfs snapshot pool/A@rift_2012-01-14_00:00:00_daily"),
    )


@freeze_time("2012-01-14")
def test_bookmark():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.snapshot, ["pool/A", "--bookmark", "--name", "rift_{datetime}_daily"])
    assert_that(
        fs.recorded,
        contains_exactly(
            "zfs snapshot pool/A@rift_2012-01-14_00:00:00_daily",
            "zfs bookmark pool/A@rift_2012-01-14_00:00:00_daily pool/A#rift_2012-01-14_00:00:00_daily",
        ),
    )


@freeze_time("2012-01-14")
def test_send():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.send, ["pool/A@s1", "pool/B", "-S", "-w", "-R", "-s"])
    assert_that(fs.recorded, includes("zfs send -w pool/A@s1 | zfs receive -s pool/B"))


@freeze_time("2012-01-14")
def test_send_push():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B", "userB@remoteB"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.send, ["pool/A@s1", "userB@remoteB:pool/B", "-S", "-w", "-R", "-s"])
    assert_that(fs.recorded, includes("zfs send -w pool/A@s1 | ssh userB@remoteB -- zfs receive -s pool/B"))


@freeze_time("2012-01-14")
def test_send_pull():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A", "userA@remoteA").snapshot("s1"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.send, ["userA@remoteA:pool/A@s1", "pool/B", "-S", "-w", "-R", "-s"])
    assert_that(fs.recorded, includes("ssh userA@remoteA -- zfs send -w pool/A@s1 | zfs receive -s pool/B"))


@freeze_time("2012-01-14")
def test_send_broker():
    runner = CliRunner()
    fs = InMemoryFS.of(
        InMemoryDataset("pool/A", "userA@remoteA").snapshot("s1"), InMemoryDataset("pool/B", "userB@remoteB")
    )
    rift.cli.runner = fs
    runner.invoke(rift.cli.send, ["userA@remoteA:pool/A@s1", "userB@remoteB:pool/B", "-S", "-w", "-R", "-s"])
    assert_that(
        fs.recorded, includes("ssh userA@remoteA -- zfs send -w pool/A@s1 | ssh userB@remoteB -- zfs receive -s pool/B")
    )


@freeze_time("2012-01-14")
def test_send_ssh_options():
    runner = CliRunner()
    fs = InMemoryFS.of(
        InMemoryDataset("pool/A", "userA@remoteA").snapshot("s1"), InMemoryDataset("pool/B", "userB@remoteB")
    )
    rift.cli.runner = fs
    runner.invoke(
        rift.cli.send,
        [
            "userA@remoteA:pool/A@s1",
            "userB@remoteB:pool/B",
            "-S",
            "-w",
            "-R",
            "-s",
            "-s",
            "Compression=yes",
            "-s",
            "Port=23",
            "-t",
            "Port=24",
        ],
    )
    assert_that(
        fs.recorded,
        includes(
            "ssh userA@remoteA -o Compression=yes -o Port=23 -- zfs send -w pool/A@s1 | ssh userB@remoteB -o Port=24 -- zfs receive -s pool/B"
        ),
    )


@freeze_time("2012-01-14")
def test_send_pipes():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(
        rift.cli.send,
        [
            "pool/A@s1",
            "pool/B",
            "-S",
            "-w",
            "-R",
            "-s",
            "--pipe",
            "mbuffer -r 1M",
            "--pipe",
            "mbuffer -r 1M",
            "--pipe",
            "pv -s {size}",
        ],
    )
    assert_that(
        fs.recorded,
        includes("zfs send -w pool/A@s1 | mbuffer -r 1M | mbuffer -r 1M | pv -s 3711767360 | zfs receive -s pool/B"),
    )


@freeze_time("2012-01-14")
def test_sync():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1", "s2"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.sync, ["pool/A", "pool/B", "--filter", ".*", "-S", "-w", "-R", "-s"])
    assert_that(
        fs.recorded,
        includes(
            "zfs send -w pool/A@s1 | zfs receive -s pool/B",
            "zfs send -w -i pool/A@s1 pool/A@s2 | zfs receive -s pool/B",
        ),
    )


@freeze_time("2012-01-14")
def test_sync_pipes():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1", "s2"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(
        rift.cli.sync, ["pool/A", "pool/B", "--filter", ".*", "-S", "-w", "-R", "-s", "--pipe", "pv -s {size}"]
    )
    assert_that(
        fs.recorded,
        includes(
            "zfs send -w pool/A@s1 | pv -s 3711767360 | zfs receive -s pool/B",
            "zfs send -w -i pool/A@s1 pool/A@s2 | pv -s 3711767360 | zfs receive -s pool/B",
        ),
    )


@freeze_time("2012-01-14")
def test_sync_filter():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("f1", "s1", "s2", "f2"), InMemoryDataset("pool/B"))
    rift.cli.runner = fs
    runner.invoke(rift.cli.sync, ["pool/A", "pool/B", "--filter", "s.*", "-S", "-w", "-R", "-s"])
    assert_that(
        fs.recorded,
        includes(
            "zfs send -w pool/A@s1 | zfs receive -s pool/B",
            "zfs send -w -i pool/A@s1 pool/A@s2 | zfs receive -s pool/B",
        ),
    )
    assert_that(
        fs.find("pool/B").snapshots(),
        contains_exactly("pool/B@s1\tuuid:pool/A@s1\t655", "pool/B@s2\tuuid:pool/A@s2\t656"),
    )


@freeze_time("2012-01-14")
def test_prune():
    runner = CliRunner()
    fs = InMemoryFS.of(InMemoryDataset("pool/A").snapshot("s1_weekly", "s2_weekly", "s3_daily", "s4_monthly"))
    rift.cli.runner = fs
    runner.invoke(
        rift.cli.prune, ["pool/A", "--keep", ".*_daily", "5", "--keep", ".*_weekly", "1", "--keep", ".*_monthly", "0"]
    )
    assert_that(fs.recorded, includes("zfs destroy pool/A@s1_weekly,s4_monthly"))
    assert_that(
        fs.find("pool/A").snapshots(),
        contains_exactly("pool/A@s3_daily\tuuid:pool/A@s3_daily\t898", "pool/A@s2_weekly\tuuid:pool/A@s2_weekly\t897"),
    )
