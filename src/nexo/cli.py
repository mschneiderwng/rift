import logging
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from shlex import quote
from typing import Iterable

import click
import structlog

import rift.datasets
from rift.commands import SystemRunner
from rift.datasets import Dataset
from rift.snapshots import Bookmark, Snapshot
from rift.zfs import ZfsBackend


def configure_logging(verbosity):
    """Set up structlog + stdlib logging based on verbosity count."""
    # Map verbosity (-v, -vv, etc.) to logging levels
    if verbosity >= 2:
        level = logging.DEBUG
    elif verbosity == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    # structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
        processors=[
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )


class DatasetType(click.ParamType):
    """Click type which accepts and parses [user@remote:]src/data"""

    name = "DatasetType"

    def convert(self, value, param, ctx):
        remote = None
        dataset = value

        # Split off remote part if present: user@host:
        if ":" in value and re.match(r"^[^/]+@[^:]+:", value):
            remote, dataset = value.split(":", 1)

        if dataset is None or "@" in dataset:
            self.fail(
                f"Invalid snapshot reference: '{value}'. Syntax is [user@remote:]src/data",
                param,
                ctx,
            )

        return remote, dataset


class SnapshotType(click.ParamType):
    """Click type which accepts and parses [user@remote:]src/data@snap"""

    name = "SnapshotType"

    def convert(self, value, param, ctx):
        remote = None
        dataset = value
        snapshot = None

        # Split off remote part if present: user@host:
        if ":" in value and re.match(r"^[^/]+@[^:]+:", value):
            remote, dataset = value.split(":", 1)

        # Split off snapshot if present: @snapshot
        if "@" in dataset:
            dataset, snapshot = dataset.split("@", 1)

        if dataset is None or snapshot is None:
            self.fail(
                f"Invalid snapshot reference: '{value}'. Syntax is [user@remote:]src/data@snap",
                param,
                ctx,
            )

        return remote, dataset, snapshot


DATASET_TYPE = DatasetType()
SNAPSHOT_TYPE = SnapshotType()


def dry_run_option(**kwargs):
    """Reusable Click option for --dry-run."""
    # default behavior (can be overridden)
    kwargs.setdefault("is_flag", True)
    kwargs.setdefault("help", "Dry run commands without making any changes.")

    def decorator(f):
        return click.option("--dry-run", "-n", **kwargs)(f)

    return decorator


def verbose_option(**kwargs):
    """Reusable Click option for --verbose."""
    # default behavior (can be overridden)
    kwargs.setdefault("count", True)
    kwargs.setdefault("help", "Increase verbosity (-v, -vv for more detail).")

    def decorator(f):
        return click.option("--verbose", "-v", **kwargs)(f)

    return decorator


@contextmanager
def error_handler():
    # handle errors: print to stderr and log.error
    log = structlog.get_logger()
    try:
        yield  # everything inside the `with` block runs here
    except subprocess.CalledProcessError as e:
        print(e.stderr.decode(), file=sys.stderr)
        log.error(e.stderr.decode())
        sys.exit(e.returncode)
    except Exception as e:
        print(e, file=sys.stderr)
        log.error(e)
        sys.exit(1)


@click.group()
def main():
    pass


@click.command()
@click.argument("source", type=SNAPSHOT_TYPE)
@click.argument("target", type=DATASET_TYPE)
@click.option("--bwlimit", help="Bandwidth limit (needs mbuffer).")
@dry_run_option()
@verbose_option()
def send(source, target, bwlimit, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse source
        remote, path, snapshot_name = source
        source = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        # find snapshot by name
        snapshot = source.find(snapshot_name)

        # parse target
        remote, path = target
        target = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        return rift.datasets.send(snapshot, source, target, bwlimit=bwlimit, dry_run=dry_run)


@click.command()
@click.argument("source", type=DATASET_TYPE)
@click.argument("target", type=DATASET_TYPE)
@click.option(
    "--filter",
    "-f",
    "regex",
    default="rift.*",
    help="Sync only snapshots which match regex (default: 'rift.*').",
)
@click.option("--bwlimit", help="Bandwidth limit (needs mbuffer).")
@dry_run_option()
@verbose_option()
def sync(source, target, regex, bwlimit, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse source
        remote, path = source
        source = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        # parse target
        remote, path = target
        target = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        rift.datasets.sync(source, target, regex=regex, bwlimit=bwlimit, dry_run=dry_run)


@click.command()
@click.argument("dataset", type=DATASET_TYPE)
@click.option("--name", default="rift", help="Snapshot name (default: 'rift').")
@click.option("--tag", default=None, help="Snapshot tag, e.g. 'hourly'.")
@click.option(
    "--timestamp/--no-timestamp",
    default=True,
    help="Append timestamp to name (default: True).",
)
@click.option(
    "--bookmark/--no-bookmark",
    default=True,
    help="Also create bookmark of snapshot (default: True).",
)
@verbose_option()
def snapshot(dataset, name, tag, timestamp, bookmark, verbose):
    configure_logging(verbose)
    with error_handler():
        ts = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        name = f"{name}_{ts}" if timestamp else name
        name = f"{name}_{quote(tag)}" if tag is not None else name

        # parse dataset
        remote, path = dataset
        dataset: Dataset = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        # create snapshot
        dataset.snapshot(name)

        # also create bookmark
        if bookmark:
            dataset.bookmark(name)


@click.command(name="list")
@click.argument("dataset", type=DATASET_TYPE)
@click.option(
    "--filter",
    "-f",
    "regex",
    default="rift.*",
    help="Show only snapshots which match regex (default: 'rift.*').",
)
@click.option("--snapshots/--no-snapshots", default=True, help="List snapshots (default: True).")
@click.option("--bookmarks/--no-bookmarks", default=False, help="List bookmarks (default: False).")
@verbose_option()
def list_snapshots(dataset, regex, snapshots, bookmarks, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse dataset
        remote, path = dataset
        dataset: Dataset = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))
        result = (dataset.snapshots() if snapshots else ()) + (dataset.bookmarks() if bookmarks else ())

        p = re.compile(regex)
        snapshots: Iterable[Snapshot | Bookmark] = filter(lambda snap: p.match(str(snap.name)), result)

        for snap in snapshots:
            print(f"{snap.guid:<20} {snap.fqn}")


@click.command()
@click.argument("dataset", type=DATASET_TYPE)
@click.option(
    "--keep",
    nargs=2,  # expect 2 arguments per use: e.g. "24 .*_hourly"
    multiple=True,  # allow repeating the option
    type=(int, str),  # types for the 2 arguments
    help="Retention rule (e.g. '--keep 24 rift_.*_hourly --keep 4 rift_.*_weekly')",
)
@dry_run_option()
@verbose_option()
def prune(dataset, keep, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse dataset
        remote, path = dataset
        dataset: Dataset = Dataset(ZfsBackend(path=path, remote=remote, runner=SystemRunner()))

        policy = {regex: count for count, regex in keep}
        rift.datasets.prune(dataset=dataset, policy=policy, dry_run=dry_run)


main.add_command(send)
main.add_command(sync)
main.add_command(snapshot)
main.add_command(list_snapshots)
main.add_command(prune)
