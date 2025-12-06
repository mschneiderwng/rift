import logging
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime

import click
import structlog

import rift.datasets
from rift.commands import SystemRunner
from rift.datasets import Dataset, Remote
from rift.zfs import ZfsBackend

runner = SystemRunner()


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
        if value.count("@") > 0 and ":" in value:
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

        # if value contains 1 '@', it is of the form src/data@snap
        # if value contains 2 '@', it is of the form user@remote:src/data@snap
        if value.count("@") > 1:
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
@click.version_option()
def main():
    pass


@click.command()
@click.argument("source", type=SNAPSHOT_TYPE)
@click.argument("target", type=DATASET_TYPE)
@click.option("--pipes", "-p", type=str, multiple=True, help="Command which zfs send should pipe to before zfs recv.")
@click.option(
    "--source-ssh-options",
    "-s",
    multiple=True,
    help='Ssh options like -o "Compression=yes" for source. Can be used multiple times.',
)
@click.option(
    "--target-ssh-options",
    "-t",
    multiple=True,
    help='Ssh options like -o "Compression=yes" for target. Can be used multiple times.',
)
@dry_run_option()
@verbose_option()
def send(source, target, pipes, source_ssh_options, target_ssh_options, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse source
        host, path, snapshot_name = source
        remote = None if host is None else Remote(host, source_ssh_options)
        source = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        # find snapshot by name
        snapshot = source.find(snapshot_name)

        # parse target
        host, path = target
        remote = None if host is None else Remote(host, target_ssh_options)
        target = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        pipes: list[tuple[str]] = [tuple(p.split(" ")) for p in pipes]
        return rift.datasets.send(snapshot, source, target, pipes=pipes, dry_run=dry_run)


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
@click.option("--pipes", "-p", type=str, multiple=True, help="Command which zfs send should pipe to before zfs recv.")
@click.option(
    "--source-ssh-options",
    "-s",
    multiple=True,
    help='Ssh options like -o "Compression=yes" for source. Can be used multiple times.',
)
@click.option(
    "--target-ssh-options",
    "-t",
    multiple=True,
    help='Ssh options like -o "Compression=yes" for target. Can be used multiple times.',
)
@dry_run_option()
@verbose_option()
def sync(source, target, regex, pipes, source_ssh_options, target_ssh_options, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse source
        host, path = source
        remote = None if host is None else Remote(host, source_ssh_options)
        source = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        # parse target
        host, path = target
        remote = None if host is None else Remote(host, target_ssh_options)
        target = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        pipes: list[tuple[str]] = [tuple(p.split(" ")) for p in pipes]
        rift.datasets.sync(source, target, regex=regex, pipes=pipes, dry_run=dry_run)


@click.command()
@click.argument("dataset", type=DATASET_TYPE)
@click.option("--name", default="rift_{datetime}", help="Snapshot name (default: 'rift_{datetime}').")
@click.option(
    "--bookmark/--no-bookmark",
    default=True,
    help="Also create bookmark of snapshot (default: True).",
)
@click.option(
    "--ssh-options",
    "-s",
    multiple=True,
    help='Ssh options like -o "Compression=yes" for source. Can be used multiple times.',
)
@click.option("--time-format", default="%Y-%m-%d_%H:%M:%S", help="Format for timestamp (default: '%Y-%m-%d_%H:%M:%S').")
@verbose_option()
def snapshot(dataset, name, bookmark, ssh_options, time_format, verbose):
    configure_logging(verbose)
    with error_handler():
        ts = datetime.now().strftime(time_format)
        name = name.format(datetime=ts)

        # parse dataset
        host, path = dataset
        remote = None if host is None else Remote(host, ssh_options)
        dataset: Dataset = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        # create snapshot
        dataset.snapshot(name)

        # also create bookmark
        if bookmark:
            dataset.bookmark(name)


@click.command()
@click.argument("dataset", type=DATASET_TYPE)
@click.option(
    "--keep",
    nargs=2,  # expect 2 arguments per use: e.g. "24 .*_hourly"
    multiple=True,  # allow repeating the option
    type=(str, int),  # types for the 2 arguments
    help="Retention rule (e.g. '--keep rift_.*_hourly 24 --keep rift_.*_weekly 4')",
)
@dry_run_option()
@verbose_option()
def prune(dataset, keep, dry_run, verbose):
    configure_logging(verbose)
    with error_handler():
        # parse dataset
        remote, path = dataset
        dataset: Dataset = Dataset(ZfsBackend(path=path, remote=remote, runner=runner))

        policy = {regex: count for regex, count in keep}
        rift.datasets.prune(dataset=dataset, policy=policy, dry_run=dry_run)


main.add_command(send)
main.add_command(sync)
main.add_command(snapshot)
main.add_command(prune)
