# rift: zfs send, sync and prune 

__Warning: This tool is not production ready!__

I wanted a zfs replication tool which consists of many dedicate very small programs which do just as much as necessary with very few permissions. Especially, a compromised host should not be able to destroy backups on a remote. The goal is to have a well-tested small library where other tool can build upon.

rift does not have timer. systemd does a much better job with its abilty to notify on failure, persist when a system is suspended, etc. It also does not have a configuration file. I use `nix` to configure the systemd units and timers. The nix module for rift is exported by this repositories flake.

# Interface

## Send individual snapshots
```bash
rift send --help
Usage: rift send [OPTIONS] SOURCE TARGET

  Send individual snapshots.

  `rift send` automatically detects if a snapshot needs to be sent as full,
  incremental or can be resumed. It also supports incremental send from
  bookmarks.

  SOURCE the snapshot to be sent. Syntax is [user@remote:]src/data@snap

  TARGET the dataset which receives the snapshot. Syntax is
  [user@remote:]target/data

  Examples:

      rift send src/data@snap1 user@remote:back/src/data             # push
      rift send user@remote:src/data@snap1 back/src/data             # pull
      rift send user@remote:src/data@snap1 user@remote:back/src/data # broker
      rift send src/data@snap1 back/src/data                         # local copy

      # Pipe to mbuffer and then to pv. The placeholder {size} will be replaced by the stream size in bytes.
      # results in `zfs send ... | mbuffer -r 1M | pv -s 1271665 | zfs recv`
      rift send src/data@snap1 user@remote:back/src/data -p "mbuffer -r 1M" -p "pv -s {size}"

Options:
  -p, --pipe TEXT               Command which zfs send should pipe to before zfs recv.
  -S, --zfs-send-option TEXT    Options passed to zfs send. Can be used multiple times (default: '-w').
  -R, --zfs-recv-option TEXT    Options passed to zfs recv. Can be used multiple times (default: '-s', '-u', '-F').
  -s, --source-ssh-option TEXT  ssh options like -o "Compression=yes" for source. Can be used multiple times.
  -t, --target-ssh-option TEXT  ssh options like -o "Compression=yes" for target. Can be used multiple times.
  -n, --dry-run                 Dry run commands without making any changes.
  -v, --verbose                 Increase verbosity (-v, -vv for more detail).
  --help                        Show this message and exit.
```

## Send all newer snapshots (sync)
```bash
rift sync --help
Usage: rift sync [OPTIONS] SOURCE TARGET

  Send all newer snapshots (sync).

  `rift sync` automatically detects if a snapshot needs to be sent as full, incremental or can be resumed.
  It also supports incremental send from bookmarks.

  SOURCE the dataset which snapshots should be sent to the target. Syntax is
  [user@remote:]src/data

  TARGET the dataset which receives the snapshot. Syntax is
  [user@remote:]target/data

  `rift sync` has the same push/pull/local modes as `rift send`. It builds a
  list of snapshots from the source which are newer than the newest snapshot
  on the target. This list is then iterated by `rift send`.

  Examples:

      rift sync src/data user@remote:target/data # push
      rift sync user@remote:src/data target/data # pull

Options:
  -f, --filter TEXT             Sync only snapshots which match regex (default: 'rift.*').
  -p, --pipe TEXT               Command which zfs send should pipe to before zfs recv.
  -S, --zfs-send-option TEXT    Options passed to zfs send. Can be used multiple times (default: '-w').
  -R, --zfs-recv-option TEXT    Options passed to zfs recv. Can be used multiple times (default: '-s', '-u' '-F').
  -s, --source-ssh-option TEXT  ssh options like -o "Compression=yes" for source. Can be used multiple times.
  -t, --target-ssh-option TEXT  ssh options like -o "Compression=yes" for target. Can be used multiple times.
  -n, --dry-run                 Dry run commands without making any changes.
  -v, --verbose                 Increase verbosity (-v, -vv for more detail).
  --help                        Show this message and exit.
```

## Create snapshots
```bash
rift snapshot --help
Usage: rift snapshot [OPTIONS] DATASET

  Create a snapshot (and bookmark where appropriate) for a dataset.

  DATASET the dataset for which a snapshot should be created. Syntax is
  [user@remote:]src/data

  The template {datetime} in the snapshot name will be replaced by the current
  date and time.

  Examples:

      rift snapshot src/data --name rift_{datetime}_frequently
      rift snapshot src/data --name rift_{datetime}_frequently --time-format "%Y-%m-%d_%H:%M:%S"
      rift snapshot src/data --name rift_{datetime}_frequently --no-bookmark

Options:
  --name TEXT                 Snapshot name (default: 'rift_{datetime}').
  --bookmark / --no-bookmark  Also create bookmark of snapshot (default: '--bookmark').
  --time-format TEXT          Format for timestamp (default: '%Y-%m-%d_%H:%M:%S').
  -s, --ssh-option TEXT       ssh options like -o "Compression=yes". Can be used multiple times.
  -v, --verbose               Increase verbosity (-v, -vv for more detail).
  --help                      Show this message and exit.
```

## Destroy old snapshots
```bash
rift prune --help
Usage: rift prune [OPTIONS] DATASET

  Destroy snapshots according to a given retention rule.

  DATASET the dataset whose snapshots should be destroyed. Syntax is
  [user@remote:]src/data

  Retention rule policy it defined by a regex followed by an int specifying
  how many of the snapshots matching the regex should be kept. It will never
  touch snapshots which are not matched.

  Examples:

      rift prune --keep rift_.*_hourly 24 --keep rift_.*_weekly 4 src/data
      rift prune --keep rift_.*_frequently 0 user@remote:src/data # destroy all frequently snapshots

Options:
  --keep <TEXT INTEGER>...  Retention rule (e.g. '--keep rift_.*_hourly 24 --keep rift_.*_weekly 4')
  -s, --ssh-option TEXT     ssh options like -o "Compression=yes". Can be used multiple times.
  -n, --dry-run             Dry run commands without making any changes.
  -v, --verbose             Increase verbosity (-v, -vv for more detail).
  --help                    Show this message and exit.
```



# Systemd
I let `systemd` handle all the automation with the goal to give the units the least possible amount of permissions. 
- One service that creates snapshots (more precisely a service template which runs hourly, daily, ...).
- One service that purges snapshots.
- One service that sends snapshots to a remote.
    - This service assumes there is a user `rift-recv` at the remote with the zfs permissions `create,receive,mount`. That way, it is not possible to destroy backups remotely. Snapshots on the remote should be pruned with its own locally running service.

Nix is used a configuration language which creates the services and timers. The result are systemd units and timers:

- [Example systemd (daily) snapshot unit](docs/rift-snapshot-daily.service)
- [Example systemd prune unit](docs/rift-prune.service)
- [Example systemd sync unit](docs/rift-sync-nas.service)

The modules I created are available in the repository and their usage looks like in the following example:

```nix
    {
        lib,
        pkgs, 
        inputs,
        config,
        ...
    }:
    let
        schedule = [
            "frequently"
            "daily"
            "hourly"
            "weekly"
            "monthly"
            "yearly"
        ];

        shortterm = {
            frequently = 48 * 60 / 15;
            hourly = 24;
            daily = 30;
            weekly = 52;
            monthly = 12;
            yearly = 0;
        };

        datasets = [
            "rpool/user/home/me/dev"
            "rpool/user/home/me/docs"
            "rpool/user/home/me/etc"
        ];

        mapToAttr = value: datasets: builtins.listToAttrs (map (name: { inherit name value; }) datasets);
    in
    {
        # enable my onFailure units
        ash.services.notify-email.enable = true; 

        # my private key for remote sync is stored in sops
        ash.programs.sops.enable = true;         
        sops.secrets."rift/sync/key" = { };

        # use the same schedule for all datasets
        services.rift.snapshots = {
            enable = true;
            onFailure = [ "notify-email@%n.service" ];
            datasets = mapToAttr schedule datasets;
        };

        # use the same "shortterm" retention policy for all snapshots
        services.rift.prune = {
            enable = true;
            onFailure = [ "notify-email@%n.service" ];
            datasets = mapToAttr shortterm datasets;
        };

        # ssh needs the remote public key
        services.openssh.knownHosts = {
            nas.publicKey = "ssh-ed25519 AAAAC3...";
        };

        # sync all snaphots to nas (excluding frequently snapshots)
        services.rift.sync = {
            enable = true;
            remotes = {
                "rift-recv@nas:spool/backups/yoga" = {
                    name = "nas";
                    datasets = datasets;
                    sshPrivateKey = config.sops.secrets."rift/sync/key".path;
                    filter = ''rift_.*_.*(?<!frequently)$''; # send all but frequently snaps
                    pipes = [ "pv -p -e -t -r -a -b -s {size}" ];
                    zfsSendOptions = ["-w"];
                    zfsRecvOptions = ["-s" "-u" "-F"];
                };
            };
        };
    }
```

# Installation

Nix can run rift without installation via `nix run github:mschneiderwng/rift -- --help`. Alternatively install the python application with `uv`. The nixos module is exported and can be included in a flake as in the following example:

```
  inputs = {
    ...
    rift.url = "github:mschneiderwng/rift";
    ...
  };
  outputs = ...
    # incldue this in your modules:
    rift.nixosModules.rift
```

# Development

Checkout the repository and execute the following to create a virtual environment that can be used with pycharm-professional. Of course your can use a different IDE of your choice.
```bash
# create a virtual environment
nix build .#venv -o .venv
# start pycharm
nix develop .#uv2nix -c pycharm-professional
```


# FAQ

### How does it differ from sanoid/syncoid?

sanoid has been around much longer and has grown into a large, feature-rich toolkit that tries to handle nearly every aspect of ZFS snapshot management and replication. In my view, it ends up doing a bit too much, partly due to legacy design decisions and the need to support many different environments and use cases.

rift takes a different approach. Instead of embedding its own mechanisms for scheduling, isolation, and logging, it delegates these responsibilities to systemd, which is already excellent at managing processes and services. rift then uses a configuration language like Nix to generate the necessary systemd units and timers in a clean and declarative way. Because of this architecture, rift remains intentionally small and focused; the core replication logic fits into roughly 100 lines of code (excluding comments), making it easier to understand, maintain, and audit.