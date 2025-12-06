# rift: zfs send, sync and prune 

__Warning: This tool is not production ready!__

I wanted a zfs replication tool which consists of many dedicate very small programs which do just as much as necessary with very few permissions. Especially, a compromised host should not be able to destroy backups on a remote. The goal is to have a well-tested small library where other tool can build upon.

# Interface

## Send individual snapshots
`rift send` automatically detects if a snapshot needs to be sent as full, incremental or can be resumed. It also
supports incremental send from bookmarks.
```bash
rift send src/data@snap1 user@remote:back/src/data             # push
rift send user@remote:src/data@snap1 back/src/data             # pull
rift send user@remote:src/data@snap1 user@remote:back/src/data # broker
rift send src/data@snap1 back/src/data                         # local copy
```
## Send all newer snapshots (sync)
`rift sync` has the same push/pull/local modes as `rift send`. It builds a list of snapshots from the source which are
newer than the newest snapshot on the target. This list is then iterated by `rift send`.
```bash
rift sync src/data user@remote:back/src/data
```

## Create snapshot
```bash
rift snapshot --tag weekly src/data 
```

## Destroy old snapshots
```bash
rift prune --keep rift_.*_hourly 24 --keep rift_.*_weekly 4 --keep rift_.*_frequently 0 src/data
```

## Pipes
It is possible to insert commands in between `zfs send` and `zfs recv` via `--pipes/-p`. This is supported for `rift send` and `rift sync`.
```bash
rift send src/data@snap1 user@remote:back/src/data -p "mbuffer -r 1M" -p "pv -s {size}"
```
The template `{size}` with be replaced by the stream size in byes.

# Systemd
I let `systemd` handle all the automation with the goal to give the units the least possible amount of permissions. 
- One service that creates snapshots (more precisely a service template which runs hourly, daily, ...).
- One service that purges snapshots.
- One service that sends snapshots to a remote.
    - This service assumes there is a user `rift-recv` at the remote with the zfs permissions `create,receive,mount`. That way, it is not possible to destroy backups remotely. Snapshots on the remote should be pruned with its own locally running service.

`nix` is used a configuration language which creates the services and timers. The modules I created are available in the repository and their usage looks like in the following example:

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

        # ssh need the remote public key
        services.openssh.knownHosts = {
            nas.publicKey = "ssh-ed25519 AAAAC3...";
        };

        # sync all snaphots to nas (excluding frequently snapshots)
        services.rift.sync = {
            enable = true;
            remotes = {
                "rift-recv@nas:spool/backups/yoga" = {
                    name = "nas";
                    sshPrivateKey = config.sops.secrets."rift/sync/key".path;
                    filter = ''rift_.*_.*(?<!frequently)$''; # all but frequently;
                    datasets = datasets;
                };
            };
        };
    }
```