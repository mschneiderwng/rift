# rift: zfs send, sync and prune 

__Warning: This tool is not production ready!__

I wanted a zfs replication tool which comprises many dedicate very small programs which do just as much as neccessary.

# Interface

## Send individual snapshots
`rift send` automatically detects if a snapshot needs to be sent as full, incremental or can be resumed. It also
supports incremental send from bookmarks.
    
    rift send src/data@snap1 user@remote:back/src/data             # push
    rift send user@remote:src/data@snap1 back/src/data             # pull
    rift send user@remote:src/data@snap1 user@remote:back/src/data # broker
    rift send src/data@snap1 back/src/data                         # local copy

### Flags
Bandwidth limits needs `mbuffer` installed. The quantity passed to `--bwlimit` is forwarded to `mbuffer -m`.

    rift send src/data@snap1 user@remote:back/src/data --bwlimit 1M

## Send all newer snapshots (sync)
`rift sync` has the same push/pull/local modes as `rift send`. It builds a list of snapshots from the source which are
newer than the newest snapshot on the target. This list is then iterated by `rift send`.

    rift sync src/data user@remote:back/src/data

### Flags
The list of snapshots to be sent can be filtered by a regular expression. Only snapshots that match the regex will
be sent to the target. The default filter is `"rift.*"`.

    rift sync src/data user@remote:back/src/data --filter "rift.*"

## Create snapshot
    rift snapshot --tag weekly src/data 

## Destroy old snapshots
    rift prune --keep 24 hourly --keep 4 rift_.*_weekly --keep 0 rift_.*_frequently src/data

# Systemd
I let `systemd` handle all the automation with the goal to give the least possible amount of permissions. `nix` is used a configuration language which create the following services and timers.

## Daily Snapshot Timer
`systemctl cat rift-daily.timer`

    [Unit]

    [Timer]
    OnCalendar=daily
    Persistent=true
    Unit=rift-snapshot@daily.service


    [Install]
    WantedBy=timers.target

## Daily Snapshot Service
`systemctl cat rift-snapshot@daily.service`

    [Unit]
    After=zfs.target
    Description=rift snapshot service

    [Service]
    Environment="LOCALE_ARCHIVE=/nix/store/lfnnsw0p9a1cbra5wf5f5zakffd7ify4-glibc-locales-2.40-66/lib/locale/locale-archive"
    Environment="PATH=/nix/store/xs8scz9w9jp4hpqycx3n3bah5y07ymgj-coreutils-9.8/bin:/nix/store/qqvfnxa9jg71wp4hfg1l63r4m78iwvl9-findutils-4.10.0/bin:/nix/store/22r4s6lqhl43jkazn51f3c18qwk894g4-gnugrep-3.12/bin:/nix/store/zppkx0lkizglyqa9h>
    Environment="TZDIR=/nix/store/c6lkjqkkc4cl4pffj4i3l22rv4ihhpb9-tzdata-2025b/share/zoneinfo"
    BindPaths=/dev/zfs
    CacheDirectory=rift
    CapabilityBoundingSet=
    DeviceAllow=/dev/zfs
    DevicePolicy=closed
    DynamicUser=true
    Environment=tag=%i
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift snapshot -v --tag $tag rpool/user/home/me/dev
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift snapshot -v --tag $tag rpool/user/home/me/docs
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift snapshot -v --tag $tag rpool/user/home/me/etc
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift snapshot,bookmark rpool/user/home/me/dev
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift snapshot,bookmark rpool/user/home/me/docs
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift snapshot,bookmark rpool/user/home/me/etc
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift snapshot,bookmark rpool/user/home/me/dev
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift snapshot,bookmark rpool/user/home/me/docs
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift snapshot,bookmark rpool/user/home/me/etc
    Group=rift
    LockPersonality=true
    MemoryDenyWriteExecute=true
    NoNewPrivileges=true
    PrivateDevices=true
    PrivateMounts=true
    PrivateNetwork=true
    PrivateTmp=true
    PrivateUsers=false
    ProtectClock=true
    ProtectControlGroups=true
    ProtectHome=true
    ProtectHostname=true
    ProtectKernelLogs=true
    ProtectKernelModules=true
    ProtectKernelTunables=true
    ProtectProc=invisible
    ProtectSystem=strict
    RestrictAddressFamilies=none
    RestrictNamespaces=true
    RestrictRealtime=true
    RestrictSUIDSGID=true
    RuntimeDirectory=rift
    SystemCallArchitectures=native
    SystemCallFilter=
    SystemCallFilter=~@reboot
    SystemCallFilter=~@swap
    SystemCallFilter=~@obsolete
    SystemCallFilter=~@mount
    SystemCallFilter=~@module
    SystemCallFilter=~@debug
    SystemCallFilter=~@cpu-emulation
    SystemCallFilter=~@clock
    SystemCallFilter=~@raw-io
    SystemCallFilter=~@privileged
    SystemCallFilter=~@resources
    Type=oneshot
    UMask=77
    User=rift


## Prune Service
`systemctl cat rift-prune.service`

    [Unit]
    After=zfs.target
    Description=rift prune service

    [Service]
    Environment="LOCALE_ARCHIVE=/nix/store/lfnnsw0p9a1cbra5wf5f5zakffd7ify4-glibc-locales-2.40-66/lib/locale/locale-archive"
    Environment="PATH=/nix/store/xs8scz9w9jp4hpqycx3n3bah5y07ymgj-coreutils-9.8/bin:/nix/store/qqvfnxa9jg71wp4hfg1l63r4m78iwvl9-findutils-4.10.0/bin:/nix/store/22r4s6lqhl43jkazn51f3c18qwk894g4-gnugrep-3.12/bin:/nix/store/zppkx0lkizglyqa9h>
    Environment="TZDIR=/nix/store/c6lkjqkkc4cl4pffj4i3l22rv4ihhpb9-tzdata-2025b/share/zoneinfo"
    BindPaths=/dev/zfs
    CacheDirectory=rift
    CapabilityBoundingSet=
    DeviceAllow=/dev/zfs
    DevicePolicy=closed
    DynamicUser=true
    Environment=tag=%i
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift prune -v --keep 30 rift_.*_daily --keep 192 rift_.*_frequently --keep 24 rift_.*_hourly --keep 12 rift_.*_monthly --keep 0 rift_.*_tag --keep 0 rift_.*_yearly rpool/user/home/me/dev
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift prune -v --keep 30 rift_.*_daily --keep 192 rift_.*_frequently --keep 24 rift_.*_hourly --keep 12 rift_.*_monthly --keep 0 rift_.*_tag --keep 0 rift_.*_yearly rpool/user/home/me/docs
    ExecStart=/nix/store/hh47v58qyqqcicshd3d9rax1ii3z9zbi-rift-unstable-2015-11-06/bin/rift prune -v --keep 30 rift_.*_daily --keep 192 rift_.*_frequently --keep 24 rift_.*_hourly --keep 12 rift_.*_monthly --keep 0 rift_.*_tag --keep 0 rift_.*_yearly rpool/user/home/me/etc
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift destroy,mount rpool/user/home/me/dev
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift destroy,mount rpool/user/home/me/docs
    ExecStartPre=-+/run/booted-system/sw/bin/zfs allow rift destroy,mount rpool/user/home/me/etc
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift destroy,mount rpool/user/home/me/dev
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift destroy,mount rpool/user/home/me/docs
    ExecStopPost=-+/run/booted-system/sw/bin/zfs unallow rift destroy,mount rpool/user/home/me/etc
    Group=rift
    LockPersonality=true
    MemoryDenyWriteExecute=true
    NoNewPrivileges=true
    PrivateDevices=true
    PrivateMounts=true
    PrivateNetwork=true
    PrivateTmp=true
    PrivateUsers=false
    ProtectClock=true
    ProtectControlGroups=true
    ProtectHome=true
    ProtectHostname=true
    ProtectKernelLogs=true
    ProtectKernelModules=true
    ProtectKernelTunables=true
    ProtectProc=invisible
    ProtectSystem=strict
    RestrictAddressFamilies=none
    RestrictNamespaces=true
    RestrictRealtime=true
    RestrictSUIDSGID=true
    RuntimeDirectory=rift
    SystemCallArchitectures=native
    SystemCallFilter=
    SystemCallFilter=~@reboot
    SystemCallFilter=~@swap
    SystemCallFilter=~@obsolete
    SystemCallFilter=~@mount
    SystemCallFilter=~@module
    SystemCallFilter=~@debug
    SystemCallFilter=~@cpu-emulation
    SystemCallFilter=~@clock
    SystemCallFilter=~@raw-io
    SystemCallFilter=~@privileged
    SystemCallFilter=~@resources
    Type=oneshot
    UMask=77
    User=rift