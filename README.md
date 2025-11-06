# Flux: zfs send, sync and prune 

__Warning: This tool is not production ready!__

I wanted a zfs replication tool which comprises many dedicate very small programs which do just as much as neccessary.

# Interface

## Send individual snapshots
`flux send` automatically detects if a snapshot needs to be sent as full, incremental or can be resumed. It also
supports incremental send from bookmarks.
    
    flux send src/data@snap1 user@remote:back/src/data             # push
    flux send user@remote:src/data@snap1 back/src/data             # pull
    flux send user@remote:src/data@snap1 user@remote:back/src/data # broker
    flux send src/data@snap1 back/src/data                         # local copy

### Flags
Bandwidth limits needs `mbuffer` installed. The quantity passed to `--bwlimit` is forwarded to `mbuffer -m`.

    flux send src/data@snap1 user@remote:back/src/data --bwlimit 1M

## Send all newer snapshots (sync)
`flux sync` has the same push/pull/local modes as `flux send`. It builds a list of snapshots from the source which are
newer than the newest snapshot on the target. This list is then iterated by `flux send`.

    flux sync src/data user@remote:back/src/data

### Flags
The list of snapshots to be sent can be filtered by a regular expression. Only snapshots that match the regex will
be sent to the target. The defautl filter is `"flux.*"`.

    flux sync src/data user@remote:back/src/data --filter "flux.*"

## Create snapshot
    flux snapshot --tag weekly src/data 

## Destroy old snapshots
    flux prune --keep 24 hourly --keep 4 weekly --keep 0 frequently src/data

