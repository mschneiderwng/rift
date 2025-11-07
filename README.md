# nexo: zfs send, sync and prune 

__Warning: This tool is not production ready!__

I wanted a zfs replication tool which comprises many dedicate very small programs which do just as much as neccessary.

# Interface

## Send individual snapshots
`nexo send` automatically detects if a snapshot needs to be sent as full, incremental or can be resumed. It also
supports incremental send from bookmarks.
    
    nexo send src/data@snap1 user@remote:back/src/data             # push
    nexo send user@remote:src/data@snap1 back/src/data             # pull
    nexo send user@remote:src/data@snap1 user@remote:back/src/data # broker
    nexo send src/data@snap1 back/src/data                         # local copy

### Flags
Bandwidth limits needs `mbuffer` installed. The quantity passed to `--bwlimit` is forwarded to `mbuffer -m`.

    nexo send src/data@snap1 user@remote:back/src/data --bwlimit 1M

## Send all newer snapshots (sync)
`nexo sync` has the same push/pull/local modes as `nexo send`. It builds a list of snapshots from the source which are
newer than the newest snapshot on the target. This list is then iterated by `nexo send`.

    nexo sync src/data user@remote:back/src/data

### Flags
The list of snapshots to be sent can be filtered by a regular expression. Only snapshots that match the regex will
be sent to the target. The defautl filter is `"nexo.*"`.

    nexo sync src/data user@remote:back/src/data --filter "nexo.*"

## Create snapshot
    nexo snapshot --tag weekly src/data 

## Destroy old snapshots
    nexo prune --keep 24 hourly --keep 4 nexo_.*_weekly --keep 0 nexo_.*_frequently src/data

