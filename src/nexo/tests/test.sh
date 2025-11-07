dd if=/dev/zero of=/tmp/source_pool_disk.img bs=1M count=1024
dd if=/dev/zero of=/tmp/target_pool_disk.img bs=1M count=1024
sudo zpool create target /tmp/target_pool_disk.img
sudo zpool create source /tmp/source_pool_disk.img
sudo zfs create source/A
sudo zfs create target/backups
sudo zfs allow -u mschneider send,create,hold,snapshot,bookmark source/A
sudo zfs allow -u mschneider destroy,receive,create,snapshot,mount,mountpoint target/backups

sudo touch /source/A/s1.txt
sudo zfs snapshot source/A@s1

sudo touch /source/A/s2.txt
sudo zfs snapshot source/A@s2

sudo touch /source/A/s3.txt
sudo zfs snapshot source/A@s3