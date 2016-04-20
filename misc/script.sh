#!/bin/sh

#
# This script illustrates the issue with combining rdiff-backup with
# aws s3 sync.
#
# You must have a server like s3rver running at localhost:1111 for
# this to work.
#
# The issue manifests itself when the 2nd sync's execution does not
# result in $srcdir/a being uploaded to the server. Note that this is
# timing dependend, so sometimes the upload will happen, and sometimes
# not.
#

aws --version
rdiff-backup --version

srcdir=/tmp/synctest
backupdir=/tmp/rdiff-backup

mkdir -p $srcdir
echo 0 > $srcdir/a

sync () {
    aws --quiet s3 --endpoint=http://localhost:1111 sync $backupdir s3://foo/backups/rdiff-backup

}

backup () {
    rdiff-backup $srcdir $backupdir
}

statall () {
    stat $srcdir/a
    stat $backupdir/a
}

echo 1 | dd conv=notrunc of=$srcdir/a bs=1 count=1
backup
statall
sync
echo 2 | dd conv=notrunc of=$srcdir/a bs=1 count=1
# Even with this sleep here, the issue can still happen.
sleep 1
backup
statall
sync
