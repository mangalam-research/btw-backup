import subprocess
import tempfile
import os
import time
import datetime

#
# This file illustrates a problem with rdiff-backup used in the
# context of btw-backup. btw-backup creates a file (a tar, a SQL dump
# or a PostgresQL dump), which is then backed up with rdiff-backup. If
# two dumps are done too close to one another they'll have the same
# modification time stamp, and rdiff-backup will consider them to be
# the same even if they have different contents. (rdiff-backup checks
# the difference with a second resolution. So a difference less than a
# second = no difference.) This means rdiff-backup will skip the
# creation of the incremental backup if two backups are launched too
# close to one another.
#
# In the following test, there should be 4 incremental backups, but we
# get 3.
#

tmpdir = tempfile.mkdtemp()
print tmpdir

dst_path = os.path.join(tmpdir, "dst")
os.mkdir(dst_path)

src_path = os.path.join(tmpdir, "src")
os.mkdir(src_path)

for i in xrange(0, 5):
    outfile = os.path.join(src_path, "x")
    with open(outfile, 'w') as f:
        f.write(str(i))

    if i > 0:
        last = subprocess.check_output(["rdiff-backup",
                                        "--list-increments", "--parsable",
                                        dst_path]).strip().split("\n")[-1]

        parts = last.split()
        last = datetime.datetime.utcfromtimestamp(int(parts[0]))
        now = datetime.datetime.utcnow().replace(microsecond=0)
        while now - last < datetime.timedelta(seconds=1):
            time.sleep(0.5)
            now = datetime.datetime.utcnow().replace(microsecond=0)

    subprocess.check_call(["rdiff-backup", src_path, dst_path])

subprocess.check_call(["rdiff-backup", "--list-increments", dst_path])
