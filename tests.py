import unittest
import os
import sys
import multiprocessing
import tempfile
import shutil
import datetime
import subprocess
import contextlib
import psycopg2
import bz2
import pwd
import grp

import btw_backup.__main__ as main


def btw_backup_func(args, stdin, stdout, stderr):
    sys.stdin = os.fdopen(stdin, 'r')
    sys.stdout = os.fdopen(stdout, 'w')
    sys.stderr = os.fdopen(stderr, 'w')
    sys.argv = args
    try:
        ret = main.main()
    finally:
        sys.stdin.flush()
        sys.stdout.flush()
        sys.stderr.flush()
    sys.exit(ret)


class Backup(object):

    def __init__(self, args):
        args[0:0] = ["btw-backup"]
        self.args = args
        (stdin_r, stdin_w) = os.pipe()
        (stdout_r, stdout_w) = os.pipe()
        (stderr_r, stderr_w) = os.pipe()
        ret = multiprocessing.Process(
            target=btw_backup_func,
            args=[args, stdin_r, stdout_w, stderr_w])
        ret.start()
        os.close(stdin_r)
        os.close(stdout_w)
        os.close(stderr_w)
        self.proc = ret
        self.stdin = os.fdopen(stdin_w, 'w')
        self.stdout = os.fdopen(stdout_r, 'r')
        self.stderr = os.fdopen(stderr_r, 'r')

    @property
    def outstr(self):
        return self.stdout.read().strip()

    @property
    def errstr(self):
        return self.stderr.read().strip()

    @property
    def exitcode(self):
        return self.proc.exitcode

    def join(self):
        return self.proc.join()

tmpdir = None
workdir = None

preserve = os.environ.get("NOCLEANUP")

def setUp():
    # pylint: disable=global-statement
    global tmpdir
    global workdir
    tmpdir = tempfile.mkdtemp()
    workdir = os.path.join(tmpdir, "workdir")
    main.base_working_dir = workdir

def tearDown():
    if tmpdir:
        if preserve:
            print "TMPDIR:", tmpdir
        else:
            shutil.rmtree(tmpdir)

def empty_tmpdir():
    for entry in os.listdir(tmpdir):
        path = os.path.join(tmpdir, entry)
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

class BackupTestMixin(object):

    def assertNoError(self, backup, expected_output="", regexp=False):
        backup.join()
        ret = backup.outstr
        err = backup.errstr

        self.assertEqual(err, "")
        if not regexp:
            self.assertEqual(ret, expected_output)
        else:
            self.assertRegexpMatches(ret, expected_output)
        self.assertEqual(backup.exitcode, 0)
        return ret

    def assertListOutput(self, backup, expected):
        backup.join()
        outstr = backup.outstr
        self.assertEqual(backup.errstr, "")

        lines = outstr.split("\n")
        now = datetime.datetime.utcnow()
        max_delta = datetime.timedelta(minutes=1)
        parse = lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S")
        prev_date = None
        ret = []
        for x in expected:
            line = lines.pop(0)
            if x == "f":
                date = parse(line)
                ret.append({"date": date, "incrementals": []})
            elif x == "i":
                date = parse(line[1:])
                ret[-1]["incrementals"].append(date)
            else:
                raise ValueError("unknown specification: " + x)

            self.assertTrue(abs(now - date) <= max_delta,
                            "the date should be relatively close to "
                            "our current time")
            if prev_date:
                self.assertTrue(date > prev_date,
                                "the dates should be in ascending order")

            prev_date = date

        self.assertEqual(backup.exitcode, 0)

        self.assertEqual(
            len(lines), 0, "all lines should be accounted for\n" + outstr)
        return ret

    def assertError(self, backup, expected_error, expected_status):
        backup.join()
        self.assertEqual(backup.errstr, expected_error)
        self.assertEqual(backup.outstr, "")
        self.assertEqual(backup.exitcode, expected_status)

    def createSrc(self):
        """
        This method requires that the host object have a
        ``orig_src`` field pointing to the initial source.
        """
        new_path = os.path.join(tmpdir, "src")
        shutil.copytree(self.src, new_path)
        return new_path

    def modify(self, src):
        with open(os.path.join(src, "modified"), "a") as f:
            f.write("mod\n")

    def init(self, src, name="test"):
        backup = Backup(["fs-init", src, name])
        out = self.assertNoError(
            backup,
            ur"^btw-backup: created /tmp/.*?/test\..*?$",
            regexp=True)
        return out.lstrip("btw-backup: created ")

    def list(self, expected):
        self.assertListOutput(Backup(["list", self.dst]), expected)

class FSInitTest(unittest.TestCase, BackupTestMixin):

    src = os.path.join(os.getcwd(), "test-data/src")

    def tearDown(self):
        empty_tmpdir()

    def test_lacking_all_params(self):
        self.assertError(Backup(["fs-init"]),
                         "usage: btw-backup fs-init [-h] src name\n"
                         "btw-backup fs-init: error: too few arguments",
                         2)

    def test_lacking_name(self):
        self.assertError(Backup(["fs-init", "."]),
                         "usage: btw-backup fs-init [-h] src name\n"
                         "btw-backup fs-init: error: too few arguments",
                         2)

    def test_not_absolute(self):
        self.assertError(Backup(["fs-init", ".", "test"]),
                         "btw-backup: the source path must be absolute",
                         1)

    def test_new_setup(self):
        out = self.assertNoError(
            Backup(["fs-init", self.src, "test"]),
            ur"^btw-backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)

        workdir_path = out.lstrip("btw-backup: created ")
        src = os.readlink(os.path.join(workdir_path, "src"))
        self.assertEqual(src, self.src)

        shutil.rmtree(workdir_path)

    def test_duplicate_setup(self):
        out = self.assertNoError(
            Backup(["fs-init", self.src, "test"]),
            ur"^btw-backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)
        workdir_path = out.lstrip("btw-backup: created ")

        self.assertError(
            Backup(["fs-init", self.src, "test2"]),
            "btw-backup: there is already a directory for this path",
            1)

        shutil.rmtree(workdir_path)

class FSTest(unittest.TestCase, BackupTestMixin):
    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = os.path.join(tmpdir, "dst")
        os.mkdir(self.dst)

        out = self.assertNoError(
            Backup(["fs-init", self.src, "test"]),
            ur"^btw-backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)
        self.workdir_path = out.lstrip("btw-backup: created ")

    def tearDown(self):
        empty_tmpdir()

    def test_no_setup(self):
        empty_tmpdir()
        self.assertError(
            Backup(["fs", self.src, self.dst]),
            "btw-backup: no working directory for: "
            "/home/ldd/src/git-repos/btw-backup/test-data/src",
            1)

    def test_no_params(self):
        self.assertError(
            Backup(["fs"]),
            "usage: btw-backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw-backup fs: error: too few arguments",
            2)

    def test_no_dst(self):
        self.assertError(
            Backup(["fs", self.src]),
            "usage: btw-backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw-backup fs: error: too few arguments",
            2)

    def test_new_backup(self):
        self.assertNoError(Backup(["fs", self.src, self.dst]))

        backups = self.assertListOutput(
            Backup(["list", self.dst]), "f")

        # Check that something was saved!
        restore_path = tempfile.mkdtemp(dir=tmpdir)
        last_date = backups[-1]["date"].isoformat()
        subprocess.check_output(
            ["rdiff-backup", "-r",
             last_date,
             os.path.join(self.dst, last_date), restore_path])

        paths = os.listdir(restore_path)
        self.assertEqual(paths, ["backup.tar"])
        t_path = os.path.join(restore_path, "t")
        os.mkdir(t_path)
        subprocess.check_call(["tar", "-C", t_path, "-xf",
                               os.path.join(restore_path, "backup.tar")])

        # Check the files.
        subprocess.check_call(["diff", "-rN", t_path, self.src])

    def test_incremental_backup(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertListOutput(Backup(["list", self.dst]), "fi")

    def test_two_incremental_backups_no_change(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertNoError(Backup(["fs", src, self.dst]))

        # The second incremental backup did nothing.
        self.assertListOutput(Backup(["list", self.dst]), "fi")

    def test_max_incremental_count(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_COUNT=1\n")

        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertListOutput(Backup(["list", self.dst]), "fi")

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertListOutput(Backup(["list", self.dst]), "fif")

    def test_max_incremental_span(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_SPAN='0s'\n")

        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertListOutput(Backup(["list", self.dst]), "ff")

    def test_identical_full_backup(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_SPAN='0s'\n")

        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertNoError(Backup(["fs", src, self.dst]))

        # Only one backup.
        self.assertListOutput(Backup(["list", self.dst]), "f")

        # Check the log
        self.assertRegexpMatches(
            open(os.path.join(self.dst, "log.txt"), 'r').read(),
            ur"^.*: no change in the data to be backed up: "
            "skipping creation of new full backup\n$")


class ListTest(unittest.TestCase, BackupTestMixin):

    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = os.path.join(tmpdir, "dst")
        os.mkdir(self.dst)

    def tearDown(self):
        empty_tmpdir()

    def test_empty(self):
        self.assertNoError(Backup(["list", self.dst]))

    def test_one_full_backup(self):
        self.init(self.src)
        self.assertNoError(Backup(["fs", self.src, self.dst]))
        self.assertListOutput(Backup(["list", self.dst]), "f")

    def test_one_full_backup_one_incremental(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertListOutput(Backup(["list", self.dst]), "fi")

    def test_one_full_backup_two_incremental(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertListOutput(Backup(["list", self.dst]), "fii")

@contextlib.contextmanager
def fake_dumpall_cmd(new):
    old = main.dumpall_cmd
    main.dumpall_cmd = new
    yield
    main.dumpall_cmd = old

class CommonDB(BackupTestMixin):

    dst = None
    db_config_dir = None
    config_path = None

    def backup(self, args=None):
        raise NotImplementedError

    def alter_db(self):
        raise NotImplementedError

    def assertEqual(self, *args, **kwargs):
        raise NotImplementedError

    def test_initial_backup(self):
        self.backup()
        self.list("f")

    def test_duplicate_full_backup(self):
        # This forces the 2nd backup to be full too. We must do this
        # to test the logic in btw-backup proper rather than have
        # rdiff-backup decide not to add an incremental.
        with open(os.path.join(self.db_config_dir, "global.py"), 'w') as \
                conf:
            conf.write("MAX_INCREMENTAL_COUNT = 0\n")

        self.backup()
        self.backup()

        self.list("f")

    def test_incremental_backup(self):
        self.backup()

        self.alter_db()
        self.backup()

        self.list("fi")

    def test_duplicate_incremental_backup(self):
        self.backup()

        self.alter_db()
        self.backup()
        self.backup()

        self.list("fi")

    def test_two_incremental_backups(self):
        self.backup()

        self.alter_db()
        self.backup()

        self.alter_db()
        self.backup()

        self.list("fii")

    def test_max_incremental_count(self):
        with open(self.config_path, 'w') as \
                conf:
            conf.write("MAX_INCREMENTAL_COUNT = 0\n")

        self.backup()

        self.alter_db()
        self.backup()

        self.list("ff")

    def test_max_incremental_span(self):
        with open(self.config_path, 'w') as \
                conf:
            conf.write("MAX_INCREMENTAL_SPAN = '0s'\n")

        self.backup()
        self.alter_db()
        self.backup()

        self.list("ff")

    def test_uid(self):
        # We are limited as to what we can test. This only tests that
        # the code that is run when --uid is specified does not crash,
        # but it does not test that the operation is happening.
        uid = os.getuid()
        gid = os.getgid()
        uid_str = pwd.getpwuid(uid).pw_name
        gid_str = grp.getgrgid(gid).gr_name
        self.backup(["--uid", uid_str + ":" + gid_str])


class GlobalDBTest(unittest.TestCase, CommonDB):

    count = 1

    def setUp(self):
        self.dst = os.path.join(tmpdir, "dst")
        os.mkdir(self.dst)
        self.db_config_dir = os.path.join(workdir, "db")
        self.config_path = os.path.join(self.db_config_dir, "global.py")
        os.makedirs(self.db_config_dir)
        self.old_dumpall_cmd = main.dumpall_cmd
        self.alter_db()
        self.previous_contents = None

    def tearDown(self):
        main.dumpall_cmd = self.old_dumpall_cmd
        if not preserve:
            empty_tmpdir()

    def backup(self, args=None):
        if args is None:
            args = []
        self.assertNoError(Backup(["db", "-g"] + args + [self.dst]))

    def alter_db(self):
        self.previous_contents = "foo" + str(self.__class__.count)
        main.dumpall_cmd = ["echo", self.previous_contents]
        # echo adds a newline.
        self.previous_contents += "\n"
        self.__class__.count += 1

    def test_no_params(self):
        self.assertError(
            Backup(["db"]),
            "usage: btw-backup db [-h] [-g] [-u UID[:GID]] [db] dst\n"
            "btw-backup db: error: too few arguments",
            2)

    def test_no_database(self):
        self.assertError(
            Backup(["db", self.dst]),
            "btw-backup: either -g (--global) or a database name "
            "must be specified",
            1)

    def test_contents(self):
        self.backup()
        self.alter_db()
        self.backup()

        last = sorted([x for x in os.listdir(self.dst)
                       if main.fs_backup_re.match(x)])[-1]
        last_backup = os.path.join(self.dst, last, "global.sql.bz2")
        contents = None
        with bz2.BZ2File(last_backup, 'r') as f:
            contents = f.read()

        self.assertEqual(contents, self.previous_contents)


class DBTest(unittest.TestCase, CommonDB):

    db_name = "btw-backup-test"
    conn = None
    count = 1

    @classmethod
    def setUpClass(cls):
        subprocess.check_call(["createdb", cls.db_name])
        try:
            cls.conn = conn = psycopg2.connect("dbname=" + cls.db_name)
            conn.autocommit = True
        except:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        subprocess.check_call(["dropdb", cls.db_name])

    def setUp(self):
        self.dst = os.path.join(tmpdir, "dst")
        os.mkdir(self.dst)
        self.db_config_dir = os.path.join(workdir, "db")
        self.config_path = os.path.join(self.db_config_dir,
                                        self.db_name + ".py")
        os.makedirs(self.db_config_dir)

    def tearDown(self):
        empty_tmpdir()

    def alter_db(self):
        conn = self.conn
        with conn.cursor() as cur:
            cur.execute("create table foo%s (id integer not null)",
                        (self.__class__.count, ))
            self.__class__.count += 1

    def backup(self, args=None):
        if args is None:
            args = []
        self.assertNoError(Backup(["db"] + args + [self.db_name, self.dst]))

    def list(self, expected):
        self.assertListOutput(Backup(["list", self.dst]), expected)

    def test_bad_db_name(self):
        self.assertError(Backup(["db", "@GARBAGE@", self.dst]),
                         "btw-backup: pg_dump exited with code: 1",
                         1)

    def test_contents(self):
        self.backup()
        self.alter_db()
        self.backup()

        last = sorted([x for x in os.listdir(self.dst)
                       if main.fs_backup_re.match(x)])[-1]
        last_backup = os.path.join(self.dst, last, self.db_name + ".dump")
        contents = subprocess.check_output(["pg_restore", last_backup])
        expected_contents = \
            subprocess.check_output(["pg_dump", self.db_name])

        self.assertEqual(contents, expected_contents)
