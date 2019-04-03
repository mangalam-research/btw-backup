from __future__ import print_function
import unittest
import os
import tempfile
import shutil
import datetime
import subprocess
import psycopg2
import bz2
import pwd
import grp
import sys
import multiprocessing
import re
from cStringIO import StringIO

import btw_backup.__main__ as main
from btw_backup.sync import SyncState, S3
from btw_backup.errors import ImproperlyConfigured

backup_env = dict(os.environ)

class Backup(object):

    def __init__(self, args):
        self.args = args
        full_args = ["python", "-m", "btw_backup",
                     "--config-dir=" + config_dir] + args

        proc = subprocess.Popen(full_args, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=backup_env)
        self.proc = proc
        self.stdout = proc.stdout
        self.stderr = proc.stderr

    @property
    def outstr(self):
        return self.stdout.read().strip()

    @property
    def errstr(self):
        return self.stderr.read().strip()

    @property
    def exitcode(self):
        return self.proc.returncode

    def join(self):
        return self.proc.wait()

tmpdir = None
root_dir = None
config_dir = None
server = None
server_dir = None
s3cmd_config_path = None

preserve = os.environ.get("NOCLEANUP")

def check_aws_profile(profile):
    args = ["aws", "configure", "list"]
    if profile is not None:
        args += ["--profile", profile]

    failed = False
    if profile is None:
        stdout = subprocess.check_output(args,
                                         env=backup_env)

        # This is crude but it works.
        failed = (stdout != """\
      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key                <not set>             None    None
secret_key                <not set>             None    None
    region                <not set>             None    None
""")
    else:
        child = subprocess.Popen(args, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=backup_env)
        child.wait()
        failed = child.returncode == 0
        stdout = child.stdout.read()

    if failed:
        print(stdout, file=sys.stderr)
        raise ValueError("there is a {0} aws profile!"
                         .format(profile or "default"))


def setUp():
    # pylint: disable=global-statement
    global tmpdir
    global root_dir
    global config_dir
    global server
    global server_dir
    global s3cmd_config_path

    tmpdir = tempfile.mkdtemp()

    backup_env["HOME"] = tmpdir
    # We do a double check to make sure we are not going to interfere
    # with the configuration of the user running the tests.
    check_aws_profile(None)
    check_aws_profile("btw-backup-test")

    root_dir = os.path.join(tmpdir, "root")
    config_dir = os.path.join(tmpdir, "config_dir")
    server_dir = os.path.join(tmpdir, "server")
    os.mkdir(server_dir)
    os.mkdir(root_dir)

    # Start a fake aws server
    server_host = "localhost"
    server_port = 4999
    server_endpoint = "http://{0}:{1}".format(server_host, server_port)
    server = subprocess.Popen(["./node_modules/.bin/s3rver", "-p",
                               str(server_port),
                               "-s",  # Silent
                               "-d", server_dir])

    # Make btw-backup connect to our server.
    backup_env["BTW_BACKUP_S3_SERVER"] = server_endpoint

    # Create a test profile for awscli
    aws_dir = os.path.join(tmpdir, ".aws")
    os.mkdir(aws_dir)
    with open(os.path.join(aws_dir, "config"), 'w') as config:
        # Yes, the config file needs [profile <profile name>] whereas
        # the credential files needs [<profile name>]. Go figure!
        config.write("""
[profile btw-backup-test]
region=us-east-1
output=json
""")
    with open(os.path.join(aws_dir, "credentials"), 'w') as config:
        config.write("""
[btw-backup-test]
aws_access_key_id=S3RVER
aws_secret_access_key=S3RVER
""")
    subprocess.check_call(["aws", "s3",
                           "--endpoint=" + server_endpoint,
                           "--profile=btw-backup-test", "mb", "s3://foo"],
                          env=backup_env)

    s3cmd_config_path = os.path.join(tmpdir, "s3cmd")
    backup_env["S3CMD_CONFIG"] = s3cmd_config_path
    with open(s3cmd_config_path, 'w') as config:
        config.write("""
[default]
access_key = S3RVER
# access_token = x
add_encoding_exts =
add_headers =
bucket_location = US
ca_certs_file =
cache_file =
check_ssl_certificate = True
check_ssl_hostname = True
cloudfront_host = cloudfront.amazonaws.com
default_mime_type = binary/octet-stream
delay_updates = False
delete_after = False
delete_after_fetch = False
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
expiry_date =
expiry_days =
expiry_prefix =
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase =
guess_mime_type = True
host_base = {host}:{port}
host_bucket = {host}:{port}
human_readable_sizes = False
invalidate_default_index_on_cf = False
invalidate_default_index_root_on_cf = True
invalidate_on_cf = False
kms_key =
limitrate = 0
list_md5 = False
log_target_prefix =
long_listing = False
max_delete = -1
mime_type =
multipart_chunk_size_mb = 15
multipart_max_chunks = 10000
preserve_attrs = True
progress_meter = True
proxy_host =
proxy_port = 0
put_continue = False
recursive = False
recv_chunk = 65536
reduced_redundancy = False
requester_pays = False
restore_days = 1
secret_key = S3RVER
send_chunk = 65536
server_side_encryption = False
signature_v2 = False
simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 300
stats = False
stop_on_error = False
storage_class =
urlencoding_mode = normal
use_https = False
use_mime_magic = True
verbosity = WARNING
website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
website_error =
website_index = index.html
""".format(host=server_host, port=server_port))

    subprocess.check_call(["s3cmd", "ls", "s3://foo"], env=backup_env)
    reset_config()


def tearDown():
    server.kill()
    if tmpdir:
        if preserve:
            print("TMPDIR:", tmpdir)
        else:
            shutil.rmtree(tmpdir)

def clean_dir(to_clean):
    for entry in os.listdir(to_clean):
        path = os.path.join(to_clean, entry)
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

def reset_config():
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    clean_dir(config_dir)
    with open(os.path.join(config_dir, "config.py"), 'w') as config:
        config.write("""
ROOT_PATH={0}
AWSCLI_PROFILE="btw-backup-test"
S3_URI_PREFIX="s3://foo/backups/"
S3CMD_CONFIG={1}
""".format(repr(root_dir), repr(s3cmd_config_path)))

def reset_tmpdir():
    reset_config()
    clean_dir(root_dir)

def reset_server():
    if not preserve:
        clean_dir(server_dir)
        # Create the bucket "foo". Yes, we are cheating.
        os.mkdir(os.path.join(server_dir, "foo"))

def diff_against_server(path, server_relative_path):
    diff_dir = os.path.join(tmpdir, "diff")
    done = False
    while not done:
        os.mkdir(diff_dir)
        try:
            subprocess.check_call(["aws", "s3", "sync",
                                   "--only-show-errors",
                                   "--profile=btw-backup-test",
                                   "--endpoint=http://localhost:4999",
                                   os.path.join("s3://foo/backups",
                                                server_relative_path),
                                   diff_dir],
                                  env=backup_env)
            try:
                subprocess.check_call(["diff", "-rN", path, diff_dir])
                done = True
            except subprocess.CalledProcessError:
                import time
                time.sleep(2)
                print("Trying again")
        finally:
            shutil.rmtree(diff_dir)

def exists_on_server(file_path):
    # s3rver used to just store files with the same name as how they appear to
    # s3 clients. But it now stores files as 3 different files on dist.  To
    # check for the existence of a file we check that one of the internally
    # named files exists.
    return os.path.exists(os.path.join(server_dir,
                                       "{}._S3rver_object".format(file_path)))

class BackupTestMixin(object):

    def __init__(self, *args, **kwargs):
        self.tmp_src = None
        super(BackupTestMixin, self).__init__(*args, **kwargs)

    def tearDown(self):
        if self.tmp_src is not None and os.path.exists(self.tmp_src):
            shutil.rmtree(self.tmp_src)
        super(BackupTestMixin, self).tearDown()

    def assertNoError(self, backup, expected_output="", regexp=False,
                      dont_compare=False):
        backup.join()
        ret = backup.outstr
        err = backup.errstr

        self.assertEqual(err, "")
        if not regexp:
            self.assertEqual(ret, expected_output)
        else:
            self.assertRegexpMatches(ret, expected_output)
        self.assertEqual(backup.exitcode, 0)

        if not dont_compare and \
           (backup.args[0] in ("db", "fs") or
                (backup.args[0] == "sync" and backup.args[1] != "--list")):
            self.check_off_site_sync()

        return ret

    def assertRdiffListOutput(self, backup, expected):
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

    def assertTarListOutput(self, backup, expected):
        backup.join()
        outstr = backup.outstr
        self.assertEqual(backup.errstr, "")

        lines = outstr.split("\n")
        now = datetime.datetime.utcnow()
        max_delta = datetime.timedelta(minutes=1)
        parse = lambda x: datetime.datetime.strptime(
            x, "%Y-%m-%dT%H:%M:%S.tbz")
        prev_date = None
        ret = []
        for _ in range(0, expected):
            line = lines.pop(0)
            date = parse(line)
            ret.append(date)
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
        new_path = self.tmp_src = os.path.join(tmpdir, "src")
        shutil.copytree(self.src, new_path)
        return new_path

    def modify(self, src):
        with open(os.path.join(src, "modified"), "a") as f:
            f.write("mod\n")

    def init(self, src, name="test"):
        backup = Backup(["fs-init", "--type=rdiff", src, name])
        out = self.assertNoError(
            backup,
            ur"^btw_backup: created /tmp/.*?/test\..*?$",
            regexp=True)
        return out[len("btw_backup: created "):]

    def list(self, expected):
        self.assertRdiffListOutput(Backup(["list", self.dst]), expected)

    def check_off_site_sync(self):
        diff_against_server(self.dst_full, self.dst)

time_re = re.compile(ur"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
                     re.MULTILINE)

def clean_times(src):
    return time_re.sub("2016-01-01T12:00:00", src)

class BaseStateTest(unittest.TestCase):

    def setUp(self):
        self.state_path = os.path.join(tmpdir, "test_state")

    def tearDown(self):
        if os.path.exists(self.state_path):
            os.unlink(self.state_path)
        reset_tmpdir()

    def assertStateFile(self, expected, raw=False):
        with open(self.state_path, 'r') as state_file:
            actual = state_file.read()
            if not raw:
                actual = clean_times(actual)
            self.assertEqual(actual, expected)

    def storeToState(self, store):
        with open(self.state_path, 'w') as state_file:
            state_file.write(store)


class SyncStateTest(BaseStateTest):

    def test_is_mutually_exclusive(self):
        state = SyncState(self.state_path)

        # We have to do this in a different process because our own process
        # can reopen the same file multiple times.
        def target():
            with self.assertRaises(IOError):
                SyncState(self.state_path)
        p = multiprocessing.Process(target=target)
        p.start()
        p.join()

    def test_records_state_in_memory(self):
        state = SyncState(self.state_path)
        state.push_path("a")
        state.push_path("b")
        state.sync_path("c")
        state.sync_path("d")
        self.assertEqual(state.current_state, {
            "push": set(("a", "b")),
            "sync": set(("c", "d"))
        })

        state.push_done("a")
        state.sync_done("d")

        self.assertEqual(state.current_state, {
            "push": set(("b",)),
            "sync": set(("c",))
        })

    def test_saves_to_file(self):
        state = SyncState(self.state_path)
        state.push_path("a")
        state.push_path("b")
        state.sync_path("c")
        state.sync_path("d")

        self.assertStateFile("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
""")

        state.push_done("a")
        state.sync_done("d")

        self.assertStateFile("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
2016-01-01T12:00:00 -push a
2016-01-01T12:00:00 -sync d
""")

    def test_does_not_modify_a_file_if_nothing_changes(self):
        self.storeToState("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
""")
        state = SyncState(self.state_path)
        state.current_state

        self.assertStateFile("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
""", raw=True)

    def test_reads_from_file(self):
        self.storeToState("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
""")
        state = SyncState(self.state_path).current_state
        self.assertEqual(state, {
            "push": set(("a", "b")),
            "sync": set(("c", "d"))
        })

        self.storeToState("""\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 +sync c
2016-01-01T12:00:00 +sync d
2016-01-01T12:00:00 -push a
2016-01-01T12:00:00 -sync d
""")

        state = SyncState(self.state_path).current_state
        self.assertEqual(state, {
            "push": set(("b",)),
            "sync": set(("c",))
        })

        # This simulates a push that happened on the path "", which is
        # a valid path. It is equivalent to ROOT_PATH.
        self.storeToState("""\
2016-01-01T12:00:00 +sync \n\
""")

        state = SyncState(self.state_path).current_state
        self.assertEqual(state, {
            "push": set(),
            "sync": set(("",))
        })

    def test_emits_on_push_path(self):
        state = SyncState(self.state_path)
        paths = []
        state.ee.on('push', paths.append)
        state.push_path("a")
        self.assertEqual(paths, ["a"])

    def test_emits_on_sync_path(self):
        state = SyncState(self.state_path)
        paths = []
        state.ee.on('sync', paths.append)
        state.sync_path("a")
        self.assertEqual(paths, ["a"])

class S3Null(S3):

    def __init__(self, *args, **kwargs):
        super(S3Null, self).__init__(*args, **kwargs)
        self._pushed = set()
        self._synced = set()
        self._fail_on = set()

    def _do_push(self, path):
        if path in self._fail_on:
            raise Exception("faked error: " + path)
        self._pushed.add(path)

    def _do_sync(self, path):
        if path in self._fail_on:
            raise Exception("faked error: " + path)
        self._synced.add(path)

class S3Test(BaseStateTest):

    def test_raises_if_s3_uri_prefix_missing(self):
        state = SyncState(self.state_path)
        with self.assertRaisesRegexp(
                ImproperlyConfigured,
                ur"^you must specify S3_URI_PREFIX in the general "
                ur"configuration$"):
            S3Null({}, state)

    def test_raises_if_roo_path_missing(self):
        state = SyncState(self.state_path)
        with self.assertRaisesRegexp(
                ImproperlyConfigured,
                ur"^you must specify ROOT_PATH in the general "
                ur"configuration$"):
            S3Null({
                "S3_URI_PREFIX": "foo"
            }, state)

    def test_raises_error_if_syncing_nonexistent_path(self):
        state = SyncState(self.state_path)
        s3 = S3Null({
            "S3_URI_PREFIX": "s3://foo",
            "ROOT_PATH": root_dir,
        }, state)
        with self.assertRaisesRegexp(
                ValueError,
                ur"^trying to sync a non-existent path: nonexistent"):
            state.sync_path("nonexistent")

    def test_raises_error_if_syncing_a_non_directory(self):
        state = SyncState(self.state_path)
        s3 = S3Null({
            "S3_URI_PREFIX": "s3://foo",
            "ROOT_PATH": root_dir,
        }, state)

        with open(os.path.join(root_dir, "foo"), 'w') as foo:
            pass

        with self.assertRaisesRegexp(
                ValueError,
                ur"^trying to sync a path which is not a directory: foo"):
            state.sync_path("foo")

    def test_pushes_and_syncs(self):
        state = SyncState(self.state_path)
        s3 = S3Null({
            "S3_URI_PREFIX": "s3://foo",
            "ROOT_PATH": root_dir,
        }, state)

        os.mkdir(os.path.join(root_dir, "server"))

        state.sync_path("server")
        state.sync_path("")
        # We can abuse push_path because pushed paths are not checked.
        state.push_path("a")
        state.push_path("b")

        s3.run()

        self.assertEqual(s3._pushed, set(("a", "b")))
        self.assertEqual(s3._synced, set(("server", "")))

    def test_survives_fatal_errors(self):
        state = SyncState(self.state_path)
        s3 = S3Null({
            "S3_URI_PREFIX": "s3://foo",
            "ROOT_PATH": root_dir,
        }, state)

        os.mkdir(os.path.join(root_dir, "server"))

        state.sync_path("server")
        state.sync_path("")
        # We can abuse push_path because pushed paths are not checked.
        state.push_path("a")
        state.push_path("b")

        stderr = StringIO()
        s3._cached_stderr = stderr
        s3._fail_on = set(("a", ""))
        s3.run()

        # Some went through.
        self.assertEqual(s3._pushed, set(("b", )))
        self.assertEqual(s3._synced, set(("server",)))

        # Some failed.
        self.assertRegexpMatches(
            stderr.getvalue(),
            re.compile(ur"^Error while processing: a$", re.MULTILINE))
        self.assertRegexpMatches(
            stderr.getvalue(),
            re.compile(ur"^Error while processing: $", re.MULTILINE))

        # Those that failed still need to be done.
        self.assertEqual(state.current_state, {
            "push": set(("a", )),
            "sync": set(("", ))
        })

        # The state on disk does not show the failures as done.
        # The funky "\n\" in what follows is to prevent git from swallowing
        # the space at the end of the line.
        self.assertStateFile("""\
2016-01-01T12:00:00 +sync server
2016-01-01T12:00:00 +sync \n\
2016-01-01T12:00:00 +push a
2016-01-01T12:00:00 +push b
2016-01-01T12:00:00 -push b
2016-01-01T12:00:00 -sync server
""")

class CommonTests(BackupTestMixin, unittest.TestCase):

    def tearDown(self):
        reset_tmpdir()
        super(CommonTests, self).tearDown()

    def test_no_root(self):
        open(os.path.join(config_dir, "config.py"), 'w').close()

        self.assertError(
            Backup(["fs-init", "--type=rdiff", "/tmp", "test"]),
            "btw_backup: you must specify ROOT_PATH in the general "
            "configuration", 1)

    def test_no_s3cmd_config(self):
        with open(os.path.join(config_dir, "config.py"), 'w') as config:
            config.write("""
ROOT_PATH={0}
S3_URI_PREFIX="q"
""".format(repr(root_dir)))

        self.assertError(
            Backup(["db", "-g", "test"]),
            "btw_backup: you must specify S3CMD_CONFIG in the general "
            "configuration", 1)

    def test_no_s3_uri_prefix(self):
        with open(os.path.join(config_dir, "config.py"), 'w') as config:
            config.write("""
ROOT_PATH={0}
S3CMD_CONFIG="q"
""".format(repr(root_dir)))

        self.assertError(
            Backup(["db", "-g", "test"]),
            "btw_backup: you must specify S3_URI_PREFIX in the general "
            "configuration", 1)


class FSInitTest(BackupTestMixin, unittest.TestCase):

    src = os.path.join(os.getcwd(), "test-data/src")

    def tearDown(self):
        reset_tmpdir()
        super(FSInitTest, self).tearDown()

    def test_lacking_all_params(self):
        self.assertError(
            Backup(["fs-init"]),
            "usage: btw_backup fs-init [-h] --type {rdiff,tar} src name\n"
            "btw_backup fs-init: error: too few arguments",
            2)

    def test_lacking_name(self):
        self.assertError(
            Backup(["fs-init", "."]),
            "usage: btw_backup fs-init [-h] --type {rdiff,tar} src name\n"
            "btw_backup fs-init: error: too few arguments",
            2)

    def test_lacking_type(self):
        self.assertError(
            Backup(["fs-init", "/", "test"]),
            "usage: btw_backup fs-init [-h] --type {rdiff,tar} src name\n"
            "btw_backup fs-init: error: argument --type is required",
            2)

    def test_not_absolute(self):
        self.assertError(Backup(["fs-init", "--type=rdiff", ".", "test"]),
                         "btw_backup: the source path must be absolute",
                         1)

    def test_new_setup(self):
        out = self.assertNoError(
            Backup(["fs-init", "--type=rdiff", self.src, "test"]),
            ur"^btw_backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)

        workdir_path = out[len("btw_backup: created "):]
        src = os.readlink(os.path.join(workdir_path, "src"))
        self.assertEqual(src, self.src)

        shutil.rmtree(workdir_path)

    def test_duplicate_setup(self):
        out = self.assertNoError(
            Backup(["fs-init", "--type=rdiff", self.src, "test"]),
            ur"^btw_backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)
        workdir_path = out[len("btw_backup: created "):]

        self.assertError(
            Backup(["fs-init", "--type=rdiff", self.src, "test2"]),
            "btw_backup: there is already a directory for this path",
            1)

        shutil.rmtree(workdir_path)

class FSRdiffTest(BackupTestMixin, unittest.TestCase):
    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)
        os.mkdir(self.dst_full)

        self.assertNoError(
            Backup(["fs-init", "--type=rdiff", self.src, "test"]),
            ur"^btw_backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_tmpdir()
        reset_server()
        super(FSRdiffTest, self).tearDown()

    def test_no_setup(self):
        reset_tmpdir()
        self.assertError(
            Backup(["fs", self.src, self.dst]),
            "btw_backup: no working directory for: "
            "/home/ldd/src/git-repos/btw-backup/test-data/src",
            1)

    def test_no_params(self):
        self.assertError(
            Backup(["fs"]),
            "usage: btw_backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw_backup fs: error: too few arguments",
            2)

    def test_no_dst(self):
        self.assertError(
            Backup(["fs", self.src]),
            "usage: btw_backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw_backup fs: error: too few arguments",
            2)

    def test_new_backup(self):
        self.assertNoError(Backup(["fs", self.src, self.dst]))

        backups = self.assertRdiffListOutput(
            Backup(["list", self.dst]), "f")

        # Check that something was saved!
        restore_path = tempfile.mkdtemp(dir=tmpdir)
        last_date = backups[-1]["date"].isoformat()
        subprocess.check_output(
            ["rdiff-backup", "-r",
             last_date,
             os.path.join(self.dst_full, last_date), restore_path])

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
        self.assertRdiffListOutput(Backup(["list", self.dst]), "fi")

    def test_two_incremental_backups_no_change(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertNoError(Backup(["fs", src, self.dst]))

        # The second incremental backup did nothing.
        self.assertRdiffListOutput(Backup(["list", self.dst]), "fi")

    def test_max_incremental_count(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_COUNT=1\n")

        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertRdiffListOutput(Backup(["list", self.dst]), "fi")

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertRdiffListOutput(Backup(["list", self.dst]), "fif")

    def test_max_incremental_span(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_SPAN='0s'\n")

        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertRdiffListOutput(Backup(["list", self.dst]), "ff")

    def test_identical_full_backup(self):
        src = self.createSrc()
        workdir_path = self.init(src)
        config_path = os.path.join(workdir_path, "config.py")
        with open(config_path, "a") as f:
            f.write("MAX_INCREMENTAL_SPAN='0s'\n")

        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertNoError(Backup(["fs", src, self.dst]))

        # Only one backup.
        self.assertRdiffListOutput(Backup(["list", self.dst]), "f")

        # Check the log
        self.assertRegexpMatches(
            open(os.path.join(self.dst_full, "log.txt"), 'r').read(),
            ur"^.*: no change in the data to be backed up: "
            "skipping creation of new full backup\n$")


class FSTarTest(BackupTestMixin, unittest.TestCase):
    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)
        os.mkdir(self.dst_full)

        self.assertNoError(
            Backup(["fs-init", "--type=tar", self.src, "test"]),
            ur"^btw_backup: created /tmp/.*?/test.uqsE0Q",
            regexp=True)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_tmpdir()
        reset_server()
        super(FSTarTest, self).tearDown()

    def init(self, src, name="test"):
        backup = Backup(["fs-init", "--type=tar", src, name])
        out = self.assertNoError(
            backup,
            ur"^btw_backup: created /tmp/.*?/test\..*?$",
            regexp=True)
        return out[len("btw_backup: created "):]

    def test_no_setup(self):
        reset_tmpdir()
        self.assertError(
            Backup(["fs", self.src, self.dst]),
            "btw_backup: no working directory for: "
            "/home/ldd/src/git-repos/btw-backup/test-data/src",
            1)

    def test_no_params(self):
        self.assertError(
            Backup(["fs"]),
            "usage: btw_backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw_backup fs: error: too few arguments",
            2)

    def test_no_dst(self):
        self.assertError(
            Backup(["fs", self.src]),
            "usage: btw_backup fs [-h] [-u UID[:GID]] src dst\n"
            "btw_backup fs: error: too few arguments",
            2)

    def test_new_backup(self):
        self.assertNoError(Backup(["fs", self.src, self.dst]))

        backups = self.assertTarListOutput(
            Backup(["list", self.dst]), 1)

        # Check that something was saved!
        restore_path = tempfile.mkdtemp(dir=tmpdir)
        last_date = backups[-1].isoformat()
        subprocess.check_call(["tar", "-C", restore_path, "-xf",
                               os.path.join(self.dst_full,
                                            last_date + ".tbz")])

        # Check the files.
        subprocess.check_call(["diff", "-rN", restore_path, self.src])

    def test_identical_backup(self):
        src = self.createSrc()
        workdir_path = self.init(src)

        self.assertNoError(Backup(["fs", src, self.dst]))
        self.assertNoError(Backup(["fs", src, self.dst]))

        # Only one backup.
        self.assertTarListOutput(Backup(["list", self.dst]), 1)

        # Check the log
        self.assertRegexpMatches(
            open(os.path.join(self.dst_full, "log.txt"), 'r').read(),
            ur"^.*: no change in the data to be backed up: "
            "dropping backup\n$")

class ListTest(BackupTestMixin, unittest.TestCase):

    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)
        os.mkdir(self.dst_full)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_tmpdir()
        reset_server()
        super(ListTest, self).tearDown()

    def test_empty(self):
        self.assertNoError(Backup(["list", self.dst]))

    def test_one_full_backup(self):
        self.init(self.src)
        self.assertNoError(Backup(["fs", self.src, self.dst]))
        self.assertRdiffListOutput(Backup(["list", self.dst]), "f")

    def test_one_full_backup_one_incremental(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertRdiffListOutput(Backup(["list", self.dst]), "fi")

    def test_one_full_backup_two_incremental(self):
        src = self.createSrc()
        self.init(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.modify(src)
        self.assertNoError(Backup(["fs", src, self.dst]))

        self.assertRdiffListOutput(Backup(["list", self.dst]), "fii")

class SyncTest(BackupTestMixin, unittest.TestCase):

    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_tmpdir()
        reset_server()
        super(SyncTest, self).tearDown()

    def test_full(self):
        # Create some files in dst
        shutil.copytree(self.src, self.dst_full)

        self.assertNoError(Backup(["sync", "--full"]))

    def test_default(self):
        # Create some files in dst
        shutil.copytree(self.src, self.dst_full)

        # Write a fake state.
        with open(os.path.join(config_dir, "sync_state"), 'w') as state:
            state.write("""\
2016-01-01T12:00:00 +push dst/a
""")

        self.assertNoError(Backup(["sync"]), dont_compare=True)

        self.assertTrue(exists_on_server("foo/backups/dst/a"))


class SyncStateCommandTest(BackupTestMixin, unittest.TestCase):
    src = os.path.join(os.getcwd(), "test-data/src")

    def setUp(self):
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_tmpdir()
        reset_server()
        super(SyncStateCommandTest, self).tearDown()

    def test_list(self):
        # Create some files in dst
        shutil.copytree(self.src, self.dst_full)

        # Write a fake state.
        with open(os.path.join(config_dir, "sync_state"), 'w') as state:
            state.write("""\
2016-01-01T12:00:00 +push dst/a
2016-01-01T12:00:00 +push dst/b
2016-01-01T12:00:00 +sync dst
""")

        self.assertNoError(Backup(["sync-state", "--list"]), """\
Must sync: dst
Must push: dst/a
Must push: dst/b\
""")

    def test_reset_fails(self):
        # Create some files in dst
        shutil.copytree(self.src, self.dst_full)

        # Write a fake state.
        with open(os.path.join(config_dir, "sync_state"), 'w') as state:
            state.write("""\
2016-01-01T12:00:00 +push dst/a
2016-01-01T12:00:00 +push dst/b
2016-01-01T12:00:00 +sync dst
""")

        self.assertError(Backup(["sync-state", "--reset"]),
                         "btw_backup: cannot reset: some files "
                         "must be synced or pushed", 1)

    def test_reset_works(self):
        # Create some files in dst
        shutil.copytree(self.src, self.dst_full)

        # Write a fake state.
        with open(os.path.join(config_dir, "sync_state"), 'w') as state:
            state.write("""\
2016-01-01T12:00:00 +push dst/a
2016-01-01T12:00:00 +push dst/b
2016-01-01T12:00:00 +sync dst
2016-01-01T12:00:00 -push dst/a
2016-01-01T12:00:00 -push dst/b
2016-01-01T12:00:00 -sync dst
""")

        self.assertNoError(Backup(["sync-state", "--reset"]),
                           "The state was reset")


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
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)
        os.mkdir(self.dst_full)
        self.db_config_dir = os.path.join(config_dir, "db")
        self.config_path = os.path.join(self.db_config_dir, "global.py")
        os.makedirs(self.db_config_dir)
        self.dumpall_cmd = None
        self.previous_contents = None
        self.alter_db()

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_server()
        if not preserve:
            reset_tmpdir()
        super(GlobalDBTest, self).tearDown()

    def backup(self, args=None):
        if args is None:
            args = []
        self.assertNoError(Backup(["db", "-g"] +
                                  ([] if self.dumpall_cmd is None else
                                   ["--fake-dumpall",
                                    " ".join(self.dumpall_cmd)]) +
                                  args +
                                  [self.dst]))

    def alter_db(self):
        self.previous_contents = "foo" + str(self.__class__.count)
        self.dumpall_cmd = ["echo", self.previous_contents]
        # echo adds a newline.
        self.previous_contents += "\n"
        self.__class__.count += 1

    def test_no_params(self):
        self.assertError(
            Backup(["db"]),
            "usage: btw_backup db [-h] [-g] [-u UID[:GID]] [db] dst\n"
            "btw_backup db: error: too few arguments",
            2)

    def test_no_database(self):
        self.assertError(
            Backup(["db", self.dst]),
            "btw_backup: either -g (--global) or a database name "
            "must be specified",
            1)

    def test_contents(self):
        self.backup()
        self.alter_db()
        self.backup()

        last = sorted([x for x in os.listdir(self.dst_full)
                       if main.fs_backup_re.match(x)])[-1]
        last_backup = os.path.join(self.dst_full, last, "global.sql.bz2")
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
        self.dst = "dst"
        self.dst_full = os.path.join(root_dir, self.dst)
        os.mkdir(self.dst_full)
        self.db_config_dir = os.path.join(config_dir, "db")
        self.config_path = os.path.join(self.db_config_dir,
                                        self.db_name + ".py")
        os.makedirs(self.db_config_dir)

    def tearDown(self):
        if os.path.exists(self.dst_full):
            shutil.rmtree(self.dst_full)
        reset_server()
        reset_tmpdir()
        super(DBTest, self).tearDown()

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
        self.assertRdiffListOutput(Backup(["list", self.dst]), expected)

    def test_bad_db_name(self):
        self.assertError(
            Backup(["db", "@GARBAGE@", self.dst]),
            'pg_dump: [archiver (db)] connection to database "@GARBAGE@" '
            'failed: FATAL:  database "@GARBAGE@" does not exist\n'
            'btw_backup: pg_dump exited with code: 1',
            1)

    def test_contents(self):
        self.backup()
        self.alter_db()
        self.backup()

        last = sorted([x for x in os.listdir(self.dst_full)
                       if main.fs_backup_re.match(x)])[-1]
        last_backup = os.path.join(self.dst_full, last,
                                   self.db_name + ".dump")
        contents = subprocess.check_output(["pg_restore", last_backup])
        expected_contents = \
            subprocess.check_output(["pg_dump", self.db_name])

        self.assertEqual(contents, expected_contents)
