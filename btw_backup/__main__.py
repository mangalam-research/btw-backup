import sys
import argparse
import os
import re
import subprocess
from subprocess import CalledProcessError
import shutil
import base64
import datetime
import time
import filecmp
import tempfile
import bz2
import pwd
import grp
from contextlib import closing

import pytimeparse
from pyhash import murmur3_32

from .sync import SyncState, S3Cmd
from .errors import FatalUserError

dirname = os.path.dirname(__file__)

__version__ = open(os.path.join(dirname, '..', 'VERSION')).read().strip()

fs_backup_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
prog = None
dumpall_cmd = ["pg_dumpall", "-g"]

os.umask(0077)

class Exit(Exception):

    def __init__(self, status):
        super(Exit, self).__init__(status)

def fatal(msg):
    sys.stderr.write("{0}: {1}\n".format(prog, msg))
    raise Exit(1)

def info(msg):
    sys.stdout.write("{0}: {1}\n".format(prog, msg))

def echo(msg):
    sys.stdout.write(msg + "\n")

def int_as_bytearray(i):
    # Doing this avoids the leading 0x and trailing L (on longs) that
    # hex() adds.
    as_hex = "%x" % i

    # The ``if... else...`` bit is to ensure that we have an even
    # number of digits.
    return bytearray.fromhex(as_hex if not len(as_hex) % 2
                             else "0" + as_hex)

hasher = murmur3_32()
def get_hash(src):
    return base64.urlsafe_b64encode(int_as_bytearray(hasher(src))) \
                 .rstrip("=")

def get_src_path(working_dir):
    return os.path.join(working_dir, "src")

class Command(object):

    def __init__(self, args):
        """
        A command on the command line. This backup software takes a
        command as its first argument.
        """
        self.args = args
        self._general_config = None
        self.root = self.general_config.get("ROOT_PATH", None)
        if self.root is None:
            fatal("you must specify ROOT_PATH in the general configuration")

    @property
    def general_config(self):
        if self._general_config is not None:
            return self._general_config

        general_config_path = os.path.join(self.args.config_dir,
                                           "config.py")
        self._general_config = {}
        execfile(general_config_path, self._general_config)
        return self._general_config

    def execute(self):
        "Executes the command"
        pass

    def sync(self):
        "Synchronizes with offsite storage"
        # Not all commands store anything offsite...
        pass

class SourceCommand(Command):

    def __init__(self, args):
        """
        A command that takes an ``src`` argument on the command
        line. Besides need a ``src`` argument, these commands depend
        on a "working directory" in which they find configuration
        options and create files while they perform their work.

        :param args: The arguments passed on the command line.
        """
        super(SourceCommand, self).__init__(args)
        self.src = args.src
        self._working_dir = None
        self._config = None
        self._backup_dir = None

    @property
    def working_dir(self):
        """
        The working directory for this command.
        """
        if self._working_dir is not None:
            return self._working_dir

        h = get_hash(self.src)

        try:
            working_dirs = os.listdir(self.args.config_dir)
        except OSError:
            working_dirs = []

        suffix = "." + h
        candidates = [d for d in working_dirs if d.endswith(suffix)]
        if len(candidates) > 1:
            fatal("duplicate working directories: " + ", ".join(candidates))

        if not candidates:
            return None

        candidate = os.path.join(self.args.config_dir, candidates[0])

        src_file = get_src_path(candidate)
        link = os.readlink(src_file)

        if link != self.src:
            fatal("found a working directory with the wrong path: " +
                  candidate)

        self._working_dir = candidate
        return candidate

    @property
    def config(self):
        """
        The configuration for this command. The configuration is stored in
        a ``config.py`` file located under the working directory. This
        property is the *configuration itself*, not the file name.
        """
        if self._config is not None:
            return self._config

        wd = self.working_dir
        conf = {}
        execfile(os.path.join(wd, "config.py"), conf)

        self._config = conf
        return conf

    @property
    def backup_dir(self):
        """
        The backup directory is a directory under the working directory
        named "backup", that this command may use for creating a new
        backup, before moving it to its final location.
        """
        if self._backup_dir is not None:
            return self._backup_dir

        self._backup_dir = ret = os.path.join(self.working_dir, "backup")
        return ret


class FSBackupInit(SourceCommand):

    """
    Initializes the working directory for fs backups.
    """

    def execute(self):
        src = self.src
        backup_type = self.args.backup_type
        name = self.args.name

        if not os.path.isabs(src):
            fatal("the source path must be absolute")

        existing = self.working_dir
        if existing:
            fatal("there is already a directory for this path")

        h = get_hash(src)
        working_dir = os.path.join(self.args.config_dir, name + "." + h)
        src_file = get_src_path(working_dir)
        os.makedirs(working_dir)
        try:
            os.symlink(src, src_file)
        except:  # pylint: disable=bare-except
            # Don't leave the fs with an incomplete directory.
            shutil.rmtree(working_dir)

        config_path = os.path.join(working_dir, "config.py")
        with open(config_path, "w") as f:
            if backup_type == "rdiff":
                f.write("""\
TYPE="rdiff"
MAX_INCREMENTAL_COUNT=10
MAX_INCREMENTAL_SPAN="24h"
""")
            elif backup_type == "tar":
                f.write("""\
TYPE="tar"
""")
            else:
                fatal("unknown backup type: " + backup_type)

        info("created " + working_dir)
        return 0

class BaseBackupCommand(Command):

    def __init__(self, args):
        """
        A mixin or base class for all backup commands.
        """
        super(BaseBackupCommand, self).__init__(args)
        sync_state_path = os.path.join(self.args.config_dir, "sync_state")
        self.sync_state = SyncState(sync_state_path)
        self.sync_backend = S3Cmd(self.general_config, self.sync_state)

    @property
    def config(self):
        """
        Not implemented here.
        """
        raise NotImplementedError

    def execute_backup(self):
        """
        This is the method that actually performs the backup itself. It
        must be implemented by derived classes.
        """
        raise NotImplementedError

    def sync(self):
        self.sync_backend.run()

    def chownif(self, path):
        """
        Change ownership of a file only if the command-line arguments
        requested it.
        """
        if self.args.uid:
            self.chown(path)

    def chown(self, path):
        """
        Change ownership of a file on the basis of the command-line arguments
        passed.
        """
        os.chown(path, self.args.uid, self.args.gid)

    def compare(self, a, b):
        """
        :returns: ``True`` if the files are the same, ``False`` if not.
        """
        return filecmp.cmp(a, b, shallow=False)

    def log(self, msg):
        """
        Stores a message into the log file. A newline is automatically
        added to the message. This supposes that there is a ``dst``
        directory specified on the command line. This method creates
        the file if it does not yet exist and sets ownership according
        to the command-line arguments.

        :params msg: The message to store in the log file.
        """
        log_relative_path = "log.txt"
        log_path = os.path.join(self.root, self.args.dst, log_relative_path)
        with open(log_path, "a") as f:
            f.write(msg + "\n")
        self.chownif(log_path)
        self.push_path(os.path.join(self.args.dst, log_relative_path))

    def push_path(self, path):
        self.sync_state.push_path(path)

    def sync_path(self, path):
        self.sync_state.sync_path(path)


class Sync(BaseBackupCommand):
    def __init__(self, args):
        """
        Sync ROOT_PATH with S3 storage. You would use this if you already
        have backups on the local machine and want to synchronize them
        to a new S3 location.
        """
        super(Sync, self).__init__(args)
        if self.args.full and self.args.list:
            raise Exception("--full and --list are not allowed together")
        self.suppress_sync = False

    def sync(self):
        if not self.suppress_sync:
            return super(Sync, self).sync()

    def execute(self):
        if self.args.list:
            current_state = self.sync_state.current_state
            for path in current_state["sync"]:
                print("Must sync: " + path)

            for path in current_state["push"]:
                print("Must push: " + path)
            self.suppress_sync = True
        elif self.args.full:
            self.sync_path("")

        # We do not execute self.sync because this is done by default
        # with all command executions.

class RdiffBackupCommand(BaseBackupCommand):

    def __init__(self, args):
        """
        A backup command that uses ``rdiff-backup`` to perform the backup.

        :param args: The command-line arguments.
        """
        super(RdiffBackupCommand, self).__init__(args)
        self._outfile = None

    @property
    def backup_dir(self):
        """
        This class does not know anything about the backup_dir so does not
        implement the property.
        """
        raise NotImplementedError

    @property
    def outfile(self):
        """
        The complete path of the file into which to store the backup
        operation.
        """
        if self._outfile is not None:
            return self._outfile

        self._outfile = ret = os.path.join(self.backup_dir,
                                           self.outfile_base)
        return ret

    @property
    def outfile_base(self):
        """
        The basename of ``outfile`` derived classes must override this
        property to provide an actual name.
        """
        raise NotImplementedError

    def rdiff_backup(self, src, dst):
        """
        Executes ``rdiff-backup``. This method will also change ownership
        of the created files.

        :param src: The source path to backup.
        :param dst: The destination to backup to.
        """
        subprocess.check_call(["rdiff-backup", src, dst])
        # We perform the test with self.args.uid here and just call
        # self.chown instead of calling self.chownif repeatedly.
        if self.args.uid:
            self.chown(dst)
            for (root, dirnames, filenames) in os.walk(dst):
                for f in dirnames + filenames:
                    self.chown(os.path.join(root, f))

    def execute_backup(self):
        dst = os.path.join(self.root, self.args.dst)
        config = self.config
        backup_dir = self.backup_dir

        files = sorted(x for x in os.listdir(dst) if fs_backup_re.match(x))

        last = files[-1] if files else None
        last_path = os.path.join(dst, last) if last else None

        full_backup = True
        if last:
            incrementals = get_incrementals_for(last_path)
            max_span = pytimeparse.parse(config["MAX_INCREMENTAL_SPAN"])
            now = datetime.datetime.utcnow().replace(microsecond=0)
            if len(incrementals) < config["MAX_INCREMENTAL_COUNT"] and \
               now - datetime.datetime.strptime(last, "%Y-%m-%dT%H:%M:%S") \
               < datetime.timedelta(seconds=max_span):
                full_backup = False

        last_incremental = last and get_incrementals_for(last_path, True)[-1]
        if not full_backup:

            # We do this so that we don't start two backups in the same
            # second. It would indeed be a bizarre use of this software to
            # start two backups in the same second but we should check for
            # this eventuality anyway.
            #
            # rdiff-backup also detects the occurrence and fails
            # rather than wait.
            #
            last_incremental = datetime.datetime.strptime(
                last_incremental, "%Y-%m-%dT%H:%M:%S")
            while True:
                now = datetime.datetime.utcnow().replace(microsecond=0)
                if now - last_incremental >= datetime.timedelta(seconds=1):
                    break
                time.sleep(0.5)

            # We don't need to test ``last_path`` here as it must
            # necessarily not be ``None``.
            if self.compare(self.outfile,
                            os.path.join(last_path, self.outfile_base)):
                self.log(now.isoformat() +
                         ": no change in the data to be backed up: "
                         "skipping creation of new incremental backup")
            else:
                # rdiff-backup appears to first test the modification
                # time of a file with a resolution of a second. If the
                # modification time of the file is the same as what is
                # stored in the previous backup, then it is considered
                # "unchanged" and rdiff-backup does not further
                # examine the file... This is a problem for us. If we
                # are here, we've determined that the file is in fact
                # different.
                #
                # So we force the issue by touching the file here.
                os.utime(self.outfile, None)

                self.rdiff_backup(backup_dir, last_path)
                # This path won't have the final "/" unless we add it.
                self.sync_path(os.path.join(self.args.dst, last) + "/")
        else:
            #
            # We do this so that we don't start two backups in the same
            # second. It would indeed be a bizarre use of this software to
            # start two backups in the same second but we should check for
            # this eventuality anyway.
            #
            # We also check the last incremental. There's no hard reason
            # to prevent the next full backup from being on the same
            # second as the last incremental but it does simplify testing
            # a little bit and is consistent with the rest of the
            # software.
            #
            while True:
                now = datetime.datetime.utcnow().replace(microsecond=0)
                new_dir_name = now.isoformat()
                if last is None or (new_dir_name != last
                                    and new_dir_name != last_incremental):
                    break
                time.sleep(0.5)

            # Don't save the full backup unless it is actually different
            # from the previous one.
            if last_path is not None and \
               self.compare(self.outfile,
                            os.path.join(last_path, self.outfile_base)):
                self.log(new_dir_name +
                         ": no change in the data to be backed up: "
                         "skipping creation of new full backup")
            else:
                new_dir_path = os.path.join(dst, new_dir_name)
                os.mkdir(new_dir_path)

                self.rdiff_backup(backup_dir, new_dir_path)
                # This path will never have a "/" at the end unless we
                # add it.
                self.push_path(os.path.join(self.args.dst, new_dir_name) +
                               "/")


class TarBackupCommand(SourceCommand, BaseBackupCommand):

    def __init__(self, args):
        """
        A command that backs up using ``tar``.

        :param args: The command-line arguments.
        """
        super(TarBackupCommand, self).__init__(args)

    def execute(self):
        src = self.src

        if self.working_dir is None:
            fatal("no working directory for: " + src)

        backup_dir = self.backup_dir
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        self.execute_backup()
        return 0

    def execute_backup(self):
        src = self.args.src
        dst = os.path.join(self.root, self.args.dst)

        files = sorted(x for x in os.listdir(dst) if fs_backup_re.match(x))

        last = files[-1] if files else None
        last_path = os.path.join(dst, last) if last else None

        #
        # We do this so that we don't start two backups in the same
        # second. It would indeed be a bizarre use of this software to
        # start two backups in the same second but we should check for
        # this eventuality anyway.
        #
        # We also check the last incremental. There's no hard reason
        # to prevent the next full backup from being on the same
        # second as the last incremental but it does simplify testing
        # a little bit and is consistent with the rest of the
        # software.
        #
        while True:
            now = datetime.datetime.utcnow().replace(microsecond=0)
            new_backup_name = now.isoformat() + ".tbz"
            if last is None or new_backup_name != last:
                break
            time.sleep(0.5)

        new_backup_path = os.path.join(dst, new_backup_name)
        tar_args = ["-C", src, "--exclude-tag-under=NOBACKUP-TAG",
                    "-cpjf", new_backup_path, "."]
        subprocess.check_call(["tar"] + tar_args)
        self.chownif(new_backup_path)

        if last_path is not None and \
           self.compare(new_backup_path, last_path):
            self.log(new_backup_name +
                     ": no change in the data to be backed up: "
                     "dropping backup")
            os.unlink(new_backup_path)
        else:
            self.push_path(os.path.join(self.args.dst, new_backup_name))

class SourceRdiffBackupCommand(SourceCommand, RdiffBackupCommand):
    """
    A backup command using ``rdiff-backup`` to perform the backups,
    and which depends on a ``src`` argument on the command line.
    """

    def execute(self):
        src = self.src

        if self.working_dir is None:
            fatal("no working directory for: " + src)

        backup_dir = self.backup_dir
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        outfile = self.outfile
        tar_args = ["-C", src, "--exclude-tag-under=NOBACKUP-TAG",
                    "-cpf", outfile, "."]

        subprocess.check_call(["tar"] + tar_args)
        self.chownif(outfile)

        self.execute_backup()
        return 0

    @property
    def outfile_base(self):
        return "backup.tar"

class FSBackup(SourceCommand):

    """
    Backs up a filesystem hierarchy.
    """

    def __init__(self, *args, **kwargs):
        super(FSBackup, self).__init__(*args, **kwargs)

        if self.working_dir is None:
            fatal("no working directory for: " + self.args.src)

        config = self.config
        backup_type = config.get("TYPE")
        sub = None
        if backup_type == "rdiff":
            sub = SourceRdiffBackupCommand(self.args)
        elif backup_type == "tar":
            sub = TarBackupCommand(self.args)
        else:
            fatal("unknown backup type: " + backup_type)

        self.sub = sub

    def execute(self):
        return self.sub.execute()

    def sync(self):
        return self.sub.sync()


def get_incrementals_for(fullpath, include_full=False):
    """
    Get the list of incremental backups stored by ``rdiff-backup`` at
    a specific path.

    :param fullpath: The path.

    :param include_full: Include the full backup in the
    listing. Defaults to ``False``.

    :returns: The list of backups.
    """
    out = subprocess.check_output(
        ["rdiff-backup", "-l", "--parsable-output", fullpath])
    incrementals = out.split("\n")

    if not include_full:
        # The first incremental in the list corresponds to the full
        # backup, so drop it.
        incrementals.pop(0)
    ret = []
    for incremental in incrementals:
        incremental = incremental.strip()
        if not incremental:
            continue
        parts = incremental.split()
        ret.append(datetime.datetime.utcfromtimestamp(int(parts[0]))
                   .isoformat())

    return ret

class List(Command):

    """
    Lists the backups stored at a location.
    """

    def execute(self):
        dst = os.path.join(self.root, self.args.dst)

        files = sorted(x for x in os.listdir(dst) if fs_backup_re.match(x))
        for f in files:
            echo(f)
            if not f.endswith(".tbz"):
                fullpath = os.path.join(dst, f)
                for incremental in get_incrementals_for(fullpath):
                    echo(" " + incremental)

        return 0

class DBBackup(RdiffBackupCommand):

    """
    Backs up a database.
    """

    def __init__(self, args):
        super(DBBackup, self).__init__(args)
        self._config = None
        self._backup_dir = None

        if self.args.g and self.args.db:
            fatal("-g (--global) cannot be used with a database name")

        if not self.args.g and self.args.db is None:
            fatal("either -g (--global) or a database name "
                  "must be specified")

        if self.args.db == "global":
            fatal("the name 'global' cannot be used as a database name: "
                  "either modify this software or rename your database")

    @property
    def config(self):
        """
        The configuration for this command. It is stored in a file named
        "global.py" or "<db>.py" under the base working directory of
        this software.
        """
        if self._config is not None:
            return self._config

        if self.args.g:
            config_name = "global.py"
        else:
            config_name = self.args.db + ".py"

        config_path = os.path.join(self.args.config_dir, "db", config_name)

        conf = {
            "MAX_INCREMENTAL_COUNT": 10,
            "MAX_INCREMENTAL_SPAN": "24h"
        }
        if os.path.exists(config_path):
            execfile(config_path, conf)

        self._config = conf
        return conf

    @property
    def backup_dir(self):
        """
        A temporary directory where to store the backup.
        """
        if self._backup_dir is not None:
            return self._backup_dir

        self._backup_dir = ret = tempfile.mkdtemp(prefix="btw-backup")
        return ret

    @property
    def outfile_base(self):
        return "global.sql.bz2" if self.args.g else self.args.db + ".dump"

    def execute(self):
        outfile = self.outfile
        if self.args.g:
            fake = self.args.fake_dumpall
            args = fake.split() if fake else dumpall_cmd

            dump = subprocess.Popen(args, stdout=subprocess.PIPE)

            with closing(dump.stdout), \
                    closing(bz2.BZ2File(outfile, 'wb')) as bz2file:
                shutil.copyfileobj(dump.stdout, bz2file)

            retcode = dump.wait()
            if retcode:
                fatal("pg_dumpall exited with code: " + str(retcode))

        else:
            with open(outfile, 'w') as out:
                try:
                    subprocess.check_call(["pg_dump", "-Fc", self.args.db],
                                          stdout=out)
                except CalledProcessError as ex:
                    fatal("pg_dump exited with code: " + str(ex.returncode))

        self.chownif(outfile)

        self.execute_backup()

        shutil.rmtree(self._backup_dir)
        return 0

    def compare(self, a, b):
        if self.args.g:
            # The generated files are SQL files, which we can just
            # compare directly.
            return filecmp.cmp(a, b, shallow=False)

        # The dumps created with -Fc contain a time stamp. So even if
        # the *contents* of the saved databases are the same, the
        # dumps will differ. So we have to compare the dumps after
        # converting them to SQL.

        dump_b = None
        dump_a = subprocess.Popen(["pg_restore", a], stdout=subprocess.PIPE)
        dump_b = subprocess.Popen(["pg_restore", b], stdout=subprocess.PIPE)
        try:
            pipe_a = dump_a.stdout
            pipe_b = dump_b.stdout

            bufsize = 64 * 1024
            same = True
            while same:
                buf_a = pipe_a.read(bufsize)
                buf_b = pipe_b.read(bufsize)

                len_b = len(buf_b)
                if len(buf_a) == len_b and len_b == 0:
                    break

                same = buf_a == buf_b

            return same
        finally:
            if dump_a is not None and dump_a.poll() is None:
                dump_a.kill()
            if dump_b is not None and dump_b.poll() is None:
                dump_b.kill()

uid_spec = {
    "args": ("-u", "--uid"),
    "kwargs": {
        "help": "sets the uid and gid to which the final "
        "created file will be set. The value should be "
        "in the same format as the argument passed to "
        "chown.",
        "metavar": "UID[:GID]"
    }
}


def main():
    global prog  # pylint: disable=global-statement
    # This happens if we use "python -m", for instance, not very useful.
    if sys.argv[0].endswith(os.path.join("btw_backup", "__main__.py")):
        sys.argv[0] = __loader__.fullname.split(".")[0]

    prog = sys.argv[0]

    parser = argparse.ArgumentParser(prog=prog)
    subparsers = parser.add_subparsers(title="subcommands")

    parser.add_argument("-q", "--quiet",
                        action="store_true",
                        help="makes the command run quietly")
    parser.add_argument("--config-dir",
                        action="store",
                        help="sets the general configuration directory",
                        default=os.path.join(os.environ["HOME"], ".btw-backup"))
    parser.add_argument('--version', action='version',
                        version='%(prog)s ' + __version__)

    fs_sp = subparsers.add_parser(
        "fs",
        description=FSBackup.__doc__,
        help="backs up a filesystem hierarchy",
        formatter_class=argparse.RawTextHelpFormatter)
    fs_sp.set_defaults(class_=FSBackup)
    fs_sp.add_argument(*uid_spec["args"], **uid_spec["kwargs"])
    fs_sp.add_argument("src", help="the source to backup")
    fs_sp.add_argument("dst",
                       help="the final destination directory where to "
                       "put the backup")

    fs_init_sp = subparsers.add_parser(
        "fs-init",
        description=FSBackupInit.__doc__,
        help="initializes the working directory for backups",
        formatter_class=argparse.RawTextHelpFormatter)
    fs_init_sp.set_defaults(class_=FSBackupInit)
    fs_init_sp.add_argument("--type",
                            help="the type of backup",
                            choices=("rdiff", "tar"),
                            required=True,
                            dest="backup_type")
    fs_init_sp.add_argument("src",
                            help="the source to backup")
    fs_init_sp.add_argument(
        "name", help="a mnemonic to use for the working directory")

    list_sp = subparsers.add_parser(
        "list",
        description=List.__doc__,
        help="lists backups",
        formatter_class=argparse.RawTextHelpFormatter)
    list_sp.set_defaults(class_=List)
    list_sp.add_argument(
        "dst",
        help="the directory the backups are stored")

    db_sp = subparsers.add_parser(
        "db",
        description=DBBackup.__doc__,
        help="makes a database backup",
        formatter_class=argparse.RawTextHelpFormatter)
    db_sp.set_defaults(class_=DBBackup)
    db_sp.add_argument("-g", "--global",
                       dest="g",
                       action="store_true",
                       help="back up the global database")
    db_sp.add_argument("db",
                       nargs="?",
                       help="the name of the database to backup")
    db_sp.add_argument(*uid_spec["args"], **uid_spec["kwargs"])
    db_sp.add_argument("dst",
                       help="the final destination directory where to "
                       "put the backup")

    # This is used only for testing.
    db_sp.add_argument('--fake-dumpall', action='store',
                       help=argparse.SUPPRESS)


    sync_sp = subparsers.add_parser(
        "sync",
        description=Sync.__doc__,
        help="sync files to S3 storage",
        formatter_class=argparse.RawTextHelpFormatter)
    sync_sp.set_defaults(class_=Sync)
    sync_sp.add_argument("--full",
                         dest="full",
                         action="store_true",
                         help="do a full sync of everything in ROOT_PATH to "
                         "S3 storage; the default is to sync only files "
                         "needing syncing")
    sync_sp.add_argument("--list",
                         dest="list",
                         action="store_true",
                         help="only list the files that need syncing")

    try:
        try:
            args = parser.parse_args()

            if hasattr(args, "dst") and args.dst[0] == "/":
                fatal("the dst argument cannot be an absolute path")

            # Normalize the argument to a uid and a gid argument
            uidgid = getattr(args, "uid", None)
            if uidgid:
                uid, gid = uidgid.split(":")
                args.uid = pwd.getpwnam(uid).pw_uid
                args.gid = grp.getgrnam(gid).gr_gid

            cmd = args.class_(args)
            try:
                return cmd.execute()
            finally:
                cmd.sync()
        except FatalUserError as ex:
            fatal(str(ex))
    except Exit as ex:
        sys.exit(ex.args[0])


if __name__ == "__main__":
    main()
