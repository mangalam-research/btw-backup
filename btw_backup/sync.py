
import subprocess
import os
import fcntl
import datetime
import pyee
import sys
import traceback

from .errors import ImproperlyConfigured, FatalUserError

def format_exception():
    return "".join(traceback.format_exception(*sys.exc_info()))

class SyncState(object):

    def __init__(self, path):
        """
        A SyncState object records the synchronization state of the backup
        system. It records this state to disk so as to allow recovery
        from power outages or other interruptions. The file is
        append-only, and recovery of the latest state is done by
        reading the whole file.

        :param path: The path where the state is stored on disk. This
        file will be open with an exclusive lock.
        """
        self.path = path
        self._file = open(path, 'ab+')
        fcntl.lockf(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._ee = pyee.EventEmitter()
        self._cached_raw_current_state = None
        self._cached_current_state = None

    @property
    def ee(self):
        """
        An event emiter. Each time a file is registered for pushing or
        syncing, an event is emitted on this emitter. This allows the
        actual syncing backend to inspect paths that are registered
        and raise an error proactively if a requested operation cannot
        be performed.
        """
        return self._ee

    @property
    def _raw_current_state(self):
        if self._cached_raw_current_state is not None:
            return self._cached_raw_current_state

        # Setting _cached_raw_current_state here ensures that even if
        # self._update_state tries to access self.current_state, it won't read
        # the file again.
        self._cached_raw_current_state = {
            # In theory, what we want here is a set() but sets do not maintain
            # order. dict() does maintain order so we use dicts whose values are
            # not important.
            "push": dict(),
            "sync": dict()
        }

        self._file.seek(0)
        for line in self._file:
            line = line.decode("utf8").rstrip("\r\n")
            (_, op, path) = line.split(" ", 2)
            self._update_state(op, path, reading=True)
        self._file.seek(0, os.SEEK_END)

        return self._cached_raw_current_state

    @property
    def current_state(self):
        """
        The current state. This is a dictionary whose keys are the types
        of operations supported (push and sync). The values associated
        with those keys are the lists of paths that still need having
        their respective operations performed on them.
        """
        # We just convert the dict keys to lists.
        return {k: list(v.keys()) for k, v in self._raw_current_state.items()}

    def _update_state(self, op, path, reading=False):
        op_prefix = op[0]
        op_suffix = op[1:]

        if path.startswith("/"):
            raise ValueError("path cannot be absolute: " + path)

        if op_prefix == "+":
            self._ee.emit(op_suffix, path)

        # We do not access _cached_raw_current_state directly because this
        # method may be called first from push* and sync* methods.
        target = self._raw_current_state[op_suffix]
        if op_prefix == "+":
            target[path] = 1
        elif op_prefix == "-":
            del target[path]
        else:
            raise ValueError("invalid op: " + op)

        # We do not write to file when we are reading
        if not reading:
            ss_file = self._file
            now = datetime.datetime.utcnow().replace(microsecond=0)
            ss_file.write("{0} {1} {2}\n".format(now.isoformat(), op, path)
                          .encode("utf8"))
            ss_file.flush()
            os.fsync(ss_file.fileno())

    def push_path(self, path):
        """
        Indicate that a path needs pushing to the S3 server. A "push"
        operation is an unconditional copy from the local machine to
        the S3 server.

        :param path: The path to push.
        """
        self._update_state("+push", path)

    def sync_path(self, path):
        """
        Indicate that a path needs syncing to the S3 server. A "sync"
        operation compares files on the source and the S3 server and
        copies only what needs copying.

        :param path: The path to sync.
        """
        self._update_state("+sync", path)

    def push_done(self, path):
        """
        Indicate that a path has been pushed to the S3 server.

        :param path: The path to mark as done.
        """
        self._update_state("-push", path)

    def sync_done(self, path):
        """
        Indicate that a path has been synced to the S3 server.

        :param path: The path to mark as done.
        """
        self._update_state("-sync", path)

    def reset(self):
        """
        """
        if len(self.current_state["sync"]) or len(self.current_state["push"]):
            raise FatalUserError(
                "cannot reset: some files must be synced or pushed")
        self._file.seek(0)
        self._file.truncate()


class S3(object):

    def __init__(self, general_config, state):
        self.s3_uri_prefix = general_config.get("S3_URI_PREFIX")
        if self.s3_uri_prefix is None:
            raise ImproperlyConfigured(
                "you must specify S3_URI_PREFIX in the general configuration")

        self.root = general_config.get("ROOT_PATH")
        if self.root is None:
            raise ImproperlyConfigured(
                "you must specify ROOT_PATH in the general configuration")

        # Force it to have a final forward slash to mark it as
        # a directory.
        if self.s3_uri_prefix[-1] != "/":
            self.s3_uri_prefix += "/"

        self.state = state
        self.state.ee.on('sync', self.check_sync)

        # These are meant for testing.
        self.log_sync = os.environ.get("BTW_BACKUP_LOG_SYNC")
        self.server_override = os.environ.get("BTW_BACKUP_S3_SERVER")

        self._cached_stdout = None
        self._cached_stderr = None

    def check_sync(self, path):
        full = os.path.join(self.root, path)
        if not os.path.exists(full):
            raise ValueError(
                "trying to sync a non-existent path: " + path)

        if not os.path.isdir(full):
            raise ValueError(
                "trying to sync a path which is not a directory: " + path)

    def run(self):
        current = self.state.current_state
        #
        # We capture exceptions broadly here because there's a host of
        # problems that could occur at run time. Our aim is to produce
        # an error message and keep the path whose operation failed
        # among those paths that need operating on.
        #
        for to_push in current["push"]:
            try:
                self._push(to_push)
            except:  # pylint: disable=bare-except
                stderr = self._stderr
                print("Error while processing: " + to_push, file=stderr)
                print(format_exception(), file=stderr)

        for to_sync in current["sync"]:
            try:
                self._sync(to_sync)
            except:  # pylint: disable=bare-except
                stderr = self._stderr
                print("Error while processing: " + to_sync, file=stderr)
                print(format_exception(), file=stderr)

    def _push(self, path):
        self._do_push(path)
        self.state.push_done(path)

    def _sync(self, path):
        self._do_sync(path)
        self.state.sync_done(path)

    def _do_push(self, path):
        raise NotImplementedError

    def _do_sync(self, path):
        raise NotImplementedError

    # The capability to set a stdout and stderr other than sys.stderr
    # is meant to be used for testing purposes ONLY.
    @property
    def _stdout(self):
        if self._cached_stdout is not None:
            return self._cached_stdout

        if not self.log_sync:
            return None

        self._cached_stdout = open("/tmp/btw_backup_sync_log", 'a+')

        return self._cached_stdout

    # The capability to set a stdout and stderr other than sys.stderr
    # is meant to be used for testing purposes ONLY.
    @property
    def _stderr(self):
        if self._cached_stderr is not None:
            return self._cached_stderr

        return None


class AWSCliS3(S3):

    def __init__(self, general_config, state):
        super(AWSCliS3, self).__init__(general_config, state)

        self.profile = general_config.get("AWSCLI_PROFILE")
        if self.profile is None:
            raise ImproperlyConfigured(
                "you must specify AWSCLI_PROFILE in the general configuration")

        self.args = ["aws", "s3",
                     "--profile=" + self.profile, "--sse=AES256"]

        if not self.log_sync:
            # By default we don't log anything and show only errors to STDOUT
            self.args.append("--only-show-errors")
        else:
            self.args.append("--debug")
            self.args.append("--no-paginate")

        if self.server_override is not None:
            self.args += ["--endpoint=" + self.server_override]

    def _do_sync(self, path):
        stdout = self._stdout
        subprocess.check_call(self.args +
                              ["sync", "--delete",
                               os.path.join(self.root, path),
                               os.path.join(self.s3_uri_prefix, path)],
                              stdout=stdout, stderr=stdout)

    def _do_push(self, path):
        args = self.args + ["cp"]
        src = os.path.join(self.root, path)

        if os.path.isdir(src):
            args += ["--recursive"]

        stdout = self._stdout
        subprocess.check_call(args + [src,
                                      os.path.join(self.s3_uri_prefix, path)],
                              stdout=stdout, stderr=stdout)

class S3Cmd(S3):

    def __init__(self, general_config, state):
        super(S3Cmd, self).__init__(general_config, state)

        # We call it profile internally...
        self.profile = general_config.get("S3CMD_CONFIG")
        if self.profile is None:
            raise ImproperlyConfigured(
                "you must specify S3CMD_CONFIG in the general configuration")

        self.args = ["s3cmd", "--config=" + self.profile,
                     "--server-side-encryption"]

        if not self.log_sync:
            self.args.append("--quiet")
        else:
            self.args.append("--verbose")
            self.args.append("--debug")

        if self.server_override is not None:
            suffix = self.server_override.split("://")[-1]
            self.args += ["--host=" + suffix]
            self.args += ["--host-bucket=" + suffix]

    def _do_sync(self, path):
        stdout = self._stdout
        subprocess.check_call(self.args +
                              ["sync", "--delete-removed",
                               os.path.join(self.root, path),
                               os.path.join(self.s3_uri_prefix, path)],
                              stdout=stdout, stderr=stdout)

    def _do_push(self, path):
        args = self.args + ["put"]
        src = os.path.join(self.root, path)

        if os.path.isdir(src):
            args += ["--recursive"]

        stdout = self._stdout
        subprocess.check_call(args + [src,
                                      os.path.join(self.s3_uri_prefix, path)],
                              stdout=stdout, stderr=stdout)
