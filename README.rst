Backup software for BTW

This is an extremely ad-hoc script tailored to the needs of the BTW
server, and not much else.

Synopsis
========

::

    usage: btw-backup [-h] [-q] [--config-dir CONFIG_DIR] [--version]
                      {fs,fs-init,list,db,sync,sync-state} ...

    optional arguments:
      -h, --help            show this help message and exit
      -q, --quiet           makes the command run quietly
      --config-dir CONFIG_DIR
                            sets the general configuration directory
      --version             show program's version number and exit

    subcommands:
      {fs,fs-init,list,db,sync}
        fs                  backs up a filesystem hierarchy
        fs-init             initializes the working directory for backups
        list                lists backups
        db                  makes a database backup
        sync                sync files to S3 storage

.. note:: The ``sync`` subcommand is really used for exceptional
          cases, like when syncing to S3 the first time, for checking
          whether things need to be synced or forcing a sync
          immediately. Synchronizations happen automatically after a
          backup is run.

.. note:: The ``sync-state`` command is equally unusual. It is meant to verify
          and manipulate the current sync state.

Requisites
==========

* ``apt-get install rdiff-backup``

Configuration
=============

The files used by ``btw-backup`` at run-time are expected to be in
``~/.btw-backup/``, except if you use ``--config-dir`` to override
it. This directory may contain the following files:

* ``config.py``: a general configuration file for all operations
  performed by ``btw-backup``.

* ``global.py`` a configuration file for backups of database globals,

* ``<db>.py`` a configuration file for database-specific
  backups. ``db`` is the database name.

* ``<name>.<hash>`` subdirectories. These are subdirectories created
  with ``btw-backup fs-init``. The ``<name>`` part is the name you
  passed to ``fs-init``, and ``<hash>`` is automatically created by
  ``fs-init``. Each such subdirectory may contain:

  + ``config.py``: a configuration file for this filesystem backup.

  + ``src``: a symbolic link to the source of the backup on the file system.

  + ``backup``: a directory used by ``btw-backup`` during the backup operation.

General Configuration
---------------------

The following setting is available in the general configuration:

* ``ROOT_PATH``: the path relative to which all ``dst`` parameters are
  interpreted. It is a required setting.

* ``S3CMD_CONFIG``: the path to the configuration file to use with the
  ``s3cmd`` command to store backups in S3. It is a required
  setting. ``btw-backup`` does not allow just using the default
  configuration.

* ``S3_URI_PREFIX``: a URI indicating which S3 bucket and path to use
  to store the backups off-site. It is a required setting. Example:
  ``"s3://foo/backups"``.

Backup-Specific Configuration
-----------------------------

The following settings are available in ``global.py``, the per-DB
configuration files, and in the configuration files located in the
subdirectories created with ``fs-init``:

* ``MAX_INCREMENTAL_COUNT`` the maximum number of incremental backups
  to allow before forcing a new full backup. This option only affects
  backups that use ``rdiff-backup``. The default is 10.

* ``MAX_INCREMENTAL_SPAN`` is the maximum time-span that can be
  covered by incremental backups. In other words, it is the maximum
  time allowed between two full backups. This option only affects
  backups that use ``rdiff-backup``. The default is ``"24h"``. The
  value must be parsable by ``pytimeparse.parse``. The value ``"7d"``
  would allow 7 days between full backups.

* ``TYPE`` specifies the type of backup to perform. The two possible
  values are ``"rdiff"`` and ``"tar"``. This option is only valid in
filesystem backup configuration files.

Filesystem Backups
==================

You must first create a subdirectory under ``~./btw-backup`` by doing::

    $ btw-backup fs-init --type {rdiff,tar} src name

where ``src`` is the path of what you want to backup and ``name`` is a
mnemonic. You must use ``--type rdiff`` or ``--type tar`` to
specify the type of backup. Once the command has run, a new directory
that starts with ``name`` and ends with a hash will have been created
under ``~/.btw-backup``. It will contain a default ``config.py``.

When you want perform a backup, you do::

    $ btw-backup fs [-u UID:[GID]] src dst

This will backup from ``src`` to the directory specified by
``dst``. **Remember that ``dst`` is relative to the ``ROOT_PATH``
setting**. So if ``ROOT_PATH`` is ``/tmp/x`` and ``dst`` is ``foo``,
then the backup will be stored in ``/tmp/x/foo``. You may use ``-u``
to request that the ownership of the created files be set to
``UID:GID``.

Filesystem backups may be created using one of two methods:

* ``tar``:

 #. The source path is passed to ``tar`` to create a tarfile.

 #. The tarfile is compared with the previous tarfile stored at the
    destination.

 #. If the previous tarfile differs from the current one, or if there
    is no previous tarfile, the current tarfile is saved to the
    destination. Otherwise, a note is recorded in a ``log.txt`` file
    at the destination.

* ``rdiff``:

 #. The source path is passed to ``tar`` to create a tarfile.

 #. The tarfile is compared with the previous tarfile stored at the
    destination. (``btw-backup`` takes into account full and
    incremental backups, etc.)

 #. If the previous tarfile differs from the current one, or if there
    is no previous tarfile, the current tarfile is backed up using
    ``rdiff-backup``. Otherwise, a note is recorded in a ``log.txt``
    file at the destination.

Database Backups
================

There is not initialization command for database backups. You do::

    $ btw-backup db [-g] [-u UID[:GID]] [db] dst

where ``db`` is the name of the database you want to backup and
``dst`` is the directory where to store the backup. **Remember that
``dst`` is relative to the ``ROOT_PATH`` setting**. So if
``ROOT_PATH`` is ``/tmp/x`` and ``dst`` is ``foo``, then the backup
will be stored in ``/tmp/x/foo``. You can use ``-g`` to do a backup of
the database "globals". ``btw-backup`` uses ``pg_dumpall -g`` to dump
the globals. If you use ``-g``, then you must not give a database name
on the command line. It is mandatory to give either a name or ``-g``.

All database backups use ``rdiff-backup``. The process is:

* Use ``pg_dumpall`` or ``pg_dump`` to dump the database.

* The dump is compared with the previous dump stored at the
  destination.

* If the new dump is different from the previous dump, then
  ``rdiff-backup`` is used to back up the new dump. Otherwise, a note
  is stored in a ``log.txt`` file at the destination.

Robustness
==========

``btw-backup`` is able to work around abrupt interruptions of its
operations.

If a ``tar`` backup is interrupted in the middle of the backup, the
next complete backup will detect a difference between the new tarfile
and the old and will save the new backup. If ``btw-backup`` is
interrupted in the middle of a non-atomic copy of a tarfile, the
tarfile won't be usable for recovery. There's no chance of unknowingly
recovering from a corrupted tarfile.

If a ``rdiff-backup`` backup is interrupted, ``rdiff-backup`` is able,
on the next run, to detect the incomplete backup and remove it from
consideration.

``btw-backup`` records sync state and recovers from syncs that did not
complete. This is necessary because an interrupted sync *could*
effectively corrupt a backup that was fine before the
interrupt. (``rdiff-backup`` works on multiple files, but
``btw-backup`` does not know the innards of these files so does not
send the data to the off-site location in any way that ensures
consistency.)

Robustness is the reason ``btw-backup`` does not use the ``aws`` tool
(aka "awscli") for syncing backups to the S3 server. The ``aws s3
sync`` command behaves in a way contrary to long-established practices
of syncing (think ``rsync``) and also appears buggy. Our first
implementation of syncing relied on ``aws`` but we found that some
files would sometimes not be synced to the server. One issue is that
the syncing operation alters the creation/modification times of the
files on the server. If file A is synced to the server because it does
not exist there, it will acquire a modification and creation time that
corresponds to the end of the sync, irrespective of the value of the
same times on the local file system. So file A on the server appears
newer than file A on the local drive, and on the next sync up to the
server A won't be uploaded since it is newer on the server.

Q: When a file is changed, its size changes too, right? So ``aws``
   will pick up on the size change and upload.

A: False. A one character file can be changed from containing "a" to
   containing "b" for instance. But we're affected by this too because
   we use ``tar``. And ``tar`` works in blocks of 512 bytes. So if you
   tar a 1-byte file and a 10-byte file, both resulting archives will
   take the same space on disk, despite being different. This comes up
   a lot in testing because the test files have few differences.

Q: Ok, then even if a file is changed and the same size, the next
   ``aws s3 sync`` should pick up the newer modification time. So the
   issue that uploaded files acquire a modification time which is
   later than their modification time on the local file system (which
   you pointed out above) is moot.

A: No, there is still a problem. See, ``rdiff-backup`` says it
   preserves modification times, but that's not actually true. It sets
   the modification time on the backed up files but nullifies anything
   smaller than a second. So if a file has a modification time of
   14:03:02.1234, where the numbers after the period are fractions of
   a second, ``rdiff-backup`` will "preserve" it as 14:03:02.

   In a case where operations happen in quick succession -- in
   testing, for instance -- it is possible to end up with a file X
   that is changed on the local side and has a modification time
   *older* than the corresponding file X on the S3 side, even if this
   X was modified *after* the file on the S3 side was created. This
   means a sync won't upload the newer version of X.

   The script ``misc/script.sh`` illustrates the issue.

Checking and Resetting the Sync State
=====================================

As mentioned above, ``btw-backup`` records a sync state that can allow it to
recover from being forcibly interrupted during a sync. Over the years this file
can grow quite a bit. The ``sync-state`` subcommand allows checking and
resetting this file.

You can use ``btw-backup sync-state --list`` to list the files that have not
been synced.

You can use ``btw-backup sync-state --reset`` to reset the state to empty. This
command will fail if there are any files that have not been synced.

.. warning:: ``sync-state --reset`` is not bullet-proof. Use your judgment as to
             when to run it.

Security
========

``btw-backup`` should be run as root to have access to all
files. (Unfortunately, it is not possible to perform a backup of the
entire fs tree or the database globals without having all
permissions on the fs or the database.)

``btw-backup`` **does not encrypt the backups** therefore you must
take care of setting the permissions for ``dst`` so that only
authorized users may access the files. Files that are stored off-site
from ``dst`` should be stored in containers whose access is strictly
controlled, and encrypted. (Don't shove them unencrypted on a public
ftp site.) By default, ``btw-backup`` invokes ``s3cmd`` with
``--server-side-encryption``, which encrypts the data on the S3
server.

Testing
=======

* See ``Requisites`` above.

* Create a virtual environment for this purpose.

* Activate the virtual environment.

* ``pip install -e .``

* ``npm install``

* ``python setup.py nosetests``

..  LocalWords:  btw hoc fs init subcommands py globals config src
..  LocalWords:  rdiff pytimeparse UID GID dst tarfile txt dumpall
