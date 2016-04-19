Backup software for BTW

This is an extremely ad-hoc script tailored to the needs of the BTW
server, and not much else.

Synopsis
========

::

    btw_backup [-h] [-q] [--config-dir CONFIG_DIR] [--version]
                        {fs,fs-init,list,db} ...

    optional arguments:
      -h, --help            show this help message and exit
      -q, --quiet           makes the command run quietly
      --config-dir CONFIG_DIR
                            sets the general configuration directory
      --version             show program's version number and exit

    subcommands:
      {fs,fs-init,list,db}
        fs                  backs up a filesystem hierarchy
        fs-init             initializes the working directory for backups
        list                lists backups
        db                  makes a database backup

Configuration
=============

The files used by ``btw-backup`` at run-time are expected to be in
``~/.btw-backup/``, except if you use ``--config-dir`` to override
it. This directory may contain the following files:

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

The following options are available:

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
``dst``. You may use ``-u`` to request that the ownership of the
created files be set to ``UID:GID``.

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
``dst`` is the directory where to store the backup. You can use ``-g``
to do a backup of the database "globals". ``btw-backup`` uses
``pg_dumpall -g`` to dump the globals. If you use ``-g``, then you
must not give a database name on the command line. It is mandatory to
give either a name or ``-g``.

All database backups use ``rdiff-backup``. The process is:

* Use ``pg_dumpall`` or ``pg_dump`` to dump the database.

* The dump is compared with the previous dump stored at the
  destination.

* If the new dump is different from the previous dump, then
  ``rdiff-backup`` is used to back up the new dump. Otherwise, a note
  is stored in a ``log.txt`` file at the destination.

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
ftp site.)

..  LocalWords:  btw hoc fs init subcommands py globals config src
..  LocalWords:  rdiff pytimeparse UID GID dst tarfile txt dumpall
