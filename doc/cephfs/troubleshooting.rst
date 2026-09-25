=================
 Troubleshooting
=================

Slow/stuck operations
=====================

Sometimes CephFS operations hang. The first step in troubleshooting them is to
locate the problem causing the operations to hang. Problems present in three
places:

#. in the client
#. in the MDS
#. in the network that connects the client to the MDS

First, use the procedure in :ref:`slow_requests` to determine if the client has
stuck operations or the MDS has stuck operations.

Dump the MDS cache. The contents of the MDS cache will be used to diagnose the
nature of the problem. Run the following command to dump the MDS cache:

.. prompt:: bash #

   ceph daemon mds.<name> dump cache /tmp/dump.txt

.. note:: MDS services that are not controlled by systemd dump the file 
   ``dump.txt`` to the machine that runs the MDS. MDS services that are
   controlled by systemd dump the file ``dump.txt`` to a tmpfs in the MDS
   container. Use `nsenter(1)` to locate ``dump.txt`` or specify another
   system-wide path.

If high logging levels have been set on the MDS, ``dump.txt`` can be expected
to hold the information needed to diagnose and solve the issue causing the
CephFS operations to hang.

.. _slow_requests:

Slow requests (MDS)
-------------------
List current operations via the admin socket by running the following command
from the MDS host:

.. prompt:: bash #

   ceph daemon mds.<name> dump_ops_in_flight

Identify the stuck commands and examine why they are stuck.
Usually the last "event" will have been an attempt to gather locks, or sending
the operation off to the MDS log. If it is waiting on the OSDs, fix them. 

If operations are stuck on a specific inode, then a client is likely holding
capabilities, preventing its use by other clients. This situation can be caused
by a client trying to flush dirty data, but it might be caused because you have
encountered a bug in the distributed file lock code (the file "capabilities"
["caps"] system) of CephFS.

If you have determined that the commands are stuck because of a bug in the
capabilities code, restart the MDS. Restarting the MDS is likely to resolve the
problem.

If there are no slow requests reported on the MDS, and there is no indication
that clients are misbehaving, then either there is a problem with the client
or the client's requests are not reaching the MDS.


.. _cephfs_dr_stuck_during_recovery:

Stuck during recovery
=====================

Stuck in up:replay
------------------

If your MDS is stuck in the ``up:replay`` state, then the journal is probably
very long. The presence of ``MDS_HEALTH_TRIM`` cluster warnings can indicate
that the MDS has not yet caught up while trimming its journal. Very large
journals can take hours to process. There is no working around this, but there
are things you can do to speed up the process:

Temporarily disable MDS debug logs by reducing MDS debugging to ``0``. Even
with the default settings, the MDS logs a few messages to memory for dumping in
case a fatal error is encountered. You can turn off all logging by running the
following commands:

.. prompt:: bash #

   ceph config set mds debug_mds 0
   ceph config set mds debug_ms 0
   ceph config set mds debug_monc 0

Remember that when you set ``debug_mds``, ``debug_ms``, and ``debug_monc`` to
``0``, if the MDS fails then there will be no debugging information that can be
used to determine why fatal errors occurred. If you can calculate when
``up:replay`` will complete, restore these configurations just prior to
entering the next state:

.. code:: bash

   ceph config rm mds debug_mds
   ceph config rm mds debug_ms
   ceph config rm mds debug_monc

After replay has been expedited, calculate when the MDS will complete the
replay. Examine the journal replay status:

.. code:: bash

   $ ceph tell mds.<fs_name>:0 status | jq .replay_status
   {
     "journal_read_pos": 4195244,
     "journal_write_pos": 4195244,
     "journal_expire_pos": 4194304,
     "num_events": 2,
     "num_segments": 2
   }

Replay completes when the ``journal_read_pos`` reaches the
``journal_write_pos``. The write position does not change during replay. Track
the progression of the read position to compute the expected time to complete.
The MDS emits an `MDS_ESTIMATED_REPLAY_TIME` warning when the act of replaying
the journal takes more than 30 seconds. The warning message includes an
estimated time to the completion of journal replay::

  mds.a(mds.0): replay: 50.0446% complete - elapsed time: 582s, estimated time remaining: 581s

.. _cephfs_troubleshooting_avoiding_recovery_roadblocks:

Avoiding recovery roadblocks
----------------------------

Do the following when restoring your file system: 

* **Deny all reconnection to clients.** Blocklist all existing CephFS sessions,
  causing all mounts to hang or become unavailable:

  .. prompt:: bash #

     ceph config set mds mds_deny_all_reconnect true

  Remember to undo this after the MDS becomes active.

  .. note:: This does not prevent new sessions from connecting. Use the
     ``refuse_client_session`` file-system setting to prevent new sessions from
     connecting to the CephFS.

* **Extend the MDS heartbeat grace period.** Doing this causes the system to
  avoid replacing an MDS that becomes "stuck" during an operation. Sometimes
  recovery of an MDS may involve operations that take longer than expected
  (from the programmer's perspective). This is more likely when recovery has 
  already taken longer than normal to complete (which, if you're reading this
  document, is likely the situation you find yourself in). Avoid unnecessary
  replacement loops by running the following command and extending the
  heartbeat grace period:

   .. prompt:: bash #

      ceph config set mds mds_heartbeat_grace 3600

  .. note:: This causes the MDS to continue to send beacons to the Monitors
     even when its internal "heartbeat" mechanism has not been reset (it has
     not beaten) in one hour. In the past, this was achieved with the
     ``mds_beacon_grace`` Monitor setting.

* **Disable open-file-table prefetch.** Under normal circumstances, the MDS
  prefetches directory contents during recovery as a way of heating up its
  cache. During a long recovery, the cache is probably already hot **and
  large**. If the cache is already hot and large, this prefetching is
  unnecessary and can be undesirable. Disable open-file-table prefetching by
  running the following command:

  .. prompt:: bash #

     ceph config set mds mds_oft_prefetch_dirfrags false

* **Turn off clients.** Clients that reconnect to the newly ``up:active`` MDS
  can create new load on the file system just as it is becoming operational.
  This is often undesirable. Maintenance is often necessary before allowing
  clients to connect to the file system and before resuming a regular workload.
  For example, expediting the trimming of journals may be advisable if the
  recovery took a long time due to the amount of time replay spent in reading a
  very large journal.

  Client sessions can be refused manually, or by using the
  ``refuse_client_session`` tunable as in the following command: 

  .. prompt:: bash #

     ceph fs set <fs_name> refuse_client_session true

  This command has the effect of preventing clients from establishing new
  sessions with the MDS.

* **Do not tweak max_mds.** Modifying the file-system setting variable
  ``max_mds`` may seem like a good idea during troubleshooting and recovery,
  but it probably isn't. Modifying ``max_mds`` might have the effect of further
  destabilizing the cluster. If ``max_mds`` must be changed in such
  circumstances, run the command to change ``max_mds`` with the confirmation
  flag (``--yes-i-really-mean-it``).

.. _pause-purge-threads:

* **Turn off async purge threads.** The volumes plugin spawns threads that
  asynchronously purge trashed or deleted subvolumes. During troubleshooting or
  recovery, these purge threads can be disabled by running the following
  command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_purging true

  To resume purging, run the following command:
  
  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_purging false

.. _pause-clone-threads:

* **Turn off async cloner threads.** The volumes plugin spawns threads that
  asynchronously clone subvolume snapshots. During troubleshooting or recovery,
  these cloner threads can be disabled by running the following command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_cloning true

  To resume cloning, run the following command:

  .. prompt:: bash #

     ceph config set mgr mgr/volumes/pause_cloning false



Expediting MDS journal trim
===========================

``MDS_HEALTH_TRIM`` warnings indicate that the MDS journal has grown too large.
When the MDS journal has grown too large, use the ``mds_tick_interval`` tunable
to modify the "MDS tick interval". The "tick" interval drives various upkeep
activities in the MDS, and modifying the interval will decrease the size of the
MDS journal by ensuring that it is trimmed more frequently.

Make sure that there is no significant file-system load present when modifying
``mds_tick_interval``. See
:ref:`cephfs_troubleshooting_avoiding_recovery_roadblocks` for ways to reduce
load on the CephFS.

This setting affects only MDSes in the ``up:active`` state. The MDS does not
trim its journal during recovery.

Run the following command to modify the ``mds_tick_interval`` tunable:

.. prompt:: bash #

   ceph config set mds mds_tick_interval 2


RADOS Health
============

If part of the CephFS metadata or data pools is unavailable and CephFS is not
responding, it could indicate that RADOS itself is unhealthy. 

Resolve problems with RADOS before attempting to locate any problems in CephFS.
See the :ref:`RADOS troubleshooting documentation<rados_troubleshooting>`.

The MDS
=======

Run the ``ceph health`` command. Any operation that is hung in the MDS is
indicated by the ``slow requests are blocked`` message. 

Messages that read ``failing to respond`` indicate that a client is failing to
respond. 

The following list details potential causes of hung operations:

#. The system is overloaded. The most likely cause of system overload is an
   active file set that is larger than the MDS cache. 
   
   If you have extra RAM, increase the ``mds_cache_memory_limit``. The specific
   tunable ``mds_cache_memory_limit`` is discussed in the :ref:`MDS Cache
   Size<cephfs_cache_configuration_mds_cache_memory_limit>`. Read the :ref:`MDS
   Cache Configuration<cephfs_mds_cache_configuration>` section in full before
   making any alterations to the ``mds_cache_memory_limit`` tunable.

#. There is an older (misbehaving) client.

#. There are underlying RADOS issues. See :ref:`The RADOS troubleshooting
   documentation<rados_troubleshooting>`.

Otherwise, you have probably discovered a new bug and should report it to
the developers!

.. _ceph_fuse_debugging:

ceph-fuse debugging
===================

ceph-fuse is an alternative to the CephFS kernel driver that mounts CephFS file
systems in user space. ceph-fuse supports ``dump_ops_in_flight``. Use the following command to dump in-flight ceph-fuse operations for examination:  

..
  .. prompt:: bash #

  the command goes here - 10 Aug 2025

See the :ref:`Mount CephFS using FUSE<cephfs_mount_using_fuse>` documentation.

Debug output
------------

To get more debugging information from ceph-fuse, list current operations in
the foreground while logging to the console (``-d``), enabling client debug
(``--debug-client=20``), and enabling prints for each message sent
(``--debug-ms=1``).

.. prompt:: bash #

   ceph daemon -d mds.<name> dump_ops_in_flight --debug-client=20 --debug-ms=1

If you suspect a potential Monitor issue, enable Monitor debugging as well
(``--debug-monc=20``) by running a command of the following form:

.. prompt:: bash #

   ceph daemon -d mds.<name> dump_ops_in_flight --debug-client=20 --debug-ms=1 --debug-monc=20

.. _kernel_mount_debugging:

Kernel mount debugging
======================

The first step in diagnosing and repairing an issue with the kernel client is
determining whether the problem is in the kernel client or in the MDS. If the
kernel client itself is broken, evidence of its breakage will be in the kernel
ring buffer, which can be examined by running the following command:

.. prompt:: bash #

   dmesg

Find the relevant kernel state.


Slow requests
-------------

Unfortunately, the kernel client does not provide an admin socket. However,
if the kernel on the client has `debugfs
<https://docs.kernel.org/filesystems/debugfs.html>`_ enabled, interfaces
similar to the admin socket are available. 

Find a folder in ``/sys/kernel/debug/ceph/`` with a name like 
``28f7427e-5558-4ffd-ae1a-51ec3042759a.client25386880``.
That folder contains files that can be used to diagnose the causes of slow requests. Use ``cat`` to see their contents.  

These files are described below. The files most useful for diagnosis of slow
requests are the ``mdsc`` (current requests to the MDS) and the ``osdc``
(current operations in-flight to OSDs) files.

* ``bdi``: BDI info about the Ceph system (blocks dirtied, written, etc)
* ``caps``: counts of file "caps" structures in-memory and used
* ``client_options``: dumps the options provided to the CephFS mount
* ``dentry_lru``: Dumps the CephFS dentries currently in-memory
* ``mdsc``: Dumps current requests to the MDS
* ``mdsmap``: Dumps the current MDSMap epoch and MDSes
* ``mds_sessions``: Dumps the current sessions to MDSes
* ``monc``: Dumps the current maps from the Monitor, and any "subscriptions" held
* ``monmap``: Dumps the current monitor map epoch and Monitors
* ``osdc``: Dumps the current ops in-flight to OSDs (ie, file data I/O)
* ``osdmap``: Dumps the current OSDMap epoch, pools, and OSDs

If the data pool is in a ``NEARFULL`` condition, then the kernel CephFS client
will switch to doing writes synchronously. Synchronous writes are quite slow.

Disconnected+Remounted FS
=========================

Because CephFS has a "consistent cache", the MDS will forcibly evict (and
blocklist) clients from the cluster when the network connection has been
disrupted for a long time. When this happens, the kernel client cannot safely
write back dirty (buffered) data and this results in data loss. However: note
that this behavior is appropriate and also follows POSIX semantics. The client
has to be remounted to be able to access the file system again. This is the
default behavior but it can be overridden by the ``recover_session`` mount
option. See `the "options" section of the "mount.ceph" man page
<https://docs.ceph.com/en/latest/man/8/mount.ceph/#options>`_

You are in this situation if the output of ``dmesg`` contains something like
the following::

[Fri Aug 15 02:38:10 2025] ceph: mds0 caps stale
[Fri Aug 15 02:38:28 2025] libceph: mds0 (2)XXX.XX.XX.XX :6800 socket closed (con state OPEN)
[Fri Aug 15 02:38:28 2025] libceph: mds0 (2)XXX.XX.XX.XX:6800 session reset
[Fri Aug 15 02:38:28 2025] ceph: mds0 closed our session
[Fri Aug 15 02:38:28 2025] ceph: mds0 reconnect start
[Fri Aug 15 02:38:28 2025] ceph: mds0 reconnect denied

Mounting
========

Mount 5 Error
-------------

A ``mount 5`` error indicates a lagging MDS server or a crashed MDS server.

Ensure that at least one MDS is up and running, and the cluster is ``active +
healthy``. 

Mount 12 Error
--------------

A mount 12 error with a message reading ``cannot allocate memory`` indicates a
version mismatch between the :term:`Ceph Client` version and the :term:`Ceph
Storage Cluster` version. Check the versions using the following command:

.. prompt:: bash #

   ceph -v
	
If the Ceph Client is of an older version than the Ceph cluster, upgrade
the Client:

.. prompt:: bash #

   sudo apt-get update && sudo apt-get install ceph-common 

If this fails to resolve the problem, uninstall, autoclean, and autoremove the
``ceph-common`` package and then reinstall it to ensure that you have the
latest version of it.

Dynamic Debugging
=================

Dynamic debugging for CephFS kernel driver allows to enable or disable debug
logging. The kernel driver logs are written to the kernel ring buffer and can
be examined using ``dmesg(1)`` utility. Debug logging is disabled by default
because enabling debug logging can result in system slowness and a drop in I/O
throughput.

Enable dynamic debug against the CephFS module.

See: https://github.com/ceph/ceph/blob/master/src/script/kcon_all.sh

Note: Running the above script enables debug logging for the CephFS kernel
driver, libceph, and the kernel RBD module. To enable debug logging for a
specific component (for example, for the CephFS kernel driver), run a command of the following form:

.. prompt:: bash #

   echo 'module ceph +p' > /sys/kernel/debug/dynamic_debug/control

To disable debug logging, run a command of the following form: 

.. prompt:: bash #

   echo 'module ceph -p' > /sys/kernel/debug/dynamic_debug/control

In-memory Log Dump
==================

In-memory logs can be dumped by setting
``mds_extraordinary_events_dump_interval`` when
the log level is set to less than ``10``.
``mds_extraordinary_events_dump_interval`` is the interval in seconds for
dumping the recent in-memory logs when there is an extraordinary event.

Extraordinary events include the following:

* Client Eviction
* Missed Beacon ACK from the Monitors
* Missed Internal Heartbeats

In-memory log dump is disabled by default. This prevents production
environments from experiencing log file bloat by default.

Run the following two commands in order to enable in-memory log dumping: 

#. 
   .. prompt:: bash #

      ceph config set mds debug_mds <log_level>/<gather_level>

   Set ``log_level`` to a value of less than ``10``. Set ``gather_level`` to a
   value greater than ``10``. When those two values have been set,  in-memory
   log dump is enabled.
#. 
   .. prompt:: bash #

      ceph config set mds mds_extraordinary_events_dump_interval <seconds>

   When in-memory log dumping is enabled, the MDS checks for
   extraordinary events every ``mds_extraordinary_events_dump_interval``
   seconds. If any extraordinary event occurs, the MDS dumps the in-memory logs
   that contain relevant event details to the Ceph MDS log.

.. note:: When higher log levels are set (``log_level`` greater than or equal
   to ``10``) there is no reason to dump the in-memory logs. A lower gather
   level (``gather_level`` less than ``10``) is insufficient to gather
   in-memory logs. This means that a log level of greater than or equal to ``10``
   or a gather level of less than ``10`` in ``debug_mds`` prevents enabling
   in-memory-log dumping. In such cases, if there is a failure, you must reset
   the value of ``mds_extraordinary_events_dump_interval`` to ``0`` before
   enabling the use of the above commands.

Disable in-memory log dumping by running the following command:

.. prompt:: bash #

   ceph config set mds mds_extraordinary_events_dump_interval 0

Filesystems Become Inaccessible After an Upgrade
================================================

.. note::
   You can avoid ``operation not permitted`` errors by running this procedure
   before an upgrade. As of May 2023, it seems that ``operation not permitted``
   errors of the kind discussed here occur after upgrades after Nautilus
   (inclusive).

IF

you have CephFS file systems that have data and metadata pools that were
created by a ``ceph fs new`` command (meaning that they were not created
with the defaults)

OR

you have an existing CephFS file system and are upgrading to a new post-Nautilus
major version of Ceph

THEN

in order for the documented ``ceph fs authorize...`` commands to function as
documented (and to avoid 'operation not permitted' errors when doing file I/O
or similar security-related problems for all users except the ``client.admin``
user), you must first run:

.. prompt:: bash $

   ceph osd pool application set <your metadata pool name> cephfs metadata <your ceph fs filesystem name>

and

.. prompt:: bash $

   ceph osd pool application set <your data pool name> cephfs data <your ceph fs filesystem name>

Otherwise, when the OSDs receive a request to read or write data (not the
directory info, but file data) they will not know which Ceph file system name
to look up. This is true also of pool names, because the 'defaults' themselves
changed in the major releases, from::

   data pool=fsname
   metadata pool=fsname_metadata

to::

   data pool=fsname.data and
   metadata pool=fsname.meta

Any setup that used ``client.admin`` for all mounts did not run into this
problem, because the admin key gave blanket permissions.

A temporary fix involves changing mount requests to the 'client.admin' user and
its associated key. A less drastic but half-fix is to change the osd cap for
your user to just ``caps osd = "allow rw"``  and delete ``tag cephfs
data=....``

Disabling the Volumes Plugin
============================
In certain scenarios, the Volumes plugin may need to be disabled to prevent
compromise for the rest of the Ceph cluster. For details see:
:ref:`disabling-volumes-plugin`

.. _cephfs-dump-log-segments:

Finding out why the MDS journal is not being trimmed
====================================================

When an MDS reports ``MDS_TRIM`` ("Behind on trimming"), the number of
untrimmed log segments has exceeded ``mds_log_max_segments`` multiplied by
``mds_log_warn_factor``. The warning says how far behind the MDS is, but not
why. To get the reason::

  ceph tell mds.<id> dump log segments

With no arguments this reports two things:

``oldest_unexpired_segment``
  The oldest log segment that has not yet expired. Log segments are removed
  from the journal strictly in order -- the journal's expire position is a
  single offset, so a hole cannot be punched in it -- which means this one
  segment holds back every newer segment no matter how cleanly those expired.
  Note this is frequently the *current* segment, flagged by ``is_current``:
  ``_expired()`` will not expire a segment that is still being written to, so
  once everything older has expired the current one is what ``expire_pos``
  rests behind. That is the fully-trimmed state, not a stall.

  Its ``trim_state`` -- where it sits in the trim bookkeeping, which says
  nothing about whether its events are on disk -- is one of:

  ``untouched``
    The MDS has not tried to expire it yet. Usually nothing is wrong, even
    when ``obligations`` is not empty -- see below.

  ``expiring``
    An expiry attempt is outstanding. ``obligations`` says what it is waiting
    for, and ``blocked_for`` says for how long. This is the interesting case.

  ``expired``
    The segment expired successfully and is waiting only to be removed.
    Removal happens at *major* segment boundaries (see
    ``mds_log_minor_segments_per_major_segment``), so the segment count is
    expected to rise and fall in steps rather than smoothly. If this is what
    you see, expiry is healthy.

``expiring``
  A rollup of every segment with an expiry attempt outstanding. This matters
  because once enough segments are mid-expiry the MDS stops initiating new
  expiry work, so the oldest unexpired segment may not be the whole story.

Every segment also carries a ``note`` saying what its state implies, which is
the field to read first.

``obligations`` is not a list of things going wrong. A segment's obligation
lists are filled when its events are journaled, not when expiry is attempted,
so a segment carries them from the moment it is written and keeps them until
the metadata they refer to reaches the metadata pool. They clear themselves
as ordinary MDS activity commits that metadata, with no involvement from
trimming at all. A freshly written segment reporting several obligations is
the normal steady state. They are only *blockers* when something is actually
waiting on them, which is what ``trim_state`` and ``trim_slow`` tell you.

Each obligation names a ``category``, a ``reason``, a ``count`` and a
``blocked`` count. The reason text matches the corresponding message in the
MDS log at ``debug_mds = 10``, so you can grep for it.

``blocked`` is the number to read. ``try_to_expire()`` issues work for every
entry and the gather waits on all of them, so one stuck entry among thousands
is what actually holds the segment. ``count: 4213, blocked: 1`` says the
segment is held by a single object, and ``--detail`` names it: blocked
entries are always emitted first, so the per-category cap on the object list
can never hide the culprit behind entries that are committing perfectly well.
Each such entry carries ``blocked_on``:

``frozen``, ``exporting_tree``, ``exporting_inode``, ``fragmenting_dir``
  The object cannot be auth-pinned, so expiry is parked on ``WAIT_UNFREEZE``.
  A subtree export or a directory fragmentation is in progress, or one is
  stuck.

``lock_unstable``
  The scatter lock is mid-gather, waiting on ``WAIT_STABLE``. For
  ``dirty_dirfrag_dir`` this is usually a client that has not returned its
  capabilities.

``ambiguous_auth``
  Authority for the inode is in flux, so expiry is parked on
  ``WAIT_SINGLEAUTH``. An export is in flight or was interrupted.

``not_auth``
  This rank is not the authority; the nudge has been forwarded and the answer
  has to come from another MDS.

Common categories:

``dirty_dirfrag_dir``, ``dirty_dirfrag_nest``, ``dirty_dirfrag_dirfragtree``
  A scatter lock has accumulated directory statistics that must be written
  back before the segment can go. Only ``dirty_dirfrag_dir`` (the file lock)
  involves client capabilities; the other two wait on in-flight locks, on
  other MDS ranks, or on a frozen directory.

``open_file_table``
  Expiry is deferred until the open file table has committed at a log
  sequence past this segment. The report includes the committed sequence and
  whether a commit is in flight. A large open file table makes every commit
  slower, which paces all segment expiry.

``uncommitted_leaders``, ``uncommitted_peers``, ``uncommitted_fragments``
  A distributed operation or directory fragmentation has not completed.
  A laggy or recovering peer rank will hold these open indefinitely.

``dirty_dirfrags``, ``dirty_parent_inodes``
  Metadata still has to be written to the metadata pool. A frozen directory
  (mid-export or mid-fragment) will block this with no timeout.

``truncating_inodes``, ``purging_inodes``
  A truncate or a stray purge has not finished.

Add ``--detail`` to name the objects involved. For a lock obligation this
includes the lock's state and, for capability-bearing locks, which clients
still owe which capabilities and for how long::

  ceph tell mds.<id> dump log segments --detail

A client appearing there with a large ``revoke_age`` is failing to release
capabilities; that is the same condition reported as
``MDS_CLIENT_LATE_RELEASE``, and the two warnings commonly appear together
because one causes the other. There is no timeout that resolves it -- the
client must release, or the session must be evicted.

Other forms::

  ceph tell mds.<id> dump log segments --seq <n>   # one specific segment
  ceph tell mds.<id> dump log segments --all       # every segment

Note that ``--detail`` caps how many objects it names per category, so for a
badly backed-up segment the list is a sample rather than an exhaustive dump.

Reporting Issues
================

If you have identified a specific issue, please report it with as much
information as possible. Especially important information:

* Ceph versions installed on client and server
* Whether you are using the kernel or FUSE client
* If you are using the kernel client, what kernel version?
* How many clients are in play, doing what kind of workload?
* If a system is 'stuck', is that affecting all clients or just one?
* Any ceph health messages
* Any backtraces in the ceph logs from crashes

If you are satisfied that you have found a bug, please file it on `the bug
tracker`. For more general queries, please write to the `ceph-users mailing
list`.

.. _the bug tracker: https://tracker.ceph.com
.. _ceph-users mailing list:  https://lists.ceph.io/hyperkitty/list/ceph-users@ceph.io/
