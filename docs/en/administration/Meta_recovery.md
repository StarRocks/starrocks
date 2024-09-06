---
displayed_sidebar: docs
---

# Metadata Recovery

import Tabs from '@theme/Tabs';

import TabItem from '@theme/TabItem';

This topic describes how to recover the metadata in your StarRocks clusters when FE nodes encounter different exceptions.

Generally, you may have to resort to metadata recovery when and only when one of the following issues occurs:

- [FE fails to restart](#fe-fails-to-restart)
- [FE fails to provide services](#fe-fails-to-provide-services)

Check the issue you encountered, follow the solution provided in the corresponding section, and perform any recommended actions.

## FE fails to restart

FE nodes may fail to restart if the metadata is damaged or incompatible with the cluster after rollback.

### Metadata incompatibility after rollback

When you downgrade your StarRocks cluster, FEs may fail to restart if the metadata is incompatible with that before the downgrade.

You can identify this issue if you encounter the following exception while downgrading the cluster:

```Plain
UNKNOWN Operation Type xxx
```

You can follow these steps to recover the metadata and start FE:

1. Stop all FE nodes.
2. Back up the metadata directories `meta_dir` of all FE nodes.
3. Add the configuration `ignore_unknown_log_id = true` to the configuration files **fe.conf** of all FE nodes.
4. Start all FE nodes, and check whether your data and metadata are intact.
5. If both your data and metadata are intact, execute the following statement to create an image file for your metadata:

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. After the new image file is transmitted to the directory **meta/image** of all FE nodes, you can remove the configuration `ignore_unknown_log_id = true` from all FE configuration files and restart the FE nodes.

### Metadata damage

Both damage to BDBJE metadata and StarRocks metadata will cause failures to restart.

#### BDBJE metadata damage

##### VLSN Bug

You can identify the VLSN bug based on the following error message:

```Plain
recoveryTracker should overlap or follow on disk last VLSN of 6,684,650 recoveryFirst= 6,684,652 UNEXPECTED_STATE_FATAL: Unexpected internal state, unable to continue. Environment is invalid and must be closed.
```

You can follow these steps to fix this issue:

1. Clear the metadata directory `meta_dir` of the FE node that throws this exception.
2. Restart the FE node using the Leader FE node as the helper.

   ```Bash
   # Replace <leader_ip> with the IP address (priority_networks) 
   # of the Leader FE node, and replace <leader_edit_log_port> (Default: 9010) with 
   # the Leader FE node's edit_log_port.
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

:::tip

- This bug has been fixed in StarRocks v3.1. You can avoid this issue by upgrading your cluster to v3.1 or later. 
- This solution does not apply if more than half of the FE nodes have encountered this issue. You must follow the instructions provided in [Measure of the last resort](#7-measure-of-the-last-resort) to fix the issue.

:::

##### RollbackException

You can identify this issue based on the following error message:

```Plain
must rollback 1 total commits(1 of which were durable) to the earliest point indicated by transaction id=-14752149 time=2022-01-12 14:36:28.21 vlsn=28,069,415 lsn=0x1174/0x16e durable=false in order to rejoin the replication group. All existing ReplicatedEnvironment handles must be closed and reinstantiated. Log files were truncated to file 0x4467, offset 0x269, vlsn 28,069,413 HARD_RECOVERY: Rolled back past transaction commit or abort. Must run recovery by re-opening Environment handles Environment is invalid and must be closed.
```

This issue occurs when the Leader FE node writes BDBJE metadata but fails to synchronize it to Follower FE nodes before it hangs. After being restarted, the original Leader becomes a Follower, corrupting the metadata.

To solve this issue, you only need to restart this node again to wipe out the dirty metadata.

##### ReplicaWriteException

You can identify this issue based on the keyword `removeReplicaDb` from the FE log **fe.log**.

```Plain
Caused by: com.sleepycat.je.rep.ReplicaWriteException: (JE 18.3.16) Problem closing transaction 25000090. The current state is:REPLICA. The node transitioned to this state at:Fri Feb 23 01:31:00 UTC 2024 Problem seen replaying entry NameLN_TX/14 vlsn=1,902,818,939 isReplicated="1"  txn=-953505106 dbop=REMOVE Originally thrown by HA thread: REPLICA 10.233.132.23_9010_1684154162022(6)
        at com.sleepycat.je.rep.txn.ReadonlyTxn.disallowReplicaWrite(ReadonlyTxn.java:114) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.DbTree.checkReplicaWrite(DbTree.java:880) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.DbTree.doCreateDb(DbTree.java:579) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.DbTree.createInternalDb(DbTree.java:507) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.cleaner.ExtinctionScanner.openDb(ExtinctionScanner.java:357) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.cleaner.ExtinctionScanner.prepareForDbExtinction(ExtinctionScanner.java:1703) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.DbTree.doRemoveDb(DbTree.java:1208) ~[starrocks-bdb-je-18.3.16.jar:?]
        # highlight-start
        at com.sleepycat.je.dbi.DbTree.removeReplicaDb(DbTree.java:1261) ~[starrocks-bdb-je-18.3.16.jar:?]
        # highlight-end
        at com.sleepycat.je.rep.impl.node.Replay.applyNameLN(Replay.java:996) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.Replay.replayEntry(Replay.java:722) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.Replica$ReplayThread.run(Replica.java:1225) ~[starrocks-bdb-je-18.3.16.jar:?]
```

This issue occurs when the BDBJE version of the failed FE node (v18.3.16) mismatches that of the Leader FE node (v7.3.7).

You can follow these steps to fix this issue:

1. Drop the failed Follower or Observer node.

   ```SQL
   -- To drop a Follower node, replace <follower_host> with the IP address (priority_networks) 
   -- of the Follower node, and replace <follower_edit_log_port> (Default: 9010) with 
   -- the Follower node's edit_log_port.
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";
   
   -- To drop an Observer node, replace <observer_host> with the IP address (priority_networks) 
   -- of the Oberver node, and replace <observer_edit_log_port> (Default: 9010) with 
   -- the Observer node's edit_log_port.
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

2. Add the failed node back to the cluster.

   ```SQL
   -- Add the Follower node:
   ALTER SYSTEM ADD FOLLOWER "<follower_host>:<follower_edit_log_port>";
   
   -- Add the Observer node:
   ALTER SYSTEM ADD OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

3. Clear the metadata directory `meta_dir` of the failed node.
4. Restart the failed node using the Leader FE node as the helper.

   ```Bash
   # Replace <leader_ip> with the IP address (priority_networks) 
   # of the Leader FE node, and replace <leader_edit_log_port> (Default: 9010) with 
   # the Leader FE node's edit_log_port.
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

5. After the failed node recovers to a healthy status, you need to upgrade the BDBJE packages in your cluster to **starrocks-bdb-je-18.3.16.jar** (or upgrade your StarRocks cluster to v3.0 or later), following the order of Followers first and then the Leader.

##### InsufficientReplicasException

You can identify this issue based on the following error message:

```Plain
com.sleepycat.je.rep.InsufficientReplicasException: (JE 7.3.7) Commit policy: SIMPLE_MAJORITY required 1 replica. But none were active with this master.
```

This issue occurs when the Leader node or Follower nodes use excessive memory resources, leading to Full GC.

To solve this issue, you can either increase the JVM heap size or use the G1 GC algorithm.

##### InsufficientLogException

You can identify this issue based on the following error message:

```Plain
xxx INSUFFICIENT_LOG: Log files at this node are obsolete. Environment is invalid and must be closed.
```

This issue occurs when the Follower node requires full metadata synchronization. It may occur when one of the following situations happens:

- The metadata on the Follower node lags behind that of the Leader node, which has already done a CheckPoint of metadata within itself. The Follower node cannot perform incremental updates on its metadata, thus full metadata synchronization is required.
- The original Leader node writes and checkpoints its metadata, but fails to synchronize it to Follower FE nodes before it hangs. After being restarted, it becomes a Follower node. With dirty metadata checkpointed, the Follower node cannot perform incremental deletion of its metadata, thus full metadata synchronization is required.

Please note that this exception will be thrown when a new Follower node is added to the cluster. In this case, you do not need to take any action. If this exception is thrown for an existing Follower node or the Leader node, you only need to restart the node.

##### HANDSHAKE_ERROR: Error during the handshake between two nodes

You can identify this issue based on the following error message:

```Plain
2023-11-13 21:51:55,271 WARN (replayer|82) [BDBJournalCursor.wrapDatabaseException():97] failed to get DB names for 1 times!Got EnvironmentFailureExce
com.sleepycat.je.EnvironmentFailureException: (JE 18.3.16) Environment must be closed, caused by: com.sleepycat.je.EnvironmentFailureException: Environment invalid because of previous exception: (JE 18.3.16) 10.26.5.115_9010_1697071897979(1):/data1/meta/bdb A replica with the name: 10.26.5.115_9010_1697071897979(1) is already active with the Feeder:null HANDSHAKE_ERROR: Error during the handshake between two nodes. Some validity or compatibility check failed, preventing further communication between the nodes. Environment is invalid and must be closed.
        at com.sleepycat.je.EnvironmentFailureException.wrapSelf(EnvironmentFailureException.java:230) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.EnvironmentImpl.checkIfInvalid(EnvironmentImpl.java:1835) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.dbi.EnvironmentImpl.checkOpen(EnvironmentImpl.java:1844) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.Environment.checkOpen(Environment.java:2697) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.Environment.getDatabaseNames(Environment.java:2455) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.starrocks.journal.bdbje.BDBEnvironment.getDatabaseNamesWithPrefix(BDBEnvironment.java:478) ~[starrocks-fe.jar:?]
        at com.starrocks.journal.bdbje.BDBJournalCursor.refresh(BDBJournalCursor.java:177) ~[starrocks-fe.jar:?]
        at com.starrocks.server.GlobalStateMgr$5.runOneCycle(GlobalStateMgr.java:2148) ~[starrocks-fe.jar:?]
        at com.starrocks.common.util.Daemon.run(Daemon.java:115) ~[starrocks-fe.jar:?]
        at com.starrocks.server.GlobalStateMgr$5.run(GlobalStateMgr.java:2216) ~[starrocks-fe.jar:?]
Caused by: com.sleepycat.je.EnvironmentFailureException: Environment invalid because of previous exception: (JE 18.3.16) 10.26.5.115_9010_1697071897979(1):/data1/meta/bdb A replica with the name: 10.26.5.115_9010_1697071897979(1) is already active with the Feeder:null HANDSHAKE_ERROR: Error during the handshake between two nodes. Some validity or compatibility check failed, preventing further communication between the nodes. Environment is invalid and must be closed. Originally thrown by HA thread: UNKNOWN 10.26.5.115_9010_1697071897979(1) Originally thrown by HA thread: UNKNOWN 10.26.5.115_9010_1697071897979(1)
        at com.sleepycat.je.rep.stream.ReplicaFeederHandshake.negotiateProtocol(ReplicaFeederHandshake.java:198) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.stream.ReplicaFeederHandshake.execute(ReplicaFeederHandshake.java:250) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.Replica.initReplicaLoop(Replica.java:709) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.Replica.runReplicaLoopInternal(Replica.java:485) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.Replica.runReplicaLoop(Replica.java:412) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.RepNode.run(RepNode.java:1869) ~[starrocks-bdb-je-18.3.16.jar:?]
```

This issue occurs when the original Leader node hangs and becomes alive again while the surviving Follower nodes are trying to elect a new Leader node. The Follower nodes will try to establish a new connection with the original Leader node. However, the Leader node will reject the connection request because the old connection still exists. Once the request is rejected, the Follower node will set the environment as invalid and throw this exception.

To solve this issue, you can either increase the JVM heap size or use the G1 GC algorithm.

##### Latch timeout. com.sleepycat.je.log.LogbufferPool_FullLatch

You can identify this issue based on the following error message:

```Plain
Environment invalid because of previous exception: xxx Latch timeout. com.sleepycat.je.log.LogbufferPool_FullLatch xxx'
        at com.sleepycat.je.EnvironmentFailureException.unexpectedState(EnvironmentFailureException.java:459)
        at com.sleepycat.je.latch.LatchSupport.handleTimeout(LatchSupport.java:211)
        at com.sleepycat.je.latch.LatchWithStatsImpl.acquireExclusive(LatchWithStatsImpl.java:87)
        at com.sleepycat.je.log.LogBufferPool.bumpCurrent(LogBufferPool.java:527)
        at com.sleepycat.je.log.LogManager.flushInternal(LogManager.java:1373)
        at com.sleepycat.je.log.LogManager.flushNoSync(LogManager.java:1337)
        at com.sleepycat.je.log.LogFlusher$FlushTask.run(LogFlusher.java:232)
        at java.util.TimerThread.mainLoop(Timer.java:555)
        at java.util.TimerThread.run(Timer.java:505)
```

This issue occurs when there is excessive pressure on the local disk of the FE node.

To solve this issue, you can allocate a dedicated disk for the FE metadata, or replace the disk with a high-performance one.

##### DatabaseNotFoundException

You can identify this issue based on the following error message:

```Plain
2024-01-05 12:47:21,087 INFO (main|1) [BDBEnvironment.ensureHelperInLocal():340] skip check local environment because helper node and local node are identical.
2024-01-05 12:47:21,339 ERROR (MASTER 172.17.0.1_9112_1704430041062(-1)|1) [StarRocksFE.start():186] StarRocksFE start failed
com.sleepycat.je.DatabaseNotFoundException: (JE 18.3.16) _jeRepGroupDB
        at com.sleepycat.je.rep.impl.RepImpl.openGroupDb(RepImpl.java:1974) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.RepImpl.getGroupDb(RepImpl.java:1912) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.RepGroupDB.reinitFirstNode(RepGroupDB.java:1439) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.RepNode.reinitSelfElect(RepNode.java:1686) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.RepNode.startup(RepNode.java:874) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.node.RepNode.joinGroup(RepNode.java:2153) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.impl.RepImpl.joinGroup(RepImpl.java:618) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.ReplicatedEnvironment.joinGroup(ReplicatedEnvironment.java:558) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.ReplicatedEnvironment.<init>(ReplicatedEnvironment.java:619) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.ReplicatedEnvironment.<init>(ReplicatedEnvironment.java:464) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.ReplicatedEnvironment.<init>(ReplicatedEnvironment.java:538) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.util.DbResetRepGroup.reset(DbResetRepGroup.java:262) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.starrocks.journal.bdbje.BDBEnvironment.initConfigs(BDBEnvironment.java:188) ~[starrocks-fe.jar:?]
        at com.starrocks.journal.bdbje.BDBEnvironment.setup(BDBEnvironment.java:174) ~[starrocks-fe.jar:?]
        at com.starrocks.journal.bdbje.BDBEnvironment.initBDBEnvironment(BDBEnvironment.java:153) ~[starrocks-fe.jar:?]
        at com.starrocks.journal.JournalFactory.create(JournalFactory.java:31) ~[starrocks-fe.jar:?]
        at com.starrocks.server.GlobalStateMgr.initJournal(GlobalStateMgr.java:1201) ~[starrocks-fe.jar:?]
        at com.starrocks.server.GlobalStateMgr.initialize(GlobalStateMgr.java:1150) ~[starrocks-fe.jar:?]
        at com.starrocks.StarRocksFE.start(StarRocksFE.java:129) ~[starrocks-fe.jar:?]
        at com.starrocks.StarRocksFE.main(StarRocksFE.java:83) ~[starrocks-fe.jar:?]
```

This issue occurs when you add the configuration `metadata_failure_recovery = true` in the FE configuration file **fe.conf**.

To solve this issue, you need to remove the configuration and restart the node.

#### StarRocks metadata damage

You can identify the StarRocks metadata damage issue based on one of the following error messages:

```Plain
failed to load journal type xxx
```

Or

```Plain
catch exception when replaying
```

:::warning

Before proceeding to recover the metadata by following the solution provided below, you are strongly advised to seek assistance from the technical experts in the StarRocks community, because this solution may lead to **data loss**.

:::

You can follow these steps to fix this issue:

1. Stop all FE nodes.
2. Back up the metadata directories `meta_dir` of all FE nodes.
3. Add the configuration `metadata_enable_recovery_mode = true` to the configuration files **fe.conf** of all FE nodes. Note that data loading is forbidden in this mode.
4. Start all FE nodes, and query tables in the cluster to check whether your data is intact.

   You must wait until metadata recovery is completed if the following error is returned when you query these tables:

   ```Plain
   ERROR 1064 (HY000): capture_consistent_versions error: version already been compacted.
   ```

   You can execute the following statement from the Leader FE node to view the progress of metadata recovery:

   ```SQL
   SHOW PROC '/meta_recovery';
   ```

   This statement will show the partitions that failed to be recovered. You can follow the advice returned to recover the partitions. If nothing is returned, it indicates the recovery is successful.

5. If both your data and metadata are intact, execute the following statement to create an image file for your metadata:

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. After the new image file is transmitted to the directory **meta/image** of all FE nodes, you can remove the configuration `metadata_enable_recovery_mode = true` from all FE configuration files and restart the FE nodes.

## FE fails to provide services

FE will not provide services when Follower FE nodes fail to perform Leader election. When this issue occurs, you may find the following log record repeating:

```Plain
wait globalStateMgr to be ready. FE type: INIT. is ready: false
```

A variety of exceptions may cause this issue. You are strongly advised to troubleshoot the issue step by step following the sections below. Applying inappropriate solutions will deteriorate the problem and probably lead to data loss.

### 1. The majority of Follower nodes are not running

If the majority of the Follower nodes are not running, the FE group will not provide services. Here, 'majority' indicates `1 + (Follower node count/2)`. Please note that the Leader FE node itself is a Follower, but Observer nodes are not Followers.

- You can identify the role of each FE node from the **fe/meta/image/ROLE** file:

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

- You can view the total number of Follower nodes from the BDBJE log:

  ```Bash
  grep "Current group size" fe/meta/bdb/je.info.0

  # The example output indicates there are three Follower nodes in the cluster.
  2024-01-24 08:21:44.754 UTC INFO [172.26.92.139_29917_1698226672727] Current group size: 3
  ```

To solve this issue, you need to start all Follower nodes in the cluster. If they cannot be restarted, please refer to [The measure of last resort](#7-measure-of-the-last-resort).

### 2. Node IP is changed 

If the `priority_networks` of the node is not configured, the FE node will randomly select an available IP address once it is restarted. If the IP address recorded in the BDBJE metadata is different from that used to start the node, the FE will not provide services.

- You can view the IP address recorded in the BDBJE metadata from the **fe/meta/image/ROLE** file:

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

  The value `172.26.92.154` before the first underscore is the IP address recorded in the BDBJE metadata.

- You can view the IP address used to start the node from the FE log:

  ```Bash
  grep "IP:" fe/log/fe.log

  2024-02-06 14:33:58,211 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  2024-02-06 14:34:27,689 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  ```

To solve this issue, you need to set the `priority_networks` of the node in the FE configuration file **fe.conf** to the IP address recorded in **fe/meta/image/ROLE**, and restart the node.

### 3. System clock among nodes is not synchronized

You can identify this issue based on the following error message from **fe.out**, **fe.log** or **fe/meta//bdb/je.info.0**:

```Plain
com.sleepycat.je.EnvironmentFailureException: (JE 7.3.7) Environment must be closed, caused by: com.sleepycat.je.EnvironmentFailureException: Environment invalid because of previous exception: (JE 7.3.7) 172.26.92.139_29917_1631006307557(2180):xxx Clock delta: 11020 ms. between Feeder: 172.26.92.154_29917_1641969377236 and this Replica exceeds max permissible delta: 5000 ms. HANDSHAKE_ERROR: Error during the handshake between two nodes. Some validity or compatibility check failed, preventing further communication between the nodes. Environment is invalid and must be closed. fetchRoot of 0x1278/0x1fcbb8 state=0 expires=never
```

You must synchronize the system clock among all nodes.

### 4. Available disk space is insufficient

After you upgrade StarRocks to v3.0 or later, or upgrade BDBJE to v18 or later, the node may fail to restart when the available space of the disk that stores `meta_dir` is less than 5 GB.

You can view the BDBJE version from the **.jar** package under the directory **fe/lib**.

To solve this issue, you can scale up the disk, or allocate a dedicated disk with larger capacity for the FE metadata.

### 5. `edit_log_port` is changed

If the `edit_log_port` recorded in the BDBJE metadata is different from that configured in **fe.conf**, the FE will not provide services.

You can view the `edit_log_port` recorded in the BDBJE metadata from the **fe/meta/image/ROLE** file:

```Bash
cat fe/meta/image/ROLE

#Fri Jan 19 20:03:14 CST 2024
role=FOLLOWER
hostType=IP
name=172.26.92.154_9312_1705568349984
```

The value `9312` before the second underscore is the `edit_log_port` recorded in the BDBJE metadata.

To solve this issue, you need to set the `edit_log_port` of the node in the FE configuration file **fe.conf** to the `edit_log_port` recorded in **fe/meta/image/ROLE**, and restart the node.

### 6. JVM heap size is insufficient

You can view the JVM memory usage using the `jstat` command:

```Plain
jstat -gcutil pid 1000 1000

  S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
  0.00 100.00  27.78  95.45  97.77  94.45     24    0.226     1    0.065    0.291
  0.00 100.00  44.44  95.45  97.77  94.45     24    0.226     1    0.065    0.291
  0.00 100.00  55.56  95.45  97.77  94.45     24    0.226     1    0.065    0.291
  0.00 100.00  72.22  95.45  97.77  94.45     24    0.226     1    0.065    0.291
  0.00 100.00  88.89  95.45  97.77  94.45     24    0.226     1    0.065    0.291
  0.00 100.00   5.26  98.88  97.80  94.45     25    0.231     1    0.065    0.297
  0.00 100.00  21.05  98.88  97.80  94.45     25    0.231     1    0.065    0.297
  0.00 100.00  31.58  98.88  97.80  94.45     25    0.231     1    0.065    0.297
  0.00 100.00  47.37  98.88  97.80  94.45     25    0.231     1    0.065    0.297
  0.00 100.00  63.16  98.88  97.80  94.45     25    0.231     1    0.065    0.297
  0.00 100.00  73.68  98.88  97.80  94.45     25    0.231     1    0.065    0.297
```

If the percentages shown in field `O` remain high, it indicates that the JVM heap size is insufficient.

To solve this issue, you must increase the JVM heap size.

### 7. Measure of the last resort

:::warning

You can try the following solution as the last resort only when none of the preceding solutions works.

:::

This solution is designed only for extreme cases, such as:

- The majority of Follower nodes cannot be restarted.
- Follower nodes cannot perform Leader election due to bugs of BDBJE.
- Exceptions other than those mentioned in preceding sections.

Follow these steps to recover the metadata:

1. Stop all FE nodes.
2. Back up the metadata directories `meta_dir` of all FE nodes.
3. Run the following commands on **all the servers that host the FE nodes** to identify the node with the latest metadata.

   - For StarRocks v2.5 and earlier:

     ```Bash
     java -jar fe/lib/je-7.3.7.jar DbPrintLog -h meta/bdb/ -vd
     ```

   - For StarRocks v3.0 and later:

     ```Bash
     # You need to specify the exact .jar package the node uses in the command 
     # as the package varies according to the StarRocks version.
     java -jar fe/lib/starrocks-bdb-je-18.3.16.jar DbPrintLog -h meta/bdb/ -vd
     ```

   Example output:

   ```Bash
   <DbPrintLog>
   file 0x3b numRepRecords = 24479 firstVLSN = 1,434,126 lastVLSN = 1,458,604
   file 0x3c numRepRecords = 22541 firstVLSN = 1,458,605 lastVLSN = 1,481,145
   file 0x3d numRepRecords = 25176 firstVLSN = 1,481,146 lastVLSN = 1,506,321
   ......
   file 0x74 numRepRecords = 26903 firstVLSN = 2,927,458 lastVLSN = 2,954,360
   file 0x75 numRepRecords = 26496 firstVLSN = 2,954,361 lastVLSN = 2,980,856
   file 0x76 numRepRecords = 18727 firstVLSN = 2,980,857 lastVLSN = 2,999,583
   ... 0 files at end
   First file: 0x3b
   Last file: 0x76
   </DbPrintLog>
   ```

   The node with the largest `lastVLSN` value has the latest metadata.

4. Find the role (Follower or Observer) of the FE node that has the latest metadata from the **fe/meta/image/ROLE** file:

   ```Bash
   cat fe/meta/image/ROLE
   
   #Fri Jan 19 20:03:14 CST 2024
   role=FOLLOWER
   hostType=IP
   name=172.26.92.154_9312_1705568349984
   ```

   If multiple nodes have the latest metadata, it is recommended to proceed with a Follower node. If multiple Follower nodes have the latest metadata, you can proceed with any of them.

5. Perform the corresponding operations based on the role of the FE node you chose in the previous step.

  <Tabs groupId="recovery_role">

    <TabItem value="follower" label="Proceed with Follower Node" default>

   If a Follower node has the latest metadata, perform the following operations:

   1. Add the following configurations to **fe.conf**:

      - For StarRocks v2.5, v3.0, v3.1.9 and earlier patch versions, and v3.2.4 and earlier patch versions:

        ```Properties
        metadata_failure_recovery = true
        ```

      - For StarRocks v3.1.10 and later patch versions, v3.2.5 and later patch versions, and v3.3 and later:

        ```Properties
        bdbje_reset_election_group = true
        ```

   2. Restart the node, and check whether your data and metadata are intact.
   3. Check whether the current FE node is the Leader FE node.

      ```SQL
      SHOW FRONTENDS;
      ```

      - If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
      - If the field `Role` is `LEADER`, this FE node is the Leader FE node.

   4. If the data and metadata are intact, and the role of the node is Leader, you can remove the configuration you added earlier and restart the node.

    </TabItem>

    <TabItem value="oberserver" label="Proceed with Observer Node" >

   If an Observer node has the latest metadata, perform the following operations:

   1. Change the role of the FE node from `OBSERVER` to `FOLLOWER` in the **fe/meta/image/ROLE** file.
   2. Add the following configurations to **fe.conf**:

      - For StarRocks v2.5, v3.0, v3.1.9 and earlier patch versions, and v3.2.4 and earlier patch versions:

        ```Properties
        metadata_failure_recovery = true
        ```

      - For StarRocks v3.1.10 and later patch versions, v3.2.5 and later patch versions, and v3.3 and later:

        ```Properties
        bdbje_reset_election_group = true
        ```

   3. Restart the node, and check whether your data and metadata are intact.
   4. Check whether the current FE node is the Leader FE node.

      ```SQL
      SHOW FRONTENDS;
      ```

      - If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
      - If the field `Role` is `LEADER`, this FE node is the Leader FE node.

   5. If the data and metadata are intact, and the role of the node is Leader, you can remove the configuration you added earlier. However, **do not restart the node**.
   6. Drop all FE nodes except the current node. It now serves as the temporary Leader node.

      ```SQL
      -- To drop a Follower node, replace <follower_host> with the IP address (priority_networks) 
      -- of the Follower node, and replace <follower_edit_log_port> (Default: 9010) with 
      -- the Follower node's edit_log_port.
      ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

      -- To drop an Observer node, replace <observer_host> with the IP address (priority_networks) 
      -- of the Oberver node, and replace <observer_edit_log_port> (Default: 9010) with 
      -- the Observer node's edit_log_port.
      ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
      ```

   7. Add a new Follower node (on a new server) to the cluster.

      ```SQL
      ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
      ```

   8. Start a new FE node on the new server using the temporary Leader FE node as the helper.

      ```Bash
      # Replace <leader_ip> with the IP address (priority_networks) 
      # of the Leader FE node, and replace <leader_edit_log_port> (Default: 9010) with 
      # the Leader FE node's edit_log_port.
      ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
      ```

   9. Once the new FE node is started successfully, check the status and roles of both FE nodes:

      ```SQL
      SHOW FRONTENDS;
      ```

      - If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
      - If the field `Role` is `FOLLOWER`, this FE node is a Follower FE node.
      - If the field `Role` is `LEADER`, this FE node is the Leader FE node.

   10. If the new Follower is successfully running in the cluster, you can then stop all nodes.
   11. Add the following configurations to the **fe.conf of the new Follower only**:

       - For StarRocks v2.5, v3.0, v3.1.9 and earlier patch versions, and v3.2.4 and earlier patch versions:

         ```Properties
         metadata_failure_recovery = true
         ```

       - For StarRocks v3.1.10 and later patch versions, v3.2.5 and later patch versions, and v3.3 and later:

         ```Properties
         bdbje_reset_election_group = true
         ```

   12. Restart the new Follower node, and check whether your data and metadata are intact.
   13. Check whether the current FE node is the Leader FE node.

       ```SQL
       SHOW FRONTENDS;
       ```

       - If the field `Alive` is `true`, this FE node is properly started and added to the cluster.
       - If the field `Role` is `LEADER`, this FE node is the Leader FE node.

   14. If the data and metadata are intact, and the role of the node is Leader, you can remove the configuration you added earlier and restart the node.

    </TabItem>

  </Tabs>

6. The surviving Follower node is now essentially the Leader node of the cluster. Drop all FE nodes except for the current node.

   ```SQL
   -- To drop a Follower node, replace <follower_host> with the IP address (priority_networks) 
   -- of the Follower node, and replace <follower_edit_log_port> (Default: 9010) with 
   -- the Follower node's edit_log_port.
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

   -- To drop an Observer node, replace <observer_host> with the IP address (priority_networks) 
   -- of the Oberver node, and replace <observer_edit_log_port> (Default: 9010) with 
   -- the Observer node's edit_log_port.
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

7. Clear the metadata directories `meta_dir` of the FE nodes that you want to add back to the cluster.
8. Start new Follower nodes using the new Leader FE node as the helper.

   ```Bash
   # Replace <leader_ip> with the IP address (priority_networks) 
   # of the Leader FE node, and replace <leader_edit_log_port> (Default: 9010) with 
   # the Leader FE node's edit_log_port.
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

9. Add the Follower nodes back to the cluster.

   ```SQL
   ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
   ```

After all nodes are added back to the cluster, the metadata is successfully recovered.

## Metadata recovery-related configurations

:::tip

You must remove the following configuration once the metadata recovery is completed.

:::

- [bdbje_reset_election_group](./management/FE_configuration.md#bdbje_reset_election_group)
- [metadata_enable_recovery_mode](./management/FE_configuration.md#metadata_enable_recovery_mode)
- [ignore_unknown_log_id](./management/FE_configuration.md#ignore_unknown_log_id)
