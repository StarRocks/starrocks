---
displayed_sidebar: docs
---

# 恢复元数据

import Tabs from '@theme/Tabs';

import TabItem from '@theme/TabItem';

本文介绍在 FE 节点遇到不同异常时如何恢复 StarRocks 集群中的元数据。

通常情况下，只有在出现以下情况时，可能需要进行元数据恢复：

- [FE 节点无法启动](#fe-节点无法启动)
- [FE 节点无法提供服务](#fe-节点无法提供服务)

请排查您所遇到的问题，并按照对应解决方案进行操作。建议按照文中推荐的操作执行。

## FE 节点无法启动

如果元数据损坏或在回滚后与集群不兼容，可能导致 FE 节点无法启动。

### 回滚后的元数据不兼容

当降级 StarRocks 集群后，FE 节点有可能因为与降级前版本元数据不兼容而无法启动。

如果在降级集群时遇到以下异常，可以确定出现了此问题：

```Plain
UNKNOWN Operation Type xxx
```

您可以按照以下步骤恢复元数据并启动 FE 节点：

1. 停止所有 FE 节点。
2. 备份所有 FE 节点的元数据目录 `meta_dir`。
3. 在所有 FE 节点的配置文件 **fe.conf** 中添加配置 `ignore_unknown_log_id = true`。
4. 启动所有 FE 节点，并检查数据和元数据是否完整。
5. 如果数据和元数据都完整，请执行以下语句为元数据创建镜像文件：

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 在新的镜像文件传输到所有 FE 节点的目录 **meta/image** 之后，需要从所有 FE 节点的配置文件中移除配置项 `ignore_unknown_log_id = true`，并重新启动 FE 节点。

### 元数据损坏

BDBJE 或 StarRocks 的元数据损坏都会导致 FE 节点无法重新启动。

#### BDBJE 元数据损坏

##### VLSN Bug

根据以下 Error Message 识别 VLSN Bug：

```Plain
recoveryTracker should overlap or follow on disk last 
VLSN of 6,684,650 recoveryFirst= 6,684,652 
UNEXPECTED_STATE_FATAL: Unexpected internal state, unable to continue. 
Environment is invalid and must be closed.
```

按照以下步骤来解决此问题：

1. 清除报错 FE 节点的元数据目录 `meta_dir`。
2. 使用 Leader FE 节点作为 Helper 重新启动当前 FE 节点。

   ```Bash
   # 将 <leader_ip> 替换为 Leader FE 节点的 IP 地址（priority_networks），
   # 并将 <leader_edit_log_port>（默认：9010）替换为 Leader FE 节点的 edit_log_port。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

:::tip

- 该 Bug 已在 StarRocks v3.1 中修复。您可以通过将集群升级到 v3.1 及以上版本避免此问题。
- 如果超过半数的 FE 节点遇到了此问题，则此解决方案不适用，必须按照 [最终应急方案](#7-最终应急方案) 中提供的说明来解决此问题。

:::

##### RollbackException

根据以下 Error Message 识别该问题：

```Plain
must rollback 1 total commits(1 of which were durable) to the earliest point indicated by transaction id=-14752149 time=2022-01-12 14:36:28.21 vlsn=28,069,415 lsn=0x1174/0x16e durable=false in order to rejoin the replication group. All existing ReplicatedEnvironment handles must be closed and reinstantiated.  Log files were truncated to file 0x4467, offset 0x269, vlsn 28,069,413 HARD_RECOVERY: Rolled back past transaction commit or abort. Must run recovery by re-opening Environment handles Environment is invalid and must be closed.
```

导致此问题原因是，Leader FE 节点在写入 BDBJE 元数据后挂起，未能将元数据同步到其他 Follower FE 节点。重新启动后，原 Leader 节点变为 Follower，导致元数据损坏。

要解决此问题，只需要重新启动报错节点，从而清除脏数据。

##### ReplicaWriteException

根据 FE 日志 **fe.log** 中的关键字 `removeReplicaDb` 来识别此问题。

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

导致此问题原因是当前 FE 节点的 BDBJE 版本（v18.3.16）与 Leader FE 节点的版本（v7.3.7）不匹配。

按照以下步骤来解决此问题：

1. 删除报错的 Follower 或 Observer 节点。

   ```SQL
   -- 如需删除 Follower 节点，请将 <follower_host> 替换为 Follower 节点的 IP 地址（priority_networks），
   -- 并将 <follower_edit_log_port>（默认值：9010）替换为 Follower 节点的 edit_log_port。
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

   -- 如需删除 Observer 节点，请将 <observer_host> 替换为 Observer 节点的 IP 地址（priority_networks），
   -- 并将 <observer_edit_log_port>（默认值：9010）替换为 Observer 节点的 edit_log_port。
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

2. 将报错节点重新添加到集群中。

   ```SQL
   -- 添加 Follower 节点。
   ALTER SYSTEM ADD FOLLOWER "<follower_host>:<follower_edit_log_port>";

   -- 添加 Observer 节点。
   ALTER SYSTEM ADD OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

3. 清除报错节点的元数据目录 `meta_dir`。
4. 使用 Leader FE 节点作为 Helper 重新启动报错节点。

   ```Bash
   # 将 <leader_ip> 替换为 Leader FE 节点的 IP 地址（priority_networks），
   # 并将 <leader_edit_log_port>（默认：9010）替换为 Leader FE 节点的 edit_log_port。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

5. 在报错节点健康状态恢复后，需要将集群中的 BDBJE 软件包升级到 **starrocks-bdb-je-18.3.16.jar**（或将 StarRocks 集群升级到 v3.0 或更高版本）。此操作需要按照先 Follower，后 Leader 的顺序进行。

##### InsufficientReplicasException

根据以下 Error Message 识别该问题：

```Plain
com.sleepycat.je.rep.InsufficientReplicasException: (JE 7.3.7) Commit policy: SIMPLE_MAJORITY required 1 replica. But none were active with this master.
```

当 Leader 节点或 Follower 节点使用过多内存资源而导致 Full GC 时，会出现此问题。

要解决此问题，可以增加 JVM 内存大小或使用 G1 GC 算法。

##### InsufficientLogException

根据以下 Error Message 识别该问题：

```Plain
xxx INSUFFICIENT_LOG: Log files at this node are obsolete. Environment is invalid and must be closed.
```

此问题原因是 Follower 节点需要进行全量元数据同步。以下情况可能导致该问题：

- Follower 节点的元数据落后于 Leader 节点，但 Leader 节点已经进行元数据 CheckPoint。Follower 节点无法对其元数据执行增量更新，因此需要进行元数据全量同步。
- 原 Leader 节点写入元数据并 CheckPoint，但在挂起之前未能将元数据同步到 Follower 节点。重新启动后，原 Leader 节点变为 Follower 节点。由于有脏元数据已经 CheckPoint，该节点无法对其元数据执行增量删除，因此需要进行元数据全量同步。

请注意，当新的 Follower 节点加入集群时，会抛出此异常。在这种情况下，无需采取任何操作。如果已有的 Follower 节点或 Leader 节点抛出此异常，则只需重新启动该节点。

##### HANDSHAKE_ERROR: Error during the handshake between two nodes

根据以下 Error Message 识别该问题：

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

导致此问题原因是，Leader 节点挂起后，剩余的 Follower 节点尝试选举新的 Leader 节点，此时原 Leader 节点重新上线。Follower 节点将尝试与原始 Leader 节点建立新连接。但是，由于旧有连接仍然存在，Leader 节点将拒绝连接请求。一旦请求被拒绝，Follower 节点会将环境设置为 Invalid 并抛出此异常。

要解决此问题，可以增加 JVM 内存大小或使用 G1 GC 算法。

##### Latch timeout. com.sleepycat.je.log.LogbufferPool_FullLatch

根据以下 Error Message 识别该问题：

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

当 FE 节点的本地磁盘压力过大时会出现此问题。

要解决此问题，可以为 FE 元数据路径分配一个专用磁盘，或者将当前磁盘替换为高性能磁盘。

##### DatabaseNotFoundException

根据以下 Error Message 识别该问题：

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

在 FE 配置文件 **fe.conf** 中添加配置 `metadata_failure_recovery = true` 后会出现此问题。

要解决此问题，需要删除该配置并重新启动节点。

#### StarRocks 元数据损坏

您可以根据以下两种 Error Message 识别 StarRocks 元数据损坏问题：

```Plain
failed to load journal type xxx
```

或

```Plain
catch exception when replaying
```

:::warning

在执行以下操作恢复元数据之前，强烈建议您在 StarRocks 社区中寻求技术专家的帮助，因为该解决方案**有可能导致数据丢失**。

:::

按照以下步骤来解决此问题：

1. 停止所有 FE 节点。
2. 备份所有 FE 节点的元数据目录 `meta_dir`。
3. 在所有 FE 节点的配置文件 **fe.conf** 中添加配置 `metadata_enable_recovery_mode = true`。注意，此模式下禁止写入数据。
4. 启动所有 FE 节点，并查询集群中的表，检查数据是否完整。

   如果在查询这些表时返回以下错误，则必须等待元数据恢复完成：

   ```Plain
   ERROR 1064 (HY000): capture_consistent_versions error: version already been compacted.
   ```

   从 Leader  FE 节点执行以下语句查看元数据恢复的进度：

   ```SQL
   SHOW PROC '/meta_recovery';
   ```

   该语句将显示无法恢复的分区。您可以按照其中返回的建议来恢复这些分区。如果没有返回任何内容，则表示恢复成功。

5. 如果数据和元数据都完整，请执行以下语句为元数据创建镜像文件：

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 在新的镜像文件传输到所有 FE 节点的目录 **meta/image** 之后，可以从所有 FE 节点的配置文件中移除配置项 `metadata_enable_recovery_mode = true`，并重新启动 FE 节点。

## FE 节点无法提供服务

当 Follower FE 节点无法选举 Leader 时， FE 将无法提供服务。当此问题发生时，以下日志记录可能会重复出现：

```Plain
wait globalStateMgr to be ready. FE type: INIT. is ready: false
```

多种异常都有可能导致此问题。强烈建议按照以下部分逐步排查问题。使用错误的解决方案会使问题恶化，并可能导致**数据丢失**。

### 1. 多数 Follower 节点未启动

如果大多数 Follower 节点未运行， FE 节点组将无法提供服务。此处“大多数”指 `1 + (Follower 节点数/2)`。请注意，Leader FE 节点本身也是一个 Follower 节点，但 Observer 节点不是 Follower 节点。

- 从 **fe/meta/image/ROLE** 文件中查看每个 FE 节点的角色：

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

- 从 BDBJE 日志中查看 Follower 节点的总数：

  ```Bash
  grep "Current group size" fe/meta/bdb/je.info.0

  # 以下示例输出表明集群中有三个 Follower 节点。
  2024-01-24 08:21:44.754 UTC INFO [172.26.92.139_29917_1698226672727] Current group size: 3
  ```

要解决此问题，需要启动集群中的所有 Follower 节点。如果无法重新启动，请参阅[最终应急方案](#7-最终应急方案)。

### 2. 节点 IP 变更

如果 FE 节点未配置 `priority_networks`，则在重新启动时系统会随机选择一个可用的 IP 地址。如果 BDBJE 元数据中记录的 IP 地址与启动节点时使用的 IP 地址不同， FE 节点将无法提供服务。

- 从 **fe/meta/image/ROLE** 文件中查看 BDBJE 元数据中记录的 IP 地址：

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

  第一个下划线之前的值是 BDBJE 元数据中记录的 IP 地址。

- 从 FE 日志中查看用于启动节点的 IP 地址：

  ```Bash
  grep "IP:" fe/log/fe.log
  
  2024-02-06 14:33:58,211 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  2024-02-06 14:34:27,689 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  ```

要解决此问题，需要将节点 **fe.conf** 中的 `priority_networks` 设置为 **fe/meta/image/ROLE** 中记录的 IP 地址，并重新启动节点。

### 3. 节点间的系统时钟未同步

通过在 **fe.out**、**fe.log** 或 **fe/meta//bdb/je.info.0** 中查找以下 Error Message 识别该问题：

```Plain
com.sleepycat.je.EnvironmentFailureException: (JE 7.3.7) Environment must be closed, caused by: com.sleepycat.je.EnvironmentFailureException: Environment invalid because of previous exception: (JE 7.3.7) 172.26.92.139_29917_1631006307557(2180):xxx Clock delta: 11020 ms. between Feeder: 172.26.92.154_29917_1641969377236 and this Replica exceeds max permissible delta: 5000 ms. HANDSHAKE_ERROR: Error during the handshake between two nodes. Some validity or compatibility check failed, preventing further communication between the nodes. Environment is invalid and must be closed. fetchRoot of 0x1278/0x1fcbb8 state=0 expires=never
```

要解决此问题，需要同步节点间的系统时钟。

### 4. 磁盘空间不足

将 StarRocks 升级到 v3.0 或更高版本，或将 BDBJE 升级到 v18 或更高版本后，如果 `meta_dir` 所在的磁盘可用空间小于 5 GB，则节点可能无法启动。

您可以从目录 **fe/lib** 下的 **.jar** 包查看 BDBJE 版本。

要解决此问题，可以扩容磁盘，或为 FE 元数据分配一个容量更大的专用磁盘。

### 5. `edit_log_port` 变更

如果 BDBJE 元数据中记录的 `edit_log_port` 与 **fe.conf** 中配置的不同， FE 节点将无法提供服务。

从 **fe/meta/image/ROLE** 文件中查看 BDBJE 元数据中记录的 `edit_log_port`：

```Bash
cat fe/meta/image/ROLE

#Fri Jan 19 20:03:14 CST 2024
role=FOLLOWER
hostType=IP
name=172.26.92.154_9312_1705568349984
```

第二个下划线之前的值是 BDBJE 元数据中记录的 `edit_log_port`。

要解决此问题，需要将节点 **fe.conf** 中的 `edit_log_port` 设置为 **fe/meta/image/ROLE** 中记录的 `edit_log_port`，并重新启动节点。

### 6. JVM 内存不足

使用 `jstat` 命令查看JVM内存使用情况：

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

如果字段 `O` 中显示的百分比较高，则表示 JVM 内存大小不足。

要解决此问题，必须增大 JVM 内存。

### 7. 最终应急方案

:::warning

只有在前述情况都不适用的情况下，才建议尝试以下解决方案，作为最终应急方案。

:::

此解决方案旨在处理极端情况，包括：

- 多数 Follower 节点无法启动。
- 由于 BDBJE 的 Bug 导致 Follower 节点无法选举 Leader。
- 前述章节提及以外的其他异常。

按照以下步骤恢复元数据：

1. 停止所有 FE 节点。
2. 备份所有 FE 节点的元数据目录 `meta_dir`。
3. 查看拥有最新元数据的节点。您需要在**所有 FE 节点的服务器**上运行以下命令。

   - StarRocks v2.5 及之前版本：

     ```Bash
     java -jar fe/lib/je-7.3.7.jar DbPrintLog -h meta/bdb/ -vd
     ```

   - StarRocks v3.0 及以后版本：

     ```Bash
     # 需要在命令中指定节点实际使用的 .jar 包名，因为该包会根据 StarRocks 版本不同而变化。
     java -jar fe/lib/starrocks-bdb-je-18.3.16.jar DbPrintLog -h meta/bdb/ -vd
     ```

   示例输出：

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

   `lastVLSN` 值最大的节点具有最新的元数据。

4. 从 **fe/meta/image/ROLE** 文件中查看元数据最新的 FE 节点的角色（Follower 或 Observer）。

   ```Bash
   cat fe/meta/image/ROLE

   #Fri Jan 19 20:03:14 CST 2024
   role=FOLLOWER
   hostType=IP
   name=172.26.92.154_9312_1705568349984
   ```

   如果有多个元数据最新的节点，建议优先选择一个 Follower 节点继续下述步骤。如果有多个 Follower 节点元数据最新，可以选择其中一个继续。

5. 根据上一步中选择的 FE 节点的角色，执行相应的操作。

  <Tabs groupId="recovery_role">

    <TabItem value="follower" label="从 Follower 节点恢复" default>

   如果元数据最新的节点为 Follower，则执行以下操作：

   1. 在 **fe.conf** 中添加以下配置：

      - StarRocks v2.5、v3.0、v3.1.9 及之前小版本和 v3.2.4 及之前小版本：

        ```Properties
        metadata_failure_recovery = true
        ```
    
      - StarRocks v3.1.10 及之后小版本、v3.2.5 及之后小版本和 v3.3 及之后版本：

        ```Properties
        bdbje_reset_election_group = true
        ```

   2. 启动该节点，并检查数据和元数据是否完整。
   3. 查看当前节点是否为 Leader 节点。

      ```SQL
      SHOW FRONTENDS;
      ```

      - 如果字段 `Alive` 为 `true`，说明该 FE 节点正常启动并加入集群。
      - 如果字段 `Role` 为 `LEADER`，说明该 FE 节点为 Leader FE 节点。

   4. 如果数据和元数据完整，且该节点的角色是 Leader，可以删除之前添加的配置并重新启动节点。

    </TabItem>

    <TabItem value="oberserver" label="从 Observer 节点恢复" >

   如果元数据最新的节点为 Observer，则执行以下操作：

   1. 在 **fe/meta/image/ROLE** 文件中将 FE 节点的角色从 `OBSERVER` 更改为 `FOLLOWER`。
   2. 在 **fe.conf** 中添加以下配置：

      - StarRocks v2.5、v3.0、v3.1.9 及之前小版本和 v3.2.4 及之前小版本：

        ```Properties
        metadata_failure_recovery = true
        ```

      - StarRocks v3.1.10 及之后小版本、v3.2.5 及之后小版本和 v3.3 及之后版本：

        ```Properties
        bdbje_reset_election_group = true
        ```

   3. 启动该节点，并检查数据和元数据是否完整。
   4. 查看当前节点是否为 Leader 节点。

      ```SQL
      SHOW FRONTENDS;
      ```

      - 如果字段 `Alive` 为 `true`，说明该 FE 节点正常启动并加入集群。
      - 如果字段 `Role` 为 `LEADER`，说明该 FE 节点为 Leader FE 节点。

   5. 如果数据和元数据完整，且该节点的角色是 Leader，可以删除之前添加的配置。**但不要重新启动该节点。**
   6. 删除除当前节点之外的所有 FE 节点。当前节点将作为临时 Leader 节点。

      ```SQL
      -- 如需删除 Follower 节点，请将 <follower_host> 替换为 Follower 节点的 IP 地址（priority_networks），
      -- 并将 <follower_edit_log_port>（默认值：9010）替换为 Follower 节点的 edit_log_port。
      ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

      -- 如需删除 Observer 节点，请将 <observer_host> 替换为 Observer 节点的 IP 地址（priority_networks），
      -- 并将 <observer_edit_log_port>（默认值：9010）替换为 Observer 节点的 edit_log_port。
      ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
      ```

   7. 向集群添加一个新的 Follower 节点（基于新的服务器）。

      ```SQL
      ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
      ```

   8. 使用临时 Leader FE 节点作为 Helper，在新服务器上启动新 FE 节点。

      ```Bash
      # 将 <leader_ip> 替换为 Leader FE 节点的 IP 地址（priority_networks），
      # 并将 <leader_edit_log_port>（默认：9010）替换为 Leader FE 节点的 edit_log_port。
      ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
      ```

   9. 新的 FE 节点成功启动后，检查两个 FE 节点的状态和角色：

      ```SQL
      SHOW FRONTENDS;
      ```

      - 如果字段 `Alive` 为 `true`，说明该 FE 节点正常启动并加入集群。
      - 如果字段 `Role` 为 `FOLLOWER`，说明该 FE 节点是 Follower FE 节点。
      - 如果字段 `Role` 为 `LEADER`，说明该 FE 节点为 Leader FE 节点。

   10. 如果新的 Follower 节点在集群中成功运行，就可以停止所有节点。
   11. **仅在新的 Follower 节点的 fe.conf 中添加以下配置：**

       - StarRocks v2.5、v3.0、v3.1.9 及之前小版本和 v3.2.4 及之前小版本：

         ```Properties
         metadata_failure_recovery = true
         ```

       - StarRocks v3.1.10 及之后小版本、v3.2.5 及之后小版本和 v3.3 及之后版本：

         ```Properties
         bdbje_reset_election_group = true
         ```

   12. 启动新的 Follower 节点，并检查数据和元数据是否完整。
   13. 查看当前节点是否为 Leader 节点。

       ```SQL
       SHOW FRONTENDS;
       ```

       - 如果字段 `Alive` 为 `true`，说明该 FE 节点正常启动并加入集群。
       - 如果字段 `Role` 为 `LEADER`，说明该 FE 节点为 Leader FE 节点。

   14. 如果数据和元数据完整，且该节点的角色是 Leader，可以删除之前添加的配置并重新启动节点。

    </TabItem>

  </Tabs>

6. 存活的 Follower 节点是集群当前实质上的 Leader 节点。删除除了当前节点外所有 FE 节点。

   ```SQL
   -- 如需删除 Follower 节点，请将 <follower_host> 替换为 Follower 节点的 IP 地址（priority_networks），
   -- 并将 <follower_edit_log_port>（默认值：9010）替换为 Follower 节点的 edit_log_port。
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

   -- 如需删除 Observer 节点，请将 <observer_host> 替换为 Observer 节点的 IP 地址（priority_networks），
   -- 并将 <observer_edit_log_port>（默认值：9010）替换为 Observer 节点的 edit_log_port。
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

7. 将需要重新添加回集群的 FE 节点的元数据目录 `meta_dir` 清除。
8. 使用新 Leader FE 节点作为 Helper 启动 Follower 节点。

   ```Bash
   # 将 <leader_ip> 替换为 Leader FE 节点的 IP 地址（priority_networks），
   # 并将 <leader_edit_log_port>（默认：9010）替换为 Leader FE 节点的 edit_log_port。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

9. 将 Follower 节点重新添加至集群。

   ```SQL
   ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
   ```

当所有节点都重新添加到集群后，元数据恢复成功。

## 元数据恢复相关配置

:::tip

元数据恢复完成后，必须删除以下配置。

:::

- [bdbje_reset_election_group](./management/FE_configuration.md#bdbje_reset_election_group)
- [metadata_enable_recovery_mode](./management/FE_configuration.md#metadata_enable_recovery_mode)
- [ignore_unknown_log_id](./management/FE_configuration.md#ignore_unknown_log_id)
