---
displayed_sidebar: docs
---

# メタデータのリカバリー

import Tabs from '@theme/Tabs';

import TabItem from '@theme/TabItem';

このトピックでは、FE ノードがさまざまな例外に遭遇した場合に、StarRocks クラスター内のメタデータをリカバリーする方法について説明します。

一般的に、次のいずれかの問題が発生した場合にのみ、メタデータのリカバリーを行う必要があります。

- [FE が再起動に失敗する](#fe-fails-to-restart)
- [FE がサービスを提供できない](#fe-fails-to-provide-services)
- [メタデータバックアップを使用して新しい FE ノードでメタデータをリカバリーする](#recover-metadata-on-a-new-fe-node-with-metadata-backup)。

遭遇した問題を確認し、対応するセクションで提供されている解決策に従い、推奨されるアクションを実行してください。

## FE が再起動に失敗する

FE ノードは、メタデータが破損しているか、ロールバック後にクラスターと互換性がない場合に再起動に失敗することがあります。

### ロールバック後のメタデータの非互換性

StarRocks クラスターをダウングレードする際、メタデータがダウングレード前と互換性がない場合、FEs は再起動に失敗することがあります。

クラスターをダウングレードする際に次の例外が発生した場合、この問題を特定できます。

```Plain
UNKNOWN Operation Type xxx
```

次の手順に従ってメタデータをリカバリーし、FE を起動できます。

1. すべての FE ノードを停止します。
2. すべての FE ノードのメタデータディレクトリ `meta_dir` をバックアップします。
3. すべての FE ノードの設定ファイル **fe.conf** に設定 `metadata_ignore_unknown_operation_type = true` を追加します。
4. すべての FE ノードを起動し、データとメタデータが完全であるか確認します。
5. データとメタデータが完全である場合、次のステートメントを実行してメタデータのイメージファイルを作成します。

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 新しいイメージファイルがすべての FE ノードのディレクトリ **meta/image** に転送された後、すべての FE 設定ファイルから設定 `ignore_unknown_log_id = true` を削除し、FE ノードを再起動します。

### メタデータの破損

BDBJE メタデータと StarRocks メタデータの両方の破損は、再起動の失敗を引き起こします。

#### BDBJE メタデータの破損

##### VLSN バグ

次のエラーメッセージに基づいて VLSN バグを特定できます。

```Plain
recoveryTracker should overlap or follow on disk last VLSN of 6,684,650 recoveryFirst= 6,684,652 UNEXPECTED_STATE_FATAL: Unexpected internal state, unable to continue. Environment is invalid and must be closed.
```

次の手順に従ってこの問題を修正できます。

1. この例外を投げる FE ノードのメタデータディレクトリ `meta_dir` をクリアします。
2. Leader FE ノードをヘルパーとして使用して FE ノードを再起動します。

   ```Bash
   # <leader_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
   # <leader_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

:::tip

- このバグは StarRocks v3.1 で修正されています。この問題を回避するには、クラスターを v3.1 以降にアップグレードしてください。
- この解決策は、FE ノードの半数以上がこの問題に遭遇した場合には適用できません。[最後の手段](#7-measure-of-the-last-resort) に従って問題を修正する必要があります。

:::

##### RollbackException

次のエラーメッセージに基づいてこの問題を特定できます。

```Plain
must rollback 1 total commits(1 of which were durable) to the earliest point indicated by transaction id=-14752149 time=2022-01-12 14:36:28.21 vlsn=28,069,415 lsn=0x1174/0x16e durable=false in order to rejoin the replication group. All existing ReplicatedEnvironment handles must be closed and reinstantiated. Log files were truncated to file 0x4467, offset 0x269, vlsn 28,069,413 HARD_RECOVERY: Rolled back past transaction commit or abort. Must run recovery by re-opening Environment handles Environment is invalid and must be closed.
```

この問題は、Leader FE ノードが BDBJE メタデータを書き込むが、ハングする前に Follower FE ノードに同期できない場合に発生します。再起動後、元の Leader は Follower になり、メタデータが破損します。

この問題を解決するには、このノードを再起動して不正なメタデータを消去するだけです。

##### ReplicaWriteException

FE ログ **fe.log** からキーワード `removeReplicaDb` に基づいてこの問題を特定できます。

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

この問題は、失敗した FE ノードの BDBJE バージョン (v18.3.*) が Leader FE ノードのバージョン (v7.3.7) と一致しない場合に発生します。

次の手順に従ってこの問題を修正できます。

1. 失敗した Follower または Observer ノードを削除します。

   ```SQL
   -- Follower ノードを削除するには、<follower_host> を Follower ノードの IP アドレス (priority_networks) に置き換え、
   -- <follower_edit_log_port> (デフォルト: 9010) を Follower ノードの edit_log_port に置き換えます。
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";
   
   -- Observer ノードを削除するには、<observer_host> を Observer ノードの IP アドレス (priority_networks) に置き換え、
   -- <observer_edit_log_port> (デフォルト: 9010) を Observer ノードの edit_log_port に置き換えます。
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

2. 失敗したノードをクラスターに再追加します。

   ```SQL
   -- Follower ノードを追加します。
   ALTER SYSTEM ADD FOLLOWER "<follower_host>:<follower_edit_log_port>";
   
   -- Observer ノードを追加します。
   ALTER SYSTEM ADD OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

3. 失敗したノードのメタデータディレクトリ `meta_dir` をクリアします。
4. Leader FE ノードをヘルパーとして使用して失敗したノードを再起動します。

   ```Bash
   # <leader_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
   # <leader_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

5. 失敗したノードが正常な状態に回復した後、クラスター内の BDBJE パッケージを **starrocks-bdb-je-18.3.16.jar** にアップグレードする必要があります (または StarRocks クラスターを v3.0 以降にアップグレードします)。この際、まず Follower を、次に Leader をアップグレードします。

##### InsufficientLogException

次のエラーメッセージに基づいてこの問題を特定できます。

```Plain
xxx INSUFFICIENT_LOG: Log files at this node are obsolete. Environment is invalid and must be closed.
```

この問題は、Follower ノードが完全なメタデータ同期を必要とする場合に発生します。次のいずれかの状況が発生した場合に発生する可能性があります。

- Follower ノードのメタデータが Leader ノードのメタデータよりも遅れており、Leader ノードがすでにメタデータのチェックポイントを実行している場合。Follower ノードはメタデータの増分更新を実行できず、完全なメタデータ同期が必要です。
- 元の Leader ノードがメタデータを書き込み、チェックポイントを実行しますが、ハングする前に Follower FE ノードに同期できません。再起動後、Follower ノードになります。不正なメタデータがチェックポイントされているため、Follower ノードはメタデータの増分削除を実行できず、完全なメタデータ同期が必要です。

この例外は、新しい Follower ノードがクラスターに追加されたときにスローされます。この場合、何もアクションを取る必要はありません。この例外が既存の Follower ノードまたは Leader ノードに対してスローされた場合、ノードを再起動するだけで問題を解決できます。

##### HANDSHAKE_ERROR: 2 つのノード間のハンドシェイク中のエラー

次のエラーメッセージに基づいてこの問題を特定できます。

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

この問題は、元の Leader ノードがハングし、再びアクティブになるときに、存続している Follower ノードが新しい Leader ノードを選出しようとしている場合に発生します。Follower ノードは元の Leader ノードと新しい接続を確立しようとします。しかし、Leader ノードは古い接続がまだ存在するため、接続要求を拒否します。要求が拒否されると、Follower ノードは環境を無効として設定し、この例外をスローします。

この問題を解決するには、JVM ヒープサイズを増やすか、G1 GC アルゴリズムを使用することができます。

##### DatabaseNotFoundException

次のエラーメッセージに基づいてこの問題を特定できます。

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

この問題は、FE 設定ファイル **fe.conf** に `metadata_failure_recovery = true` を追加した場合に発生します。

この問題を解決するには、設定を削除してノードを再起動する必要があります。

#### StarRocks メタデータの破損

次のいずれかのエラーメッセージに基づいて StarRocks メタデータの破損問題を特定できます。

```Plain
failed to load journal type xxx
```

または

```Plain
catch exception when replaying
```

:::warning

以下の解決策に従ってメタデータをリカバリーする前に、StarRocks コミュニティの技術専門家に支援を求めることを強くお勧めします。この解決策は **データ損失** を引き起こす可能性があります。

:::

次の手順に従ってこの問題を修正できます。

##### エラージャーナル ID を無視する（推奨）

1. すべての FE ノードを停止します。
2. すべての FE ノードのメタデータディレクトリ `meta_dir` をバックアップします。
3. ログから誤ったジャーナル ID を見つけます。以下のログの `xxx` は、誤ったジャーナル ID を表します。

   ```Plain
   got interrupt exception or inconsistent exception when replay journal xxx, will exit
   ```

4. すべての **fe.conf** ファイルに以下の設定を追加し、FE ノードを起動します。

   ```Plain
   metadata_journal_skip_bad_journal_ids=xxx
   ```

5. それでも起動しない場合は、ステップ 3 で新しい失敗したジャーナル ID を特定し、**fe.conf** に追加してから、以前の設定を変更せずにノードを再起動します。

   ```Plain
   metadata_journal_skip_bad_journal_ids=xxx,yyy
   ```

6. 上記の手順を実行してもシステムが起動しない場合、または失敗したジャーナル ID が多すぎる場合は、リカバリモードに進みます。

##### リカバリモード

1. すべての FE ノードを停止します。
2. すべての FE ノードのメタデータディレクトリ `meta_dir` をバックアップします。
3. すべての FE ノードの設定ファイル **fe.conf** に設定 `metadata_enable_recovery_mode = true` を追加します。このモードではデータロードが禁止されています。
4. すべての FE ノードを起動し、クラスター内のテーブルをクエリしてデータが完全であるか確認します。

   テーブルをクエリしたときに次のエラーが返された場合、メタデータのリカバリーが完了するまで待機する必要があります。

   ```Plain
   ERROR 1064 (HY000): capture_consistent_versions error: version already been compacted.
   ```

   メタデータリカバリーの進行状況を確認するために、Leader FE ノードから次のステートメントを実行できます。

   ```SQL
   SHOW PROC '/meta_recovery';
   ```

   このステートメントは、リカバリーに失敗したパーティションを表示します。返されたアドバイスに従ってパーティションをリカバリーできます。何も返されない場合、リカバリーが成功したことを示します。

5. データとメタデータが完全である場合、次のステートメントを実行してメタデータのイメージファイルを作成します。

   ```SQL
   ALTER SYSTEM CREATE IMAGE;
   ```

6. 新しいイメージファイルがすべての FE ノードのディレクトリ **meta/image** に転送された後、すべての FE 設定ファイルから設定 `metadata_enable_recovery_mode = true` を削除し、FE ノードを再起動します。

## FE がサービスを提供できない

Follower FE ノードが Leader 選出を行えない場合、FE はサービスを提供しません。この問題が発生した場合、次のログ記録が繰り返されることがあります。

```Plain
wait globalStateMgr to be ready. FE type: INIT. is ready: false
```

さまざまな例外がこの問題を引き起こす可能性があります。問題を段階的にトラブルシューティングするために、以下のセクションに従うことを強くお勧めします。不適切な解決策を適用すると、問題が悪化し、データ損失を引き起こす可能性があります。

### 1. Follower ノードの過半数が稼働していない

Follower ノードの過半数が稼働していない場合、FE グループはサービスを提供しません。ここで「過半数」とは `1 + (Follower ノード数/2)` を示します。Leader FE ノード自体は Follower ですが、Observer ノードは Follower ではありません。

- 各 FE ノードの役割は **fe/meta/image/ROLE** ファイルから確認できます。

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

- BDBJE ログから Follower ノードの総数を確認できます。

  ```Bash
  grep "Current group size" fe/meta/bdb/je.info.0

  # この例の出力は、クラスターに 3 つの Follower ノードがあることを示しています。
  2024-01-24 08:21:44.754 UTC INFO [172.26.92.139_29917_1698226672727] Current group size: 3
  ```

この問題を解決するには、クラスター内のすべての Follower ノードを起動する必要があります。再起動できない場合は、[最後の手段](#10-最後の手段) を参照してください。

### 2. ノードの IP が変更された

ノードの `priority_networks` が設定されていない場合、FE ノードは再起動時に利用可能な IP アドレスをランダムに選択します。BDBJE メタデータに記録された IP アドレスがノードの起動に使用されたものと異なる場合、FE はサービスを提供しません。

- BDBJE メタデータに記録された IP アドレスは **fe/meta/image/ROLE** ファイルから確認できます。

  ```Bash
  cat fe/meta/image/ROLE

  #Fri Jan 19 20:03:14 CST 2024
  role=FOLLOWER
  hostType=IP
  name=172.26.92.154_9312_1705568349984
  ```

  最初のアンダースコアの前の値 `172.26.92.154` が BDBJE メタデータに記録された IP アドレスです。

- ノードの起動に使用された IP アドレスは FE ログから確認できます。

  ```Bash
  grep "IP:" fe/log/fe.log

  2024-02-06 14:33:58,211 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  2024-02-06 14:34:27,689 INFO (main|1) [FrontendOptions.initAddrUseIp():249] Use IP init local addr, IP: /172.17.0.1
  ```

この問題を解決するには、FE 設定ファイル **fe.conf** のノードの `priority_networks` を **fe/meta/image/ROLE** に記録された IP アドレスに設定し、ノードを再起動する必要があります。

### 3. ノード間のシステムクロックが同期していない

次のエラーメッセージに基づいてこの問題を特定できます。**fe.out**、**fe.log** または **fe/meta//bdb/je.info.0** から確認できます。

```Plain
com.sleepycat.je.EnvironmentFailureException: (JE 7.3.7) Environment must be closed, caused by: com.sleepycat.je.EnvironmentFailureException: Environment invalid because of previous exception: (JE 7.3.7) 172.26.92.139_29917_1631006307557(2180):xxx Clock delta: 11020 ms. between Feeder: 172.26.92.154_29917_1641969377236 and this Replica exceeds max permissible delta: 5000 ms. HANDSHAKE_ERROR: Error during the handshake between two nodes. Some validity or compatibility check failed, preventing further communication between the nodes. Environment is invalid and must be closed. fetchRoot of 0x1278/0x1fcbb8 state=0 expires=never
```

すべてのノード間でシステムクロックを同期する必要があります。

### 4. 利用可能なディスクスペースが不足している

StarRocks を v3.0 以降にアップグレードするか、BDBJE を v18 以降にアップグレードした後、`meta_dir` を格納するディスクの空き容量が 5 GB 未満の場合、ノードが再起動に失敗することがあります。

BDBJE バージョンは **.jar** パッケージから確認できます。ディレクトリ **fe/lib** にあります。

この問題を解決するには、ディスクを拡張するか、FE メタデータ用により大容量の専用ディスクを割り当てることができます。

### 5. `edit_log_port` が変更された

BDBJE メタデータに記録された `edit_log_port` が **fe.conf** に設定されたものと異なる場合、FE はサービスを提供しません。

BDBJE メタデータに記録された `edit_log_port` は **fe/meta/image/ROLE** ファイルから確認できます。

```Bash
cat fe/meta/image/ROLE

#Fri Jan 19 20:03:14 CST 2024
role=FOLLOWER
hostType=IP
name=172.26.92.154_9312_1705568349984
```

2 番目のアンダースコアの前の値 `9312` が BDBJE メタデータに記録された `edit_log_port` です。

この問題を解決するには、FE 設定ファイル **fe.conf** のノードの `edit_log_port` を **fe/meta/image/ROLE** に記録された `edit_log_port` に設定し、ノードを再起動する必要があります。

### 6. JVM ヒープサイズが不足している

`jstat` コマンドを使用して JVM メモリ使用量を確認できます。

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

フィールド `O` に表示されるパーセンテージが高いままである場合、JVM ヒープサイズが不足していることを示しています。

この問題を解決するには、JVM ヒープサイズを増やす必要があります。

### 7. Latch timeout. com.sleepycat.je.log.LogbufferPool_FullLatch

次のエラーメッセージに基づいてこの問題を特定できます。

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

この問題は、FE ノードのローカルディスクに過剰な負荷がかかっている場合に発生します。

この問題を解決するには、FE メタデータ用に専用のディスクを割り当てるか、高性能なディスクに交換することができます。

### 8. InsufficientReplicasException

次のエラーメッセージに基づいてこの問題を特定できます。

```Plain
com.sleepycat.je.rep.InsufficientReplicasException: (JE 7.3.7) Commit policy: SIMPLE_MAJORITY required 1 replica. But none were active with this master.
```

この問題は、Leader ノードまたは Follower ノードが過剰なメモリリソースを使用し、Full GC が発生する場合に発生します。

この問題を解決するには、JVM ヒープサイズを増やすか、G1 GC アルゴリズムを使用することができます。

### 9. UnknownMasterException

次のエラーメッセージに基づいてこの問題を特定できます。

```Plain
com.sleepycat.je.rep.UnknownMasterException: (JE 18.3.16) Could not determine master from helpers at:[/xxx.xxx.xxx.xxx:9010, /xxx.xxx.xxx.xxx:9010]
        at com.sleepycat.je.rep.elections.Learner.findMaster(Learner.java:443) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.util.ReplicationGroupAdmin.getMasterSocket(ReplicationGroupAdmin.java:186) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.util.ReplicationGroupAdmin.doMessageExchange(ReplicationGroupAdmin.java:607) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.sleepycat.je.rep.util.ReplicationGroupAdmin.getGroup(ReplicationGroupAdmin.java:406) ~[starrocks-bdb-je-18.3.16.jar:?]
        at com.starrocks.ha.BDBHA.getElectableNodes(BDBHA.java:178) ~[starrocks-fe.jar:?]
        at com.starrocks.common.proc.FrontendsProcNode.getFrontendsInfo(FrontendsProcNode.java:96) ~[starrocks-fe.jar:?]
        at com.starrocks.common.proc.FrontendsProcNode.fetchResult(FrontendsProcNode.java:80) ~[starrocks-fe.jar:?]
        at com.starrocks.sql.ast.ShowProcStmt.getMetaData(ShowProcStmt.java:74) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.ShowExecutor.handleShowProc(ShowExecutor.java:872) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.ShowExecutor.execute(ShowExecutor.java:286) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.StmtExecutor.handleShow(StmtExecutor.java:1574) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.StmtExecutor.execute(StmtExecutor.java:688) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.ConnectProcessor.handleQuery(ConnectProcessor.java:336) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.ConnectProcessor.dispatch(ConnectProcessor.java:530) ~[starrocks-fe.jar:?]
        at com.starrocks.qe.ConnectProcessor.processOnce(ConnectProcessor.java:838) ~[starrocks-fe.jar:?]
        at com.starrocks.mysql.nio.ReadListener.lambda$handleEvent$0(ReadListener.java:69) ~[starrocks-fe.jar:?]
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
        at java.lang.Thread.run(Thread.java:829) ~[?:?]
```

`SHOW FRONTENDS` を実行する際、リーダー FE ノードが見つからない場合、いくつかの理由が考えられる：

- FE ノードの半分以上がフル GC 中であり、その時間が著しく長い場合。
- または、ログに `java.lang.OutOfMemoryError: Java heap space` というキーワードが含まれている場合。

これはメモリが不足しているためである。JVM のメモリ割り当てを増やす必要がある。

### 10. 最後の手段

:::warning

以下の解決策は、前述の解決策がすべて機能しない場合にのみ最後の手段として試すことができます。

:::

この解決策は、次のような極端なケースのみに設計されています。

- Follower ノードの過半数が再起動できない。
- Follower ノードが BDBJE のバグにより Leader 選出を行えない。
- 前述のセクションで言及されていない例外。

次の手順に従ってメタデータをリカバリーします。

1. すべての FE ノードを停止します。
2. すべての FE ノードのメタデータディレクトリ `meta_dir` をバックアップします。
3. **FE ノードをホストするすべてのサーバー** で次のコマンドを実行して、最新のメタデータを持つノードを特定します。

   ```Bash
   # ノードが使用する正確な .jar パッケージをコマンドで指定する必要があります。
   # パッケージは StarRocks バージョンによって異なります。
   java -jar fe/lib/starrocks-bdb-je-18.3.16.jar DbPrintLog -h meta/bdb/ -vd
   ```

   例の出力:

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

   最大の `lastVLSN` 値を持つノードが最新のメタデータを持っています。

4. 最新のメタデータを持つ FE ノードの役割 (Follower または Observer) を **fe/meta/image/ROLE** ファイルから確認します。

   ```Bash
   cat fe/meta/image/ROLE
   
   #Fri Jan 19 20:03:14 CST 2024
   role=FOLLOWER
   hostType=IP
   name=172.26.92.154_9312_1705568349984
   ```

   複数のノードが最新のメタデータを持っている場合、Follower ノードで進めることをお勧めします。複数の Follower ノードが最新のメタデータを持っている場合、どのノードでも進めることができます。

5. 前のステップで選択した FE ノードの役割に基づいて対応する操作を実行します。

  <Tabs groupId="recovery_role">

    <TabItem value="follower" label="Follower ノードで進める" default>

   Follower ノードが最新のメタデータを持っている場合、次の操作を実行します。

   1. **fe.conf** に次の設定を追加します。

      - StarRocks v2.5、v3.0、v3.1.9 以前のパッチバージョン、および v3.2.4 以前のパッチバージョンの場合:

        ```Properties
        metadata_failure_recovery = true
        ```

      - StarRocks v3.1.10 以降のパッチバージョン、v3.2.5 以降のパッチバージョン、および v3.3 以降の場合:

        ```Properties
        bdbje_reset_election_group = true
        ```

   2. ノードを再起動し、データとメタデータが完全であるか確認します。
   3. 現在の FE ノードが Leader FE ノードであるか確認します。

      ```SQL
      SHOW FRONTENDS;
      ```

      - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動され、クラスターに追加されています。
      - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

   4. データとメタデータが完全であり、ノードの役割が Leader である場合、以前に追加した設定を削除し、ノードを再起動できます。

    </TabItem>

    <TabItem value="observer" label="Observer ノードで進める" >

   Observer ノードが最新のメタデータを持っている場合、次の操作を実行します。

   1. **fe/meta/image/ROLE** ファイルで FE ノードの役割を `OBSERVER` から `FOLLOWER` に変更します。
   2. **fe.conf** に次の設定を追加します。

      - StarRocks v2.5、v3.0、v3.1.9 以前のパッチバージョン、および v3.2.4 以前のパッチバージョンの場合:

        ```Properties
        metadata_failure_recovery = true
        ```

      - StarRocks v3.1.10 以降のパッチバージョン、v3.2.5 以降のパッチバージョン、および v3.3 以降の場合:

        ```Properties
        bdbje_reset_election_group = true
        ```

   3. ノードを再起動し、データとメタデータが完全であるか確認します。
   4. 現在の FE ノードが Leader FE ノードであるか確認します。

      ```SQL
      SHOW FRONTENDS;
      ```

      - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動され、クラスターに追加されています。
      - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

   5. データとメタデータが完全であり、ノードの役割が Leader である場合、以前に追加した設定を削除します。ただし、**ノードを再起動しないでください**。
   6. 現在のノードを除くすべての FE ノードを削除します。これで一時的な Leader ノードとして機能します。

      ```SQL
      -- Follower ノードを削除するには、<follower_host> を Follower ノードの IP アドレス (priority_networks) に置き換え、
      -- <follower_edit_log_port> (デフォルト: 9010) を Follower ノードの edit_log_port に置き換えます。
      ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

      -- Observer ノードを削除するには、<observer_host> を Observer ノードの IP アドレス (priority_networks) に置き換え、
      -- <observer_edit_log_port> (デフォルト: 9010) を Observer ノードの edit_log_port に置き換えます。
      ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
      ```

   7. クラスターに新しい Follower ノード (新しいサーバー上) を追加します。

      ```SQL
      ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
      ```

   8. 一時的な Leader FE ノードをヘルパーとして使用して、新しいサーバー上で新しい FE ノードを起動します。

      ```Bash
      # <leader_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
      # <leader_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
      ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
      ```

   9. 新しい FE ノードが正常に起動したら、両方の FE ノードの状態と役割を確認します。

      ```SQL
      SHOW FRONTENDS;
      ```

      - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動され、クラスターに追加されています。
      - フィールド `Role` が `FOLLOWER` の場合、この FE ノードは Follower FE ノードです。
      - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

   10. 新しい Follower がクラスター内で正常に動作している場合、すべてのノードを停止できます。
   11. **新しい Follower のみの fe.conf** に次の設定を追加します。

       - StarRocks v2.5、v3.0、v3.1.9 以前のパッチバージョン、および v3.2.4 以前のパッチバージョンの場合:

         ```Properties
         metadata_failure_recovery = true
         ```

       - StarRocks v3.1.10 以降のパッチバージョン、v3.2.5 以降のパッチバージョン、および v3.3 以降の場合:

         ```Properties
         bdbje_reset_election_group = true
         ```

   12. 新しい Follower ノードを再起動し、データとメタデータが完全であるか確認します。
   13. 現在の FE ノードが Leader FE ノードであるか確認します。

       ```SQL
       SHOW FRONTENDS;
       ```

       - フィールド `Alive` が `true` の場合、この FE ノードは正常に起動され、クラスターに追加されています。
       - フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。

   14. データとメタデータが完全であり、ノードの役割が Leader である場合、以前に追加した設定を削除し、ノードを再起動できます。

    </TabItem>

  </Tabs>

6. 生き残った Follower ノードは、実質的にクラスターの Leader ノードです。現在のノードを除くすべての FE ノードを削除します。

   ```SQL
   -- Follower ノードを削除するには、<follower_host> を Follower ノードの IP アドレス (priority_networks) に置き換え、
   -- <follower_edit_log_port> (デフォルト: 9010) を Follower ノードの edit_log_port に置き換えます。
   ALTER SYSTEM DROP FOLLOWER "<follower_host>:<follower_edit_log_port>";

   -- Observer ノードを削除するには、<observer_host> を Observer ノードの IP アドレス (priority_networks) に置き換え、
   -- <observer_edit_log_port> (デフォルト: 9010) を Observer ノードの edit_log_port に置き換えます。
   ALTER SYSTEM DROP OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

7. クラスターに再追加する FE ノードのメタデータディレクトリ `meta_dir` をクリアします。
8. 新しい Leader FE ノードをヘルパーとして使用して、新しい Follower ノードを起動します。

   ```Bash
   # <leader_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
   # <leader_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

9. Follower ノードをクラスターに再追加します。

   ```SQL
   ALTER SYSTEM ADD FOLLOWER "<new_follower_host>:<new_follower_edit_log_port>";
   ```

すべてのノードがクラスターに再追加された後、メタデータは正常にリカバリーされます。

## メタデータバックアップを使用して新しい FE ノードでメタデータをリカバリーする

メタデータバックアップを使用して新しい FE ノードを起動したい場合、次の手順に従ってください。

1. バックアップされたメタデータディレクトリ `meta_dir` を新しい FE ノードにコピーします。
2. FE ノードの設定ファイルで `bdbje_reset_election_group` を `true` に設定します。

   ```Properties
   bdbje_reset_election_group = true
   ```

3. FE ノードを起動します。

   ```Bash
   ./fe/bin/start_fe.sh
   ```

4. 現在の FE ノードが Leader FE ノードであるか確認します。

   ```SQL
   SHOW FRONTENDS;
   ```

   フィールド `Role` が `LEADER` の場合、この FE ノードは Leader FE ノードです。IP アドレスが現在の FE ノードのものであることを確認してください。

5. データとメタデータが完全であり、ノードの役割が Leader である場合、設定 `bdbje_reset_election_group` を削除し、ノードを再起動する必要があります。
6. これで、メタデータバックアップを使用して新しい Leader FE ノードを正常に起動しました。新しい Leader FE ノードをヘルパーとして使用して、新しい Follower ノードを追加できます。

   ```Bash
   # <leader_ip> を Leader FE ノードの IP アドレス (priority_networks) に置き換え、
   # <leader_edit_log_port> (デフォルト: 9010) を Leader FE ノードの edit_log_port に置き換えます。
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

## メタデータリカバリー関連の設定

:::tip

メタデータのリカバリーが完了したら、次の設定を削除する必要があります。

:::

- [bdbje_reset_election_group](./management/FE_configuration.md#bdbje_reset_election_group)
- [metadata_enable_recovery_mode](./management/FE_configuration.md#metadata_enable_recovery_mode)
- [ignore_unknown_log_id](./management/FE_configuration.md#ignore_unknown_log_id)
