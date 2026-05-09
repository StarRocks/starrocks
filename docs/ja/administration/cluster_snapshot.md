---
displayed_sidebar: docs
sidebar_label: クラスタースナップショット
keywords: ['バックアップ', '復元', '共有データ', 'スナップショット']
---
<head><meta name="docsearch:pagerank" content="100"/></head>

import Beta from '../_assets/commonMarkdown/_beta.mdx'
import ClusterSnapshotTerm from '../_assets/commonMarkdown/cluster_snapshot_term.mdx'
import ClusterSnapshotTermCRDR from '../_assets/commonMarkdown/cluster_snapshot_term_crdr.mdx'
import ClusterSnapshotSyntaxParam from '../_assets/commonMarkdown/cluster_snapshot_syntax_param.mdx'
import ClusterSnapshotPurge from '../_assets/commonMarkdown/cluster_snapshot_purge.mdx'
import ManualCreateDropClusterSnapshot from '../_assets/commonMarkdown/manual_cluster_snapshot.mdx'
import ClusterSnapshotWarning from '../_assets/commonMarkdown/cluster_snapshot_warning.mdx'
import ClusterSnapshotCrossRegionRecover from '../_assets/commonMarkdown/cluster_snapshot_cross_region_recover.mdx'
import ClusterSnapshotAfterRecover from '../_assets/commonMarkdown/cluster_snapshot_after_recover.mdx'
import ClusterSnapshotAppendix from '../_assets/commonMarkdown/cluster_snapshot_appendix.mdx'

# クラスタースナップショット

<Beta />

このトピックでは、共有データクラスタでの災害復旧にクラスタースナップショットを使用する方法について説明します。

この機能は v3.4.2 以降でサポートされており、共有データクラスタでのみ利用可能です。

## 概要

共有データクラスタの災害復旧の基本的な考え方は、クラスタの全状態（データとメタデータを含む）をオブジェクトストレージに保存することです。これにより、クラスタが障害に遭遇した場合でも、データとメタデータが無事であれば、オブジェクトストレージから復元できます。さらに、クラウドプロバイダーが提供するバックアップやクロスリージョンレプリケーションなどの機能を利用して、リモート復旧やクロスリージョン災害復旧を実現できます。

共有データクラスタでは、CN 状態（データ）はオブジェクトストレージに保存されますが、FE 状態（メタデータ）はローカルに残ります。オブジェクトストレージに復元のためのすべてのクラスタ状態を確保するために、StarRocks はデータとメタデータの両方をオブジェクトストレージに保存するクラスタースナップショットをサポートしています。

### ワークフロー

![Workflow](../_assets/cluster_snapshot_workflow.png)

### 用語

- **クラスタースナップショット**

  クラスタースナップショットとは、ある瞬間のクラスタ状態のスナップショットを指します。カタログ、データベース、テーブル、ユーザーと権限、ロードタスクなど、クラスタ内のすべてのオブジェクトを含みます。ただし、外部カタログの設定ファイルやローカル UDF JAR パッケージなどのすべての外部依存オブジェクトは含まれません。

- **クラスタースナップショットの生成**

  システムは最新のクラスタ状態に密接に従うスナップショットを自動的に維持します。最新のスナップショットが作成されると、古いスナップショットはすぐに削除され、常に1つのスナップショットのみが利用可能です。

  <ClusterSnapshotTerm />

- **クラスタ復元**

  スナップショットからクラスタを復元します。

<ClusterSnapshotTermCRDR />

## 自動クラスタースナップショット

自動クラスタースナップショットはデフォルトで無効になっています。

次のステートメントを使用してこの機能を有効にします。

<ClusterSnapshotSyntaxParam />

FE がメタデータチェックポイントを完了した後に新しいメタデータイメージを作成するたびに、自動的にスナップショットが作成されます。スナップショットの名前は、`automated_cluster_snapshot_{timestamp}` という形式でシステムによって生成されます。

メタデータスナップショットは `/{storage_volume_locations}/{service_id}/meta/image/automated_cluster_snapshot_timestamp` に保存されます。データスナップショットは元のデータと同じ場所に保存されます。

FE の設定項目 `automated_cluster_snapshot_interval_seconds` はスナップショットの自動化サイクルを制御します。デフォルト値は 600 秒（10 分）です。

### 自動クラスタースナップショットを無効にする

自動クラスタースナップショットを無効にするには、次のステートメントを使用します。

```SQL
ADMIN SET AUTOMATED CLUSTER SNAPSHOT OFF
```

<ClusterSnapshotPurge />

<ManualCreateDropClusterSnapshot />

## クラスタースナップショットを表示する

最新のクラスタースナップショットとまだ削除されていないスナップショットを表示するには、ビュー `information_schema.cluster_snapshots` をクエリできます。

```SQL
SELECT * FROM information_schema.cluster_snapshots;
```

返り値:

| フィールド            | 説明                                                         |
| ------------------ | ------------------------------------------------------------ |
| snapshot_name      | スナップショットの名前。                                       |
| snapshot_type      | スナップショットのタイプ。有効な値：`automated` および `manual`。 |
| created_time       | スナップショットが作成された時間。                             |
| fe_journal_id      | FE ジャーナルの ID。                                          |
| starmgr_journal_id | StarManager ジャーナルの ID。                                 |
| properties         | まだ利用できない機能に適用されます。                           |
| storage_volume     | スナップショットが保存されているストレージボリューム。         |
| storage_path       | スナップショットが保存されているストレージパス。               |

## クラスタースナップショットジョブを表示する

クラスタースナップショットのジョブ情報を表示するには、ビュー `information_schema.cluster_snapshot_jobs` をクエリできます。

```SQL
SELECT * FROM information_schema.cluster_snapshot_jobs;
```

返り値:

| フィールド            | 説明                                                         |
| ------------------ | ------------------------------------------------------------ |
| snapshot_name      | スナップショットの名前。                                       |
| job_id             | ジョブの ID。                                                 |
| created_time       | ジョブが作成された時間。                                       |
| finished_time      | ジョブが終了した時間。                                         |
| state              | ジョブの状態。 有効な値: `INITIALIZING`, `SNAPSHOTING`, `FINISHED`, `EXPIRED`, `DELETED`, `ERROR`。 |
| detail_info        | 現在の実行ステージの具体的な進捗情報。                         |
| error_message      | ジョブのエラーメッセージ（ある場合）。                         |

## クラスタを復元する

<ClusterSnapshotWarning />

クラスタースナップショットを使用してクラスタを復元する手順は次のとおりです。

<ClusterSnapshotCrossRegionRecover />

2. Leader FE ノードを起動します。

   ```Bash
   ./fe/bin/start_fe.sh --cluster_snapshot --daemon
   ```

3. **`meta` ディレクトリをクリーニングした後に**他の FE ノードを起動します。

   ```Bash
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

   ノードをクラスターに追加します。

   ```SQL
   -- Followerノードを追加します：
   ALTER SYSTEM ADD FOLLOWER "<follower_host>:<follower_edit_log_port>";
   
   -- Observer ノードを追加します：
   ALTER SYSTEM ADD OBSERVER "<observer_host>:<observer_edit_log_port>";
   ```

4. **`storage_root_path` ディレクトリをクリーニングした後に**CN ノードを起動します。

   ```Bash
   ./be/bin/start_cn.sh --daemon
   ```

   ノードをクラスターに追加します。

   ```SQL
   ALTER SYSTEM ADD COMPUTE NODE "<cn_host>:<cn_heartbeat_service_port>";
   ```

ステップ 1 で **cluster_snapshot.yaml** を変更した場合、ノードとストレージボリュームはファイル内の情報に基づいて新しいクラスタで再構成されます。

<ClusterSnapshotAfterRecover />

<ClusterSnapshotAppendix />

## 制限事項

- 現在、スタンバイモードはサポートされていません。プライマリクラスタとセカンダリクラスタは同時にオンラインにできません。そうしないと、セカンダリクラスタの正常な動作が保証されません。
- 現在、自動クラスタスナップショットは 1 つしか保持できません。
