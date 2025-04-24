---
displayed_sidebar: docs
sidebar_label: Cluster Snapshot
keywords: ['backup', 'restore', 'shared data', 'snapshot']
---

<head><meta name="docsearch:pagerank" content="100"/></head>

import Beta from '../_assets/commonMarkdown/_beta.mdx'

# 自動クラスタースナップショット

<Beta />

このトピックでは、共有データクラスタにおける災害復旧のためのクラスタースナップショットの自動化について説明します。

この機能は v3.4.2 以降でサポートされており、共有データクラスタでのみ利用可能です。

## 概要

共有データクラスタの災害復旧の基本的な考え方は、クラスタの全状態（データとメタデータを含む）をオブジェクトストレージに保存することです。これにより、クラスタが障害に遭遇した場合でも、データとメタデータが無事であれば、オブジェクトストレージから復元できます。さらに、クラウドプロバイダーが提供するバックアップやクロスリージョンレプリケーションなどの機能を利用して、リモート復旧やクロスリージョン災害復旧を実現できます。

共有データクラスタでは、CN 状態（データ）はオブジェクトストレージに保存されますが、FE 状態（メタデータ）はローカルに残ります。オブジェクトストレージに復元のためのすべてのクラスタ状態を確保するために、StarRocks はデータとメタデータの両方をオブジェクトストレージに保存する自動クラスタースナップショットをサポートしています。

### 用語

- **クラスタースナップショット**

  クラスタースナップショットとは、ある瞬間のクラスタ状態のスナップショットを指します。カタログ、データベース、テーブル、ユーザーと権限、ロードタスクなど、クラスタ内のすべてのオブジェクトを含みます。ただし、外部カタログの設定ファイルやローカル UDF JAR パッケージなどのすべての外部依存オブジェクトは含まれません。

- **クラスタースナップショットの自動化**

  システムは最新のクラスタ状態に密接に従うスナップショットを自動的に維持します。最新のスナップショットが作成されると、古いスナップショットはすぐに削除され、常に1つのスナップショットのみが利用可能です。現在、クラスタースナップショットの自動化タスクはシステムによってのみトリガーされます。手動でスナップショットを作成することはサポートされていません。

- **クラスタ復元**

  スナップショットからクラスタを復元します。

## 使用法

### 自動クラスタースナップショットを有効にする

自動クラスタースナップショットはデフォルトで無効になっています。

次のステートメントを使用してこの機能を有効にします。

構文:

```SQL
ADMIN SET AUTOMATED CLUSTER SNAPSHOT ON
[STORAGE VOLUME <storage_volume_name>]
```

パラメータ:

`storage_volume_name`: スナップショットを保存するために使用されるストレージボリュームを指定します。このパラメータが指定されていない場合、デフォルトのストレージボリュームが使用されます。

FE がメタデータチェックポイントを完了した後に新しいメタデータイメージを作成するたびに、自動的にスナップショットが作成されます。スナップショットの名前は、`automated_cluster_snapshot_{timestamp}` という形式でシステムによって生成されます。

メタデータスナップショットは `/{storage_volume_locations}/{service_id}/meta/image/automated_cluster_snapshot_timestamp` に保存されます。データスナップショットは元のデータと同じ場所に保存されます。

FE の設定項目 `automated_cluster_snapshot_interval_seconds` はスナップショットの自動化サイクルを制御します。デフォルト値は 1800 秒（30 分）です。

### 自動クラスタースナップショットを無効にする

自動クラスタースナップショットを無効にするには、次のステートメントを使用します。

```SQL
ADMIN SET AUTOMATED CLUSTER SNAPSHOT OFF
```

自動クラスタースナップショットが無効になると、システムは自動的に古いスナップショットを削除します。

### クラスタースナップショットを表示する

最新のクラスタースナップショットとまだ削除されていないスナップショットを表示するには、ビュー `information_schema.cluster_snapshots` をクエリできます。

```SQL
SELECT * FROM information_schema.cluster_snapshots;
```

返り値:

| フィールド            | 説明                                                         |
| ------------------ | ------------------------------------------------------------ |
| snapshot_name      | スナップショットの名前。                                       |
| snapshot_type      | スナップショットのタイプ。現在は `automated` のみ利用可能です。 |
| created_time       | スナップショットが作成された時間。                             |
| fe_journal_id      | FE ジャーナルの ID。                                          |
| starmgr_journal_id | StarManager ジャーナルの ID。                                 |
| properties         | まだ利用できない機能に適用されます。                           |
| storage_volume     | スナップショットが保存されているストレージボリューム。         |
| storage_path       | スナップショットが保存されているストレージパス。               |

### クラスタースナップショットジョブを表示する

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

### クラスタを復元する

クラスタースナップショットを使用してクラスタを復元する手順は次のとおりです。

1. **(オプション)** クラスタースナップショットを保存しているストレージの場所（ストレージボリューム）が変更された場合、元のストレージパスのすべてのファイルを新しいパスにコピーする必要があります。これを実現するには、Leader FE ノードの `fe/conf` ディレクトリにある設定ファイル **cluster_snapshot.yaml** を変更する必要があります。**cluster_snapshot.yaml** のテンプレートについては、[付録](#appendix) を参照してください。

2. Leader FE ノードを起動します。

   ```Bash
   ./fe/bin/start_fe.sh --cluster_snapshot --daemon
   ```

3. **`meta` ディレクトリをクリーニングした後に**他の FE ノードを起動します。

   ```Bash
   ./fe/bin/start_fe.sh --helper <leader_ip>:<leader_edit_log_port> --daemon
   ```

4. **`storage_root_path` ディレクトリをクリーニングした後に**CN ノードを起動します。

   ```Bash
   ./be/bin/start_cn.sh --daemon
   ```

ステップ 1 で **cluster_snapshot.yaml** を変更した場合、ノードとストレージボリュームはファイル内の情報に基づいて新しいクラスタで再構成されます。

## 付録

**cluster_snapshot.yaml** のテンプレート:

```Yaml
# 復元のためにダウンロードされるクラスタースナップショットの情報。
cluster_snapshot:
    # スナップショットの URI。
    # 例 1: s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta/image/automated_cluster_snapshot_1704038400000
    # 例 2: s3://defaultbucket/test/f7265e80-631c-44d3-a8ac-cf7cdc7adec811019/meta
    cluster_snapshot_path: <cluster_snapshot_uri>
    # スナップショットを保存するストレージボリュームの名前。`storage_volumes` セクションで定義する必要があります。
    # 注意: 元のクラスタと同一でなければなりません。
    storage_volume_name: my_s3_volume

# [オプション] スナップショットが復元される新しいクラスタのノード情報。
# このセクションが指定されていない場合、復旧後の新しいクラスタには Leader FE ノードのみが含まれます。
# CN ノードは元のクラスタの情報を保持します。
# 注意: このセクションには Leader FE ノードを含めないでください。

frontends:
    # FE ホスト。
  - host: xxx.xx.xx.x1
    # FE edit_log_port。
    edit_log_port: 9010
    # FE ノードタイプ。有効な値: `follower` (デフォルト) および `observer`。
    type: follower
  - host: xxx.xx.xx.x2
    edit_log_port: 9010
    type: observer

compute_nodes:
    # CN ホスト。
  - host: xxx.xx.xx.x3
    # CN heartbeat_service_port。
    heartbeat_service_port: 9050
  - host: xxx.xx.xx.x4
    heartbeat_service_port: 9050

# 新しいクラスタのストレージボリュームの情報。クローンされたスナップショットを復元するために使用されます。
# 注意: ストレージボリュームの名前は元のクラスタと同一でなければなりません。
storage_volumes:
  # S3 互換ストレージボリュームの例。
  - name: my_s3_volume
    type: S3
    location: s3://defaultbucket/test/
    comment: my s3 volume
    properties:
      - key: aws.s3.region
        value: us-west-2
      - key: aws.s3.endpoint
        value: https://s3.us-west-2.amazonaws.com
      - key: aws.s3.access_key
        value: xxxxxxxxxx
      - key: aws.s3.secret_key
        value: yyyyyyyyyy
  # HDFS ストレージボリュームの例。
  - name: my_hdfs_volume
    type: HDFS
    location: hdfs://127.0.0.1:9000/sr/test/
    comment: my hdfs volume
    properties:
      - key: hadoop.security.authentication
        value: simple
      - key: username
        value: starrocks
```