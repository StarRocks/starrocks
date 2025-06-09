---
displayed_sidebar: docs
sidebar_label: Feature Support
---

# Feature Support: Shared-data Clusters

:::tip
以下の各機能には追加されたバージョン番号が記載されています。新しいクラスタをデプロイする場合は、バージョン 3.2 以上の最新のパッチリリースをデプロイしてください。
:::

## Overview

共有データ StarRocks クラスタは、ストレージとコンピュートを分離したアーキテクチャを特徴としています。これにより、データをリモートストレージに保存でき、共有なしクラスタと比較して、ストレージコストの削減、リソース分離の最適化、サービスの弾力性の向上が実現します。

このドキュメントでは、共有データクラスタの機能サポートについて、デプロイメント方法、ストレージ構成、キャッシングメカニズム、Compaction、主キーテーブルの機能、およびパフォーマンステストの結果を説明します。

## Deployment

共有データクラスタは、物理/仮想マシンおよび Kubernetes 上で Operator を使用してデプロイをサポートしています。

両方のデプロイメントソリューションには以下の制限があります：

- 共有なしモードと共有データモードの混在デプロイはサポートされていません。
- 共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。
- 異種デプロイメントはサポートされていません。つまり、クラスタ内のすべての CN ノードのハードウェア仕様は同じでなければなりません。

### StarRocks Kubernetes Operator

StarRocks は、Kubernetes 上での共有データデプロイメントのために [StarRocks Kubernetes Operator](https://github.com/StarRocks/starrocks-kubernetes-operator/releases) を提供しています。

共有データクラスタは以下の方法でスケールできます：

- 手動操作。
- Kubernetes HPA (Horizontal Pod Autoscaler) 戦略を使用した自動スケーリング。

## Storage

共有データクラスタは、HDFS およびオブジェクトストレージ上でのストレージボリュームの構築をサポートしています。

### HDFS

#### Location

StarRocks は、HDFS ストレージボリュームの以下の場所をサポートしています：

- HDFS: `hdfs://<host>:<port>/`

  > **NOTE**
  >
  > v3.2 以降、ストレージボリュームは NameNode HA モードが有効な HDFS クラスタをサポートします。

- WebHDFS (v3.2 からサポート): `webhdfs://<host>:<http_port>/`

- ViewFS (v3.2 からサポート): `viewfs://<ViewFS_cluster>/`

#### Authentication

StarRocks は、HDFS ストレージボリュームの以下の認証方法をサポートしています：

- Basic

- Username (v3.2 からサポート)

- Kerberos Ticket Cache (v3.2 からサポート)

  > **NOTE**
  >
  > StarRocks は自動チケットリフレッシュをサポートしていません。チケットをリフレッシュするために crontab タスクを設定する必要があります。

Kerberos Keytab と Principal ID を使用した認証はまだサポートされていません。

#### Usage notes

StarRocks は、HDFS およびオブジェクトストレージ上のストレージボリュームをサポートしています。ただし、各 StarRocks インスタンスには 1 つの HDFS ストレージボリュームのみが許可されます。複数の HDFS ストレージボリュームを作成すると、StarRocks の未知の動作を引き起こす可能性があります。

### Object storage

#### Location

StarRocks は、ストレージボリュームのために以下のオブジェクトストレージサービスをサポートしています：

- S3 互換オブジェクトストレージサービス: `s3://<s3_path>`
  - AWS S3
  - GCS, OSS, OBS, COS, TOS, KS3, MinIO, および Ceph S3
- Azure Blob Storage (v3.1.1 からサポート): `azblob://<azblob_path>`

#### Authentication

StarRocks は、異なるオブジェクトストレージサービスのために以下の認証方法をサポートしています：

- AWS S3
  - AWS SDK
  - IAM ユーザーベースの Credential
  - インスタンスプロファイル
  - Assumed Role
- GCS, OSS, OBS, COS, TOS, KS3, MinIO, および Ceph S3
  - アクセスキーペア
- Azure Blob Storage
  - 共有キー
  - 共有アクセス署名 (SAS)

#### Partitioned Prefix

v3.2.4 から、StarRocks は S3 互換オブジェクトストレージシステムのために Partitioned Prefix 機能を使用してストレージボリュームを作成することをサポートしています。この機能が有効になると、StarRocks はデータをバケット内の複数のパーティション（サブパス）に分散します。これにより、バケットに保存されたデータファイルの読み書きパフォーマンスを容易に向上させることができます。

### Storage volumes

- v3.1.0 以降、ストレージボリュームは CREATE STORAGE VOLUME ステートメントを使用して作成でき、後のバージョンではこの方法が推奨されます。
- 共有データクラスタの内部カタログ `default_catalog` は、データの永続化にデフォルトのストレージボリュームを使用します。`default_catalog` 内のデータベースおよびテーブルに対して異なるストレージボリュームを割り当てることができますが、`storage_volume` プロパティを設定しない場合、`storage_volume` プロパティはカタログ、データベース、テーブルの順に継承されます。
- 現在、ストレージボリュームはクラウドネイティブテーブルのデータ保存にのみ使用できます。将来的には、外部ストレージ管理、データロード、およびバックアップ機能がサポートされる予定です。

## Cache

### Cache types

#### File Cache

File Cache は、共有データクラスタと共に導入された初期のキャッシングメカニズムです。セグメントファイルレベルでキャッシュをロードします。File Cache は v3.1.7、v3.2.3、およびそれ以降のバージョンでは推奨されていません。

#### Data Cache

Data Cache は v3.1.7 および v3.2.3 以降でサポートされ、以前のバージョンの File Cache を置き換えます。Data Cache は、リモートストレージからデータをオンデマンドでブロック単位（MB 単位）でロードし、ファイル全体をロードする必要がありません。後のバージョンで推奨され、v3.2.3 以降でデフォルトで有効になっています。

#### Data Cache Warmup

StarRocks v3.3.0 は、データレイクおよび共有データクラスタでのクエリを加速するために Data Cache Warmup 機能を導入しました。Data Cache Warmup は、キャッシュを事前にポピュレートするアクティブなプロセスです。CACHE SELECT を実行することで、リモートストレージから必要なデータを事前に取得できます。

### Configurations

- テーブルプロパティ：
  - `datacache.enable`: ローカルディスクキャッシュを有効にするかどうか。デフォルト: `true`。
  - `datacache.partition_duration`: キャッシュされたデータの有効期間。
- BE 設定：
  - `starlet_use_star_cache`: Data Cache を有効にするかどうか。
  - `starlet_star_cache_disk_size_percent`: 共有データクラスタで Data Cache が使用できるディスク容量の割合。

### Capabilities

- データロードはローカルキャッシュを生成し、その削除は `partition_duration` ではなくキャッシュ容量制御メカニズムによってのみ管理されます。
- StarRocks は Data Cache Warmup のための定期的なタスクの設定をサポートしています。

### Limitations

- StarRocks はキャッシュされたデータの複数のレプリカをサポートしていません。

## Compaction

### Observability

#### Partition-level Compaction status

v3.1.9 以降、`information_schema.partitions_meta` をクエリすることでパーティションの Compaction ステータスを確認できます。

以下の主要なメトリクスを監視することをお勧めします：

- **AvgCS**: パーティション内のすべてのタブレットの平均 Compaction スコア。
- **MaxCS**: パーティション内のすべてのタブレットの最大 Compaction スコア。

#### Compaction task status

v3.2.0 以降、`information_schema.be_cloud_native_compactions` をクエリすることで Compaction タスクのステータスと進捗を確認できます。

以下の主要なメトリクスを監視することをお勧めします：

- **PROGRESS**: タブレットの現在の Compaction 進捗（パーセンテージ）。
- **STATUS**: Compaction タスクのステータス。エラーが発生した場合、このフィールドに詳細なエラーメッセージが返されます。

### Cancelling Compaction tasks

特定の Compaction タスクを CANCEL COMPACTION ステートメントを使用してキャンセルできます。

例：

```SQL
CANCEL COMPACTION WHERE TXN_ID = 123;
```

> **NOTE**
>
> CANCEL COMPACTION ステートメントは Leader FE ノードで実行する必要があります。

### Manual Compaction

v3.1 から、StarRocks は手動 Compaction のための SQL ステートメントを提供しています。テーブルまたはパーティションを指定して Compaction を行うことができます。詳細は [Manual Compaction](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md#manual-compaction) を参照してください。

## Primary Key tables

以下の表は、共有データクラスタにおける主キーテーブルの主要な機能とそのサポート状況を示しています：

| **Feature**                   | **Supported Version(s)** | **Description**                                              |
| ----------------------------- | ------------------------ | ------------------------------------------------------------ |
| Primary Key tables            | v3.1.0                   |                                                              |
| Primary Key index persistence | v3.2.0<br />v3.1.3       | <ul><li>現在、共有データクラスタはローカルディスク上での主キーインデックスの永続化をサポートしています。</li><li>リモートストレージでの永続化は将来のリリースでサポートされる予定です。</li></ul> |
| Partial Update                | v3.1.0                   | 共有データクラスタは v3.1.0 以降で行モードでの部分更新を、v3.3.1 以降で列モードでの部分更新をサポートしています。 |
| Conditional Update            | v3.1.0                   | 現在、条件は 'Greater' のみをサポートしています。            |
| Hybrid row-column storage     | ❌                        | 将来のリリースでサポートされる予定です。                          |

## Query performance

以下のテストでは、Data Cache を無効にした共有データクラスタ、Data Cache を有効にした共有データクラスタ、Hive のデータセットをクエリするクラスタ、および共有なしクラスタのクエリパフォーマンスを比較しています。

### Hardware Specifications

テストに使用されたクラスタには、1 つの FE ノードと 5 つの CN/BE ノードが含まれています。ハードウェア仕様は以下の通りです：

| **VM provider**       | Alibaba Cloud ECS   |
| --------------------- | ------------------- |
| **FE node**           | 8 Core 32 GB Memory |
| **CN/BE node**        | 8 Core 64 GB Memory |
| **Network bandwidth** | 8 Gbits/s           |
| **Disk**              | ESSD                |

### Software version

StarRocks v3.3.0

### Dataset

SSB 1TB データセット

:::note

この比較で使用されたデータセットとクエリは、[Star Schema Benchmark](../../benchmarking/SSB_Benchmarking.md#test-sql-and-table-creation-statements) からのものです。

:::

### Test Results

以下の表は、13 のクエリと各クラスタの合計のパフォーマンステスト結果を示しています。クエリ遅延の単位はミリ秒 (ms) です。

| **Query** | **Shared-data Without Data Cache** | **Shared-data With Data Cache** | **Hive Catalog Without Data Cache**     | **Shared-nothing** |
| --------- | ---------------------------------- | ------------------------------- | --------------------------------------- | ------------------ |
| **Q01**   | 2742                               | 858                             | 9652                                    | 3555               |
| **Q02**   | 2714                               | 704                             | 8638                                    | 3183               |
| **Q03**   | 1908                               | 658                             | 8163                                    | 2980               |
| **Q04**   | 31135                              | 8582                            | 34604                                   | 7997               |
| **Q05**   | 26597                              | 7806                            | 29183                                   | 6794               |
| **Q06**   | 21643                              | 7147                            | 24401                                   | 5602               |
| **Q07**   | 35271                              | 15490                           | 38904                                   | 19530              |
| **Q08**   | 24818                              | 7368                            | 27598                                   | 6984               |
| **Q09**   | 21056                              | 6667                            | 23587                                   | 5687               |
| **Q10**   | 2823                               | 912                             | 16663                                   | 3942               |
| **Q11**   | 50027                              | 18947                           | 52997                                   | 19636              |
| **Q12**   | 10300                              | 4919                            | 36146                                   | 8136               |
| **Q13**   | 7378                               | 3386                            | 23153                                   | 6380               |
| **SUM**   | 238412                             | 83444                           | 333689                                  | 100406             |

### Conclusion

- Data Cache を無効にし、Parallel Scan と I/O マージ最適化を有効にした共有データクラスタのクエリパフォーマンスは、Hive データをクエリするクラスタの **1.4 倍** です。
- Data Cache を有効にし、Parallel Scan と I/O マージ最適化を有効にした共有データクラスタのクエリパフォーマンスは、共有なしクラスタの **1.2 倍** です。

## Other features to be supported

- Full-text inverted index
- Hybrid row-column storage
- Global dictionary object
- Generated column
- Backup and restore