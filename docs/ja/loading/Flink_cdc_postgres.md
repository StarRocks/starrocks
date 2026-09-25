---
sidebar_position: 101
displayed_sidebar: docs
keywords:
  - PostgreSQL
  - postgres
  - sync
  - Flink CDC
description: "Flink CDC パイプラインを使用して PostgreSQL の変更をキャプチャし、StarRocks の主キーテーブルにリアルタイムで同期する方法。"
---

# PostgreSQL からのリアルタイム同期

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.mdx'
import FlinkStarRocksConnection from '../_assets/commonMarkdown/Edition_Specific_Flink_StarRocks_Connection.mdx'

このトピックでは、[Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) パイプラインを使用して PostgreSQL から StarRocks にデータをストリーミングする方法を説明します。パイプラインは各テーブルの既存の行をコピーした後、PostgreSQL のすべての INSERT、UPDATE、DELETE を数秒以内に適用し続けます。StarRocks のテーブルはパイプラインが作成するため、スキーマを移行する手順は別途必要ありません。

<InsertPrivNote />

## 仕組み

Flink CDC パイプラインは、**source**（PostgreSQL）、**sink**（StarRocks）、および任意の **route** と **transform** ルールを含む YAML ファイルです。このファイルを Flink クラスターに送信すると、Flink ジョブとして実行されます。

1. PostgreSQL の source は、条件に一致する各テーブルのスナップショットを取得した後、論理レプリケーションスロットを通じて PostgreSQL の先行書き込みログ（WAL）から変更を読み取ります。
2. StarRocks の sink は、StarRocks にまだ存在しないソーステーブルごとに[主キーテーブル](../table_design/table_types/primary_key_table.md)を作成し、[Stream Load](./StreamLoad.md) で行をロードします。すべてのテーブルに主キーがあるため、更新は既存の行を上書きし、削除はその行を削除します。

配信は at-least-once です。障害が発生すると一部の変更が再度書き込まれることがありますが、主キーテーブルのため重複は問題になりません。

:::note

パイプラインは PostgreSQL のスキーマ変更を同期しません。[テーブルのスキーマを変更する](#テーブルのスキーマを変更する)を参照してください。

:::

## 始める前に

次のものが必要です。

- PostgreSQL 10 以降。このトピックでは組み込みの論理デコーディングプラグイン `pgoutput` を使用するため、サーバー拡張機能は不要です。
- StarRocks クラスター。
- Flink 1.20 クラスターと、それを実行するための Java 11 または 17。
- すべての Flink TaskManager から PostgreSQL のポート（デフォルトは `5432`）へのネットワークアクセスと、[StarRocks に接続する](#starrocks-に接続する)で説明する StarRocks へのネットワークアクセス。
- 同期するすべての PostgreSQL テーブルの主キー。StarRocks は同じ主キーで宛先テーブルを作成します。

## ステップ 1: PostgreSQL を準備する

以下の手順は、PostgreSQL のスーパーユーザーまたはテーブルの所有者として実行します。例では、`shop` データベースの `public` スキーマにあるテーブルを同期します。

1. 論理レプリケーションを有効にします。`postgresql.conf` で `wal_level` を `logical` に設定します。

   ```properties
   wal_level = logical
   # 実行中のパイプラインは、それぞれレプリケーションスロットと WAL sender を 1 つずつ使用します。
   max_replication_slots = 4
   max_wal_senders = 4
   ```

   `wal_level` を変更した場合は PostgreSQL の再起動が必要です。Amazon RDS や Cloud SQL などのマネージドサービスでは、インスタンスのパラメータグループで同等のパラメータ（例: `rds.logical_replication = 1`）を設定します。

   再起動後に設定を確認します。

   ```sql
   SHOW wal_level;
   ```

   ```plaintext
    wal_level
   -----------
    logical
   ```

2. Flink CDC 用のユーザーを作成します。このユーザーには `REPLICATION` 属性とテーブルの読み取り権限が必要です。

   ```sql
   CREATE ROLE flink_cdc WITH LOGIN REPLICATION PASSWORD '<password>';
   GRANT CONNECT ON DATABASE shop TO flink_cdc;
   GRANT USAGE ON SCHEMA public TO flink_cdc;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
   ```

   `pg_hba.conf` でレプリケーション接続を制限している場合は、Flink TaskManager のホストからこのユーザーが接続できるようにします。例:

   ```plaintext
   # TYPE  DATABASE     USER       ADDRESS          METHOD
   host    shop         flink_cdc  10.0.0.0/24      scram-sha-256
   host    replication  flink_cdc  10.0.0.0/24      scram-sha-256
   ```

3. 同期するテーブルのパブリケーションを作成します。パブリケーションには、パイプラインが読み取るすべてのテーブル（[ステップ 4](#ステップ-4-パイプラインを定義する) の `tables` オプション）を含める必要があります。

   ```sql
   CREATE PUBLICATION flink_pub FOR TABLE public.orders, public.customers;
   ```

   :::tip

   パブリケーションは自分で作成してください。パブリケーションが存在しない場合、コネクタは `CREATE PUBLICATION ... FOR ALL TABLES` を実行しようとしますが、これを実行できるのはスーパーユーザーだけです。それ以外のユーザーではこの文が失敗し、Flink は再試行を繰り返します。その結果、報告されるエラーはパブリケーションがないことではなく、`max_wal_senders` に関するものになります。

   :::

4. 各テーブルのレプリカアイデンティティを `FULL` に設定し、すべての UPDATE と DELETE について変更前の行全体が WAL に記録されるようにします。

   ```sql
   ALTER TABLE public.orders REPLICA IDENTITY FULL;
   ALTER TABLE public.customers REPLICA IDENTITY FULL;
   ```

   これはパイプラインを開始する前に行ってください。デフォルトのレプリカアイデンティティのままだと、最初の UPDATE または DELETE で Flink ジョブが `NullPointerException` により失敗し、再起動するたびに同じ変更で失敗し続けます。

## ステップ 2: StarRocks を準備する

宛先データベースとパイプライン用のユーザーを作成します。

```sql
CREATE DATABASE shop;

CREATE USER flink_sink IDENTIFIED BY '<password>';
GRANT CREATE TABLE ON DATABASE shop TO USER flink_sink;
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON ALL TABLES IN DATABASE shop TO USER flink_sink;
```

## ステップ 3: Flink CDC をインストールする

1. Flink 1.20 をダウンロードして起動します。Flink ドキュメントの [First steps](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/local_installation/) を参照してください。

2. Flink の設定ファイル `conf/config.yaml` に次の行を追加してチェックポイントを有効にし、クラスターを再起動します。

   ```yaml
   execution.checkpointing.interval: 10s
   ```

   PostgreSQL の source は、チェックポイントが完了するたびに WAL 内の位置を PostgreSQL に報告します。チェックポイントがないと、レプリケーションスロットはパイプライン開始以降のすべての WAL セグメントを保持し続け、PostgreSQL のディスクがいっぱいになります。

3. [Flink CDC のダウンロードページ](https://flink.apache.org/downloads/#apache-flink-cdc)から、使用する Flink のバージョンに対応する Flink CDC リリースをダウンロードして展開します。Flink CDC 3.6 以降は、リリースごとに 1 つの Flink バージョン向けにビルドされています。Flink 1.20 の場合は `-2.2` のパッケージではなく、`flink-cdc-3.6.0-1.20-bin.tar.gz` をダウンロードしてください。

   ```bash
   tar -xzf flink-cdc-3.6.0-1.20-bin.tar.gz
   cd flink-cdc-3.6.0-1.20
   ```

4. PostgreSQL と StarRocks のパイプラインコネクタを Maven Central から Flink CDC の `lib` ディレクトリにダウンロードします。Flink CDC リリースと同じバージョンを使用してください。

   ```bash
   cd lib
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/3.6.0-1.20/flink-cdc-pipeline-connector-postgres-3.6.0-1.20.jar
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-starrocks/3.6.0-1.20/flink-cdc-pipeline-connector-starrocks-3.6.0-1.20.jar
   cd ..
   ```

## ステップ 4: パイプラインを定義する

`postgres-to-starrocks.yaml` という名前のファイルを作成します。

```yaml
source:
  type: postgres
  name: PostgreSQL source
  hostname: <postgres_host>
  port: 5432
  username: flink_cdc
  password: <password>
  # カンマ区切りの <database>.<schema>.<table>。テーブル名の部分には正規表現を使用できます。
  # パブリケーションと同じテーブルを指定してください。
  tables: shop.public.orders, shop.public.customers
  # 作成して使用するレプリケーションスロット。小文字、数字、アンダースコアのみ使用できます。
  slot.name: flink_starrocks
  decoding.plugin.name: pgoutput
  debezium.publication.name: flink_pub

sink:
  type: starrocks
  name: StarRocks sink
  # StarRocks のアドレスについては、後述の「StarRocks に接続する」を参照してください。
  jdbc-url: jdbc:mysql://<fe_host>:9030
  load-url: <fe_host>:8030
  username: flink_sink
  password: <password>
  # デフォルトは 300000（5 分）です。小さい値にすると変更が早く反映されます。
  sink.buffer-flush.interval-ms: 5000

route:
  # public スキーマの各テーブルを StarRocks のデータベース shop に書き込みます。
  - source-table: public.\.*
    sink-table: shop.<>
    replace-symbol: <>

pipeline:
  name: PostgreSQL to StarRocks
  parallelism: 1
```

次の設定に注意してください。

- `tables` はパブリケーションと一致させる必要があります。スナップショットは `tables` で選択されたすべてのテーブルをコピーしますが、PostgreSQL が変更を送信するのはパブリケーションに含まれるテーブルだけです。パブリケーションに含まれないテーブルは一度コピーされた後は更新されず、ジョブはエラーを報告しません。
- `tables` はテーブルを `<database>.<schema>.<table>` の形式で指定します。ただし、パイプライン内では PostgreSQL のテーブルは `<schema>.<table>` だけで識別されるため、`route` と `transform` のルールは `shop.public.orders` ではなく `public.orders` に一致させます。
- `route` ブロックがない場合、sink は各テーブルを PostgreSQL の**スキーマ**名と同じ名前の StarRocks データベースに書き込みます。そのため、`public` のテーブルは `public` という名前のデータベースに入ります。
- `sink.buffer-flush.interval-ms` のデフォルトは 5 分です。デフォルトのままだと、新しいパイプラインは開始後数分間何もしていないように見えます。
- sink が作成するテーブルにプロパティを設定するには、`sink` セクションに `table.create.properties.<property>` を追加します。例: `table.create.properties.replication_num: 1`。

source と sink のすべてのオプションについては、Flink CDC の [Postgres connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/postgres/) および [StarRocks connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/starrocks/) のリファレンスを参照してください。

### StarRocks に接続する

<FlinkStarRocksConnection />

### デフォルト値が関数である列

sink は、PostgreSQL の各列のデフォルト値を StarRocks の CREATE TABLE ステートメントにコピーします。`DEFAULT now()` のような関数呼び出しのデフォルト値は StarRocks では無効なため、ジョブは次のようなエラーで失敗します。

```plaintext
Invalid default value for 'created_at': date literal [now()] is invalid.
```

事前に StarRocks のテーブルを自分で作成しても、sink は CREATE TABLE ステートメントを送信するため、このエラーは回避できません。代わりに、その列を再計算する `transform` ルールを追加します。計算列にはデフォルト値がありません。

```yaml
transform:
  - source-table: public.orders
    projection: order_id, customer, amount, status, CAST(created_at AS TIMESTAMP(6)) AS created_at
```

`projection` にはテーブルのすべての列を指定してください。指定しなかった列は同期されません。

## ステップ 5: パイプラインを開始する

`FLINK_HOME` に Flink のインストール先を設定し、Flink CDC のディレクトリからパイプラインを送信します。

```bash
export FLINK_HOME=/path/to/flink-1.20
bin/flink-cdc.sh postgres-to-starrocks.yaml
```

```plaintext
Pipeline has been submitted to cluster.
Job ID: cda97bac3b504442d8106a2d70c6074c
Job Description: PostgreSQL to StarRocks
```

ジョブは Flink Web UI（デフォルトは `http://<jobmanager_host>:8081`）に表示され、ステータスは `RUNNING` のままになるはずです。`RESTARTING` に変わった場合は、ジョブの **Exceptions** タブを開き、[トラブルシューティング](#トラブルシューティング)を参照してください。

## ステップ 6: 同期を確認する

1. スナップショットが完了すると、既存の行が StarRocks に入っています。

   ```sql
   SELECT * FROM shop.orders ORDER BY order_id;
   ```

2. PostgreSQL でデータを変更します。

   ```sql
   INSERT INTO public.orders (order_id, customer, amount, status) VALUES (5, 'erin', 42.00, 'new');
   UPDATE public.orders SET status = 'returned' WHERE order_id = 3;
   DELETE FROM public.orders WHERE order_id = 4;
   ```

3. 数秒後（フラッシュ間隔とロード時間の合計）、StarRocks でもう一度クエリを実行します。PostgreSQL で同じクエリを実行した場合と同じ行が返されます。

## パイプラインを管理する

### レプリケーションスロットを監視する

レプリケーションスロットは、パイプラインが読み取りを確認するまで PostgreSQL サーバーに WAL を保持させます。パイプラインが停止している間や遅れている場合、WAL は蓄積されます。スロットが保持している WAL の量を確認します。

```sql
SELECT slot_name,
       active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

パイプラインを完全に停止する場合は、PostgreSQL が WAL を解放できるようにスロットを削除します。

```sql
SELECT pg_drop_replication_slot('flink_starrocks');
```

### テーブルのスキーマを変更する

パイプラインは PostgreSQL のスキーマ変更を取り込みません。PostgreSQL で列を追加すると、ジョブは実行を続けますが、StarRocks のテーブルにその列を追加しても新しい列は書き込まれません。

スキーマ変更後にテーブルを同期するには、次の手順を実行します。

1. StarRocks のテーブルに同じ変更を加えます。たとえば [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) を使用します。
2. [スナップショットを取り直します](#スナップショットを取り直す)。

### スナップショットを取り直す

PostgreSQL の現在の状態からテーブルをコピーし直し、ストリーミングを再開するには、次の手順を実行します。

1. Flink ジョブをキャンセルします。
2. [レプリケーションスロットを監視する](#レプリケーションスロットを監視する)に示す方法で、レプリケーションスロットを削除します。
3. パイプラインが書き込むすべての StarRocks テーブルを TRUNCATE します。

   ```sql
   TRUNCATE TABLE shop.orders;
   TRUNCATE TABLE shop.customers;
   ```

4. パイプラインを再送信します。パイプラインはすべてのテーブルのスナップショットを取り直した後、変更のストリーミングを続けます。

TRUNCATE の手順は省略しないでください。ジョブをキャンセルしてから新しいスナップショットが始まるまでの間に PostgreSQL で行われた変更は再生されません。スナップショットは残っている行を上書きしますが、その間に PostgreSQL で削除された行は StarRocks に残ったままになります。スナップショットが完了するまで、TRUNCATE したテーブルは空か、一部のデータだけが入った状態になります。

## トラブルシューティング

ジョブが `RESTARTING` の場合、Flink Web UI の **Exceptions** タブに原因が表示されます。最も内側の `Caused by` 行を確認してください。

| エラー | 原因と解決方法 |
| --- | --- |
| `number of requested standby connections exceeds max_wal_senders` | 多くの場合、それ以前の失敗の症状です。再試行のたびにレプリケーション接続が開かれるためです。PostgreSQL サーバーのログで最初のエラーを確認してください。よくあるのは `CREATE PUBLICATION` での `permission denied for database` で、これはパブリケーションが存在しないことを意味します。[ステップ 1](#ステップ-1-postgresql-を準備する) を参照してください。 |
| `DebeziumSchemaDataTypeInference` または `extractBeforeDataRecord` での `NullPointerException` | テーブルに `REPLICA IDENTITY FULL` が設定されていません。設定してから、[スナップショットを取り直します](#スナップショットを取り直す)。 |
| `Invalid default value for '<column>'` | 列の PostgreSQL のデフォルト値が関数です。[デフォルト値が関数である列](#デフォルト値が関数である列)を参照してください。 |
| `Connect to <host>:8040 ... Connection refused` | FE がロードをリダイレクトした先の BE または CN に TaskManager が接続できません。BE と CN が登録されているアドレスで、すべての BE と CN の HTTP ポートに TaskManager から接続できることを確認してください。 |
| ジョブは `RUNNING` だが、StarRocks にデータが届かない | フラッシュ間隔が経過するまで待ちます。`sink.buffer-flush.interval-ms` のデフォルトは 5 分です。 |
| 行が `public` という名前のデータベースに入る | パイプラインに `route` ルールがありません。[ステップ 4](#ステップ-4-パイプラインを定義する) を参照してください。 |

## 関連項目

- [MySQL からのリアルタイム同期](./Flink_cdc_load.md)
- [Apache Flink® からデータを継続的にロードする](./Flink-connector-starrocks.md)
- [ロードによるデータ変更](./Load_to_Primary_Key_tables.md)
