---
displayed_sidebar: docs
sidebar_position: 40
---

# パーティション化されたマテリアライズドビューを作成する

このトピックでは、さまざまなユースケースに対応するためにパーティション化されたマテリアライズドビューを作成する方法を紹介します。

## 概要

StarRocks の非同期マテリアライズドビューは、さまざまなパーティショニング戦略と機能をサポートしており、次の効果を達成できます。

- **インクリメンタル構築**

  パーティション化されたマテリアライズドビューを作成する際、パーティションをバッチでリフレッシュするように作成タスクを設定することで、過剰なリソース消費を避けることができます。

- **インクリメンタルリフレッシュ**

  ベーステーブルの特定のパーティションでデータの変更を検出した場合、マテリアライズドビューの対応するパーティションのみを更新するようにリフレッシュタスクを設定できます。パーティションレベルのリフレッシュは、マテリアライズドビュー全体をリフレッシュするために使用されるリソースの無駄を大幅に防ぐことができます。

- **部分的なマテリアライゼーション**

  マテリアライズドビューパーティションに TTL を設定することで、データの部分的なマテリアライゼーションを可能にします。

- **透過的なクエリの書き換え**

  クエリは、更新されたマテリアライズドビューパーティションのみに基づいて透過的に書き換えられます。古いと見なされるパーティションはクエリプランに関与せず、クエリはデータの一貫性を保証するためにベーステーブルで実行されます。

## 制限事項

パーティション化されたマテリアライズドビューは、パーティション化されたベーステーブル（通常はファクトテーブル）にのみ作成できます。ベーステーブルとマテリアライズドビューの間のパーティション関係をマッピングすることで、両者のシナジーを構築できます。

現在、StarRocks は次のデータソースからのテーブルに対してパーティション化されたマテリアライズドビューの構築をサポートしています。

- **デフォルトカタログ内の StarRocks OLAP テーブル**
  - サポートされているパーティショニング戦略: レンジパーティション化
  - パーティショニングキーのサポートされているデータ型: INT, DATE, DATETIME, STRING
  - サポートされているテーブルタイプ: Primary Key, Duplicate Key, Aggregate Key, Unique Key
  - 共有なしクラスタと共有データクラスタの両方でサポート
- **Hive Catalog, Hudi Catalog, Iceberg Catalog, Paimon Catalog のテーブル**
  - サポートされているパーティショニングレベル: プライマリレベル
  - パーティショニングキーのサポートされているデータ型: INT, DATE, DATETIME, STRING

:::note

- 非パーティション化されたベース（ファクト）テーブルに基づいてパーティション化されたマテリアライズドビューを作成することはできません。
- StarRocks OLAP テーブルの場合:
  - 現在、リストパーティション化と式に基づくパーティション化はサポートされていません。
  - ベーステーブルの隣接する2つのパーティションは連続した範囲を持つ必要があります。
- 外部カタログの多層パーティション化されたベーステーブルの場合、プライマリレベルのパーティショニングパスのみを使用してパーティション化されたマテリアライズドビューを作成できます。たとえば、`yyyyMMdd/hour` 形式でパーティション化されたテーブルの場合、`yyyyMMdd` でパーティション化されたマテリアライズドビューのみを構築できます。
- v3.2.3 以降、StarRocks は Iceberg テーブルに対して [Partition Transforms](https://iceberg.apache.org/spec/#partition-transforms) を使用したパーティション化されたマテリアライズドビューの作成をサポートしており、マテリアライズドビューは変換後の列でパーティション化されます。詳細については、[Data lake query acceleration with materialized views - Choose a suitable data_lake_query_acceleration_with_materialized_views.mdterialized_views.md#choose-a-suitable-refresh-strategy](data_lake_query_acceleration_with_materialized_views.md#choose-a-suitable-refresh-strategy) を参照してください。

:::

## ユースケース

次のようなベーステーブルがあるとします。

```SQL
CREATE TABLE IF NOT EXISTS par_tbl1 (
  datekey      DATE,       -- パーティショニングキーとして使用される DATE 型の日付列。
  k1           STRING,
  v1           INT,
  v2           INT
)
ENGINE=olap
PARTITION BY RANGE (datekey) (
  START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(k1);

CREATE TABLE IF NOT EXISTS par_tbl2 (
  datekey      STRING,     -- パーティショニングキーとして使用される STRING 型の日付列。
  k1           STRING,
  v1           INT,
  v2           INT
)
ENGINE=olap
PARTITION BY RANGE (str2date(datekey, '%Y-%m-%d')) (
  START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(k1);

CREATE TABLE IF NOT EXISTS par_tbl3 (
  datekey_new  DATE,       -- par_tbl1.datekey と同等の列。
  k1           STRING,
  v1           INT,
  v2           INT
)
ENGINE=olap
PARTITION BY RANGE (datekey_new) (
  START ("2021-01-01") END ("2021-01-04") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH(k1);
```

### パーティションを一対一で揃える

同じパーティショニングキーを使用して、ベーステーブルのパーティションに対応するパーティションを持つマテリアライズドビューを作成できます。

![Partitioned Materialized View-1](../../../_assets/partitioned_mv-1.png)

- ベーステーブルのパーティショニングキーが DATE または DATETIME 型の場合、マテリアライズドビューに同じパーティショニングキーを直接指定できます。

  ```SQL
  PARTITION BY <base_table_partitioning_column>
  ```

  例:

  ```SQL
  CREATE MATERIALIZED VIEW par_mv1
  REFRESH ASYNC
  PARTITION BY datekey
  AS 
  SELECT 
    k1, 
    sum(v1) AS SUM, 
    datekey 
  FROM par_tbl1 
  GROUP BY datekey, k1;
  ```

- ベーステーブルのパーティショニングキーが STRING 型の場合、[str2date](../../../sql-reference/sql-functions/date-time-functions/str2date.md) 関数を使用して日付文字列を DATE または DATETIME 型に変換できます。

  ```SQL
  PARTITION BY str2date(<base_table_partitioning_column>, <format>)
  ```

  例:

  ```SQL
  CREATE MATERIALIZED VIEW par_mv2
  REFRESH ASYNC
  PARTITION BY str2date(datekey, '%Y-%m-%d')
  AS 
  SELECT 
    k1, 
    sum(v1) AS SUM, 
    datekey 
  FROM par_tbl2 
  GROUP BY datekey, k1;
  ```

### パーティションを時間の粒度でロールアップする

ベーステーブルのパーティショニングキーに [date_trunc](../../../sql-reference/sql-functions/date-time-functions/date_trunc.md) 関数を使用することで、ベーステーブルよりも大きなパーティショニング粒度を持つマテリアライズドビューを作成できます。ベーステーブルのパーティションでデータの変更が検出されると、StarRocks はマテリアライズドビューの対応するロールアップパーティションをリフレッシュします。

![Partitioned Materialized View-2](../../../_assets/partitioned_mv-2.png)

- ベーステーブルのパーティショニングキーが DATE または DATETIME 型の場合、ベーステーブルのパーティショニングキーに直接 date_trunc 関数を使用できます。

  ```SQL
  PARTITION BY date_trunc(<format>, <base_table_partitioning_column>)
  ```

  例:

  ```SQL
  CREATE MATERIALIZED VIEW par_mv3
  REFRESH ASYNC
  PARTITION BY date_trunc('month', datekey)
  AS 
  SELECT 
    k1, 
    sum(v1) AS SUM, 
    datekey 
  FROM par_tbl1 
  GROUP BY datekey, k1;
  ```

- ベーステーブルのパーティショニングキーが STRING 型の場合、SELECT リスト内でベーステーブルのパーティショニングキーを DATE または DATETIME 型に変換し、エイリアスを設定して、それを date_trunc 関数で使用してマテリアライズドビューのパーティショニングキーを指定する必要があります。

  ```SQL
  PARTITION BY 
  date_trunc(<format>, <mv_partitioning_column>)
  AS
  SELECT 
    str2date(<base_table_partitioning_column>, <format>) AS <mv_partitioning_column>
  ```

  例:

  ```SQL
  CREATE MATERIALIZED VIEW par_mv4
  REFRESH ASYNC
  PARTITION BY date_trunc('month', mv_datekey)
  AS 
  SELECT 
    datekey,
    k1, 
    sum(v1) AS SUM, 
    str2date(datekey, '%Y-%m-%d') AS mv_datekey
  FROM par_tbl2 
  GROUP BY datekey, k1;
  ```

### カスタマイズされた時間の粒度でパーティションを揃える

上記のパーティションロールアップ方法は、特定の時間の粒度に基づいてマテリアライズドビューをパーティション化することのみを許可し、パーティションの時間範囲をカスタマイズすることは許可しません。ビジネスシナリオでカスタマイズされた時間の粒度を使用してパーティション化する必要がある場合、date_trunc 関数と [time_slice](../../../sql-reference/sql-functions/date-time-functions/time_slice.md) 関数を使用して、指定された時間の粒度に基づいて与えられた時間を時間間隔の開始または終了に変換することができます。

SELECT リスト内でベーステーブルのパーティショニングキーに time_slice 関数を使用して新しい時間の粒度（間隔）を定義し、エイリアスを設定して、それを date_trunc 関数で使用してマテリアライズドビューのパーティショニングキーを指定する必要があります。

```SQL
PARTITION BY
date_trunc(<format>, <mv_partitioning_column>)
AS
SELECT 
  -- time_slice を使用できます。
  time_slice(<base_table_partitioning_column>, <interval>) AS <mv_partitioning_column>
```

例:

```SQL
CREATE MATERIALIZED VIEW par_mv5
REFRESH ASYNC
PARTITION BY date_trunc('day', mv_datekey)
AS 
SELECT 
  k1, 
  sum(v1) AS SUM, 
  time_slice(datekey, INTERVAL 5 MINUTE) AS mv_datekey 
FROM par_tbl1 
GROUP BY datekey, k1;
```

### インクリメンタルリフレッシュと透過的な書き換えを実現する

パーティションごとにリフレッシュするパーティション化されたマテリアライズドビューを作成することで、マテリアライズドビューのインクリメンタル更新と部分的なデータマテリアライゼーションによる透過的なクエリの書き換えを実現できます。

これらの目標を達成するために、マテリアライズドビューを作成する際には次の点を考慮する必要があります。

- **リフレッシュの粒度**

  プロパティ `partition_refresh_number` を使用して、各リフレッシュ操作の粒度を指定できます。`partition_refresh_number` は、リフレッシュがトリガーされたときにリフレッシュタスクでリフレッシュされる最大パーティション数を制御します。リフレッシュされるパーティションの数がこの値を超える場合、StarRocks はリフレッシュタスクを分割し、バッチで完了します。パーティションは、最も古いパーティションから最新のパーティション（将来のために動的に作成されたパーティションを除く）まで、時間順にリフレッシュされます。`partition_refresh_number` のデフォルト値は `-1` で、リフレッシュタスクは分割されません。

- **マテリアライゼーションの範囲**

  マテリアライズドデータの範囲は、バージョン v3.1.5 より前の `partition_ttl_number` または v3.1.5 以降で推奨される `partition_ttl` プロパティによって制御されます。`partition_ttl_number` は保持する最新のパーティションの数を指定し、`partition_ttl` は保持するマテリアライズドビューのデータの時間範囲を指定します。各リフレッシュ時に、StarRocks はパーティションを時間順に並べ、TTL 要件を満たすものだけを保持します。

- **リフレッシュ戦略**

  - 自動リフレッシュ戦略（`REFRESH ASYNC`）を持つマテリアライズドビューは、ベーステーブルのデータが変更されるたびに自動的にリフレッシュされます。
  - 定期的なリフレッシュ戦略（`REFRESH ASYNC [START (<start_time>)] EVERY (INTERVAL <interval>)`）を持つマテリアライズドビューは、定義された間隔で定期的にリフレッシュされます。

  :::note

  自動リフレッシュ戦略と定期的なリフレッシュ戦略を持つマテリアライズドビューは、リフレッシュタスクがトリガーされると自動的にリフレッシュされます。StarRocks はベーステーブルの各パーティションのデータバージョンを記録し比較します。データバージョンの変更は、パーティション内のデータの変更を示します。StarRocks がベーステーブルのパーティションでデータの変更を検出すると、マテリアライズドビューの対応するパーティションをリフレッシュします。ベーステーブルのパーティションでデータの変更が検出されない場合、対応するマテリアライズドビューパーティションのリフレッシュはスキップされます。

  :::

  - 手動リフレッシュ戦略（`REFRESH MANUAL`）を持つマテリアライズドビューは、REFRESH MATERIALIZED VIEW ステートメントを手動で実行することによってのみリフレッシュできます。マテリアライズドビュー全体をリフレッシュするのを避けるために、リフレッシュするパーティションの時間範囲を指定できます。ステートメントで `FORCE` を指定すると、StarRocks はベーステーブルのデータが変更されたかどうかに関係なく、対応するマテリアライズドビューまたはパーティションを強制的にリフレッシュします。ステートメントに `WITH SYNC MODE` を追加することで、リフレッシュタスクの同期呼び出しを行うことができ、StarRocks はタスクが成功または失敗したときにのみタスク結果を返します。

次の例では、パーティション化されたマテリアライズドビュー `par_mv8` を作成します。StarRocks がベーステーブルのパーティションでデータの変更を検出すると、マテリアライズドビューの対応するパーティションをリフレッシュします。リフレッシュタスクはバッチに分割され、それぞれが1つのパーティションのみをリフレッシュします（`"partition_refresh_number" = "1"`）。最新の2つのパーティションのみが保持され（`"partition_ttl_number" = "2"`）、他のパーティションはリフレッシュ中に削除されます。

```SQL
CREATE MATERIALIZED VIEW par_mv8
REFRESH ASYNC
PARTITION BY datekey
PROPERTIES(
  "partition_ttl_number" = "2",
  "partition_refresh_number" = "1"
)
AS 
SELECT 
  k1, 
  sum(v1) AS SUM, 
  datekey 
FROM par_tbl1 
GROUP BY datekey, k1;
```

このマテリアライズドビューをリフレッシュするには、REFRESH MATERIALIZED VIEW ステートメントを使用できます。次の例では、特定の時間範囲内で `par_mv8` のいくつかのパーティションを強制的にリフレッシュする同期呼び出しを行います。

```SQL
REFRESH MATERIALIZED VIEW par_mv8
PARTITION START ("2021-01-03") END ("2021-01-04")
FORCE WITH SYNC MODE;
```

出力:

```Plain
+--------------------------------------+
| QUERY_ID                             |
+--------------------------------------+
| 1d1c24b8-bf4b-11ee-a3cf-00163e0e23c9 |
+--------------------------------------+
1 row in set (1.12 sec)
```

TTL 機能を使用することで、`par_mv8` の一部のパーティションのみが保持されます。これにより、ほとんどのクエリが最近のデータに対して行われるシナリオで重要な部分的なデータのマテリアライゼーションを達成しました。TTL 機能により、マテリアライズドビューを使用して新しいデータ（たとえば、1週間または1か月以内）のクエリを透過的に高速化しながら、ストレージコストを大幅に節約できます。この時間範囲に該当しないクエリはベーステーブルにルーティングされます。

次の例では、クエリ 1 は `par_mv8` に保持されているパーティションにヒットするため、マテリアライズドビューによって高速化されますが、クエリ 2 は保持されているパーティションの時間範囲に該当しないため、ベーステーブルにルーティングされます。

```SQL
-- クエリ 1
SELECT 
  k1, 
  sum(v1) AS SUM, 
  datekey 
FROM par_tbl1
WHERE datekey='2021-01-04'
GROUP BY datekey, k1;

-- クエリ 2
SELECT 
  k1, 
  sum(v1) AS SUM, 
  datekey 
FROM par_tbl1
WHERE datekey='2021-01-01'
GROUP BY datekey, k1;
```

![Partitioned Materialized View-4](../../../_assets/partitioned_mv-4.png)