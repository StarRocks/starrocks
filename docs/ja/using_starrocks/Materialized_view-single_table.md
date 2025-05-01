---
displayed_sidebar: docs
---

# 同期マテリアライズドビュー

このトピックでは、**同期マテリアライズドビュー (Rollup)** の作成、使用、および管理方法について説明します。

同期マテリアライズドビューでは、ベーステーブルのすべての変更が対応する同期マテリアライズドビューに同時に更新されます。同期マテリアライズドビューのリフレッシュは自動的にトリガーされます。同期マテリアライズドビューは、維持と更新が非常に安価であり、リアルタイムの単一テーブル集計クエリの透明なアクセラレーションに適しています。

StarRocks の同期マテリアライズドビューは、[the default catalog](../data_source/catalog/default_catalog.md) の単一のベーステーブルにのみ作成できます。これらは本質的にクエリアクセラレーションのための特別なインデックスです。同期マテリアライズドビューを直接クエリすることはできません。

v2.4以降、StarRocks は非同期マテリアライズドビューを提供しており、複数のテーブルとより多くの集計演算子の作成をサポートしています。**非同期マテリアライズドビュー** の使用については、[Asynchronous materialized view](../using_starrocks/Materialized_view.md) を参照してください。

以下の表は、StarRocks v2.5、v2.4 の非同期マテリアライズドビュー (ASYNC MVs) と同期マテリアライズドビュー (SYNC MV) のサポートする機能の観点からの比較です。

|                       | **単一テーブル集計** | **複数テーブルジョイン** | **クエリの書き換え** | **リフレッシュ戦略** | **ベーステーブル** |
| --------------------- | -------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MV** | Yes | Yes | Yes | <ul><li>非同期リフレッシュ</li><li>手動リフレッシュ</li></ul> | 複数のテーブルから:<ul><li>Default catalog</li><li>External catalogs (v2.5)</li><li>既存のマテリアライズドビュー (v2.5)</li><li>既存のビュー (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | [集計関数](#correspondence-of-aggregate-functions) の限られた選択肢 | No | Yes | データロード中の同期リフレッシュ | Default catalog の単一テーブル |

## 基本概念

- **ベーステーブル**

  ベーステーブルは、マテリアライズドビューの駆動テーブルです。

  StarRocks の同期マテリアライズドビューでは、ベーステーブルは [default catalog](../data_source/catalog/default_catalog.md) の単一の内部テーブルでなければなりません。StarRocks は、重複キーテーブルと集計テーブルに同期マテリアライズドビューを作成することをサポートしています。

- **リフレッシュ**

  同期マテリアライズドビューは、ベーステーブルのデータが変更されるたびに自動的に更新されます。手動でリフレッシュをトリガーする必要はありません。

- **クエリの書き換え**

  クエリの書き換えとは、マテリアライズドビューが構築されたベーステーブルに対してクエリを実行する際に、システムがマテリアライズドビューの事前計算結果をクエリに再利用できるかどうかを自動的に判断することを意味します。再利用できる場合、システムは関連するマテリアライズドビューからデータを直接ロードし、時間とリソースを消費する計算やジョインを回避します。

  同期マテリアライズドビューは、一部の集計演算子に基づいたクエリの書き換えをサポートしています。詳細については、[集計演算子の対応関係](#correspondence-of-aggregate-operators) を参照してください。

## 準備

同期マテリアライズドビューを作成する前に、データウェアハウスが同期マテリアライズドビューによるクエリアクセラレーションに適しているかどうかを確認してください。たとえば、クエリが特定のサブクエリステートメントを再利用しているかどうかを確認します。

以下の例は、各トランザクションのトランザクション ID `record_id`、販売員 ID `seller_id`、店舗 ID `store_id`、日付 `sale_date`、および販売額 `sale_amt` を含むテーブル `sales_records` に基づいています。次の手順に従ってテーブルを作成し、データを挿入します。

```SQL
CREATE TABLE sales_records(
    record_id INT,
    seller_id INT,
    store_id INT,
    sale_date DATE,
    sale_amt BIGINT
) DISTRIBUTED BY HASH(record_id);

INSERT INTO sales_records
VALUES
    (001,01,1,"2022-03-13",8573),
    (002,02,2,"2022-03-14",6948),
    (003,01,1,"2022-03-14",4319),
    (004,03,3,"2022-03-15",8734),
    (005,03,3,"2022-03-16",4212),
    (006,02,2,"2022-03-17",9515);
```

この例のビジネスシナリオでは、異なる店舗の販売額に関する頻繁な分析が求められます。その結果、各クエリで `sum()` 関数が使用され、大量の計算リソースを消費します。クエリを実行してその時間を記録し、EXPLAIN コマンドを使用してクエリプロファイルを表示できます。

```Plain
MySQL > SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+----------+-----------------+
| store_id | sum(`sale_amt`) |
+----------+-----------------+
|        2 |           16463 |
|        3 |           12946 |
|        1 |           12892 |
+----------+-----------------+
3 rows in set (0.02 sec)

MySQL > EXPLAIN SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:3: store_id | 6: sum                                          |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: 3: store_id                                  |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(6: sum)                                                    |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: 3: store_id                                           |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(5: sale_amt)                                               |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: sales_records                                                  |
|      tabletRatio=10/10                                                      |
|      tabletList=12049,12053,12057,12061,12065,12069,12073,12077,12081,12085 |
|      cardinality=1                                                          |
|      avgRowSize=2.0                                                         |
|      numNodes=0                                                             |
+-----------------------------------------------------------------------------+
45 rows in set (0.00 sec)
```

クエリが約0.02秒かかることが観察され、クエリプロファイルの `rollup` フィールドの値が `sales_records` であるため、クエリを加速するために同期マテリアライズドビューが使用されていないことがわかります。

## 同期マテリアライズドビューの作成

特定のクエリステートメントに基づいて同期マテリアライズドビューを作成するには、[CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md) を使用します。

テーブル `sales_records` と上記のクエリステートメントに基づいて、各店舗の販売額の合計を分析するために同期マテリアライズドビュー `store_amt` を作成する以下の例を示します。

```SQL
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
```

> **注意**
>
> - 同期マテリアライズドビューで集計関数を使用する場合、GROUP BY 句を使用し、SELECT リストに少なくとも1つの GROUP BY 列を指定する必要があります。
> - 同期マテリアライズドビューは、1つの集計関数を複数の列に使用することをサポートしていません。`sum(a+b)` の形式のクエリステートメントはサポートされていません。
> - 同期マテリアライズドビューは、1つの列に複数の集計関数を使用することをサポートしていません。`select sum(a), min(a) from table` の形式のクエリステートメントはサポートされていません。
> - 同期マテリアライズドビューを作成する際に、JOIN および WHERE 句はサポートされていません。
> - ALTER TABLE DROP COLUMN を使用してベーステーブルの特定の列を削除する場合、ベーステーブルのすべての同期マテリアライズドビューに削除された列が含まれていないことを確認する必要があります。そうでない場合、削除操作を実行できません。同期マテリアライズドビューで使用されている列を削除するには、まずその列を含むすべての同期マテリアライズドビューを削除し、その後に列を削除する必要があります。
> - テーブルに対して同期マテリアライズドビューを作成しすぎると、データロードの効率に影響を与えます。ベーステーブルにデータがロードされている間、同期マテリアライズドビューとベーステーブルのデータは同期的に更新されます。ベーステーブルに `n` 個の同期マテリアライズドビューが含まれている場合、ベーステーブルにデータをロードする効率は `n` 個のテーブルにデータをロードする効率とほぼ同じです。
> - 現在、StarRocks は同時に複数の同期マテリアライズドビューを作成することをサポートしていません。新しい同期マテリアライズドビューは、前のものが完了した後にのみ作成できます。

## 同期マテリアライズドビューの構築状況を確認する

同期マテリアライズドビューの作成は非同期操作です。CREATE MATERIALIZED VIEW の実行が成功すると、マテリアライズドビューの作成タスクが正常に提出されたことを示します。データベース内で同期マテリアライズドビューの構築状況を [SHOW ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) を使用して確認できます。

```Plain
MySQL > SHOW ALTER MATERIALIZED VIEW\G
*************************** 1. row ***************************
          JobId: 12090
      TableName: sales_records
     CreateTime: 2022-08-25 19:41:10
   FinishedTime: 2022-08-25 19:41:39
  BaseIndexName: sales_records
RollupIndexName: store_amt
       RollupId: 12091
  TransactionId: 10
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
1 row in set (0.00 sec)
```

`RollupIndexName` セクションは同期マテリアライズドビューの名前を示し、`State` セクションは構築が完了したかどうかを示します。

## 同期マテリアライズドビューを使用したクエリ

作成した同期マテリアライズドビューには、クエリステートメントに従った事前計算結果の完全なセットが含まれています。以降のクエリはそのデータを使用します。準備段階で行ったのと同じクエリを実行して、クエリ時間をテストできます。

```Plain
MySQL > SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+----------+-----------------+
| store_id | sum(`sale_amt`) |
+----------+-----------------+
|        2 |           16463 |
|        3 |           12946 |
|        1 |           12892 |
+----------+-----------------+
3 rows in set (0.01 sec)
```

クエリ時間が0.01秒に短縮されたことが観察されます。

## クエリが同期マテリアライズドビューにヒットしたかどうかを確認する

EXPLAIN コマンドを再度実行して、クエリが同期マテリアライズドビューにヒットしたかどうかを確認します。

```Plain
MySQL > EXPLAIN SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:3: store_id | 6: sum                                          |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: 3: store_id                                  |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(6: sum)                                                    |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: 3: store_id                                           |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(5: sale_amt)                                               |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: store_amt                                                      |
|      tabletRatio=10/10                                                      |
|      tabletList=12092,12096,12100,12104,12108,12112,12116,12120,12124,12128 |
|      cardinality=6                                                          |
|      avgRowSize=2.0                                                         |
|      numNodes=0                                                             |
+-----------------------------------------------------------------------------+
45 rows in set (0.00 sec)
```

クエリプロファイルの `rollup` セクションの値が `store_amt` に変わっていることが観察されます。これは、クエリが同期マテリアライズドビューにヒットしたことを意味します。

## 同期マテリアライズドビューを表示する

テーブルとその下位の同期マテリアライズドビューのスキーマを確認するには、DESC \<tbl_name\> ALL を実行できます。

```Plain
MySQL > DESC sales_records ALL;
+---------------+---------------+-----------+--------+------+-------+---------+-------+
| IndexName     | IndexKeysType | Field     | Type   | Null | Key   | Default | Extra |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
| sales_records | DUP_KEYS      | record_id | INT    | Yes  | true  | NULL    |       |
|               |               | seller_id | INT    | Yes  | true  | NULL    |       |
|               |               | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_date | DATE   | Yes  | false | NULL    | NONE  |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
|               |               |           |        |      |       |         |       |
| store_amt     | AGG_KEYS      | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | SUM   |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
8 rows in set (0.00 sec)
```

## 同期マテリアライズドビューを削除する

以下の状況では、同期マテリアライズドビューを削除する必要があります。

- 間違ったマテリアライズドビューを作成してしまい、構築が完了する前に削除する必要がある。
- マテリアライズドビューを作りすぎて、ロードパフォーマンスが大幅に低下し、一部のマテリアライズドビューが重複している。
- 関連するクエリの頻度が低く、比較的高いクエリ遅延を許容できる。

### 未完成の同期マテリアライズドビューを削除する

進行中の作成タスクをキャンセルすることで、作成中の同期マテリアライズドビューを削除できます。まず、[同期マテリアライズドビューの構築状況を確認する](#check-the-building-status-of-a-synchronous-materialized-view) ことで、マテリアライズドビュー作成タスクのジョブ ID `JobID` を取得する必要があります。ジョブ ID を取得した後、CANCEL ALTER コマンドを使用して作成タスクをキャンセルします。

```Plain
CANCEL ALTER TABLE ROLLUP FROM sales_records (12090);
```

### 既存の同期マテリアライズドビューを削除する

既存の同期マテリアライズドビューは、[DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP_MATERIALIZED_VIEW.md) コマンドを使用して削除できます。

```SQL
DROP MATERIALIZED VIEW store_amt;
```

## ベストプラクティス

### 正確なカウントディスティンクト

以下の例は、広告ビジネス分析テーブル `advertiser_view_record` に基づいており、広告が表示された日付 `click_time`、広告の名前 `advertiser`、広告のチャンネル `channel`、および広告を表示したユーザーの ID `user_id` を記録しています。

```SQL
CREATE TABLE advertiser_view_record(
    click_time DATE,
    advertiser VARCHAR(10),
    channel VARCHAR(10),
    user_id INT
) distributed BY hash(click_time);
```

分析は主に広告の UV に焦点を当てています。

```SQL
SELECT advertiser, channel, count(distinct user_id)
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

正確なカウントディスティンクトを加速するために、このテーブルに基づいて同期マテリアライズドビューを作成し、bitmap_union 関数を使用してデータを事前集計することができます。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

同期マテリアライズドビューが作成された後、後続のクエリ内のサブクエリ `count(distinct user_id)` は自動的に `bitmap_union_count (to_bitmap(user_id))` に書き換えられ、同期マテリアライズドビューにヒットするようになります。

### おおよそのカウントディスティンクト

上記のテーブル `advertiser_view_record` を再び例として使用します。おおよそのカウントディスティンクトを加速するために、このテーブルに基づいて同期マテリアライズドビューを作成し、[hll_union()](../sql-reference/sql-functions/aggregate-functions/hll_union.md) 関数を使用してデータを事前集計することができます。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv2 AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

### 追加のソートキーを設定する

ベーステーブル `tableA` に `k1`、`k2`、`k3` の列が含まれており、`k1` と `k2` のみがソートキーであるとします。`where k3=x` を含むクエリを加速する必要がある場合、`k3` を最初の列として同期マテリアライズドビューを作成できます。

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA
```

## 集計関数の対応関係

同期マテリアライズドビューを使用してクエリを実行する場合、元のクエリステートメントは自動的に書き換えられ、同期マテリアライズドビューに保存された中間結果をクエリするために使用されます。以下の表は、元のクエリの集計関数と同期マテリアライズドビューを構築するために使用される集計関数の対応関係を示しています。ビジネスシナリオに応じて、対応する集計関数を選択して同期マテリアライズドビューを構築できます。

| **元のクエリの集計関数**           | **マテリアライズドビューの集計関数** |
| ------------------------------------------------------ | ----------------------------------------------- |
| sum                                                    | sum                                             |
| min                                                    | min                                             |
| max                                                    | max                                             |
| count                                                  | count                                           |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                    |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                       |
```