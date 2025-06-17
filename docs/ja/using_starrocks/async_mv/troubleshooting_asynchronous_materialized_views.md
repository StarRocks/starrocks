---
displayed_sidebar: docs
sidebar_position: 30
---

# 非同期マテリアライズドビューのトラブルシューティング

このトピックでは、非同期マテリアライズドビューを調査し、作業中に遭遇した問題を解決する方法について説明します。

> **注意**
>
> 以下に示す機能の一部は、StarRocks v3.1以降でのみサポートされています。

## 非同期マテリアライズドビューの調査

作業中の非同期マテリアライズドビューの全体像を把握するために、まずその動作状態、リフレッシュ履歴、リソース消費を確認できます。

### 非同期マテリアライズドビューの動作状態を確認

非同期マテリアライズドビューの動作状態を確認するには、[SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md)を使用します。返される情報の中で、次のフィールドに注目できます。

- `is_active`: マテリアライズドビューの状態がアクティブかどうか。アクティブなマテリアライズドビューのみがクエリアクセラレーションと書き換えに使用できます。
- `last_refresh_state`: 最後のリフレッシュの状態。PENDING、RUNNING、FAILED、SUCCESSがあります。
- `last_refresh_error_message`: 最後のリフレッシュが失敗した理由（マテリアライズドビューの状態がアクティブでない場合）。
- `rows`: マテリアライズドビュー内のデータ行数。この値は、実際の行数と異なる場合があります。更新が遅延することがあるためです。

他の返されるフィールドの詳細については、[SHOW MATERIALIZED VIEWS - Returns](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md#returns)を参照してください。

例:

```Plain
MySQL > SHOW MATERIALIZED VIEWS LIKE 'mv_pred_2'\G
***************************[ 1. row ]***************************
id                                   | 112517
database_name                        | ssb_1g
name                                 | mv_pred_2
refresh_type                         | ASYNC
is_active                            | true
inactive_reason                      | <null>
partition_type                       | UNPARTITIONED
task_id                              | 457930
task_name                            | mv-112517
last_refresh_start_time              | 2023-08-04 16:46:50
last_refresh_finished_time           | 2023-08-04 16:46:54
last_refresh_duration                | 3.996
last_refresh_state                   | SUCCESS
last_refresh_force_refresh           | false
last_refresh_start_partition         |
last_refresh_end_partition           |
last_refresh_base_refresh_partitions | {}
last_refresh_mv_refresh_partitions   |
last_refresh_error_code              | 0
last_refresh_error_message           |
rows                                 | 0
text                                 | CREATE MATERIALIZED VIEW `mv_pred_2` (`lo_quantity`, `lo_revenue`, `sum`)
DISTRIBUTED BY HASH(`lo_quantity`, `lo_revenue`) BUCKETS 2
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `lineorder`.`lo_quantity`, `lineorder`.`lo_revenue`, sum(`lineorder`.`lo_tax`) AS `sum`
FROM `ssb_1g`.`lineorder`
WHERE `lineorder`.`lo_linenumber` = 1
GROUP BY 1, 2;

1 row in set
Time: 0.003s
```

### 非同期マテリアライズドビューのリフレッシュ履歴を表示

非同期マテリアライズドビューのリフレッシュ履歴を表示するには、データベース `information_schema` のテーブル `task_runs` をクエリします。返される情報の中で、次のフィールドに注目できます。

- `CREATE_TIME` と `FINISH_TIME`: リフレッシュタスクの開始と終了時間。
- `STATE`: リフレッシュタスクの状態。PENDING、RUNNING、FAILED、SUCCESSがあります。
- `ERROR_MESSAGE`: リフレッシュタスクが失敗した理由。

例:

```Plain
MySQL > SELECT * FROM information_schema.task_runs WHERE task_name ='mv-112517' \G
***************************[ 1. row ]***************************
QUERY_ID      | 7434cee5-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:50
FINISH_TIME   | 2023-08-04 16:46:54
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:50
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":false,"mvPartitionsToRefresh":[],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{}}
PROPERTIES    | {"FORCE":"false"}
***************************[ 2. row ]***************************
QUERY_ID      | 72dd2f16-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:48
FINISH_TIME   | 2023-08-04 16:46:53
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:48
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":true,"mvPartitionsToRefresh":["mv_pred_2"],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{"lineorder":["lineorder"]}}
PROPERTIES    | {"FORCE":"true"}
```

### 非同期マテリアライズドビューのリソース消費を監視

リフレッシュ中およびリフレッシュ後の非同期マテリアライズドビューによるリソース消費を監視および分析できます。

#### リフレッシュ中のリソース消費を監視

リフレッシュタスク中に、そのリアルタイムのリソース消費を[SHOW PROC '/current_queries'](../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md)を使用して監視できます。

返される情報の中で、次のフィールドに注目できます。

- `ScanBytes`: スキャンされたデータのサイズ。
- `ScanRows`: スキャンされたデータ行数。
- `MemoryUsage`: 使用されたメモリのサイズ。
- `CPUTime`: CPU時間のコスト。
- `ExecTime`: クエリの実行時間。

例:

```Plain
MySQL > SHOW PROC '/current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2023-08-04 17:01:30
QueryId       | 806eed7d-32a5-11ee-b73a-8e20563011de
ConnectionId  | 0
Database      | ssb_1g
User          | root
ScanBytes     | 70.981 MB
ScanRows      | 6001215 rows
MemoryUsage   | 73.748 MB
DiskSpillSize | 0.000
CPUTime       | 2.515 s
ExecTime      | 2.583 s
```

#### リフレッシュ後のリソース消費を分析

リフレッシュタスク後に、クエリプロファイルを通じてリソース消費を分析できます。

非同期マテリアライズドビューがリフレッシュされる際に、INSERT OVERWRITE ステートメントが実行されます。対応するクエリプロファイルを確認して、リフレッシュタスクによる時間とリソース消費を分析できます。

返される情報の中で、次のメトリクスに注目できます。

- `Total`: クエリによる総消費時間。
- `QueryCpuCost`: クエリの総CPU時間コスト。CPU時間コストは同時プロセスに対して集計されます。その結果、このメトリックの値はクエリの実際の実行時間よりも大きくなることがあります。
- `QueryMemCost`: クエリの総メモリコスト。
- 個々のオペレーターに関する他のメトリクス、例えばジョインオペレーターや集計オペレーター。

クエリプロファイルの確認方法や他のメトリクスの理解についての詳細は、[Analyze query profile](../../administration/query_profile_overview.md)を参照してください。

### 非同期マテリアライズドビューによるクエリの書き換えを確認

非同期マテリアライズドビューを使用してクエリが書き換えられるかどうかを、クエリプランから確認できます。[EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md)を使用します。

クエリプランのメトリクス `SCAN` に対応するマテリアライズドビューの名前が表示されている場合、クエリはマテリアライズドビューによって書き換えられています。

例1:

```Plain
MySQL > SHOW CREATE TABLE mv_agg\G
***************************[ 1. row ]***************************
Materialized View        | mv_agg
Create Materialized View | CREATE MATERIALIZED VIEW `mv_agg` (`c_custkey`)
DISTRIBUTED BY RANDOM
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"replicated_storage" = "true",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`
FROM `ssb_1g`.`customer`
GROUP BY `customer`.`c_custkey`;

MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+-----------------------------------------------------------------------------------+
| Explain String                                                                    |
+-----------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                         |
|     - SCAN [mv_agg] => [1:c_custkey]                                              |
|             Estimates: {row: 30000, cpu: ?, memory: ?, network: ?, cost: 15000.0} |
|             partitionRatio: 1/1, tabletRatio: 12/12                               |
|             1:c_custkey := 10:c_custkey                                           |
+-----------------------------------------------------------------------------------+
```

クエリの書き換え機能を無効にすると、StarRocksは通常のクエリプランを採用します。

例2:

```Plain
MySQL > SET enable_materialized_view_rewrite = false;
MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+---------------------------------------------------------------------------------------+
| Explain String                                                                        |
+---------------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                             |
|     - AGGREGATE(GLOBAL) [1:c_custkey]                                                 |
|             Estimates: {row: 15000, cpu: ?, memory: ?, network: ?, cost: 120000.0}    |
|         - SCAN [mv_bitmap] => [1:c_custkey]                                           |
|                 Estimates: {row: 60000, cpu: ?, memory: ?, network: ?, cost: 30000.0} |
|                 partitionRatio: 1/1, tabletRatio: 12/12                               |
+---------------------------------------------------------------------------------------+
```

## 問題の診断と解決

ここでは、非同期マテリアライズドビューを使用中に遭遇する可能性のある一般的な問題と、それに対応する解決策をいくつか紹介します。

### マテリアライズドビューの構築失敗

非同期マテリアライズドビューの作成に失敗した場合、つまりCREATE MATERIALIZED VIEWステートメントが実行できない場合、次の点を確認できます。

- **同期マテリアライズドビュー用のSQLステートメントを誤って使用していないか確認する。**

  StarRocksは、同期マテリアライズドビューと非同期マテリアライズドビューの2種類のマテリアライズドビューを提供しています。

  同期マテリアライズドビューを作成するために使用される基本的なSQLステートメントは次のとおりです。

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  AS <query>
  ```

  しかし、非同期マテリアライズドビューを作成するために使用されるSQLステートメントには、より多くのパラメータが含まれています。

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  REFRESH ASYNC -- 非同期マテリアライズドビューのリフレッシュ戦略。
  DISTRIBUTED BY HASH(<column>) -- 非同期マテリアライズドビューのデータ分散戦略。
  AS <query>
  ```

  SQLステートメントに加えて、2つのマテリアライズドビューの主な違いは、非同期マテリアライズドビューはStarRocksが提供するすべてのクエリ構文をサポートしますが、同期マテリアライズドビューは限られた集計関数の選択肢のみをサポートすることです。

- **正しいパーティション列を指定しているか確認する。**

  非同期マテリアライズドビューを作成する際に、パーティショニング戦略を指定できます。これにより、より細かい粒度レベルでマテリアライズドビューをリフレッシュできます。

  現在、StarRocksはレンジパーティション化のみをサポートしており、マテリアライズドビューを構築するために使用されるクエリステートメントのSELECT式から単一の列を参照することのみをサポートしています。date_trunc()関数を使用して列を切り捨て、パーティショニング戦略の粒度レベルを変更できます。他の式はサポートされていないことに注意してください。

- **マテリアライズドビューを作成するための必要な権限を持っているか確認する。**

  非同期マテリアライズドビューを作成する際に、クエリされるすべてのオブジェクト（テーブル、ビュー、マテリアライズドビュー）のSELECT権限が必要です。クエリでUDFが使用されている場合、関数のUSAGE権限も必要です。

### マテリアライズドビューのリフレッシュ失敗

マテリアライズドビューがリフレッシュに失敗した場合、つまりリフレッシュタスクの状態がSUCCESSでない場合、次の点を確認できます。

- **不適切なリフレッシュ戦略を採用していないか確認する。**

  デフォルトでは、マテリアライズドビューは作成後すぐにリフレッシュされます。ただし、v2.5およびそれ以前のバージョンでは、MANUALリフレッシュ戦略を採用したマテリアライズドビューは作成後にリフレッシュされません。REFRESH MATERIALIZED VIEWを使用して手動でリフレッシュする必要があります。

- **リフレッシュタスクがメモリ制限を超えていないか確認する。**

  通常、非同期マテリアライズドビューが大規模な集計またはジョイン計算を含む場合、メモリリソースを使い果たします。この問題を解決するには、次のことができます。

  - マテリアライズドビューにパーティショニング戦略を指定し、各パーティションを1回ずつリフレッシュします。
  - リフレッシュタスクのためにディスクへのスピル機能を有効にします。v3.1以降、StarRocksはマテリアライズドビューをリフレッシュする際に中間結果をディスクにスピルすることをサポートしています。ディスクへのスピルを有効にするには、次のステートメントを実行します。

  ```SQL
  -- マテリアライズドビューを作成する際にプロパティを定義します。
  CREATE MATERIALIZED VIEW mv1 
  REFRESH ASYNC
  PROPERTIES ( 'session.enable_spill'='true' )
  AS <query>;

  -- 既存のマテリアライズドビューにプロパティを追加します。
  ALTER MATERIALIZED VIEW mv2 SET ('session.enable_spill' = 'true');
  ```

### マテリアライズドビューのリフレッシュタイムアウト

大規模なマテリアライズドビューは、リフレッシュタスクがタイムアウト期間を超えるためにリフレッシュに失敗することがあります。この問題を解決するために、次の解決策を検討できます。

- **マテリアライズドビューにパーティショニング戦略を指定して、細粒度のリフレッシュを実現する**

  [Create partitioned materialized views](use_cases/create_partitioned_materialized_view.md)で説明されているように、マテリアライズドビューをパーティショニングすることで、インクリメンタルビルドとリフレッシュを実現し、初期リフレッシュ時の過剰なリソース消費の問題を回避できます。

- **タイムアウト期間を長く設定する**

デフォルトのマテリアライズドビューのリフレッシュタスクのタイムアウトは、v3.2より前のバージョンでは5分、v3.2以降では1時間です。タイムアウト例外が発生した場合、次のステートメントを使用してタイムアウト期間を調整できます。

  ```sql
  ALTER MATERIALIZED VIEW mv2 SET ('session.query_timeout' = '4000');
  ```

- **マテリアライズドビューのリフレッシュのパフォーマンスボトルネックを分析する**

  複雑な計算を伴うマテリアライズドビューのリフレッシュは時間がかかります。リフレッシュタスクのクエリプロファイルを分析することで、そのパフォーマンスボトルネックを分析できます。

  - `information_schema.task_runs`をクエリして、リフレッシュタスクに対応する`query_id`を取得します。
  - 次のステートメントを使用してリフレッシュタスクのクエリプロファイルを分析します。
    - [GET_QUERY_PROFILE](../../sql-reference/sql-functions/utility-functions/get_query_profile.md): `query_id`に基づいて元のクエリプロファイルを取得します。
    - [ANALYZE PROFILE](../../sql-reference/sql-statements/cluster-management/plan_profile/ANALYZE_PROFILE.md): フラグメントごとにクエリプロファイルを分析し、ツリー構造で表示します。

### マテリアライズドビューの状態がアクティブでない

マテリアライズドビューがクエリを書き換えたりリフレッシュしたりできず、マテリアライズドビューの状態`is_active`が`false`の場合、ベーステーブルのスキーマ変更が原因である可能性があります。この問題を解決するには、次のステートメントを実行してマテリアライズドビューの状態を手動でアクティブに設定できます。

```SQL
ALTER MATERIALIZED VIEW mv1 ACTIVE;
```

マテリアライズドビューの状態をアクティブに設定しても効果がない場合、マテリアライズドビューを削除して再作成する必要があります。

### マテリアライズドビューのリフレッシュタスクが過剰なリソースを使用する

リフレッシュタスクがシステムリソースを過剰に使用している場合、次の点を確認できます。

- **マテリアライズドビューが大きすぎるか確認する。**

  多くのテーブルをジョインして大量の計算を引き起こす場合、リフレッシュタスクは多くのリソースを占有します。この問題を解決するには、マテリアライズドビューのサイズを評価し、再計画する必要があります。

- **不必要に頻繁なリフレッシュ間隔を設定していないか確認する。**

  固定間隔のリフレッシュ戦略を採用している場合、リフレッシュ頻度を下げることで問題を解決できます。リフレッシュタスクがベーステーブルのデータ変更によってトリガーされる場合、データを頻繁にロードすることもこの問題を引き起こす可能性があります。この問題を解決するには、マテリアライズドビューに適切なリフレッシュ戦略を定義する必要があります。

- **マテリアライズドビューがパーティション化されているか確認する。**

  パーティション化されていないマテリアライズドビューは、リフレッシュにコストがかかる可能性があります。StarRocksは毎回マテリアライズドビュー全体をリフレッシュするためです。この問題を解決するには、マテリアライズドビューにパーティショニング戦略を指定し、各パーティションを1回ずつリフレッシュする必要があります。

過剰なリソースを占有するリフレッシュタスクを停止するには、次の方法があります。

- マテリアライズドビューの状態を非アクティブに設定し、そのすべてのリフレッシュタスクを停止します。

  ```SQL
  ALTER MATERIALIZED VIEW mv1 INACTIVE;
  ```

- 実行中のリフレッシュタスクを[CANCEL REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CANCEL_REFRESH_MATERIALIZED_VIEW.md)を使用して終了します。

  ```SQL
  CANCEL REFRESH MATERIALIZED VIEW mv1;
  ```

### マテリアライズドビューのクエリ書き換え失敗

マテリアライズドビューが関連するクエリを書き換えられない場合、次の点を確認できます。

- **TRACEを使用して書き換え失敗を診断する**

  StarRocksは、マテリアライズドビューの書き換え失敗を診断するためのTRACEステートメントを提供しています。

    - `TRACE LOGS MV <query>`: v3.2以降で利用可能。このコマンドは詳細な書き換えプロセスと失敗の理由を分析します。
    - `TRACE REASON MV <query>`: v3.2.8以降で利用可能。このコマンドは書き換え失敗の簡潔な理由を提供します。

  ```SQL
  MySQL > TRACE REASON MV SELECT sum(c1) FROM `glue_ice`.`iceberg_test`.`ice_test3`;
  +----------------------------------------------------------------------------------------------------------------------+
  | Explain String                                                                                                       |
  +----------------------------------------------------------------------------------------------------------------------+
  |     MV rewrite fail for mv1: Rewrite aggregate rollup sum(1: c1) failed: only column-ref is supported after rewrite  |
  |     MV rewrite fail for mv1: Rewrite aggregate function failed, cannot get rollup function: sum(1: c1)               |
  |     MV rewrite fail for mv1: Rewrite rollup aggregate failed: cannot rewrite aggregate functions                     |
  +----------------------------------------------------------------------------------------------------------------------+
  ```

- **マテリアライズドビューとクエリが一致しているか確認する。**

  - StarRocksは、マテリアライズドビューとクエリをテキストベースではなく構造ベースのマッチング技術で一致させます。そのため、クエリがマテリアライズドビューと似ているからといって、必ずしも書き換えられるわけではありません。
  - マテリアライズドビューは、SPJG（Select/Projection/Join/Aggregation）タイプのクエリのみを書き換えることができます。ウィンドウ関数、ネストされた集計、またはジョインと集計を含むクエリはサポートされていません。
  - マテリアライズドビューは、Outer Joinsで複雑なジョイン述語を含むクエリを書き換えることができません。例えば、`A LEFT JOIN B ON A.dt > '2023-01-01' AND A.id = B.id`のような場合、`JOIN ON`句からの述語を`WHERE`句で指定することをお勧めします。

  マテリアライズドビューのクエリ書き換えの制限についての詳細は、[Query rewrite with materialized views - Limitations](use_cases/query_rewrite_with_materialized_views.md#limitations)を参照してください。

- **マテリアライズドビューの状態がアクティブであるか確認する。**

  StarRocksは、クエリを書き換える前にマテリアライズドビューの状態を確認します。クエリは、マテリアライズドビューの状態がアクティブな場合にのみ書き換えられます。この問題を解決するには、次のステートメントを実行してマテリアライズドビューの状態を手動でアクティブに設定できます。

  ```SQL
  ALTER MATERIALIZED VIEW mv1 ACTIVE;
  ```

- **マテリアライズドビューがデータの一貫性要件を満たしているか確認する。**

  StarRocksは、マテリアライズドビューとベーステーブルデータのデータの一貫性を確認します。デフォルトでは、マテリアライズドビューのデータが最新である場合にのみクエリが書き換えられます。この問題を解決するには、次のことができます。

  - マテリアライズドビューに`PROPERTIES('query_rewrite_consistency'='LOOSE')`を追加して、一貫性チェックを無効にします。
  - `PROPERTIES('mv_rewrite_staleness_second'='5')`を追加して、一定のデータ不整合を許容します。ベーステーブルのデータが変更されているかどうかに関係なく、最後のリフレッシュがこの時間間隔の前であれば、クエリは書き換えられます。

- **マテリアライズドビューのクエリステートメントに出力列が不足しているか確認する。**

  レンジおよびポイントクエリを書き換えるには、マテリアライズドビューのクエリステートメントのSELECT式でフィルタリング述語として使用される列を指定する必要があります。マテリアライズドビューのSELECTステートメントを確認し、クエリのWHEREおよびORDER BY句で参照されている列が含まれていることを確認する必要があります。

例1: マテリアライズドビュー`mv1`はネストされた集計を使用しています。そのため、クエリを書き換えることはできません。

```SQL
CREATE MATERIALIZED VIEW mv1 REFRESH ASYNC AS
select count(distinct cnt) 
from (
    select c_city, count(*) cnt 
    from customer 
    group by c_city
) t;
```

例2: マテリアライズドビュー`mv2`はジョインと集計を使用しています。そのため、クエリを書き換えることはできません。この問題を解決するには、集計を含むマテリアライズドビューを作成し、その後、前のものに基づいてジョインを含むネストされたマテリアライズドビューを作成できます。

```SQL
CREATE MATERIALIZED VIEW mv2 REFRESH ASYNC AS
select *
from (
    select lo_orderkey, lo_custkey, p_partkey, p_name
    from lineorder
    join part on lo_partkey = p_partkey
) lo
join (
    select c_custkey
    from customer
    group by c_custkey
) cust
on lo.lo_custkey = cust.c_custkey;
```

例3: マテリアライズドビュー`mv3`は、`SELECT c_city, sum(tax) FROM tbl WHERE dt='2023-01-01' AND c_city = 'xxx'`のパターンのクエリを書き換えることができません。なぜなら、述語が参照する列がSELECT式に含まれていないからです。

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT c_city, sum(tax) FROM tbl GROUP BY c_city;
```

この問題を解決するには、次のようにマテリアライズドビューを作成できます。

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT dt, c_city, sum(tax) FROM tbl GROUP BY dt, c_city;
```