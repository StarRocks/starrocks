---
displayed_sidebar: docs
---

# クエリ分析

クエリパフォーマンスを最適化する方法は、よくある質問です。遅いクエリはユーザーエクスペリエンスやクラスターのパフォーマンスに悪影響を及ぼします。クエリパフォーマンスを分析し、最適化することが重要です。

クエリ情報は `fe/log/fe.audit.log` で確認できます。各クエリには `QueryID` が対応しており、これを使用してクエリの `QueryPlan` と `Profile` を検索できます。`QueryPlan` は、SQL文を解析してFEが生成する実行プランです。`Profile` はBEの実行結果で、各ステップに費やされた時間や処理されたデータ量などの情報を含みます。

## プラン分析

StarRocksでは、SQL文のライフサイクルはクエリ解析、クエリプランニング、クエリ実行の3つのフェーズに分けられます。分析ワークロードに必要なQPSが高くないため、クエリ解析は一般的にボトルネックにはなりません。

StarRocksのクエリパフォーマンスは、クエリプランニングとクエリ実行によって決まります。クエリプランニングはオペレーター (Join/Order/Aggregate) を調整し、クエリ実行は具体的な操作を実行します。

クエリプランは、DBAがクエリ情報にアクセスするためのマクロな視点を提供します。クエリプランはクエリパフォーマンスの鍵であり、DBAが参考にするための良いリソースです。以下のコードスニペットは、`TPCDS query96` を例にとり、クエリプランの表示方法を示しています。

[EXPLAIN](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) ステートメントを使用して、クエリのプランを表示します。

~~~SQL
EXPLAIN select  count(*)
from store_sales
    ,household_demographics
    ,time_dim
    , store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by count(*) limit 100;
~~~

クエリプランには2種類あります。論理クエリプランと物理クエリプランです。ここで説明するクエリプランは論理クエリプランを指します。`TPCDS query96.sql` に対応するクエリプランは以下の通りです。

~~~sql
+------------------------------------------------------------------------------+
| Explain String                                                               |
+------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                              |
|  OUTPUT EXPRS:<slot 11>                                                      |
|   PARTITION: UNPARTITIONED                                                   |
|   RESULT SINK                                                                |
|   12:MERGING-EXCHANGE                                                        |
|      limit: 100                                                              |
|      tuple ids: 5                                                            |
|                                                                              |
| PLAN FRAGMENT 1                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 12                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   8:TOP-N                                                                    |
|   |  order by: <slot 11> ASC                                                 |
|   |  offset: 0                                                               |
|   |  limit: 100                                                              |
|   |  tuple ids: 5                                                            |
|   |                                                                          |
|   7:AGGREGATE (update finalize)                                              |
|   |  output: count(*)                                                        |
|   |  group by:                                                               |
|   |  tuple ids: 4                                                            |
|   |                                                                          |
|   6:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_store_sk` = `s_store_sk`                       |
|   |  tuple ids: 0 2 1 3                                                      |
|   |                                                                          |
|   |----11:EXCHANGE                                                           |
|   |       tuple ids: 3                                                       |
|   |                                                                          |
|   4:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_hdemo_sk`=`household_demographics`.`hd_demo_sk`|
|   |  tuple ids: 0 2 1                                                        |
|   |                                                                          |
|   |----10:EXCHANGE                                                           |
|   |       tuple ids: 1                                                       |
|   |                                                                          |
|   2:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: table not in same group                        |
|   |  equal join conjunct: `ss_sold_time_sk` = `time_dim`.`t_time_sk`         |
|   |  tuple ids: 0 2                                                          |
|   |                                                                          |
|   |----9:EXCHANGE                                                            |
|   |       tuple ids: 2                                                       |
|   |                                                                          |
|   0:OlapScanNode                                                             |
|      TABLE: store_sales                                                      |
|      PREAGGREGATION: OFF. Reason: `ss_sold_time_sk` is value column          |
|      partitions=1/1                                                          |
|      rollup: store_sales                                                     |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 0                                                            |
|                                                                              |
| PLAN FRAGMENT 2                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|                                                                              |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 11                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   5:OlapScanNode                                                             |
|      TABLE: store                                                            |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `store`.`s_store_name` = 'ese'                              |
|      partitions=1/1                                                          |
|      rollup: store                                                           |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 3                                                            |
|                                                                              |
| PLAN FRAGMENT 3                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 10                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   3:OlapScanNode                                                             |
|      TABLE: household_demographics                                           |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `household_demographics`.`hd_dep_count` = 5                 |
|      partitions=1/1                                                          |
|      rollup: household_demographics                                          |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 1                                                            |
|                                                                              |
| PLAN FRAGMENT 4                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 09                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   1:OlapScanNode                                                             |
|      TABLE: time_dim                                                         |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `time_dim`.`t_hour` = 8, `time_dim`.`t_minute` >= 30        |
|      partitions=1/1                                                          |
|      rollup: time_dim                                                        |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 2                                                            |
+------------------------------------------------------------------------------+
128 rows in set (0.02 sec)
~~~

クエリ96は、いくつかのStarRocksの概念を含むクエリプランを示しています。

|名前|説明|
|--|--|
|avgRowSize|スキャンされたデータ行の平均サイズ|
|cardinality|スキャンされたテーブルのデータ行の総数|
|colocate|テーブルがコロケートモードかどうか|
|numNodes|スキャンされるノードの数|
|rollup|マテリアライズドビュー|
|preaggregation|事前集計|
|predicates|述語、クエリフィルター|

クエリ96のクエリプランは、0から4までの5つのフラグメントに分かれています。クエリプランは、下から上に一つずつ読み取ることができます。

フラグメント4は、`time_dim` テーブルをスキャンし、関連するクエリ条件 (つまり、`time_dim.t_hour = 8 and time_dim.t_minute >= 30`) を事前に実行する役割を担っています。このステップは述語プッシュダウンとも呼ばれます。StarRocksは、集計テーブルに対して `PREAGGREGATION` を有効にするかどうかを決定します。前の図では、`time_dim` の事前集計は無効になっています。この場合、`time_dim` のすべてのディメンション列が読み取られ、多くのディメンション列がテーブルにある場合、パフォーマンスに悪影響を及ぼす可能性があります。`time_dim` テーブルがデータ分割に `range partition` を選択した場合、クエリプランでいくつかのパーティションがヒットし、無関係なパーティションは自動的にフィルタリングされます。マテリアライズドビューがある場合、StarRocksはクエリに基づいてマテリアライズドビューを自動的に選択します。マテリアライズドビューがない場合、クエリは自動的にベーステーブルにヒットします（前の図の `rollup: time_dim` など）。

スキャンが完了すると、フラグメント4は終了します。データは、前の図の EXCHANGE ID : 09 に示されているように、他のフラグメントに渡され、受信ノード9に渡されます。

クエリ96のクエリプランでは、フラグメント2、3、4は似た機能を持っていますが、異なるテーブルをスキャンする役割を担っています。具体的には、クエリ内の `Order/Aggregation/Join` 操作はフラグメント1で実行されます。

フラグメント1は、`BROADCAST` メソッドを使用して `Order/Aggregation/Join` 操作を実行します。つまり、小さなテーブルを大きなテーブルにブロードキャストします。両方のテーブルが大きい場合、`SHUFFLE` メソッドを使用することをお勧めします。現在、StarRocksは `HASH JOIN` のみをサポートしています。`colocate` フィールドは、結合された2つのテーブルが同じ方法でパーティション化およびバケット化されていることを示し、データを移動せずにローカルでジョイン操作を実行できることを示します。ジョイン操作が完了すると、上位レベルの `aggregation`、`order by`、および `top-n` 操作が実行されます。

特定の式を削除して（オペレーターのみを保持）、クエリプランをよりマクロな視点で提示することができます。以下の図に示されています。

![8-5](../_assets/8-5.png)

## クエリヒント

クエリヒントは、クエリオプティマイザにクエリの実行方法を明示的に示唆する指示またはコメントです。現在、StarRocksは2種類のヒントをサポートしています：変数設定ヒントとジョインヒント。ヒントは単一のクエリ内でのみ有効です。

### 変数設定ヒント

`SET_VAR` ヒントを使用して、[システム変数](../sql-reference/System_variable.md) を1つ以上設定できます。SELECTおよびSUBMIT TASKステートメントで、またはCREATE MATERIALIZED VIEW AS SELECTやCREATE VIEW AS SELECTなどのステートメントに含まれるSELECT句で、構文 `/*+ SET_VAR(var_name = value) */` の形式で使用します。

#### 構文

~~~SQL
[...] SELECT [/*+ SET_VAR(key=value [, key = value]*) */] ...
SUBMIT [/*+ SET_VAR(key=value [, key = value]*) */] TASK ...
~~~

#### 例

集計クエリの集計方法をシステム変数 `streaming_preaggregation_mode` と `new_planner_agg_stage` を設定してヒントします。

~~~SQL
SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage = '2') */ SUM(sales_amount) AS total_sales_amount FROM sales_orders;
~~~

SUBMIT TASKステートメントでシステム変数 `query_timeout` を設定して、クエリのタスク実行タイムアウト期間をヒントします。

~~~SQL
SUBMIT /*+ SET_VAR(query_timeout=3) */ TASK AS CREATE TABLE temp AS SELECT count(*) AS cnt FROM tbl1;
~~~

マテリアライズドビューを作成する際にSELECT句でシステム変数 `query_timeout` を設定して、クエリの実行タイムアウト期間をヒントします。

~~~SQL
CREATE MATERIALIZED VIEW mv 
PARTITION BY dt 
DISTRIBUTED BY HASH(`key`) 
BUCKETS 10 
REFRESH ASYNC 
AS SELECT /*+ SET_VAR(query_timeout=500) */ * from dual;
~~~

### ジョインヒント

複数テーブルのジョインクエリでは、オプティマイザは通常、最適なジョイン実行方法を選択します。特別な場合には、ジョインヒントを使用してオプティマイザにジョイン実行方法を明示的に示唆したり、Join Reorderを無効にしたりできます。現在、ジョインヒントは、Shuffle Join、Broadcast Join、Bucket Shuffle Join、またはColocate Joinをジョイン実行方法として示唆することをサポートしています。ジョインヒントが使用されると、オプティマイザはJoin Reorderを実行しません。そのため、右側のテーブルとして小さなテーブルを選択する必要があります。さらに、[Colocate Join](../using_starrocks/Colocate_join.md) またはBucket Shuffle Joinをジョイン実行方法として示唆する場合、結合されたテーブルのデータ分布がこれらのジョイン実行方法の要件を満たしていることを確認してください。そうでない場合、示唆されたジョイン実行方法は効果を発揮できません。

#### 構文

~~~SQL
... JOIN { [BROADCAST] | [SHUFFLE] | [BUCKET] | [COLOCATE] | [UNREORDER]} ...
~~~

> **注意**
>
> ジョインヒントは大文字小文字を区別しません。

#### 例

- Shuffle Join

  ジョイン操作が実行される前に、テーブルAとBから同じバケッティングキー値を持つデータ行を同じマシンにシャッフルする必要がある場合、ジョイン実行方法をShuffle Joinとしてヒントできます。

  ~~~SQL
  select k1 from t1 join [SHUFFLE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ~~~

- Broadcast Join
  
  テーブルAが大きなテーブルで、テーブルBが小さなテーブルの場合、ジョイン実行方法をBroadcast Joinとしてヒントできます。テーブルBのデータは、テーブルAのデータが存在するマシンに完全にブロードキャストされ、その後ジョイン操作が実行されます。Shuffle Joinと比較して、Broadcast JoinはテーブルAのデータをシャッフルするコストを節約します。

  ~~~SQL
  select k1 from t1 join [BROADCAST] t2 on t1.k1 = t2.k2 group by t2.k2;
  ~~~

- Bucket Shuffle Join

  ジョインクエリのジョイン等値結合式にテーブルAのバケッティングキーが含まれている場合、特にテーブルAとBの両方が大きなテーブルである場合、ジョイン実行方法をBucket Shuffle Joinとしてヒントできます。テーブルBのデータは、テーブルAのデータ分布に従って、テーブルAのデータが存在するマシンにシャッフルされ、その後ジョイン操作が実行されます。Broadcast Joinと比較して、Bucket Shuffle Joinは、テーブルBのデータがグローバルに一度だけシャッフルされるため、データ転送を大幅に削減します。Bucket Shuffle Joinに参加するテーブルは、非パーティション化されているか、コロケートされている必要があります。

  ~~~SQL
  select k1 from t1 join [BUCKET] t2 on t1.k1 = t2.k2 group by t2.k2;
  ~~~

- Colocate Join
  
  テーブルAとBが、テーブル作成時に指定された同じColocation Groupに属している場合、テーブルAとBから同じバケッティングキー値を持つデータ行が同じBEノードに分散されます。ジョインクエリのジョイン等値結合式にテーブルAとBのバケッティングキーが含まれている場合、ジョイン実行方法をColocate Joinとしてヒントできます。同じキー値を持つデータがローカルで直接結合され、ノード間のデータ伝送にかかる時間を削減し、クエリパフォーマンスを向上させます。

  ~~~SQL
  select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ~~~

### ジョイン実行方法の確認

`EXPLAIN` コマンドを使用して、実際のジョイン実行方法を確認します。返された結果がジョインヒントと一致する場合、ジョインヒントが有効であることを意味します。

~~~SQL
EXPLAIN select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
~~~

![8-9](../_assets/8-9.png)

## SQLフィンガープリント

SQLフィンガープリントは、遅いクエリを最適化し、システムリソースの利用を改善するために使用されます。StarRocksは、遅いクエリログ (`fe.audit.log.slow_query`) 内のSQL文を正規化し、SQL文を異なるタイプに分類し、各SQLタイプのMD5ハッシュ値を計算して遅いクエリを識別します。MD5ハッシュ値はフィールド `Digest` によって指定されます。

~~~SQL
2021-12-27 15:13:39,108 [slow_query] |Client=172.26.xx.xxx:54956|User=root|Db=default_cluster:test|State=EOF|Time=2469|ScanBytes=0|ScanRows=0|ReturnRows=6|StmtId=3|QueryId=824d8dc0-66e4-11ec-9fdc-00163e04d4c2|IsQuery=true|feIp=172.26.92.195|Stmt=select count(*) from test_basic group by id_bigint|Digest=51390da6b57461f571f0712d527320f4
~~~

SQL文の正規化は、ステートメントテキストをより正規化された形式に変換し、重要なステートメント構造のみを保持します。

- データベース名やテーブル名などのオブジェクト識別子を保持します。

- 定数を疑問符 (?) に変換します。

- コメントを削除し、スペースをフォーマットします。

例えば、以下の2つのSQL文は、正規化後に同じタイプに属します。

- 正規化前のSQL文

~~~SQL
SELECT * FROM orders WHERE customer_id=10 AND quantity>20



SELECT * FROM orders WHERE customer_id = 20 AND quantity > 100
~~~

- 正規化後のSQL文

~~~SQL
SELECT * FROM orders WHERE customer_id=? AND quantity>?
~~~