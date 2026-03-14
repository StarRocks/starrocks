---
displayed_sidebar: docs
---

# CREATE MATERIALIZED VIEW

import MVWarehouse from '../../../_assets/commonMarkdown/mv_warehouse.mdx'

CREATE MATERIALIZED VIEW は、マテリアライズドビューを作成します。マテリアライズドビューの使用方法については、[同期マテリアライズドビュー](../../../using_starrocks/Materialized_view-single_table.md) および [非同期マテリアライズドビュー](../../../using_starrocks/async_mv/Materialized_view.md) を参照してください。

> **注意**
>
> - ベーステーブルが存在するデータベースで CREATE MATERIALIZED VIEW 権限を持つユーザーのみが、マテリアライズドビューを作成できます。
> - v3.4.0 以降、StarRocks は共有データクラスタでの同期マテリアライズドビューの作成をサポートしています。

マテリアライズドビューの作成は非同期操作です。このコマンドが正常に実行されると、マテリアライズドビューの作成タスクが正常に送信されたことを示します。データベース内の同期マテリアライズドビューの構築ステータスは [SHOW ALTER MATERIALIZED VIEW](SHOW_ALTER_MATERIALIZED_VIEW.md) コマンドで確認でき、非同期マテリアライズドビューの構築ステータスは [Information Schema](../../information_schema/information_schema.md) のメタデータビュー [`tasks`](../../information_schema/tasks.md) および [`task_runs`](../../information_schema/task_runs.md) をクエリすることで確認できます。

StarRocks は v2.4 から非同期マテリアライズドビューをサポートしています。以前のバージョンの非同期マテリアライズドビューと同期マテリアライズドビューの主な違いは次のとおりです。

|                       | **単一テーブル集計** | **複数テーブルジョイン** | **クエリの書き換え** | **リフレッシュ戦略** | **ベーステーブル** |
| --------------------- | ---------------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MV** | はい | はい | はい | <ul><li>非同期リフレッシュ</li><li>手動リフレッシュ</li></ul> | 以下からの複数のテーブル:<ul><li>Default Catalog</li><li>external catalog (v2.5)</li><li>既存のマテリアライズドビュー (v2.5)</li><li>既存のビュー (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | 集計関数の選択肢は限定的 | いいえ | はい | データロード中の同期リフレッシュ | Default Catalog 内の単一テーブル |

## 同期マテリアライズドビュー

### 構文

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

角括弧 `[]` 内のパラメータはオプションです。

### パラメータ

**mv_name** （必須）

マテリアライズドビューの名前。命名要件は次のとおりです。

- 名前は、文字（a-z または A-Z）、数字（0-9）、またはアンダースコア（\_）で構成されている必要があり、文字で始める必要があります。
- 名前の長さは64文字を超えることはできません。
- 名前は大文字と小文字が区別されます。

**COMMENT** （オプション）

マテリアライズドビューに関するコメント。 `COMMENT` は `mv_name` の後に配置する必要があります。そうしないと、マテリアライズドビューを作成できません。

**query_statement** （必須）

マテリアライズドビューを作成するためのクエリステートメント。その結果がマテリアライズドビューのデータになります。構文は次のとおりです。

```SQL
SELECT select_expr[, select_expr ...]
[WHERE where_expr]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr (必須)

  クエリステートメント内のすべてのカラム、つまりマテリアライズドビューのスキーマ内のすべてのカラム。このパラメータは、次の値をサポートします。

  - `SELECT a, abs(b), min(c) FROM table_a` のような単純なカラムまたは集計カラム。ここで、`a`、`b`、および `c` はベーステーブルのカラム名です。マテリアライズドビューのカラム名を指定しない場合、StarRocks は自動的にカラム名を割り当てます。
  - `SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a` のような式。ここで、`a+1`、`b+2`、および `c*c` はベーステーブルのカラムを参照する式であり、`x`、`y`、および `z` はマテリアライズドビューのカラムに割り当てられたエイリアスです。

  > **NOTE**
  >
  > - `select_expr` には、少なくとも1つのカラムを指定する必要があります。
  > - 集計関数を使用して同期マテリアライズドビューを作成する場合は、GROUP BY 句を指定し、`select_expr` に少なくとも1つの GROUP BY カラムを指定する必要があります。
  > - 同期マテリアライズドビューは、JOIN などの句や GROUP BY の HAVING 句をサポートしていません。
  > - v3.1 以降、各同期マテリアライズドビューは、ベーステーブルの各カラムに対して複数の集計関数をサポートできます。たとえば、`select b, sum(a), min(a) from table group by b` のようなクエリステートメントです。
  > - v3.1 以降、同期マテリアライズドビューは、SELECT および集計関数の複雑な式をサポートします。たとえば、`select b, sum(a + 1) as sum_a1, min(cast (a as bigint)) as min_a from table group by b` や `select abs(b) as col1, a + 1 as col2, cast(a as bigint) as col3 from table` のようなクエリステートメントです。同期マテリアライズドビューに使用される複雑な式には、次の制限が課せられます。
  >   - 各複雑な式にはエイリアスが必要であり、ベーステーブルのすべての同期マテリアライズドビューの中で、異なる複雑な式には異なるエイリアスを割り当てる必要があります。たとえば、クエリステートメント `select b, sum(a + 1) as sum_a from table group by b` と `select b, sum(a) as sum_a from table group by b` は、同じベーステーブルの同期マテリアライズドビューを作成するために使用できません。複雑な式に異なるエイリアスを設定できます。
  >   - `EXPLAIN <sql_statement>` を実行して、複雑な式で作成された同期マテリアライズドビューによってクエリが書き換えられるかどうかを確認できます。詳細については、[クエリプラン](../../../best_practices/query_tuning/query_planning.md) を参照してください。

- WHERE (オプション)

  v3.1.8 以降、同期マテリアライズドビューは、マテリアライズドビューに使用される行をフィルタリングできる WHERE 句をサポートしています。

- GROUP BY (オプション)

  クエリの GROUP BY カラム。このパラメータが指定されていない場合、データはデフォルトでグループ化されません。

- ORDER BY (オプション)

  クエリの ORDER BY カラム。

  - ORDER BY 句のカラムは、`select_expr` のカラムと同じ順序で宣言する必要があります。
  - クエリステートメントに GROUP BY 句が含まれている場合、ORDER BY カラムは GROUP BY カラムと同一である必要があります。
  - このパラメータが指定されていない場合、システムは次のルールに従って ORDER BY カラムを自動的に補完します。
    - マテリアライズドビューが AGGREGATE タイプの場合、すべての GROUP BY カラムが自動的にソートキーとして使用されます。
    - マテリアライズドビューが AGGREGATE タイプでない場合、StarRocks はプレフィックスカラムに基づいてソートキーを自動的に選択します。

### 同期マテリアライズドビューのクエリ

同期マテリアライズドビューは、物理テーブルではなく、基本的にベーステーブルのインデックスであるため、ヒント `[_SYNC_MV_]` を使用してのみ、同期マテリアライズドビューをクエリできます。

```SQL
-- Do not omit the brackets [] in the hint.
SELECT * FROM <mv_name> [_SYNC_MV_];
```

> **CAUTION**
>
> 現在、同期マテリアライズドビューのカラムにエイリアスを指定した場合でも、StarRocks はカラム名を自動的に生成します。

### 同期マテリアライズドビューによる自動クエリの書き換え

同期マテリアライズドビューのパターンに従うクエリが実行されると、元のクエリステートメントが自動的に書き換えられ、マテリアライズドビューに格納されている中間結果が使用されます。

次の表は、元のクエリの集計関数と、マテリアライズドビューの構築に使用される集計関数との対応関係を示しています。ビジネスシナリオに応じて、対応する集計関数を選択してマテリアライズドビューを構築できます。

| **元のクエリの集計関数**                               | **マテリアライズドビューの集計関数**           |
| ------------------------------------------------------ | ----------------------------------------------- |
| sum                                                    | sum                                             |
| min                                                    | min                                             |
| max                                                    | max                                             |
| count                                                  | count                                           |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                    |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                       |
| percentile_approx, percentile_union                    | percentile_union                                |

上記の関数に加えて、StarRocks v3.4.0 以降では、同期マテリアライズドビューは汎用集計関数もサポートしています。汎用集計関数の詳細については、[汎用集計関数状態](../../../table_design/table_types/aggregate_table.md#use-generic-aggregate-states-in-materialized-views) を参照してください。

```SQL
-- Create a synchronous materialized view test_mv1 to store aggregate states.
CREATE MATERIALIZED VIEW test_mv1 
AS
SELECT 
    dt,
    -- Original aggregate functions.
    min(id) AS min_id,
    max(id) AS max_id,
    sum(id) AS sum_id,
    bitmap_union(to_bitmap(id)) AS bitmap_union_id,
    hll_union(hll_hash(id)) AS hll_union_id,
    percentile_union(percentile_hash(id)) AS percentile_union_id,
    -- Generic aggregate state functions.
    ds_hll_count_distinct_union(ds_hll_count_distinct_state(id)) AS hll_id,
    avg_union(avg_state(id)) AS avg_id,
    array_agg_union(array_agg_state(id)) AS array_agg_id,
    min_by_union(min_by_state(province, id)) AS min_by_province_id
FROM t1
GROUP BY dt;
```

## 非同期マテリアライズドビュー

### 構文

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
-- You must specify either `distribution_desc` or `refresh_scheme`, or both.
-- distribution_desc
[DISTRIBUTED BY HASH(<bucket_key>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]]
-- refresh_desc
[REFRESH 
-- refresh_moment
    [IMMEDIATE | DEFERRED]
-- refresh_scheme
    [ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>) | MANUAL]
]
-- partition_expression
[PARTITION BY 
  [ <partition_column> [,...] ] | [ <date_function_expr> ]
]
-- order_by_expression
[ORDER BY (<sort_key>)]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

角括弧 `[]` 内のパラメータはオプションです。

### パラメータ

**mv_name** (必須)

マテリアライズドビューの名前。命名要件は次のとおりです。

- 名前は、文字 (a-z または A-Z)、数字 (0-9)、またはアンダースコア (\_) で構成されている必要があり、文字で始める必要があります。
- 名前の長さは 64 文字を超えることはできません。
- 名前は大文字と小文字が区別されます。

> **注意**
>
> 複数のマテリアライズドビューを同じベーステーブル上に作成できますが、同じデータベース内のマテリアライズドビューの名前を重複させることはできません。

**COMMENT** (オプション)

マテリアライズドビューに関するコメント。 `COMMENT` は `mv_name` の後に配置する必要があります。そうしないと、マテリアライズドビューを作成できません。

**distribution_desc** (オプション)

非同期マテリアライズドビューのバケッティング戦略。 StarRocks は、ハッシュバケッティングとランダムバケット法 (v3.1 以降) をサポートしています。このパラメータを指定しない場合、StarRocks はランダムバケット法を使用し、バケット数を自動的に設定します。

> **注**
>
> 非同期マテリアライズドビューを作成する際は、`distribution_desc` または `refresh_scheme` 、あるいはその両方を指定する必要があります。

- **ハッシュバケッティング**:

  構文

```SQL
  DISTRIBUTED BY HASH (<bucket_key1>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
  ```

詳細については、 [データ分散] (../../../table_design/data_distribution/Data_distribution.md#data-distribution) を参照してください。

  > **NOTE**
  >
  > v2.5.7以降、 StarRocks は、テーブルの作成時またはパーティションの追加時に、バケット数 (BUCKETS) を自動的に設定できます。 バケット数を手動で設定する必要はなくなりました。 詳細については、 [バケット数の設定] (../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets) を参照してください。

- **ランダムバケット法**:

  ランダムバケット法を選択し、 StarRocks がバケット数を自動的に設定できるようにする場合、 `distribution_desc` を指定する必要はありません。 ただし、バケット数を手動で設定する場合は、次の構文を参照してください。

```SQL
  DISTRIBUTED BY RANDOM BUCKETS <bucket_number>
  ```

> **CAUTION**
>
> ランダムバケット法を使用する非同期マテリアライズドビューは、 Colocation Group に割り当てることはできません。

詳細については、[ランダムバケット法](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31) を参照してください。

**refresh_moment** (オプション)

マテリアライズドビューのリフレッシュ時点。デフォルト値: `IMMEDIATE`。有効な値:

- `IMMEDIATE`: 非同期マテリアライズドビューの作成直後にリフレッシュします。
- `DEFERRED`: 非同期マテリアライズドビューは、作成後にリフレッシュされません。マテリアライズドビューを手動でリフレッシュするか、定期的なリフレッシュタスクをスケジュールできます。

**refresh_scheme** (オプション)

> **NOTE**
>
> - 非同期マテリアライズドビューを作成する際は、`distribution_desc` または `refresh_scheme` 、あるいはその両方を指定する必要があります。
> - 外部テーブルのマテリアライズドビューは、**ベーステーブルのデータ変更によってトリガーされる** 自動リフレッシュをサポートしていません。非同期の**固定間隔**リフレッシュと手動リフレッシュのみをサポートします。

非同期マテリアライズドビューのリフレッシュ戦略。有効な値:

- `ASYNC`: 自動リフレッシュモード。ベーステーブルのデータが変更されるたびに、マテリアライズドビューが自動的にリフレッシュされます。
- `ASYNC [START (<start_time>)] EVERY(INTERVAL <interval>)`: 定期リフレッシュモード。マテリアライズドビューは、定義された間隔で定期的にリフレッシュされます。間隔は、`EVERY (interval n day/hour/minute/second)` のように指定でき、次の単位を使用します: `DAY`、`HOUR`、`MINUTE`、および `SECOND`。デフォルト値は `10 MINUTE` です。リフレッシュ開始時刻を `START('yyyy-MM-dd hh:mm:ss')` としてさらに指定できます。開始時刻が指定されていない場合は、現在の時刻が使用されます。例: `ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)`。
- `MANUAL`: 手動リフレッシュモード。手動でリフレッシュタスクをトリガーしない限り、マテリアライズドビューはリフレッシュされません。

このパラメータが指定されていない場合、デフォルト値の `MANUAL` が使用されます。

**partition_expression** (オプション)

非同期マテリアライズドビューのパーティション化戦略。このパラメータが指定されていない場合、デフォルトではパーティション化戦略は採用されません。

有効な値:

- `partition_column`: パーティション化に使用される列。式 `PARTITION BY dt` は、`dt` 列に従ってマテリアライズドビューをパーティション化することを意味します。
- `date_function_expr`: パーティション化に使用される日付関数を含む複雑な式。
  - `date_trunc` 関数: 時間単位を切り捨てるために使用される関数。`PARTITION BY date_trunc("MONTH", dt)` は、`dt` 列が月単位に切り捨てられてパーティション化されることを意味します。`date_trunc` 関数は、`YEAR`、`MONTH`、`DAY`、`HOUR`、および `MINUTE` を含む単位への時間の切り捨てをサポートします。
  - `str2date` 関数: ベーステーブルの文字列型のパーティションを日付型に変換するために使用される関数。`PARTITION BY str2date(dt, "%Y%m%d")` は、`dt` 列が日付形式 `"%Y%m%d"` の STRING 型の日付であることを意味します。`str2date` 関数は多くの日付形式をサポートしており、詳細については [str2date](../../sql-functions/date-time-functions/str2date.md) を参照してください。v3.1.4 以降でサポートされています。
  - `time_slice` 関数: v3.1 以降では、これらの関数を使用して、指定された時間粒度に基づいて、指定された時間を時間間隔の開始または終了に変換できます。たとえば、`PARTITION BY date_trunc("MONTH", time_slice(dt, INTERVAL 7 DAY))` などです。ここで、time_slice は date_trunc よりも細かい粒度を持つ必要があります。これらを使用して、パーティションキーよりも細かい粒度で GROUP BY 列を指定できます。たとえば、`GROUP BY time_slice(dt, INTERVAL 1 MINUTE) PARTITION BY date_trunc('DAY', ts)` などです。

v3.5.0 以降、非同期マテリアライズドビューは複数列のパーティション式をサポートします。マテリアライズドビューに複数のパーティション列を指定して、ベーステーブルのパーティション列の全部または一部をマップできます。

**複数列のパーティション式に関する注意点**:

- 現在、マテリアライズドビューの複数列パーティションは、ベーステーブルのパーティション列に直接マップすることのみ可能です。ベーステーブルのパーティション列に関数や式を使用したマッピングはサポートされていません。
- Iceberg のパーティション式は `transform` 関数をサポートしているため、Iceberg のパーティション式を StarRocks のマテリアライズドビューのパーティション式にマッピングする場合は、追加の処理が必要です。マッピングの関係は次のとおりです。

  | Iceberg Transform | Iceberg partition expression   | Materialized view partition expression   |
  | ----------------- | ------------------------------ | ---------------------------------------- |
  | Identity          | `<col>`                        | `<col>`                                  |
  | hour              | `hour(<col>)`                  | `date_trunc('hour', <col>)`              |
  | day               | `day(<col>)`                   | `date_trunc('day', <col>)`               |
  | month             | `month(<col>)`                 | `date_trunc('month', <col>)`              |
  | year              | `year(<col>)`                  | `date_trunc('year', <col>)`              |
  | bucket            | `bucket(<col>, <n>)`           | サポートされていません                         |
  | truncate          | `truncate(<col>)`              | サポートされていません                         |

- Iceberg 以外のパーティション列の場合、パーティション式の計算が含まれない場合は、追加のパーティション式処理は必要ありません。直接マップできます。

複数列のパーティション式の詳細な手順については、[例 -5](#examples) を参照してください。

**order_by_expression** (オプション)

非同期マテリアライズドビューのソートキー。ソートキーを指定しない場合、StarRocks は SELECT 列からプレフィックス列の一部をソートキーとして選択します。たとえば、`select a, b, c, d` では、ソートキーは `a` と `b` になります。このパラメータは StarRocks v3.0 以降でサポートされています。

> **NOTE**
> マテリアライズドビューでは、`ORDER BY` には 2 つの異なる用途があります。
> - CREATE MATERIALIZED VIEW ステートメントの `ORDER BY` は、マテリアライズドビューのソートキーを定義します。これは、ソートキーに基づくクエリの高速化に役立ちます。これは、マテリアライズドビューの SPJG ベースの透過的な高速化機能には影響しませんが、マテリアライズドビューのクエリ結果のグローバルな順序付けを保証するものではありません。
> - マテリアライズドビューのクエリ定義の `ORDER BY` は、クエリ結果のグローバルな順序付けを保証しますが、マテリアライズドビューが SPJG ベースの透過的なクエリの書き換えに使用されるのを防ぎます。したがって、MV がクエリの書き換えに使用される場合は、マテリアライズドビューのクエリ定義で `ORDER BY` を使用しないでください。

**INDEX** (オプション)

非同期マテリアライズドビューは、クエリパフォーマンスを高速化するために ​Bitmap​ および ​BloomFilter​ インデックスをサポートしており、その使用法は通常のテーブルと同じです。​Bitmap​ および ​BloomFilter​ インデックスの使用例と情報については、[Bitmap Index](../../../table_design/indexes/Bitmap_index.md) および [Bloom filter Index](../../../table_design/indexes/Bloomfilter_index.md) を参照してください。

Bitmap インデックスの使用:

```sql
-- Create an index  
CREATE INDEX <index_name> ON <mv_name>(<column_name>) USING BITMAP COMMENT '<comment>';  

-- Check index creation progress  
SHOW ALTER TABLE COLUMN;  

-- View indexes  
SHOW INDEXES FROM <mv_name>;  

-- Drop an index  
DROP INDEX <index_name> ON <mv_name>;  
```

Bloom Filter インデックスの使用：

```sql
-- Create an index  
ALTER MATERIALIZED VIEW <mv_name> SET ("bloom_filter_columns" = "<col1,col2,col3,...>");  

-- View indexes  
SHOW CREATE MATERIALIZED VIEW <mv_name>;  

-- Drop an index  
ALTER MATERIALIZED VIEW <mv_name> SET ("bloom_filter_columns" = "");  
```

**PROPERTIES** （オプション）

非同期マテリアライズドビューのプロパティ。既存のマテリアライズドビューのプロパティは、[ALTER MATERIALIZED VIEW](ALTER_MATERIALIZED_VIEW.md) を使用して変更できます。

- `session.`: マテリアライズドビューのセッション変数関連のプロパティを変更する場合は、プロパティに `session.` というプレフィックスを追加する必要があります。例：`session.insert_timeout`。セッション以外のプロパティ（例：`mv_rewrite_staleness_second`）には、プレフィックスを指定する必要はありません。
- `replication_num`: 作成するマテリアライズドビューのレプリカ数。
- `storage_medium`: ストレージメディアのタイプ。有効な値：`HDD` および `SSD`。
- `storage_cooldown_time`: パーティションのストレージクールダウン時間。HDD と SSD の両方のストレージメディアを使用する場合、SSD ストレージ内のデータは、このプロパティで指定された時間が経過すると HDD ストレージに移動されます。形式：「yyyy-MM-dd HH:mm:ss」。指定された時間は、現在の時間よりも後である必要があります。このプロパティが明示的に指定されていない場合、ストレージのクールダウンはデフォルトでは実行されません。
- `bloom_filter_columns`: ブルームフィルターインデックスを有効にするカラム名の配列。ブルームフィルターインデックスの詳細については、[ブルームフィルターインデックス](../../../table_design/indexes/Bloomfilter_index.md) を参照してください。
- `partition_ttl`: パーティションの Time-To-Live (TTL)。指定された時間範囲内のデータを持つパーティションが保持されます。期限切れのパーティションは自動的に削除されます。単位：`YEAR`、`MONTH`、`DAY`、`HOUR`、および `MINUTE`。たとえば、このプロパティを `2 MONTH` として指定できます。このプロパティは `partition_ttl_number` よりも推奨されます。v3.1.5 以降でサポートされています。
- `partition_ttl_number`: 保持する最新のマテリアライズドビューのパーティション数。開始時刻が現在の時刻よりも前のパーティションの場合、これらのパーティションの数がこの値を超えると、古いパーティションが削除されます。StarRocks は、FE 構成項目 `dynamic_partition_check_interval_seconds` で指定された時間間隔に従ってマテリアライズドビューのパーティションを定期的にチェックし、期限切れのパーティションを自動的に削除します。[動的パーティション化](../../../table_design/data_distribution/dynamic_partitioning.md) 戦略を有効にした場合、事前に作成されたパーティションはカウントされません。値が `-1` の場合、マテリアライズドビューのすべてのパーティションが保持されます。デフォルト：`-1`。
- `partition_refresh_number`: 1 回のリフレッシュでリフレッシュするパーティションの最大数。リフレッシュするパーティションの数がこの値を超えると、StarRocks はリフレッシュタスクを分割し、バッチで完了させます。前のバッチのパーティションが正常にリフレッシュされた場合にのみ、StarRocks は次のバッチのパーティションのリフレッシュを続行し、すべてのパーティションがリフレッシュされるまで続行します。いずれかのパーティションのリフレッシュに失敗した場合、後続のリフレッシュタスクは生成されません。値が `-1` の場合、リフレッシュタスクは分割されません。デフォルト値は v3.3 以降、`-1` から `1` に変更されました。これは、StarRocks がパーティションを 1 つずつリフレッシュすることを意味します。
- `partition_refresh_strategy`: 1 回のリフレッシュ操作中のマテリアライズドビューのリフレッシュ戦略。`adaptive` に設定すると、リフレッシュするパーティションの数は、ベーステーブルのパーティションのデータ量に基づいて自動的に決定され、リフレッシュ効率が大幅に向上します。このプロパティが指定されていない場合、デフォルトの戦略は `strict` です。これは、1 回の操作でリフレッシュされるパーティションの数が `partition_refresh_number` によって厳密に制御されることを意味します。
- `excluded_trigger_tables`: マテリアライズドビューのベーステーブルがここにリストされている場合、ベーステーブルのデータが変更されても、自動リフレッシュタスクはトリガーされません。このパラメータは、ロードトリガーリフレッシュ戦略にのみ適用され、通常はプロパティ `auto_refresh_partitions_limit` と組み合わせて使用​​されます。形式：`[db_name.]table_name`。値が空の文字列の場合、すべてのベーステーブルのデータ変更により、対応するマテリアライズドビューのリフレッシュがトリガーされます。デフォルト値は空の文字列です。

- `excluded_refresh_tables`: このプロパティにリストされているベーステーブルは、データが変更されてもマテリアライズドビューに更新されません。形式：`[db_name.]table_name`。デフォルト値は空の文字列です。値が空の文字列の場合、すべてのベーステーブルのデータ変更により、対応するマテリアライズドビューのリフレッシュがトリガーされます。

  :::tip
  `excluded_trigger_tables` と `excluded_refresh_tables` の違いは次のとおりです。
  - `excluded_trigger_tables` は、リフレッシュをトリガーするかどうかを制御し、リフレッシュに参加するかどうかは制御しません。たとえば、パーティション化されたマテリアライズドビューは、2 つのパーティション化されたテーブル A と B をジョインすることによって取得され、2 つのテーブル A と B のパーティションは 1 対 1 で対応します。`excluded_trigger_table` にはテーブル A が含まれています。一定期間、テーブル A はパーティション `[1,2,3]` を更新しましたが、`excluded_trigger_table` であるため、マテリアライズドビューのリフレッシュはトリガーされません。このとき、テーブル B はパーティション `[3]` を更新し、マテリアライズドビューはリフレッシュをトリガーし、3 つのパーティション `[1, 2, 3]` をリフレッシュします。ここでは、`excluded_trigger_table` がリフレッシュをトリガーするかどうかのみを制御することがわかります。テーブル A の更新はマテリアライズドビューのリフレッシュをトリガーできませんが、テーブル B の更新がマテリアライズドビューのリフレッシュをトリガーすると、テーブル A によって更新されたパーティションもリフレッシュタスクに追加されます。
  - `excluded_refresh_tables` は、リフレッシュに参加するかどうかを制御します。上記の例で、テーブル A が `excluded_trigger_table` と `excluded_refresh_tables` の両方に存在する場合、テーブル B の更新がマテリアライズドビューのリフレッシュをトリガーすると、パーティション `[3]` のみがリフレッシュされます。
  :::

- `auto_refresh_partitions_limit`: マテリアライズドビューのリフレッシュがトリガーされたときにリフレッシュする必要がある最新のマテリアライズドビューのパーティション数。このプロパティを使用して、リフレッシュ範囲を制限し、リフレッシュコストを削減できます。ただし、すべてのパーティションがリフレッシュされるわけではないため、マテリアライズドビューのデータがベーステーブルと一致しない場合があります。デフォルト：`-1`。値が `-1` の場合、すべてのパーティションがリフレッシュされます。値が正の整数 N の場合、StarRocks は既存のパーティションを時系列順にソートし、現在のパーティションと N-1 個の最新のパーティションをリフレッシュします。パーティションの数が N より少ない場合、StarRocks は既存のすべてのパーティションをリフレッシュします。マテリアライズドビューに事前に作成された動的パーティションがある場合、StarRocks は事前に作成されたすべてのパーティションをリフレッシュします。
- `mv_rewrite_staleness_second`: マテリアライズドビューの最後のリフレッシュがこのプロパティで指定された時間間隔内にある場合、ベーステーブルのデータが変更されたかどうかに関係なく、このマテリアライズドビューをクエリの書き換えに直接使用できます。最後のリフレッシュがこの時間間隔より前の場合、StarRocks はベーステーブルが更新されたかどうかを確認して、マテリアライズドビューをクエリの書き換えに使用できるかどうかを判断します。単位：秒。このプロパティは v3.0 以降でサポートされています。
- `colocate_with`: 非同期マテリアライズドビューの Colocation Group。詳細については、[Colocate Join](../../../using_starrocks/Colocate_join.md) を参照してください。このプロパティは v3.0 以降でサポートされています。
- `unique_constraints` および `foreign_key_constraints`: View Delta Join シナリオでクエリを書き換えるために非同期マテリアライズドビューを作成する場合の、ユニークキー制約と外部キー制約。[非同期マテリアライズドビュー - View Delta Join シナリオでのクエリの書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md) を参照してください。このプロパティは v3.0 以降でサポートされています。
- `excluded_refresh_tables`: このプロパティにリストされているベーステーブルは、データが変更されてもマテリアライズドビューへのデータリフレッシュをトリガーしません。このプロパティは通常、`excluded_trigger_tables` プロパティと一緒に使用されます。形式：`[db_name.]table_name`。デフォルト値は空の文字列です。値が空の文字列の場合、すべてのベーステーブルのデータ変更により、対応するマテリアライズドビューのリフレッシュがトリガーされます。
- `task_priority`: リフレッシュタスクの優先度。このプロパティは通常、`refresh_scheme` `ASYNC` と一緒に使用されます。デフォルト値は 90 で、値は 0〜100 である必要があります。

  > **注意**
  >
  > ユニークキー制約と外部キー制約は、クエリの書き換えにのみ使用されます。テーブルにデータをロードするときに、外部キー制約のチェックは保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。

- `resource_group`: マテリアライズドビューのリフレッシュタスクが属するリソースグループ。このプロパティのデフォルト値は `default_mv_wg` で、これはマテリアライズドビューのリフレッシュ専用にシステム定義されたリソースグループです。`default_mv_wg` の `cpu_core_limit` は `1`、`mem_limit` は `0.8` です。リソースグループの詳細については、[リソースグループ](../../../administration/management/resource_management/resource_group.md) を参照してください。
- `query_rewrite_consistency`: 非同期マテリアライズドビューのクエリ書き換えルール。このプロパティは v3.2 以降でサポートされています。有効な値：
  - `disable`: 非同期マテリアライズドビューの自動クエリ書き換えを無効にします。
  - `checked` (デフォルト値): マテリアライズドビューがタイムリー性の要件を満たしている場合にのみ、自動クエリ書き換えを有効にします。つまり：
    - `mv_rewrite_staleness_second` が指定されていない場合、マテリアライズドビューは、そのデータがすべてのベーステーブルのデータと一致する場合にのみ、クエリ書き換えに使用できます。
    - `mv_rewrite_staleness_second` が指定されている場合、マテリアライズドビューは、最後のリフレッシュが鮮度時間間隔内にある場合に、クエリ書き換えに使用できます。
  - `loose`: 自動クエリ書き換えを直接有効にし、整合性チェックは必要ありません。
  - `force_mv`: v3.5.0 以降、StarRocks のマテリアライズドビューは共通パーティション式 TTL をサポートしています。`force_mv` セマンティクスは、このシナリオ専用に設計されています。このセマンティクスを有効にすると：
    - マテリアライズドビューに `partition_retention_condition` プロパティがない場合、ベーステーブルが更新されたかどうかに関係なく、常にクエリ書き換えにマテリアライズドビューの使用を強制します。
    - マテリアライズドビューに `partition_retention_condition` プロパティがある場合：
      - TTL 範囲内のパーティションの場合、ベーステーブルが更新されたかどうかに関係なく、マテリアライズドビューに基づくクエリ書き換えは常に利用可能です。
      - TTL 範囲外のパーティションの場合、ベーステーブルが更新されたかどうかに関係なく、マテリアライズドビューとベーステーブル間の Union 補正が必要です。

    たとえば、マテリアライズドビューに `partition_retention_condition` プロパティが定義されており、`20241131` のパーティションが期限切れになっているが、`20241203` のベーステーブルデータが更新されている一方で、`20241203` のマテリアライズドビューデータがリフレッシュされていない場合、`query_rewrite_consistency` プロパティが `force_mv` に設定されている場合は、以下が適用されます。
    - マテリアライズドビューは、`partition_retention_condition` で定義された TTL 範囲内のパーティション（たとえば、`20241201` から `20241203` まで）に対するクエリが常に透過的に書き換えられることを保証します。
    - `partition_retention_condition` 範囲外のパーティションに対するクエリの場合、マテリアライズドビューとベーステーブルの Union に基づいて自動的に補正が行われます。

    `force_mv` セマンティクスと `partition_retention_condition` の詳細な手順については、[例 6](#examples) を参照してください。

- `storage_volume`: 共有データクラスタを使用している場合に、作成する非同期マテリアライズドビューの保存に使用するストレージボリュームの名前。このプロパティは v3.1 以降でサポートされています。このプロパティが指定されていない場合、デフォルトのストレージボリュームが使用されます。例：`"storage_volume" = "def_volume"`。
- `force_external_table_query_rewrite`: external catalog ベースのマテリアライズドビューのクエリ書き換えを有効にするかどうか。このプロパティは v3.2 以降でサポートされています。有効な値：
  - `true` (v3.3 以降のデフォルト値): external catalog ベースのマテリアライズドビューのクエリ書き換えを有効にします。
  - `false`: external catalog ベースのマテリアライズドビューのクエリ書き換えを無効にします。

  ベーステーブルと external catalog ベースのマテリアライズドビューの間で強力なデータ整合性が保証されないため、この機能はデフォルトで `false` に設定されています。この機能を有効にすると、`query_rewrite_consistency` で指定されたルールに従って、マテリアライズドビューがクエリ書き換えに使用されます。
- `enable_query_rewrite`: クエリ書き換えにマテリアライズドビューを使用するかどうか。マテリアライズドビューが多い場合、マテリアライズドビューに基づくクエリ書き換えは、オプティマイザの時間消費に影響を与える可能性があります。このプロパティを使用すると、マテリアライズドビューをクエリ書き換えに使用できるかどうかを制御できます。この機能は v3.3.0 以降でサポートされています。有効な値：
  - `default` (デフォルト): システムはマテリアライズドビューに対してセマンティックチェックを実行しませんが、SPJG タイプのマテリアライズドビューのみをクエリ書き換えに使用できます。テキストベースのクエリ書き換えが有効になっている場合、SPJG 以外のタイプのマテリアライズドビューもクエリ書き換えに使用できることに注意してください。
  - `true`: システムは、マテリアライズドビューの作成または変更時にセマンティックチェックを実行します。マテリアライズドビューがクエリ書き換えの対象にならない場合（つまり、マテリアライズドビューの定義が SPJG タイプのクエリではない場合）、エラーが返されます。
  - `false`: マテリアライズドビューはクエリ書き換えに使用されません。
- [プレビュー] `transparent_mv_rewrite_mode`: **マテリアライズドビューに対して直接実行されるクエリ** の透過的な書き換えモードを指定します。この機能は v3.3.0 以降でサポートされています。有効な値：
  - `false` (デフォルト、以前のバージョンの動作と互換性があります): マテリアライズドビューに対して直接実行されるクエリは書き換えられず、マテリアライズドビューに存在するデータのみが返されます。これらのクエリの結果は、マテリアライズドビューのリフレッシュステータス（データ整合性）に応じて、マテリアライズドビューの定義に基づくクエリの結果と異なる場合があります。
  - `true`: マテリアライズドビューに対して直接実行されるクエリは書き換えられ、最新のデータが返されます。これは、マテリアライズドビューの定義クエリの結果と一致します。マテリアライズドビューが非アクティブであるか、透過的なクエリ書き換えをサポートしていない場合、これらのクエリはマテリアライズドビューの定義クエリとして実行されることに注意してください。
  - `transparent_or_error`: マテリアライズドビューに対して直接実行されるクエリは、対象となる場合は常に書き換えられます。マテリアライズドビューが非アクティブであるか、透過的なクエリ書き換えをサポートしていない場合、これらのクエリはエラーで返されます。
  - `transparent_or_default`: マテリアライズドビューに対して直接実行されるクエリは、対象となる場合は常に書き換えられます。マテリアライズドビューが非アクティブであるか、透過的なクエリ書き換えをサポートしていない場合、これらのクエリはマテリアライズドビューに存在するデータで返されます。
- `partition_retention_condition`: v3.5.0 以降、StarRocks のマテリアライズドビューは共通パーティション式 TTL をサポートしています。このプロパティは、動的に保持されるパーティションを宣言する式です。式内の条件を満たさないパーティションは定期的に削除されます。例：`'partition_retention_condition' = 'dt >= CURRENT_DATE() - INTERVAL 3 MONTH'`。
  - 式には、パーティションカラムと定数のみを含めることができます。非パーティションカラムはサポートされていません。
  - 共通パーティション式は、リストパーティションとレンジパーティションに異なる方法で適用されます。
    - リストパーティションを持つマテリアライズドビューの場合、StarRocks は共通パーティション式でフィルタリングされたパーティションの削除をサポートします。
    - レンジパーティションを持つマテリアライズドビューの場合、StarRocks は FE のパーティションプルーニング機能を使用してパーティションをフィルタリングおよび削除することしかできません。パーティションプルーニングでサポートされていない述語に対応するパーティションは、フィルタリングおよび削除できません。

  `force_mv` セマンティクスと `partition_retention_condition` の詳細な手順については、[例 6](#examples) を参照してください。

- `refresh_mode`: マテリアライズドビューのリフレッシュ方法を制御します。StarRocks v4.1 で導入されました。有効な値：

  - `PCT`: (デフォルト) パーティション化されたマテリアライズドビューの場合、データが変更されると、影響を受けるパーティションのみがリフレッシュされ、そのパーティションの結果の一貫性が保証されます。パーティション化されていないマテリアライズドビューの場合、ベーステーブルのデータが変更されると、マテリアライズドビューの完全なリフレッシュがトリガーされます。
  - `AUTO`: 可能な限り増分リフレッシュを使用しようとします。マテリアライズドビューのクエリ定義が増分リフレッシュをサポートしていない場合、その操作では自動的に `PCT` モードにフォールバックします。PCT リフレッシュ後、条件が許せば、将来のリフレッシュは増分リフレッシュに戻る可能性があります。
  - `INCREMENTAL`: 増分リフレッシュのみが実行されるようにします。マテリアライズドビューがその定義に基づいて増分リフレッシュをサポートしていない場合、または非増分データが発生した場合、作成またはリフレッシュは失敗します。
  - `FULL`: マテリアライズドビューが増分リフレッシュまたはパーティションレベルのリフレッシュをサポートしているかどうかに関係なく、毎回すべてのデータの完全なリフレッシュを強制します。

<MVWarehouse />

**query_statement** （必須）

非同期マテリアライズドビューを作成するためのクエリステートメント。v3.1.6 以降、StarRocks は共通テーブル式 (CTE) を使用した非同期マテリアライズドビューの作成をサポートしています。

非同期マテリアライズドビューのクエリ

非同期マテリアライズドビューは物理テーブルです。**非同期マテリアライズドビューに直接データをロードできない**ことを除き、通常テーブルと同様に操作できます。

### 非同期マテリアライズドビューによる自動クエリの書き換え

StarRocks v2.5 では、SPJG タイプの非同期マテリアライズドビューに基づく、自動的かつ透過的なクエリの書き換えをサポートしています。SPJG タイプのマテリアライズドビューとは、プランに Scan、Filter、Project、Aggregate タイプの operator のみが含まれるマテリアライズドビューを指します。SPJG タイプのマテリアライズドビューのクエリの書き換えには、単一テーブルのクエリの書き換え、Join クエリの書き換え、集計クエリの書き換え、Union クエリの書き換え、およびネストされたマテリアライズドビューに基づくクエリの書き換えが含まれます。

詳細については、[非同期マテリアライズドビュー - 非同期マテリアライズドビューによるクエリの書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md) を参照してください。

### サポートされるデータ型

- StarRocks の default catalog に基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています。

  - **Date**: DATE、DATETIME
  - **String**: CHAR、VARCHAR
  - **Numeric**: BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMAL、PERCENTILE
  - **Semi-structured**: ARRAY、JSON、MAP (v3.1 以降)、STRUCT (v3.1 以降)
  - **Other**: BITMAP、HLL

> **NOTE**
>
> BITMAP、HLL、および PERCENTILE は v2.4.5 以降でサポートされています。

- StarRocks の external catalog に基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています。

  - Hive catalog

    - **Numeric**: INT/INTEGER、BIGINT、DOUBLE、FLOAT、DECIMAL
    - **Date**: TIMESTAMP
    - **String**: STRING、VARCHAR、CHAR
    - **Semi-structured**: ARRAY

  - Hudi catalog

    - **Numeric**: BOOLEAN、INT、LONG、FLOAT、DOUBLE、DECIMAL
    - **Date**: DATE、TimeMillis/TimeMicros、TimestampMillis/TimestampMicros
    - **String**: STRING
    - **Semi-structured**: ARRAY

  - Iceberg catalog

    - **Numeric**: BOOLEAN、INT、LONG、FLOAT、DOUBLE、DECIMAL(P, S)
    - **Date**: DATE、TIME、TIMESTAMP
    - **String**: STRING、UUID、FIXED(L)、BINARY
    - **Semi-structured**: LIST

StarRocks v4.1 では、マテリアライズドビューのリフレッシュ動作を制御するために `refresh_mode` パラメータが導入されました。各マテリアライズドビューの作成時に `refresh_mode` を指定できます。マテリアライズドビューの作成時に `refresh_mode` が設定されていない場合、システムは `default_mv_refresh_mode` パラメータによって制御されるデフォルト値 `PCT` を使用します (デフォルト: `pct`)。以下の使用に関するガイダンスに注意してください。

- `refresh_mode` を調整する際には制限があります。
  - レガシーなマテリアライズドビュー (例えば、タイプが `PCT` のもの) を `AUTO` または `INCREMENTAL` リフレッシュモードを使用するように変更することはできません。これを行うには、マテリアライズドビューを再構築する必要があります。
  - `AUTO` または `INCREMENTAL` タイプからマテリアライズドビューを変更する場合、システムは増分リフレッシュが可能かどうかを確認します。不可能な場合、操作は失敗します。
- 増分マテリアライズドビューは、パーティションリフレッシュの指定をサポートしていません。
  - `INCREMENTAL` マテリアライズドビューの場合、パーティションリフレッシュを試みると例外がスローされます。
  - `AUTO` マテリアライズドビューの場合、StarRocks はリフレッシュ操作のために自動的に `PCT` モードに切り替わります。

### サポートされている増分オペレーター

増分リフレッシュは、ベーステーブルに対する追加のみのオペレーションのみをサポートします。`UPDATE`、`MERGE`、`OVERWRITE`などのサポートされていないオペレーションが実行された場合：
- `refresh_mode`が`INCREMENTAL`に設定されている場合、マテリアライズドビューのリフレッシュは失敗します。
- `refresh_mode`が`AUTO`に設定されている場合、システムは自動的にリフレッシュのために`PCT`モードにフォールバックします。

現在、増分リフレッシュでは次のオペレーターがサポートされています。

| Operator                   | Incremental Refresh Support                                                                                               |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------|
| Select                     | サポート                                                                                                                |
| From `<Table>`             | Iceberg/Paimon テーブルでのみサポートされます。他のテーブルタイプではまだ利用できません。                                              |
| Filter                     | サポート                                                                                                                |
| Aggregate with Group By    | サポート <ul><li>`distinct`を使用した集計関数はまだサポートされていません。</li><li>GROUP BYなしの集計はまだサポートされていません。</li></ul> |
| Inner Join                 | サポート                                                                                                                |
| Union All                  | サポート                                                                                                                |
| Left/Right/Full Outer Join | まだサポートされていません                                                                                                        |

:::note
上記のオペレーターは一般的に増分リフレッシュをサポートしていますが、特定のオペレーターの組み合わせには現在制限があります。
- Join 後の集計と UNION 後の集計では、増分計算がサポートされています。
- ただし、集計後の Join または集計後の UNION ALL を実行する場合、増分計算は**サポートされていません**。
:::

### 例

```SQL
CREATE MATERIALIZED VIEW test_mv1 PARTITION BY dt 
REFRESH DEFERRED MANUAL 
properties
(
    "refresh_mode" = "INCREMENTAL"
)
AS SELECT 
  t1.dt, t1.col1 as col11, t2.col1 as col21, t3.col1 as col31, t4.col1 as col41, t5.col1 as col51,
  sum(t1.col2) as col12, sum(t2.col2) as col22, sum(t3.col2) as col32, sum(t4.col2) as col42, sum(t5.col2) as col52,
  avg(t1.col2) as col13, avg(t2.col2) as col23, avg(t3.col2) as col33, avg(t4.col2) as col43, avg(t5.col2) as col53,
  min(t1.col2) as col14, min(t2.col2) as col24, min(t3.col2) as col34, min(t4.col2) as col44, min(t5.col2) as col54,
  max(t1.col2) as col15, max(t2.col2) as col25, max(t3.col2) as col35, max(t4.col2) as col45, max(t5.col2) as col55,
  count(t1.col2) as col16, count(t2.col2) as col26, count(t3.col2) as col36, count(t4.col2) as col46, count(t5.col2) as col56,
  approx_count_distinct(t1.col2) as col17, approx_count_distinct(t2.col2) as col27, approx_count_distinct(t3.col2) as col37, approx_count_distinct(t4.col2) as col47, approx_count_distinct(t5.col2) as col57
FROM 
  iceberg_catalog.iceberg_test_dbt1 
  JOIN iceberg_catalog.iceberg_test_dbt2 ON t1.dt = t2.dt
  JOIN iceberg_catalog.iceberg_test_dbt3 ON t1.dt = t3.dt
  JOIN iceberg_catalog.iceberg_test_dbt4 ON t1.dt = t4.dt
  JOIN iceberg_catalog.iceberg_test_dbt5 ON t1.dt = t5.dt
 GROUP BY t1.dt, t1.col1, t2.col1, t3.col1, t4.col1, t5.col1;
 
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;
```

`information_schema.task_runs` の `EXTRA_MESSAGE` カラムに `refreshMode` フィールドが追加され、`TaskRun` のリフレッシュモードを示すようになりました。詳細については、[materialized_view_task_run_details](../../../using_starrocks/async_mv/materialized_view_task_run_details.md) を参照してください。

```SQL
mysql> select * from information_schema.task_runs order by CREATE_TIME desc limit 1\G;
     QUERY_ID: 0199f00e-2152-70a8-83da-26d6a8321ac6
    TASK_NAME: mv-78190
  CREATE_TIME: 2025-10-17 10:44:41
  FINISH_TIME: 2025-10-17 10:44:44
        STATE: SUCCESS
      CATALOG: NULL
     DATABASE: test_mv_async_db_621c29ff_ab02_11f0_9e41_00163e09349d
   DEFINITION: insert overwrite `test_mv_case_iceberg_transform_day_44` SELECT `t1`.`id`, `t1`.`v1`, `t1`.`v2`, `t1`.`dt` FROM `iceberg_catalog_621c2b62_ab02_11f0_a703_00163e09349d`.`iceberg_db_621c2bc9_ab02_11f0_885d_00163e09349d`.`t1` WHERE (`t1`.`id` > 1) AND (`t1`.`dt` >= '2025-06-01')
  EXPIRE_TIME: 2025-10-24 10:44:41
   ERROR_CODE: 0
ERROR_MESSAGE: NULL
     PROGRESS: 100%
EXTRA_MESSAGE: {"forceRefresh":false,"mvPartitionsToRefresh":["p20250718000000","p20250715000000","p20250721000000","p20250615000000","p20250618000000","p20250524000000","p20250621000000","p20250518000000"],"refBasePartitionsToRefreshMap":{"t1":["p20250718000000","p20250721000000","p20250618000000","p20250524000000","p20250621000000","p20250518000000","p20250715000000","p20250615000000","pNULL","p20250521000000","p20250624000000","p20250724000000","p20250515000000"]},"basePartitionsToRefreshMap":{},"processStartTime":1760669082430,"executeOption":{"priority":80,"taskRunProperties":{"FORCE":"false","mvId":"78190","warehouse":"default_warehouse"},"isMergeRedundant":false,"isManual":true,"isSync":true,"isReplay":false},"planBuilderMessage":{},"refreshMode":"INCREMENTAL"}
   PROPERTIES: {"FORCE":"false","mvId":"78190","warehouse":"default_warehouse"}
       JOB_ID: 0199f00e-2152-76b0-987c-76a9a19e77f9
```

## Usage notes

- 現在のバージョンの StarRocks では、複数のマテリアライズドビューを同時に作成することはサポートされていません。新しいマテリアライズドビューは、前のものが完了した場合にのみ作成できます。

- 同期マテリアライズドビューについて：

  - 同期マテリアライズドビューは、単一カラムに対する集計関数のみをサポートします。`sum(a+b)` の形式のクエリステートメントはサポートされていません。
  - 同期マテリアライズドビューは、ベーステーブルの各カラムに対して 1 つの集計関数のみをサポートします。`select sum(a), min(a) from table` のようなクエリステートメントはサポートされていません。
  - 集計関数を使用して同期マテリアライズドビューを作成する場合は、GROUP BY 句を指定し、SELECT で少なくとも 1 つの GROUP BY カラムを指定する必要があります。
  - 同期マテリアライズドビューは、JOIN などの句や、GROUP BY の HAVING 句をサポートしていません。
  - ALTER TABLE DROP COLUMN を使用してベーステーブルの特定のカラムを削除する場合、ベーステーブルのすべての同期マテリアライズドビューに、削除されたカラムが含まれていないことを確認する必要があります。そうでない場合、削除操作は失敗します。カラムを削除する前に、そのカラムを含むすべての同期マテリアライズドビューを最初に削除する必要があります。
  - テーブルに対して同期マテリアライズドビューを多数作成すると、データロードの効率に影響します。データがベーステーブルにロードされると、同期マテリアライズドビューとベーステーブルのデータが同期的に更新されます。ベーステーブルに `n` 個の同期マテリアライズドビューが含まれている場合、ベーステーブルにデータをロードする効率は、`n` 個のテーブルにデータをロードする効率とほぼ同じです。

- ネストされた非同期マテリアライズドビューについて：

  - 各マテリアライズドビューのリフレッシュ戦略は、対応するマテリアライズドビューにのみ適用されます。
  - 現在、StarRocks はネスティングレベルの数を制限していません。本番環境では、ネスティングレイヤーの数が 3 つを超えないようにすることをお勧めします。

- external catalog 非同期マテリアライズドビューについて：

  - external catalog マテリアライズドビューは、非同期固定間隔リフレッシュと手動リフレッシュのみをサポートします。
  - マテリアライズドビューと external catalog のベーステーブルの間で厳密な整合性は保証されません。
  - 現在、外部リソースに基づいてマテリアライズドビューを構築することはサポートされていません。
  - 現在、StarRocks は external catalog のベーステーブルデータが変更されたかどうかを認識できないため、ベーステーブルがリフレッシュされるたびに、すべてのパーティションがデフォルトでリフレッシュされます。[REFRESH MATERIALIZED VIEW](REFRESH_MATERIALIZED_VIEW.md) を使用して、一部のパーティションのみを手動でリフレッシュできます。

## 例

### 同期マテリアライズドビューの例

ベーステーブルのスキーマは次のとおりです。

```Plain Text
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```

例1：元のテーブル（k1、k2）の列のみを含む同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2 as
select k1, k2 from duplicate_table;
```

マテリアライズドビューには、集計なしで k1 と k2 の 2 つのカラムのみが含まれています。

```plain text
+-----------------+-------+--------+------+------+---------+-------+
| IndexName       | Field | Type   | Null | Key  | Default | Extra |
+-----------------+-------+--------+------+------+---------+-------+
| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
|                 | k2    | INT    | Yes  | true | N/A     |       |
+-----------------+-------+--------+------+------+---------+-------+
```

例2: k2 でソートされた同期マテリアライズドビューを作成します。

```sql
create materialized view k2_order as
select k2, k1 from duplicate_table order by k2;
```

マテリアライズドビューのスキーマを以下に示します。このマテリアライズドビューには、k2 と k1 の 2 つのカラムのみが含まれており、カラム k2 は集計なしのソートカラムです。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
+-----------------+-------+--------+------+-------+---------+-------+
```

例 3: k1 と k2 でグループ化し、k3 で SUM 集計を行う同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2_sumk3 as
select k1, k2, sum(k3) from duplicate_table group by k1, k2;
```

マテリアライズドビューのスキーマを以下に示します。このマテリアライズドビューには、k1、k2、sum(k3) の 3 つのカラムが含まれています。k1 と k2 はグループ化されたカラムで、sum(k3) は k1 と k2 に従ってグループ化された k3 カラムの合計です。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
+-----------------+-------+--------+------+-------+---------+-------+
```

マテリアライズドビューはソート列を宣言しておらず、集計関数を採用しているため、StarRocks はデフォルトでグループ化された列 k1 と k2 を補完します。

例 4: 同期マテリアライズドビューを作成して、重複する行を削除します。

```sql
create materialized view deduplicate as
select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
```

マテリアライズドビューのスキーマを以下に示します。このマテリアライズドビューには k1、k2、k3、および k4 の列が含まれており、重複する行はありません。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | true  | N/A     |       |
|                 | k4    | BIGINT | Yes  | true  | N/A     |       |
+-----------------+-------+--------+------+-------+---------+-------+
```

例5：ソート列を宣言しない、非集計の同期マテリアライズドビューを作成します。

ベーステーブルのスキーマを以下に示します。

```plain text
+-------+--------------+------+-------+---------+-------+
| Field | Type         | Null | Key   | Default | Extra |
+-------+--------------+------+-------+---------+-------+
| k1    | TINYINT      | Yes  | true  | N/A     |       |
| k2    | SMALLINT     | Yes  | true  | N/A     |       |
| k3    | INT          | Yes  | true  | N/A     |       |
| k4    | BIGINT       | Yes  | true  | N/A     |       |
| k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
| k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
| k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
+-------+--------------+------+-------+---------+-------+
```

マテリアライズドビューには k3、k4、k5、k6、k7 のカラムが含まれており、ソートカラムは宣言されていません。以下のステートメントでマテリアライズドビューを作成します。

```sql
create materialized view mv_1 as
select k3, k4, k5, k6, k7 from all_type_table;
```

StarRocks はデフォルトでソート列として k3、k4、k5 を自動的に使用します。これら 3 つの列タイプが占めるバイト数の合計は、4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 < 36 です。したがって、これら 3 つの列がソート列として追加されます。

マテリアライズドビューのスキーマは次のとおりです。

```plain text
+----------------+-------+--------------+------+-------+---------+-------+
| IndexName      | Field | Type         | Null | Key   | Default | Extra |
+----------------+-------+--------------+------+-------+---------+-------+
| mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
|                | k4    | BIGINT       | Yes  | true  | N/A     |       |
|                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
|                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
|                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
+----------------+-------+--------------+------+-------+---------+-------+
```

k3、k4、k5 カラムの `key` フィールドが `true` であることがわかります。これは、それらがソートキーであることを示しています。k6、k7 カラムの key フィールドは `false` であり、これらはソートキーではないことを示しています。

例 6: WHERE 句と複雑な式を含む同期マテリアライズドビューを作成します。

```SQL
-- Create the base table: user_event
CREATE TABLE user_event (
      ds date   NOT NULL,
      id  varchar(256)    NOT NULL,
      user_id int DEFAULT NULL,
      user_id1    varchar(256)    DEFAULT NULL,
      user_id2    varchar(256)    DEFAULT NULL,
      column_01   int DEFAULT NULL,
      column_02   int DEFAULT NULL,
      column_03   int DEFAULT NULL,
      column_04   int DEFAULT NULL,
      column_05   int DEFAULT NULL,
      column_06   DECIMAL(12,2)   DEFAULT NULL,
      column_07   DECIMAL(12,3)   DEFAULT NULL,
      column_08   JSON   DEFAULT NULL,
      column_09   DATETIME    DEFAULT NULL,
      column_10   DATETIME    DEFAULT NULL,
      column_11   DATE    DEFAULT NULL,
      column_12   varchar(256)    DEFAULT NULL,
      column_13   varchar(256)    DEFAULT NULL,
      column_14   varchar(256)    DEFAULT NULL,
      column_15   varchar(256)    DEFAULT NULL,
      column_16   varchar(256)    DEFAULT NULL,
      column_17   varchar(256)    DEFAULT NULL,
      column_18   varchar(256)    DEFAULT NULL,
      column_19   varchar(256)    DEFAULT NULL,
      column_20   varchar(256)    DEFAULT NULL,
      column_21   varchar(256)    DEFAULT NULL,
      column_22   varchar(256)    DEFAULT NULL,
      column_23   varchar(256)    DEFAULT NULL,
      column_24   varchar(256)    DEFAULT NULL,
      column_25   varchar(256)    DEFAULT NULL,
      column_26   varchar(256)    DEFAULT NULL,
      column_27   varchar(256)    DEFAULT NULL,
      column_28   varchar(256)    DEFAULT NULL,
      column_29   varchar(256)    DEFAULT NULL,
      column_30   varchar(256)    DEFAULT NULL,
      column_31   varchar(256)    DEFAULT NULL,
      column_32   varchar(256)    DEFAULT NULL,
      column_33   varchar(256)    DEFAULT NULL,
      column_34   varchar(256)    DEFAULT NULL,
      column_35   varchar(256)    DEFAULT NULL,
      column_36   varchar(256)    DEFAULT NULL,
      column_37   varchar(256)    DEFAULT NULL
  )
  PARTITION BY date_trunc("day", ds)
  DISTRIBUTED BY hash(id);

  -- Create the materialized view with the WHERE clause and complex expresssions.
  CREATE MATERIALIZED VIEW test_mv1
  AS 
  SELECT
  ds,
  column_19,
  column_36,
  sum(column_01) as column_01_sum,
  bitmap_union(to_bitmap( user_id)) as user_id_dist_cnt,
  bitmap_union(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,
  bitmap_union(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,
  bitmap_union(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,
  bitmap_union(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,
  bitmap_union(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
  FROM user_event
  WHERE ds >= '2023-11-02'
  GROUP BY
  ds,
  column_19,
  column_36;
 ```

### 非同期マテリアライズドビューの例

以下の例は、以下のベーステーブルに基づいています。

```SQL
CREATE TABLE `lineorder` (
  `lo_orderkey` int(11) NOT NULL COMMENT "",
  `lo_linenumber` int(11) NOT NULL COMMENT "",
  `lo_custkey` int(11) NOT NULL COMMENT "",
  `lo_partkey` int(11) NOT NULL COMMENT "",
  `lo_suppkey` int(11) NOT NULL COMMENT "",
  `lo_orderdate` int(11) NOT NULL COMMENT "",
  `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
  `lo_shippriority` int(11) NOT NULL COMMENT "",
  `lo_quantity` int(11) NOT NULL COMMENT "",
  `lo_extendedprice` int(11) NOT NULL COMMENT "",
  `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
  `lo_discount` int(11) NOT NULL COMMENT "",
  `lo_revenue` int(11) NOT NULL COMMENT "",
  `lo_supplycost` int(11) NOT NULL COMMENT "",
  `lo_tax` int(11) NOT NULL COMMENT "",
  `lo_commitdate` int(11) NOT NULL COMMENT "",
  `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
PARTITION BY RANGE(`lo_orderdate`)
(PARTITION p1 VALUES [("-2147483648"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(`lo_orderkey`);

CREATE TABLE IF NOT EXISTS `customer` (
  `c_custkey` int(11) NOT NULL COMMENT "",
  `c_name` varchar(26) NOT NULL COMMENT "",
  `c_address` varchar(41) NOT NULL COMMENT "",
  `c_city` varchar(11) NOT NULL COMMENT "",
  `c_nation` varchar(16) NOT NULL COMMENT "",
  `c_region` varchar(13) NOT NULL COMMENT "",
  `c_phone` varchar(16) NOT NULL COMMENT "",
  `c_mktsegment` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`);

CREATE TABLE IF NOT EXISTS `dates` (
  `d_datekey` int(11) NOT NULL COMMENT "",
  `d_date` varchar(20) NOT NULL COMMENT "",
  `d_dayofweek` varchar(10) NOT NULL COMMENT "",
  `d_month` varchar(11) NOT NULL COMMENT "",
  `d_year` int(11) NOT NULL COMMENT "",
  `d_yearmonthnum` int(11) NOT NULL COMMENT "",
  `d_yearmonth` varchar(9) NOT NULL COMMENT "",
  `d_daynuminweek` int(11) NOT NULL COMMENT "",
  `d_daynuminmonth` int(11) NOT NULL COMMENT "",
  `d_daynuminyear` int(11) NOT NULL COMMENT "",
  `d_monthnuminyear` int(11) NOT NULL COMMENT "",
  `d_weeknuminyear` int(11) NOT NULL COMMENT "",
  `d_sellingseason` varchar(14) NOT NULL COMMENT "",
  `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
  `d_holidayfl` int(11) NOT NULL COMMENT "",
  `d_weekdayfl` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_datekey`);

CREATE TABLE IF NOT EXISTS `supplier` (
  `s_suppkey` int(11) NOT NULL COMMENT "",
  `s_name` varchar(26) NOT NULL COMMENT "",
  `s_address` varchar(26) NOT NULL COMMENT "",
  `s_city` varchar(11) NOT NULL COMMENT "",
  `s_nation` varchar(16) NOT NULL COMMENT "",
  `s_region` varchar(13) NOT NULL COMMENT "",
  `s_phone` varchar(16) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`);

CREATE TABLE IF NOT EXISTS `part` (
  `p_partkey` int(11) NOT NULL COMMENT "",
  `p_name` varchar(23) NOT NULL COMMENT "",
  `p_mfgr` varchar(7) NOT NULL COMMENT "",
  `p_category` varchar(8) NOT NULL COMMENT "",
  `p_brand` varchar(10) NOT NULL COMMENT "",
  `p_color` varchar(12) NOT NULL COMMENT "",
  `p_type` varchar(26) NOT NULL COMMENT "",
  `p_size` int(11) NOT NULL COMMENT "",
  `p_container` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`);

create table orders ( 
    dt date NOT NULL, 
    order_id bigint NOT NULL, 
    user_id int NOT NULL, 
    merchant_id int NOT NULL, 
    good_id int NOT NULL, 
    good_name string NOT NULL, 
    price int NOT NULL, 
    cnt int NOT NULL, 
    revenue int NOT NULL, 
    state tinyint NOT NULL 
) 
PRIMARY KEY (dt, order_id) 
PARTITION BY RANGE(`dt`) 
( PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')), 
PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')) ) 
DISTRIBUTED BY HASH(order_id)
PROPERTIES (
    "replication_num" = "3", 
    "enable_persistent_index" = "true"
);
```

例1：パーティション化されていないマテリアライズドビューを作成します。

```SQL
-- create an unpartitioned materialized view sorted by lo_custkey
CREATE MATERIALIZED VIEW lo_mv1
DISTRIBUTED BY HASH(`lo_orderkey`)
ORDER BY `lo_custkey`
REFRESH ASYNC
AS
select
    lo_orderkey, 
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_custkey;
```

例 2: パーティション化されたマテリアライズドビューを作成します。

```SQL
-- create a partitioned materialized view partitioned by `lo_orderdate` and sorted by `lo_custkey`.
CREATE MATERIALIZED VIEW lo_mv2
PARTITION BY `lo_orderdate`
DISTRIBUTED BY HASH(`lo_orderkey`)
ORDER BY `lo_custkey`
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    lo_orderkey,
    lo_orderdate,
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_orderdate, lo_custkey;

-- Use the date_trunc() function to partition the materialized view by month.
CREATE MATERIALIZED VIEW order_mv1
PARTITION BY date_trunc('month', `dt`)
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    dt,
    order_id,
    user_id,
    sum(cnt) as total_cnt,
    sum(revenue) as total_revenue, 
    count(state) as state_count
from orders
group by dt, order_id, user_id;
```

例 3：非同期マテリアライズドビューを作成します。

```SQL
CREATE MATERIALIZED VIEW flat_lineorder
DISTRIBUTED BY HASH(`lo_orderkey`)
REFRESH MANUAL
AS
SELECT
    l.LO_ORDERKEY AS LO_ORDERKEY,
    l.LO_LINENUMBER AS LO_LINENUMBER,
    l.LO_CUSTKEY AS LO_CUSTKEY,
    l.LO_PARTKEY AS LO_PARTKEY,
    l.LO_SUPPKEY AS LO_SUPPKEY,
    l.LO_ORDERDATE AS LO_ORDERDATE,
    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
    l.LO_QUANTITY AS LO_QUANTITY,
    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
    l.LO_DISCOUNT AS LO_DISCOUNT,
    l.LO_REVENUE AS LO_REVENUE,
    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
    l.LO_TAX AS LO_TAX,
    l.LO_COMMITDATE AS LO_COMMITDATE,
    l.LO_SHIPMODE AS LO_SHIPMODE,
    c.C_NAME AS C_NAME,
    c.C_ADDRESS AS C_ADDRESS,
    c.C_CITY AS C_CITY,
    c.C_NATION AS C_NATION,
    c.C_REGION AS C_REGION,
    c.C_PHONE AS C_PHONE,
    c.C_MKTSEGMENT AS C_MKTSEGMENT,
    s.S_NAME AS S_NAME,
    s.S_ADDRESS AS S_ADDRESS,
    s.S_CITY AS S_CITY,
    s.S_NATION AS S_NATION,
    s.S_REGION AS S_REGION,
    s.S_PHONE AS S_PHONE,
    p.P_NAME AS P_NAME,
    p.P_MFGR AS P_MFGR,
    p.P_CATEGORY AS P_CATEGORY,
    p.P_BRAND AS P_BRAND,
    p.P_COLOR AS P_COLOR,
    p.P_TYPE AS P_TYPE,
    p.P_SIZE AS P_SIZE,
    p.P_CONTAINER AS P_CONTAINER FROM lineorder AS l 
INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
```

例 4：パーティション化されたマテリアライズドビューを作成し、`str2date` を使用して、ベーステーブルの STRING 型のパーティションキーをマテリアライズドビューの日付型に変換します。

``` SQL

-- Hive Table with string partition column.
CREATE TABLE `part_dates` (
  `d_date` varchar(20) DEFAULT NULL,
  `d_dayofweek` varchar(10) DEFAULT NULL,
  `d_month` varchar(11) DEFAULT NULL,
  `d_year` int(11) DEFAULT NULL,
  `d_yearmonthnum` int(11) DEFAULT NULL,
  `d_yearmonth` varchar(9) DEFAULT NULL,
  `d_daynuminweek` int(11) DEFAULT NULL,
  `d_daynuminmonth` int(11) DEFAULT NULL,
  `d_daynuminyear` int(11) DEFAULT NULL,
  `d_monthnuminyear` int(11) DEFAULT NULL,
  `d_weeknuminyear` int(11) DEFAULT NULL,
  `d_sellingseason` varchar(14) DEFAULT NULL,
  `d_lastdayinweekfl` int(11) DEFAULT NULL,
  `d_lastdayinmonthfl` int(11) DEFAULT NULL,
  `d_holidayfl` int(11) DEFAULT NULL,
  `d_weekdayfl` int(11) DEFAULT NULL,
  `d_datekey` varchar(11) DEFAULT NULL
) partition by (d_datekey);


-- Create the materialied view  with `str2date`.
CREATE MATERIALIZED VIEW IF NOT EXISTS `test_mv` 
PARTITION BY str2date(`d_datekey`,'%Y%m%d')
DISTRIBUTED BY HASH(`d_date`, `d_month`, `d_month`) 
REFRESH MANUAL 
AS
SELECT
`d_date` ,
  `d_dayofweek`,
  `d_month` ,
  `d_yearmonthnum` ,
  `d_yearmonth` ,
  `d_daynuminweek`,
  `d_daynuminmonth`,
  `d_daynuminyear` ,
  `d_monthnuminyear` ,
  `d_weeknuminyear` ,
  `d_sellingseason`,
  `d_lastdayinweekfl`,
  `d_lastdayinmonthfl`,
  `d_holidayfl` ,
  `d_weekdayfl`,
   `d_datekey`
FROM
 `hive_catalog`.`ssb_1g_orc`.`part_dates` ;
```

例 5：Iceberg Catalog (Spark) のベーステーブルに基づいて、複数カラムのパーティション式を持つパーティション化されたマテリアライズドビューを作成します。

Spark におけるベーステーブルの定義：

```SQL
-- The partition expression of the base table contains multiple columns and a `days` transform.
CREATE TABLE lineitem_days (
      l_orderkey    BIGINT,
      l_partkey     INT,
      l_suppkey     INT,
      l_linenumber  INT,
      l_quantity    DECIMAL(15, 2),
      l_extendedprice  DECIMAL(15, 2),
      l_discount    DECIMAL(15, 2),
      l_tax         DECIMAL(15, 2),
      l_returnflag  VARCHAR(1),
      l_linestatus  VARCHAR(1),
      l_shipdate    TIMESTAMP,
      l_commitdate  TIMESTAMP,
      l_receiptdate TIMESTAMP,
      l_shipinstruct VARCHAR(25),
      l_shipmode     VARCHAR(10),
      l_comment      VARCHAR(44)
) USING ICEBERG
PARTITIONED BY (l_returnflag, l_linestatus, days(l_shipdate));
```

ベーステーブルのパーティション列と1対1でマッピングされたパーティション列を持つマテリアライズドビューを作成します。

```SQL
CREATE MATERIALIZED VIEW test_days
PARTITION BY (l_returnflag, l_linestatus, date_trunc('day', l_shipdate))
REFRESH DEFERRED MANUAL
AS 
SELECT * FROM iceberg_catalog.test_db.lineitem_days;
```

例 6: パーティション化されたマテリアライズドビューを作成し、それに共通パーティション式 TTL を定義し、クエリの書き換えのために `force_mv` セマンティクスを有効にします。

```SQL
CREATE MATERIALIZED VIEW test_mv1 
PARTITION BY (dt, province)
REFRESH MANUAL 
PROPERTIES (
    "partition_retention_condition" = "dt >= CURRENT_DATE() - INTERVAL 3 MONTH",
    "query_rewrite_consistency" = "force_mv"
)
AS SELECT * from t1;
```

例7：特定のソートキーを持つパーティションマテリアライズドビューを作成する：

```SQL
CREATE MATERIALIZED VIEW lo_mv2
PARTITION BY `lo_orderdate`
DISTRIBUTED BY HASH(`lo_orderkey`)
ORDER BY `lo_custkey`
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    lo_orderkey,
    lo_orderdate,
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_orderdate, lo_custkey;
```