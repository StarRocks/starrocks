---
displayed_sidebar: docs
---

# CREATE MATERIALIZED VIEW

## 説明

マテリアライズドビューを作成します。マテリアライズドビューの使用方法については、[Synchronous materialized view](../../../using_starrocks/Materialized_view-single_table.md) および [Asynchronous materialized view](../../../using_starrocks/Materialized_view.md) を参照してください。

> **注意**
>
> ベーステーブルが存在するデータベースで CREATE MATERIALIZED VIEW 権限を持つユーザーのみがマテリアライズドビューを作成できます。

マテリアライズドビューの作成は非同期操作です。このコマンドが正常に実行されると、マテリアライズドビューの作成タスクが正常に送信されたことを示します。データベース内の同期マテリアライズドビューの構築状況は [SHOW ALTER MATERIALIZED VIEW](../data-manipulation/SHOW_ALTER_MATERIALIZED_VIEW.md) コマンドで確認でき、非同期マテリアライズドビューの状況は [Information Schema](../../../administration/information_schema.md) 内のメタデータテーブル `tasks` および `task_runs` をクエリすることで確認できます。

StarRocks は v2.4 から非同期マテリアライズドビューをサポートしています。以前のバージョンの同期マテリアライズドビューとの主な違いは次のとおりです：

|                       | **シングルテーブル集計** | **マルチテーブルジョイン** | **クエリの書き換え** | **リフレッシュ戦略** | **ベーステーブル** |
| --------------------- | ------------------------ | ------------------------- | -------------------- | ------------------- | ----------------- |
| **ASYNC MV** | はい | はい | はい | <ul><li>非同期リフレッシュ</li><li>手動リフレッシュ</li></ul> | 複数のテーブルから:<ul><li>Default catalog</li><li>External catalogs (v2.5)</li><li>既存のマテリアライズドビュー (v2.5)</li></ul> |
| **SYNC MV (Rollup)**  | 集計関数の選択肢が限られている | いいえ | はい | データロード中の同期リフレッシュ | Default catalog 内の単一テーブル |

## 同期マテリアライズドビュー

### 構文

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

角括弧 [] 内のパラメータはオプションです。

### パラメータ

**mv_name** (必須)

マテリアライズドビューの名前。命名要件は次のとおりです：

- 名前は文字 (a-z または A-Z)、数字 (0-9)、またはアンダースコア (\_) で構成され、文字で始める必要があります。
- 名前の長さは 64 文字を超えてはなりません。
- 名前は大文字と小文字を区別します。

**COMMENT** (オプション)

マテリアライズドビューに対するコメント。`COMMENT` は `mv_name` の後に配置する必要があります。そうでない場合、マテリアライズドビューを作成できません。

**query_statement** (必須)

マテリアライズドビューを作成するためのクエリステートメント。その結果がマテリアライズドビューのデータです。構文は次のとおりです：

```SQL
SELECT select_expr[, select_expr ...]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr (必須)

  クエリステートメント内のすべての列、つまりマテリアライズドビューのスキーマ内のすべての列。このパラメータは次の値をサポートします：

  - 単純な列または集計列、例：`SELECT a, abs(b), min(c) FROM table_a`。ここで `a`、`b`、`c` はベーステーブル内の列名です。マテリアライズドビューの列名を指定しない場合、StarRocks は自動的に列に名前を割り当てます。
  - 式、例：`SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a`。ここで `a+1`、`b+2`、`c*c` はベーステーブル内の列を参照する式であり、`x`、`y`、`z` はマテリアライズドビュー内の列に割り当てられたエイリアスです。

  > **注意**
  >
  > - `select_expr` には少なくとも 1 つの列を指定する必要があります。
  > - 同期マテリアライズドビューは、単一列に対する集計関数のみをサポートします。`sum(a+b)` の形式のクエリステートメントはサポートされていません。
  > - 集計関数を使用して同期マテリアライズドビューを作成する場合、GROUP BY 句を指定し、`select_expr` に少なくとも 1 つの GROUP BY 列を指定する必要があります。
  > - 同期マテリアライズドビューは、JOIN、WHERE、GROUP BY の HAVING 句などの句をサポートしていません。
  > - ALTER TABLE DROP COLUMN を使用してベーステーブル内の特定の列を削除する場合、ベーステーブルのすべての同期マテリアライズドビューに削除された列が含まれていないことを確認する必要があります。そうでない場合、削除操作は失敗します。列を削除する前に、その列を含むすべての同期マテリアライズドビューを削除する必要があります。
  > - テーブルに対して同期マテリアライズドビューを作成しすぎると、データロードの効率に影響を与えます。ベーステーブルにデータがロードされると、同期マテリアライズドビューとベーステーブルのデータが同期的に更新されます。ベーステーブルに `n` 個の同期マテリアライズドビューが含まれている場合、ベーステーブルへのデータロードの効率は `n` 個のテーブルへのデータロードの効率とほぼ同じです。

- GROUP BY (オプション)

  クエリの GROUP BY 列。このパラメータが指定されていない場合、データはデフォルトでグループ化されません。

- ORDER BY (オプション)

  クエリの ORDER BY 列。

  - ORDER BY 句の列は、`select_expr` の列と同じ順序で宣言する必要があります。
  - クエリステートメントに GROUP BY 句が含まれている場合、ORDER BY 列は GROUP BY 列と同一である必要があります。
  - このパラメータが指定されていない場合、システムは次のルールに従って ORDER BY 列を自動的に補完します：
    - マテリアライズドビューが AGGREGATE タイプの場合、すべての GROUP BY 列が自動的にソートキーとして使用されます。
    - マテリアライズドビューが AGGREGATE タイプでない場合、StarRocks はプレフィックス列に基づいてソートキーを自動的に選択します。

### 同期マテリアライズドビューによる自動クエリの書き換え

同期マテリアライズドビューのパターンに従ったクエリが実行されると、元のクエリステートメントが自動的に書き換えられ、マテリアライズドビューに格納された中間結果が使用されます。

次の表は、元のクエリの集計関数とマテリアライズドビューを構築するために使用される集計関数の対応を示しています。ビジネスシナリオに応じて、対応する集計関数を選択してマテリアライズドビューを構築できます。

| **元のクエリの集計関数**           | **マテリアライズドビューの集計関数** |
| ------------------------------------------------------ | ----------------------------------------------- |
| sum                                                    | sum                                             |
| min                                                    | min                                             |
| max                                                    | max                                             |
| count                                                  | count                                           |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                    |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                       |
| percentile_approx, percentile_union                    | percentile_union                                |

## 非同期マテリアライズドビュー

### 構文

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]<mv_name>
[COMMENT ""]
-- `distribution_desc` または `refresh_scheme` のいずれか、または両方を指定する必要があります。
-- distribution_desc
DISTRIBUTED BY HASH(<bucket_key>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
-- refresh_scheme
[REFRESH
    [ASYNC | ASYNC [START (<start_time>)] EVERY (INTERVAL <refresh_interval>) | MANUAL]
]
-- partition_expression
[PARTITION BY 
    {<date_column> | date_trunc(fmt, <date_column>)}
]
-- order_by_expression
[ORDER BY (<sort_key>)]
[PROPERTIES ("key"="value", ...)]
AS 
<query_statement>
```

角括弧 [] 内のパラメータはオプションです。

### パラメータ

**mv_name** (必須)

マテリアライズドビューの名前。命名要件は次のとおりです：

- 名前は文字 (a-z または A-Z)、数字 (0-9)、またはアンダースコア (\_) で構成され、文字で始める必要があります。
- 名前の長さは 64 文字を超えてはなりません。
- 名前は大文字と小文字を区別します。

> **注意**
>
> 同じベーステーブルに対して複数のマテリアライズドビューを作成できますが、同じデータベース内でマテリアライズドビューの名前を重複させることはできません。

**COMMENT** (オプション)

マテリアライズドビューに対するコメント。`COMMENT` は `mv_name` の後に配置する必要があります。そうでない場合、マテリアライズドビューを作成できません。

**distribution_desc** (必須)

非同期マテリアライズドビューのバケッティング戦略。

構文

```SQL
DISTRIBUTED BY HASH (<bucket_key1>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
```

詳細については、[Data distribution](../../../table_design/Data_distribution.md#data-distribution) を参照してください。

> **注意**
>
> v2.5.7 以降、StarRocks はテーブルを作成する際やパーティションを追加する際に、バケツ数 (BUCKETS) を自動的に設定できます。バケツ数を手動で設定する必要はありません。詳細情報については、[determine the number of buckets](../../../table_design/Data_distribution.md#determine-the-number-of-buckets) を参照してください。

**refresh_scheme** (オプション)

> **注意**
>
> 非同期マテリアライズドビューを作成する際、`distribution_desc` または `refresh_scheme` のいずれか、または両方を指定する必要があります。

非同期マテリアライズドビューのリフレッシュ戦略。有効な値：

- `ASYNC`: 自動リフレッシュモード。ベーステーブルのデータが変更されるたびに、マテリアライズドビューが自動的にリフレッシュされます。
- `ASYNC [START (<start_time>)] EVERY(INTERVAL <interval>) `: 定期リフレッシュモード。マテリアライズドビューは定義された間隔で定期的にリフレッシュされます。間隔は `EVERY (interval n day/hour/minute/second)` として指定でき、使用可能な単位は `DAY`、`HOUR`、`MINUTE`、`SECOND` です。デフォルト値は `10 MINUTE` です。リフレッシュ開始時間は `START('yyyy-MM-dd hh:mm:ss')` としてさらに指定できます。開始時間が指定されていない場合、現在の時間が使用されます。例：`ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)`。
- `MANUAL`: 手動リフレッシュモード。マテリアライズドビューは手動でリフレッシュタスクをトリガーしない限りリフレッシュされません。

このパラメータが指定されていない場合、デフォルト値 `MANUAL` が使用されます。

**partition_expression** (オプション)

非同期マテリアライズドビューのパーティショニング戦略。StarRocks の現在のバージョンでは、非同期マテリアライズドビューを作成する際に 1 つのパーティション式のみがサポートされています。

> **注意**
>
> 現在、非同期マテリアライズドビューはリストパーティション化戦略をサポートしていません。

有効な値：

- `column_name`: パーティショニングに使用される列の名前。式 `PARTITION BY dt` は、`dt` 列に従ってマテリアライズドビューをパーティショニングすることを意味します。
- date_trunc 関数: 時間単位を切り捨てるために使用される関数。`PARTITION BY date_trunc("MONTH", dt)` は、`dt` 列を月単位に切り捨ててパーティショニングすることを意味します。date_trunc 関数は、`YEAR`、`MONTH`、`DAY`、`HOUR`、`MINUTE` などの単位に時間を切り捨てることをサポートしています。

このパラメータが指定されていない場合、デフォルトでパーティショニング戦略は採用されません。

**PROPERTIES** (オプション)

非同期マテリアライズドビューのプロパティ。既存のマテリアライズドビューのプロパティを [ALTER MATERIALIZED VIEW](./ALTER_MATERIALIZED_VIEW.md) を使用して変更できます。

- `replication_num`: 作成するマテリアライズドビューのレプリカ数。
- `storage_medium`: 記憶媒体のタイプ。有効な値：`HDD` と `SSD`。
- `storage_cooldown_time`: パーティションのストレージクールダウン時間。HDD と SSD の記憶媒体が両方使用されている場合、このプロパティで指定された時間の後に SSD ストレージのデータが HDD ストレージに移動されます。形式："yyyy-MM-dd HH:mm:ss"。指定された時間は現在の時間よりも後でなければなりません。このプロパティが明示的に指定されていない場合、デフォルトではストレージクールダウンは実行されません。
- `partition_ttl_number`: 最新のマテリアライズドビューのパーティションを保持する数。現在の時間よりも前の開始時間を持つパーティションの数がこの値を超えると、より古いパーティションが削除されます。StarRocks は FE 設定項目 `dynamic_partition_check_interval_seconds` で指定された時間間隔に従ってマテリアライズドビューのパーティションを定期的にチェックし、期限切れのパーティションを自動的に削除します。[動的パーティション化](../../../table_design/dynamic_partitioning.md) 戦略を有効にした場合、事前に作成されたパーティションはカウントされません。値が `-1` の場合、マテリアライズドビューのすべてのパーティションが保持されます。デフォルト：`-1`。
- `partition_refresh_number`: 単一のリフレッシュでリフレッシュする最大パーティション数。リフレッシュするパーティションの数がこの値を超える場合、StarRocks はリフレッシュタスクを分割し、バッチで完了します。前のバッチのパーティションが正常にリフレッシュされると、StarRocks は次のバッチのパーティションをリフレッシュし続け、すべてのパーティションがリフレッシュされるまで続けます。パーティションのいずれかがリフレッシュに失敗した場合、後続のリフレッシュタスクは生成されません。値が `-1` の場合、リフレッシュタスクは分割されません。デフォルト：`-1`。
- `excluded_trigger_tables`: マテリアライズドビューのベーステーブルがここにリストされている場合、ベーステーブルのデータが変更されたときに自動リフレッシュタスクはトリガーされません。このパラメータはロードトリガーリフレッシュ戦略にのみ適用され、通常はプロパティ `auto_refresh_partitions_limit` と一緒に使用されます。形式：`[db_name.]table_name`。値が空の文字列の場合、すべてのベーステーブルのデータ変更が対応するマテリアライズドビューのリフレッシュをトリガーします。デフォルト値は空の文字列です。
- `auto_refresh_partitions_limit`: マテリアライズドビューのリフレッシュがトリガーされたときにリフレッシュする必要がある最新のマテリアライズドビューのパーティション数。このプロパティを使用してリフレッシュ範囲を制限し、リフレッシュコストを削減できます。ただし、すべてのパーティションがリフレッシュされないため、マテリアライズドビューのデータがベーステーブルと一致しない場合があります。デフォルト：`-1`。値が `-1` の場合、すべてのパーティションがリフレッシュされます。値が正の整数 N の場合、StarRocks は既存のパーティションを時系列順に並べ替え、現在のパーティションと N-1 の最新パーティションをリフレッシュします。パーティションの数が N より少ない場合、StarRocks はすべての既存のパーティションをリフレッシュします。マテリアライズドビューに事前に作成された動的パーティションがある場合、StarRocks はすべての事前作成されたパーティションをリフレッシュします。

> **注意**
>
> 一意キー制約と外部キー制約はクエリの書き換えにのみ使用されます。外部キー制約のチェックは、テーブルにデータがロードされるときに保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。

**query_statement** (必須)

非同期マテリアライズドビューを作成するためのクエリステートメント。

> **注意**
>
> 現在、StarRocks はリストパーティション化戦略で作成されたベーステーブルを持つ非同期マテリアライズドビューの作成をサポートしていません。

### 非同期マテリアライズドビューのクエリ

非同期マテリアライズドビューは物理テーブルです。通常のテーブルと同様に操作できますが、**非同期マテリアライズドビューに直接データをロードすることはできません**。

### 非同期マテリアライズドビューによる自動クエリの書き換え

StarRocks v2.5 は、SPJG タイプの非同期マテリアライズドビューに基づく自動かつ透明なクエリの書き換えをサポートしています。SPJG タイプのマテリアライズドビューは、プランに Scan、Filter、Project、および Aggregate タイプのオペレーターのみを含むマテリアライズドビューを指します。SPJG タイプのマテリアライズドビューのクエリの書き換えには、単一テーブルのクエリの書き換え、ジョインクエリの書き換え、集計クエリの書き換え、ユニオンクエリの書き換え、およびネストされたマテリアライズドビューに基づくクエリの書き換えが含まれます。

詳細については、[Asynchronous materialized view - Rewrite queries with the asynchronous materialized view](../../../using_starrocks/Materialized_view.md#rewrite_queries_with_the_asynchronous_materialized_view) を参照してください。

### サポートされているデータ型

- StarRocks のデフォルトカタログに基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています：

  - **日付**: DATE, DATETIME
  - **文字列**: CHAR, VARCHAR
  - **数値**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, PERCENTILE
  - **半構造化**: ARRAY, JSON
  - **その他**: BITMAP, HLL

> **注意**
>
> BITMAP, HLL, および PERCENTILE は v2.4.5 以降でサポートされています。

- StarRocks の外部カタログに基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています：

  - Hive Catalog

    - **数値**: INT/INTEGER, BIGINT, DOUBLE, FLOAT, DECIMAL
    - **日付**: TIMESTAMP
    - **文字列**: STRING, VARCHAR, CHAR
    - **半構造化**: ARRAY

  - Hudi Catalog

    - **数値**: BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL
    - **日付**: DATE, TimeMillis/TimeMicros, TimestampMillis/TimestampMicros
    - **文字列**: STRING
    - **半構造化**: ARRAY

  - Iceberg Catalog

    - **数値**: BOOLEAN, INT, LONG, FLOAT, DOUBLE, DECIMAL(P, S)
    - **日付**: DATE, TIME, TIMESTAMP
    - **文字列**: STRING, UUID, FIXED(L), BINARY
    - **半構造化**: LIST

## 使用上の注意

- 現在のバージョンの StarRocks は、同時に複数のマテリアライズドビューを作成することをサポートしていません。新しいマテリアライズドビューは、前のものが完了した後にのみ作成できます。

- 同期マテリアライズドビューについて：

  - 同期マテリアライズドビューは、単一列に対する集計関数のみをサポートします。`sum(a+b)` の形式のクエリステートメントはサポートされていません。
  - 同期マテリアライズドビューは、ベーステーブルの各列に対して 1 つの集計関数のみをサポートします。`select sum(a), min(a) from table` のようなクエリステートメントはサポートされていません。
  - 集計関数を使用して同期マテリアライズドビューを作成する場合、GROUP BY 句を指定し、SELECT で少なくとも 1 つの GROUP BY 列を指定する必要があります。
  - 同期マテリアライズドビューは、JOIN、WHERE、GROUP BY の HAVING 句などの句をサポートしていません。
  - ALTER TABLE DROP COLUMN を使用してベーステーブル内の特定の列を削除する場合、ベーステーブルのすべての同期マテリアライズドビューに削除された列が含まれていないことを確認する必要があります。そうでない場合、削除操作は失敗します。列を削除する前に、その列を含むすべての同期マテリアライズドビューを削除する必要があります。
  - テーブルに対して同期マテリアライズドビューを作成しすぎると、データロードの効率に影響を与えます。ベーステーブルにデータがロードされると、同期マテリアライズドビューとベーステーブルのデータが同期的に更新されます。ベーステーブルに `n` 個の同期マテリアライズドビューが含まれている場合、ベーステーブルへのデータロードの効率は `n` 個のテーブルへのデータロードの効率とほぼ同じです。

- ネストされた非同期マテリアライズドビューについて：

  - 各マテリアライズドビューのリフレッシュ戦略は、対応するマテリアライズドビューにのみ適用されます。
  - 現在、StarRocks はネストのレベル数を制限していません。実稼働環境では、ネストのレイヤー数が THREE を超えないことをお勧めします。

- 外部カタログの非同期マテリアライズドビューについて：

  - 外部カタログのマテリアライズドビューは、非同期の固定間隔リフレッシュと手動リフレッシュのみをサポートします。
  - 外部カタログ内のマテリアライズドビューとベーステーブルの間の厳密な整合性は保証されません。
  - 現在、外部リソースに基づいてマテリアライズドビューを構築することはサポートされていません。
  - 現在、StarRocks は外部カタログ内のベーステーブルデータが変更されたかどうかを認識できないため、ベーステーブルがリフレッシュされるたびにすべてのパーティションがデフォルトでリフレッシュされます。いくつかのパーティションのみを手動でリフレッシュするには、[REFRESH MATERIALIZED VIEW](../data-manipulation/REFRESH_MATERIALIZED_VIEW.md) を使用できます。

## 例

### 同期マテリアライズドビューの例

ベーステーブルのスキーマは次のとおりです：

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

例 1: 元のテーブルの列 (k1, k2) のみを含む同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2 as
select k1, k2 from duplicate_table;
```

マテリアライズドビューには、集計なしで k1 と k2 の 2 つの列のみが含まれています。

```plain text
+-----------------+-------+--------+------+------+---------+-------+
| IndexName       | Field | Type   | Null | Key  | Default | Extra |
+-----------------+-------+--------+------+------+---------+-------+
| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
|                 | k2    | INT    | Yes  | true | N/A     |       |
+-----------------+-------+--------+------+------+---------+-------+
```

例 2: k2 でソートされた同期マテリアライズドビューを作成します。

```sql
create materialized view k2_order as
select k2, k1 from duplicate_table order by k2;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには、集計なしで k2 と k1 の 2 つの列が含まれており、k2 列がソート列です。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
+-----------------+-------+--------+------+-------+---------+-------+
```

例 3: k1 と k2 でグループ化し、k3 の SUM 集計を行う同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2_sumk3 as
select k1, k2, sum(k3) from duplicate_table group by k1, k2;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには、k1、k2、sum(k3) の 3 つの列が含まれており、k1、k2 はグループ化された列であり、sum(k3) は k1 と k2 に従ってグループ化された k3 列の合計です。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
+-----------------+-------+--------+------+-------+---------+-------+
```

マテリアライズドビューはソート列を宣言していないため、StarRocks はデフォルトでグループ化された列 k1 と k2 を補完します。

例 4: 重複行を削除する同期マテリアライズドビューを作成します。

```sql
create materialized view deduplicate as
select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには k1、k2、k3、k4 の列が含まれており、重複行はありません。

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

例 5: ソート列を宣言しない非集計同期マテリアライズドビューを作成します。

ベーステーブルのスキーマは以下の通りです：

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

マテリアライズドビューには k3、k4、k5、k6、k7 の列が含まれており、ソート列は宣言されていません。以下のステートメントでマテリアライズドビューを作成します：

```sql
create materialized view mv_1 as
select k3, k4, k5, k6, k7 from all_type_table;
```

StarRocks はデフォルトで k3、k4、k5 をソート列として自動的に使用します。これらの 3 つの列タイプが占めるバイト数の合計は 4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 < 36 です。したがって、これらの 3 つの列がソート列として追加されます。

マテリアライズドビューのスキーマは以下の通りです。

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

k3、k4、k5 列の `key` フィールドが `true` であることが観察されます。これは、それらがソートキーであることを示しています。k6、k7 列の `key` フィールドは `false` であり、それらがソートキーではないことを示しています。

### 非同期マテリアライズドビューの例

以下の例は、以下のベーステーブルに基づいています：

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

例 1: 非パーティション化されたマテリアライズドビューを作成します。

```SQL
CREATE MATERIALIZED VIEW lo_mv1
DISTRIBUTED BY HASH(`lo_orderkey`)
REFRESH ASYNC
AS
select
    lo_orderkey, 
    lo_custkey, 
    sum(lo_quantity) as total_quantity, 
    sum(lo_revenue) as total_revenue, 
    count(lo_shipmode) as shipmode_count
from lineorder 
group by lo_orderkey, lo_custkey 
order by lo_orderkey;
```

例 2: パーティション化されたマテリアライズドビューを作成します。

```SQL
CREATE MATERIALIZED VIEW lo_mv2
PARTITION BY `lo_orderdate`
DISTRIBUTED BY HASH(`lo_orderkey`)
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
group by lo_orderkey, lo_orderdate, lo_custkey
order by lo_orderkey;

-- date_trunc() 関数を使用して、マテリアライズドビューを月単位でパーティション化します。
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

例 3: 非同期マテリアライズドビューを作成します。

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