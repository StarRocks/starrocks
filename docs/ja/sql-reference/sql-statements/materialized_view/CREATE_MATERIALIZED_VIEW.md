---
displayed_sidebar: docs
---

# CREATE MATERIALIZED VIEW

## 説明

マテリアライズドビューを作成します。マテリアライズドビューの使用方法については、[同期マテリアライズドビュー](../../../using_starrocks/Materialized_view-single_table.md)および[非同期マテリアライズドビュー](../../../using_starrocks/async_mv/Materialized_view.md)を参照してください。

> **注意**
>
> - ベーステーブルが存在するデータベースでCREATE MATERIALIZED VIEW権限を持つユーザーのみがマテリアライズドビューを作成できます。
> - v3.4.0以降、StarRocksは共有データクラスタでの同期マテリアライズドビューの作成をサポートしています。

マテリアライズドビューの作成は非同期操作です。このコマンドが成功すると、マテリアライズドビューの作成タスクが正常に送信されたことを示します。データベース内の同期マテリアライズドビューの構築状況は[SHOW ALTER MATERIALIZED VIEW](SHOW_ALTER_MATERIALIZED_VIEW.md)コマンドで確認でき、非同期マテリアライズドビューの状況はメタデータビュー[`tasks`](../../information_schema/tasks.md)および[`task_runs`](../../information_schema/task_runs.md)を[Information Schema](../../information_schema/information_schema.md)でクエリすることで確認できます。

StarRocksはv2.4から非同期マテリアライズドビューをサポートしています。以前のバージョンにおける非同期マテリアライズドビューと同期マテリアライズドビューの主な違いは次のとおりです。

|                       | **単一テーブル集計** | **複数テーブルジョイン** | **クエリの書き換え** | **リフレッシュ戦略** | **ベーステーブル** |
| --------------------- | -------------------- | ------------------------ | ------------------- | ------------------- | ----------------- |
| **ASYNC MV** | はい | はい | はい | <ul><li>非同期リフレッシュ</li><li>手動リフレッシュ</li></ul> | 複数のテーブルから:<ul><li>Default Catalog</li><li>External catalogs (v2.5)</li><li>既存のマテリアライズドビュー (v2.5)</li><li>既存のビュー (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | 集計関数の選択肢が限られる | いいえ | はい | データロード中の同期リフレッシュ | Default Catalog内の単一テーブル |

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

マテリアライズドビューの名前。命名要件は次のとおりです。

- 名前は文字 (a-z または A-Z)、数字 (0-9)、またはアンダースコア (\_) で構成され、文字で始まる必要があります。
- 名前の長さは64文字を超えてはなりません。
- 名前は大文字と小文字を区別します。

**COMMENT** (オプション)

マテリアライズドビューに関するコメント。`COMMENT`は`mv_name`の後に配置する必要があります。そうでない場合、マテリアライズドビューは作成されません。

**query_statement** (必須)

マテリアライズドビューを作成するためのクエリステートメント。その結果がマテリアライズドビューのデータとなります。構文は次のとおりです。

```SQL
SELECT select_expr[, select_expr ...]
[WHERE where_expr]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]
```

- select_expr (必須)

  クエリステートメント内のすべての列、つまりマテリアライズドビューのスキーマ内のすべての列。このパラメータは次の値をサポートします。

  - 単純な列または集計列、例: `SELECT a, abs(b), min(c) FROM table_a`。ここで`a`、`b`、`c`はベーステーブルの列名です。マテリアライズドビューの列名を指定しない場合、StarRocksは自動的に列に名前を付けます。
  - 式、例: `SELECT a+1 AS x, b+2 AS y, c*c AS z FROM table_a`。ここで`a+1`、`b+2`、`c*c`はベーステーブルの列を参照する式で、`x`、`y`、`z`はマテリアライズドビューの列に割り当てられたエイリアスです。

  > **注意**
  >
  > - `select_expr`で少なくとも1つの列を指定する必要があります。
  > - 集計関数を持つ同期マテリアライズドビューを作成する場合、GROUP BY句を指定し、`select_expr`で少なくとも1つのGROUP BY列を指定する必要があります。
  > - 同期マテリアライズドビューはJOINやGROUP BYのHAVING句などの句をサポートしていません。
  > - v3.1以降、各同期マテリアライズドビューはベーステーブルの各列に対して複数の集計関数をサポートできます。例: `select b, sum(a), min(a) from table group by b`のようなクエリステートメント。
  > - v3.1以降、同期マテリアライズドビューはSELECTおよび集計関数の複雑な式をサポートします。例: `select b, sum(a + 1) as sum_a1, min(cast (a as bigint)) as min_a from table group by b`や`select abs(b) as col1, a + 1 as col2, cast(a as bigint) as col3 from table`のようなクエリステートメント。同期マテリアライズドビューで使用される複雑な式には次の制限があります:
  >   - 各複雑な式にはエイリアスが必要で、ベーステーブルのすべての同期マテリアライズドビュー間で異なる複雑な式には異なるエイリアスを割り当てる必要があります。例: `select b, sum(a + 1) as sum_a from table group by b`と`select b, sum(a) as sum_a from table group by b`のようなクエリステートメントは、同じベーステーブルに対して同期マテリアライズドビューを作成するために使用できません。複雑な式に異なるエイリアスを設定できます。
  >   - 複雑な式で作成された同期マテリアライズドビューによってクエリが書き換えられているかどうかを確認するには、`EXPLAIN <sql_statement>`を実行します。詳細については、[クエリ分析](../../../administration/Query_planning.md)を参照してください。

- WHERE (オプション)

  v3.1.8以降、同期マテリアライズドビューはマテリアライズドビューに使用される行をフィルタリングするWHERE句をサポートします。

- GROUP BY (オプション)

  クエリのGROUP BY列。このパラメータが指定されていない場合、データはデフォルトでグループ化されません。

- ORDER BY (オプション)

  クエリのORDER BY列。

  - ORDER BY句の列は、`select_expr`の列と同じ順序で宣言する必要があります。
  - クエリステートメントにGROUP BY句が含まれている場合、ORDER BY列はGROUP BY列と同一でなければなりません。
  - このパラメータが指定されていない場合、システムは次のルールに従ってORDER BY列を自動的に補完します:
    - マテリアライズドビューがAGGREGATEタイプの場合、すべてのGROUP BY列が自動的にソートキーとして使用されます。
    - マテリアライズドビューがAGGREGATEタイプでない場合、StarRocksはプレフィックス列に基づいてソートキーを自動的に選択します。

### 同期マテリアライズドビューのクエリ

同期マテリアライズドビューは本質的にベーステーブルのインデックスであり、物理テーブルではないため、ヒント`[_SYNC_MV_]`を使用してのみ同期マテリアライズドビューをクエリできます。

```SQL
-- ヒント内の角括弧 [] を省略しないでください。
SELECT * FROM <mv_name> [_SYNC_MV_];
```

> **注意**
>
> 現在、StarRocksは、エイリアスを指定しても同期マテリアライズドビューの列に対して自動的に名前を生成します。

### 同期マテリアライズドビューによる自動クエリの書き換え

同期マテリアライズドビューのパターンに従ったクエリが実行されると、元のクエリステートメントが自動的に書き換えられ、マテリアライズドビューに保存された中間結果が使用されます。

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
-- `distribution_desc`または`refresh_scheme`のいずれか、または両方を指定する必要があります。
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

マテリアライズドビューの名前。命名要件は次のとおりです。

- 名前は文字 (a-z または A-Z)、数字 (0-9)、またはアンダースコア (\_) で構成され、文字で始まる必要があります。
- 名前の長さは64文字を超えてはなりません。
- 名前は大文字と小文字を区別します。

> **注意**
>
> 同じベーステーブルに対して複数のマテリアライズドビューを作成できますが、同じデータベース内でマテリアライズドビューの名前を重複させることはできません。

**COMMENT** (オプション)

マテリアライズドビューに関するコメント。`COMMENT`は`mv_name`の後に配置する必要があります。そうでない場合、マテリアライズドビューは作成されません。

**distribution_desc** (オプション)

非同期マテリアライズドビューのバケット戦略。StarRocksはハッシュバケット法とランダムバケット法 (v3.1以降) をサポートしています。このパラメータを指定しない場合、StarRocksはランダムバケット戦略を使用し、バケット数を自動的に設定します。

> **注意**
>
> 非同期マテリアライズドビューを作成する際、`distribution_desc`または`refresh_scheme`のいずれか、または両方を指定する必要があります。

- **ハッシュバケット法**:

  構文

  ```SQL
  DISTRIBUTED BY HASH (<bucket_key1>[,<bucket_key2> ...]) [BUCKETS <bucket_number>]
  ```

  詳細については、[データ分散](../../../table_design/data_distribution/Data_distribution.md#data-distribution)を参照してください。

  > **注意**
  >
  > v2.5.7以降、StarRocksはテーブルを作成する際やパーティションを追加する際にバケット数 (BUCKETS) を自動的に設定できます。バケット数を手動で設定する必要はありません。詳細については、[バケット数の設定](../../../table_design/data_distribution/Data_distribution.md#set-the-number-of-buckets)を参照してください。

- **ランダムバケット法**:

  ランダムバケット戦略を選択し、StarRocksにバケット数を自動的に設定させる場合、`distribution_desc`を指定する必要はありません。ただし、バケット数を手動で設定したい場合は、次の構文を参照してください。

  ```SQL
  DISTRIBUTED BY RANDOM BUCKETS <bucket_number>
  ```

  > **注意**
  >
  > ランダムバケット戦略を持つ非同期マテリアライズドビューは、コロケーショングループに割り当てることができません。

  詳細については、[ランダムバケット法](../../../table_design/data_distribution/Data_distribution.md#random-bucketing-since-v31)を参照してください。

**refresh_moment** (オプション)

マテリアライズドビューのリフレッシュタイミング。デフォルト値: `IMMEDIATE`。有効な値:

- `IMMEDIATE`: 非同期マテリアライズドビューは作成後すぐにリフレッシュされます。
- `DEFERRED`: 非同期マテリアライズドビューは作成後にリフレッシュされません。マテリアライズドビューを手動でリフレッシュするか、定期的なリフレッシュタスクをスケジュールできます。

**refresh_scheme** (オプション)

> **注意**
>
> 非同期マテリアライズドビューを作成する際、`distribution_desc`または`refresh_scheme`のいずれか、または両方を指定する必要があります。

非同期マテリアライズドビューのリフレッシュ戦略。有効な値:

- `ASYNC`: 自動リフレッシュモード。ベーステーブルデータが変更されるたびに、マテリアライズドビューが自動的にリフレッシュされます。
- `ASYNC [START (<start_time>)] EVERY(INTERVAL <interval>)`: 定期リフレッシュモード。定義された間隔でマテリアライズドビューが定期的にリフレッシュされます。間隔は`EVERY (interval n day/hour/minute/second)`として指定できます。使用可能な単位は`DAY`、`HOUR`、`MINUTE`、`SECOND`です。デフォルト値は`10 MINUTE`です。リフレッシュ開始時間を`START('yyyy-MM-dd hh:mm:ss')`としてさらに指定できます。開始時間が指定されていない場合、現在の時間が使用されます。例: `ASYNC START ('2023-09-12 16:30:25') EVERY (INTERVAL 5 MINUTE)`。
- `MANUAL`: 手動リフレッシュモード。リフレッシュタスクを手動でトリガーしない限り、マテリアライズドビューはリフレッシュされません。

このパラメータが指定されていない場合、デフォルト値`MANUAL`が使用されます。

**partition_expression** (オプション)

非同期マテリアライズドビューのパーティション戦略。StarRocksの現在のバージョンでは、非同期マテリアライズドビューを作成する際に1つのパーティション式のみがサポートされています。

> **注意**
>
> v3.3.3以降、StarRocksはリストパーティション化戦略を使用した非同期マテリアライズドビューの作成をサポートしています。
>
> - リストパーティション化または式に基づくパーティション化戦略で作成されたテーブルに基づいて、リストパーティション化されたマテリアライズドビューを作成できます。
> - 現在、リストパーティション化戦略でマテリアライズドビューを作成する際には、1つのパーティションキーのみを指定できます。ベーステーブルに複数のパーティションキーがある場合は、1つのパーティションキーを選択する必要があります。
> - リストパーティション化戦略を持つマテリアライズドビューのリフレッシュ動作とクエリの書き換えロジックは、レンジパーティション化戦略を持つものと一致しています。

有効な値:

- `column_name`: パーティション化に使用される列の名前。`PARTITION BY dt`という式は、`dt`列に従ってマテリアライズドビューをパーティション化することを意味します。
- `date_trunc`関数: 時間単位を切り捨てるために使用される関数。`PARTITION BY date_trunc("MONTH", dt)`は、`dt`列を月単位で切り捨ててパーティション化することを意味します。`date_trunc`関数は、`YEAR`、`MONTH`、`DAY`、`HOUR`、`MINUTE`の単位で時間を切り捨てることをサポートします。
- `str2date`関数: ベーステーブルの文字列型パーティションをマテリアライズドビューのパーティションに変換するために使用される関数。`PARTITION BY str2date(dt, "%Y%m%d")`は、`dt`列が`"%Y%m%d"`の日付形式を持つ文字列日付型であることを意味します。`str2date`関数は多くの日付形式をサポートしており、詳細については[str2date](../../sql-functions/date-time-functions/str2date.md)を参照してください。v3.1.4からサポートされています。
- `time_slice`関数: v3.1以降、これらの関数を使用して、指定された時間の粒度に基づいて、与えられた時間を時間間隔の開始または終了に変換することができます。例: `PARTITION BY date_trunc("MONTH", time_slice(dt, INTERVAL 7 DAY))`。time_sliceはdate_truncよりも細かい粒度を持たなければなりません。これらを使用して、パーティションキーよりも細かい粒度を持つGROUP BY列を指定することができます。例: `GROUP BY time_slice(dt, INTERVAL 1 MINUTE) PARTITION BY date_trunc('DAY', ts)`。

このパラメータが指定されていない場合、デフォルトではパーティション戦略は採用されません。

**order_by_expression** (オプション)

非同期マテリアライズドビューのソートキー。このソートキーを指定しない場合、StarRocksはSELECT列からいくつかのプレフィックス列をソートキーとして選択します。例: `select a, b, c, d`では、ソートキーとして`a`と`b`を使用できます。このパラメータはStarRocks v3.0以降でサポートされています。

**PROPERTIES** (オプション)

非同期マテリアライズドビューのプロパティ。既存のマテリアライズドビューのプロパティを変更するには、[ALTER MATERIALIZED VIEW](ALTER_MATERIALIZED_VIEW.md)を使用できます。

- `session.`: マテリアライズドビューのセッション変数関連のプロパティを変更したい場合、プロパティに`session.`プレフィックスを追加する必要があります。例: `session.query_timeout`。非セッションプロパティの場合、プレフィックスを指定する必要はありません。例: `mv_rewrite_staleness_second`。
- `replication_num`: 作成するマテリアライズドビューのレプリカの数。
- `storage_medium`: 記憶媒体のタイプ。有効な値: `HDD`と`SSD`。
- `storage_cooldown_time`: パーティションのストレージクールダウン時間。HDDとSSDの両方の記憶媒体が使用されている場合、このプロパティで指定された時間の後、SSDストレージのデータはHDDストレージに移動されます。形式: "yyyy-MM-dd HH:mm:ss"。指定された時間は現在の時間より後でなければなりません。このプロパティが明示的に指定されていない場合、デフォルトではストレージクールダウンは実行されません。
- `partition_ttl`: パーティションの有効期限 (TTL)。指定された時間範囲内のデータを持つパーティションが保持されます。期限切れのパーティションは自動的に削除されます。単位: `YEAR`、`MONTH`、`DAY`、`HOUR`、`MINUTE`。例: `2 MONTH`としてこのプロパティを指定できます。このプロパティは`partition_ttl_number`より推奨されます。v3.1.5以降でサポートされています。
- `partition_ttl_number`: 保持する最新のマテリアライズドビューのパーティション数。開始時間が現在の時間より前のパーティションについて、この値を超えると、古いパーティションが削除されます。StarRocksはFE設定項目`dynamic_partition_check_interval_seconds`で指定された時間間隔に従ってマテリアライズドビューパーティションを定期的にチェックし、期限切れのパーティションを自動的に削除します。[動的パーティション化](../../../table_design/data_distribution/dynamic_partitioning.md)戦略を有効にした場合、事前に作成されたパーティションはカウントされません。値が`-1`の場合、マテリアライズドビューのすべてのパーティションが保持されます。デフォルト: `-1`。
- `partition_refresh_number`: 単一のリフレッシュでリフレッシュする最大パーティション数。リフレッシュするパーティションの数がこの値を超える場合、StarRocksはリフレッシュタスクを分割し、バッチで完了します。前のバッチのパーティションが正常にリフレッシュされると、StarRocksは次のバッチのパーティションをリフレッシュし続け、すべてのパーティションがリフレッシュされるまで続けます。パーティションのいずれかがリフレッシュに失敗した場合、後続のリフレッシュタスクは生成されません。値が`-1`の場合、リフレッシュタスクは分割されません。デフォルト値はv3.3以降`-1`から`1`に変更され、StarRocksはパーティションを1つずつリフレッシュします。
- `excluded_trigger_tables`: マテリアライズドビューのベーステーブルがここにリストされている場合、ベーステーブルのデータが変更されても自動リフレッシュタスクはトリガーされません。このパラメータはロードトリガー型リフレッシュ戦略にのみ適用され、通常はプロパティ`auto_refresh_partitions_limit`と一緒に使用されます。形式: `[db_name.]table_name`。値が空文字列の場合、すべてのベーステーブルのデータ変更が対応するマテリアライズドビューのリフレッシュをトリガーします。デフォルト値は空文字列です。
- `auto_refresh_partitions_limit`: マテリアライズドビューのリフレッシュがトリガーされたときにリフレッシュする必要がある最新のマテリアライズドビューパーティションの数。このプロパティを使用してリフレッシュ範囲を制限し、リフレッシュコストを削減できます。ただし、すべてのパーティションがリフレッシュされないため、マテリアライズドビューのデータがベーステーブルと一致しない場合があります。デフォルト: `-1`。値が`-1`の場合、すべてのパーティションがリフレッシュされます。値が正の整数Nの場合、StarRocksは既存のパーティションを時系列順に並べ替え、現在のパーティションとN-1の最新パーティションをリフレッシュします。パーティションの数がN未満の場合、StarRocksはすべての既存のパーティションをリフレッシュします。マテリアライズドビューに事前に作成された動的パーティションがある場合、StarRocksはすべての事前作成されたパーティションをリフレッシュします。
- `mv_rewrite_staleness_second`: マテリアライズドビューの最終リフレッシュがこのプロパティで指定された時間間隔内である場合、マテリアライズドビューはクエリの書き換えに直接使用できます。ベーステーブルのデータが変更されているかどうかに関係なく、マテリアライズドビューがクエリの書き換えに使用されるかどうかを決定します。単位: 秒。このプロパティはv3.0からサポートされています。
- `colocate_with`: 非同期マテリアライズドビューのコロケーショングループ。詳細については、[Colocate Join](../../../using_starrocks/Colocate_join.md)を参照してください。このプロパティはv3.0からサポートされています。
- `unique_constraints`および`foreign_key_constraints`: View Delta Joinシナリオでクエリの書き換えを行うために非同期マテリアライズドビューを作成する際のユニークキー制約と外部キー制約。詳細については、[非同期マテリアライズドビュー - View Delta Joinシナリオでのクエリの書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md)を参照してください。このプロパティはv3.0からサポートされています。
- `excluded_refresh_tables`: このプロパティにリストされているベーステーブルのデータが変更されても、マテリアライズドビューへのデータリフレッシュはトリガーされません。このプロパティは通常、`excluded_trigger_tables`プロパティと一緒に使用されます。形式: `[db_name.]table_name`。デフォルト値は空文字列です。値が空文字列の場合、すべてのベーステーブルのデータ変更が対応するマテリアライズドビューのリフレッシュをトリガーします。

  > **注意**
  >
  > ユニークキー制約と外部キー制約はクエリの書き換えにのみ使用されます。外部キー制約のチェックは、テーブルにデータがロードされるときに保証されません。テーブルにロードされるデータが制約を満たしていることを確認する必要があります。

- `resource_group`: マテリアライズドビューのリフレッシュタスクが属するリソースグループ。このプロパティのデフォルト値は`default_mv_wg`で、マテリアライズドビューのリフレッシュ専用に使用されるシステム定義のリソースグループです。`default_mv_wg`の`cpu_core_limit`は`1`、`mem_limit`は`0.8`です。リソースグループの詳細については、[リソースグループ](../../../administration/management/resource_management/resource_group.md)を参照してください。
- `query_rewrite_consistency`: 非同期マテリアライズドビューのクエリの書き換えルール。このプロパティはv3.2からサポートされています。有効な値:
  - `disable`: 非同期マテリアライズドビューの自動クエリの書き換えを無効にします。
  - `checked` (デフォルト値): マテリアライズドビューが新鮮さの要件を満たしている場合にのみ自動クエリの書き換えを有効にします。つまり:
    - `mv_rewrite_staleness_second`が指定されていない場合、マテリアライズドビューはそのデータがすべてのベーステーブルのデータと一致している場合にのみクエリの書き換えに使用できます。
    - `mv_rewrite_staleness_second`が指定されている場合、マテリアライズドビューはその最終リフレッシュが新鮮さの時間間隔内である場合にクエリの書き換えに使用できます。
  - `loose`: 自動クエリの書き換えを直接有効にし、一貫性のチェックは必要ありません。
- `storage_volume`: 共有データクラスタを使用している場合、作成する非同期マテリアライズドビューを保存するために使用されるストレージボリュームの名前。このプロパティはv3.1以降でサポートされています。このプロパティが指定されていない場合、デフォルトのストレージボリュームが使用されます。例: `"storage_volume" = "def_volume"`。
- `force_external_table_query_rewrite`: 外部カタログベースのマテリアライズドビューのクエリの書き換えを有効にするかどうか。このプロパティはv3.2からサポートされています。有効な値:
  - `true`(v3.3以降のデフォルト値): 外部カタログベースのマテリアライズドビューのクエリの書き換えを有効にします。
  - `false`: 外部カタログベースのマテリアライズドビューのクエリの書き換えを無効にします。

  外部カタログベースのマテリアライズドビューとベーステーブル間の強いデータ一貫性は保証されないため、この機能はデフォルトで`false`に設定されています。この機能を有効にすると、`query_rewrite_consistency`で指定されたルールに従ってマテリアライズドビューがクエリの書き換えに使用されます。
- `enable_query_rewrite`: マテリアライズドビューをクエリの書き換えに使用するかどうか。マテリアライズドビューが多い場合、マテリアライズドビューに基づくクエリの書き換えはオプティマイザの時間消費に影響を与える可能性があります。このプロパティを使用して、マテリアライズドビューがクエリの書き換えに使用できるかどうかを制御できます。この機能はv3.3.0以降でサポートされています。有効な値:
  - `default` (デフォルト): システムはマテリアライズドビューに対してセマンティックチェックを行いませんが、SPJGタイプのマテリアライズドビューのみがクエリの書き換えに使用できます。テキストベースのクエリの書き換えが有効になっている場合、非SPJGタイプのマテリアライズドビューもクエリの書き換えに使用できます。
  - `true`: マテリアライズドビューの作成または変更時にシステムがセマンティックチェックを行います。マテリアライズドビューがクエリの書き換えに適していない場合 (つまり、マテリアライズドビューの定義がSPJGタイプのクエリではない場合)、失敗が返されます。
  - `false`: マテリアライズドビューはクエリの書き換えに使用されません。
- [プレビュー] `transparent_mv_rewrite_mode`: **マテリアライズドビューに直接対するクエリ**の透過的な書き換えモードを指定します。この機能はv3.3.0以降でサポートされています。有効な値:
  - `false` (デフォルト、以前のバージョンの動作と互換性あり): マテリアライズドビューに直接対するクエリは書き換えられず、マテリアライズドビューに存在するデータのみが返されます。これらのクエリの結果は、マテリアライズドビューの定義に基づくクエリの結果と、マテリアライズドビューのリフレッシュ状態 (データの一貫性) に応じて異なる場合があります。
  - `true`: マテリアライズドビューに直接対するクエリは書き換えられ、マテリアライズドビューの定義クエリの結果と一致する最新のデータが返されます。マテリアライズドビューが非アクティブであるか、透過的なクエリの書き換えをサポートしていない場合、これらのクエリはマテリアライズドビューの定義クエリとして実行されます。
  - `transparent_or_error`: マテリアライズドビューに直接対するクエリは、適格であれば書き換えられます。マテリアライズドビューが非アクティブであるか、透過的なクエリの書き換えをサポートしていない場合、これらのクエリはエラーとして返されます。
  - `transparent_or_default`: マテリアライズドビューに直接対するクエリは、適格であれば書き換えられます。マテリアライズドビューが非アクティブであるか、透過的なクエリの書き換えをサポートしていない場合、これらのクエリはマテリアライズドビューに存在するデータで返されます。

**query_statement** (必須)

非同期マテリアライズドビューを作成するためのクエリステートメント。v3.1.6以降、StarRocksはCommon Table Expression (CTE)を使用した非同期マテリアライズドビューの作成をサポートしています。

### 非同期マテリアライズドビューのクエリ

非同期マテリアライズドビューは物理テーブルです。通常のテーブルと同様に操作できますが、**非同期マテリアライズドビューに直接データをロードすることはできません**。

### 非同期マテリアライズドビューによる自動クエリの書き換え

StarRocks v2.5は、SPJGタイプの非同期マテリアライズドビューに基づく自動かつ透過的なクエリの書き換えをサポートしています。SPJGタイプのマテリアライズドビューは、プランにScan、Filter、Project、Aggregateタイプのオペレーターのみを含むマテリアライズドビューを指します。SPJGタイプのマテリアライズドビューのクエリの書き換えには、単一テーブルのクエリの書き換え、ジョインクエリの書き換え、集計クエリの書き換え、ユニオンクエリの書き換え、およびネストされたマテリアライズドビューに基づくクエリの書き換えが含まれます。

詳細については、[非同期マテリアライズドビュー - 非同期マテリアライズドビューを使用したクエリの書き換え](../../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md)を参照してください。

### サポートされているデータ型

- StarRocksのデフォルトカタログに基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています:

  - **日付**: DATE, DATETIME
  - **文字列**: CHAR, VARCHAR
  - **数値**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL, PERCENTILE
  - **半構造化**: ARRAY, JSON, MAP (v3.1以降), STRUCT (v3.1以降)
  - **その他**: BITMAP, HLL

> **注意**
>
> BITMAP, HLL, PERCENTILEはv2.4.5以降でサポートされています。

- StarRocksの外部カタログに基づいて作成された非同期マテリアライズドビューは、次のデータ型をサポートしています:

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

- 現在のバージョンのStarRocksは、同時に複数のマテリアライズドビューを作成することをサポートしていません。新しいマテリアライズドビューは、前のものが完了したときにのみ作成できます。

- 同期マテリアライズドビューについて:

  - 同期マテリアライズドビューは、単一の列に対する集計関数のみをサポートします。`sum(a+b)`の形式のクエリステートメントはサポートされていません。
  - 同期マテリアライズドビューは、ベーステーブルの各列に対して1つの集計関数のみをサポートします。`select sum(a), min(a) from table`のようなクエリステートメントはサポートされていません。
  - 集計関数を持つ同期マテリアライズドビューを作成する場合、GROUP BY句を指定し、SELECTで少なくとも1つのGROUP BY列を指定する必要があります。
  - 同期マテリアライズドビューは、JOINやGROUP BYのHAVING句などの句をサポートしていません。
  - ALTER TABLE DROP COLUMNを使用してベーステーブルの特定の列を削除する場合、ベーステーブルのすべての同期マテリアライズドビューに削除された列が含まれていないことを確認する必要があります。そうでない場合、削除操作は失敗します。列を削除する前に、その列を含むすべての同期マテリアライズドビューを削除する必要があります。
  - テーブルに対して同期マテリアライズドビューを作成しすぎると、データロードの効率に影響を与えます。ベーステーブルにデータがロードされるとき、同期マテリアライズドビューとベーステーブルのデータが同期的に更新されます。ベーステーブルに`n`個の同期マテリアライズドビューが含まれている場合、ベーステーブルにデータをロードする効率は`n`個のテーブルにデータをロードする効率とほぼ同じです。

- ネストされた非同期マテリアライズドビューについて:

  - 各マテリアライズドビューのリフレッシュ戦略は、対応するマテリアライズドビューにのみ適用されます。
  - 現在、StarRocksはネストのレベル数を制限していません。実稼働環境では、ネストのレイヤー数が3を超えないことをお勧めします。

- 外部カタログ非同期マテリアライズドビューについて:

  - 外部カタログマテリアライズドビューは、非同期固定間隔リフレッシュと手動リフレッシュのみをサポートします。
  - 外部カタログ内のマテリアライズドビューとベーステーブル間の厳密な一貫性は保証されません。
  - 現在、外部リソースに基づくマテリアライズドビューの構築はサポートされていません。
  - 現在、StarRocksは外部カタログ内のベーステーブルデータが変更されたかどうかを認識できないため、ベーステーブルがリフレッシュされるたびにすべてのパーティションがデフォルトでリフレッシュされます。[REFRESH MATERIALIZED VIEW](REFRESH_MATERIALIZED_VIEW.md)を使用して、一部のパーティションのみを手動でリフレッシュできます。

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

例1: 元のテーブルの列 (k1, k2) のみを含む同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2 as
select k1, k2 from duplicate_table;
```

マテリアライズドビューには、集計なしで2つの列k1とk2のみが含まれます。

```plain text
+-----------------+-------+--------+------+------+---------+-------+
| IndexName       | Field | Type   | Null | Key  | Default | Extra |
+-----------------+-------+--------+------+------+---------+-------+
| k1_k2           | k1    | INT    | Yes  | true | N/A     |       |
|                 | k2    | INT    | Yes  | true | N/A     |       |
+-----------------+-------+--------+------+------+---------+-------+
```

例2: k2でソートされた同期マテリアライズドビューを作成します。

```sql
create materialized view k2_order as
select k2, k1 from duplicate_table order by k2;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには、集計なしで2つの列k2とk1が含まれ、列k2はソート列です。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k1    | INT    | Yes  | false | N/A     | NONE  |
+-----------------+-------+--------+------+-------+---------+-------+
```

例3: k1とk2でグループ化し、k3に対してSUM集計を行う同期マテリアライズドビューを作成します。

```sql
create materialized view k1_k2_sumk3 as
select k1, k2, sum(k3) from duplicate_table group by k1, k2;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには、グループ化された列k1、k2と、k1とk2に基づいてグループ化されたk3列の合計であるsum (k3)が含まれます。

```plain text
+-----------------+-------+--------+------+-------+---------+-------+
| IndexName       | Field | Type   | Null | Key   | Default | Extra |
+-----------------+-------+--------+------+-------+---------+-------+
| k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
|                 | k2    | INT    | Yes  | true  | N/A     |       |
|                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
+-----------------+-------+--------+------+-------+---------+-------+
```

マテリアライズドビューはソート列を宣言していないため、StarRocksはデフォルトでグループ化された列k1とk2を補完します。

例4: 重複行を削除する同期マテリアライズドビューを作成します。

```sql
create materialized view deduplicate as
select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
```

マテリアライズドビューのスキーマは以下の通りです。マテリアライズドビューには、重複行のないk1、k2、k3、k4列が含まれます。

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

例5: ソート列を宣言しない非集計同期マテリアライズドビューを作成します。

ベーステーブルのスキーマは以下の通りです。

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

マテリアライズドビューには、k3、k4、k5、k6、k7列が含まれ、ソート列は宣言されていません。次のステートメントでマテリアライズドビューを作成します。

```sql
create materialized view mv_1 as
select k3, k4, k5, k6, k7 from all_type_table;
```

StarRocksはデフォルトでk3、k4、k5をソート列として自動的に使用します。これら3つの列タイプが占めるバイト数の合計は4 (INT) + 8 (BIGINT) + 16 (DECIMAL) = 28 < 36です。したがって、これら3つの列がソート列として追加されます。

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

k3、k4、k5列の`key`フィールドが`true`であることが観察され、これらがソートキーであることを示しています。k6、k7列の`key`フィールドは`false`であり、これらがソートキーではないことを示しています。

例6: WHERE句と複雑な式を含む同期マテリアライズドビューを作成します。

```SQL
-- ベーステーブル: user_eventを作成
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

  -- WHERE句と複雑な式を含むマテリアライズドビューを作成
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

例1: 非パーティション化されたマテリアライズドビューを作成します。

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

例2: パーティション化されたマテリアライズドビューを作成します。

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

-- date_trunc()関数を使用して、マテリアライズドビューを月単位でパーティション化します。
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

例3: 非同期マテリアライズドビューを作成します。

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

例4: パーティション化されたマテリアライズドビューを作成し、`str2date`を使用してベーステーブルのSTRING型パーティションキーをマテリアライズドビューのDATE型に変換します。

``` SQL

-- 文字列パーティション列を持つHiveテーブル。
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


-- `str2date`を使用してマテリアライズドビューを作成します。
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