---
displayed_sidebar: docs
sidebar_position: 10
---

# CBO の統計情報を収集する

このトピックでは、StarRocks のコストベースオプティマイザ (CBO) の基本概念と、CBO が最適なクエリプランを選択するための統計情報の収集方法について説明します。StarRocks 2.4 では、正確なデータ分布統計を収集するためにヒストグラムが導入されました。

v3.2.0 以降、StarRocks は Hive、Iceberg、および Hudi テーブルからの統計情報の収集をサポートしており、他のメタストアシステムへの依存を減らしています。構文は StarRocks 内部テーブルの収集と似ています。

## CBO とは

CBO はクエリ最適化において重要です。SQL クエリが StarRocks に到着すると、論理実行プランに解析されます。CBO は論理プランを複数の物理実行プランに書き換え、変換します。CBO はプラン内の各オペレータの実行コスト (CPU、メモリ、ネットワーク、I/O など) を推定し、最もコストの低いクエリパスを最終的な物理プランとして選択します。

StarRocks CBO は StarRocks 1.16.0 で導入され、1.19 以降はデフォルトで有効になっています。Cascades フレームワークに基づいて開発された StarRocks CBO は、さまざまな統計情報に基づいてコストを推定します。数万の実行プランの中から最もコストの低い実行プランを選択でき、複雑なクエリの効率とパフォーマンスを大幅に向上させます。

統計情報は CBO にとって重要です。これにより、コスト推定が正確で有用かどうかが決まります。以下のセクションでは、統計情報の種類、収集ポリシー、および統計情報の収集方法と統計情報の表示方法について詳しく説明します。

## 統計情報の種類

StarRocks はコスト推定の入力としてさまざまな統計情報を収集します。

### 基本統計情報

デフォルトで、StarRocks はテーブルと列の以下の基本統計情報を定期的に収集します。

- row_count: テーブル内の総行数

- data_size: 列のデータサイズ

- ndv: 列のカーディナリティ (列内の異なる値の数)

- null_count: 列内の NULL 値を持つデータの量

- min: 列内の最小値

- max: 列内の最大値

完全な統計情報は `_statistics_` データベースの `column_statistics` に保存されます。このテーブルを表示してテーブルの統計情報をクエリできます。以下はこのテーブルから統計データをクエリする例です。

```sql
SELECT * FROM _statistics_.column_statistics\G
*************************** 1. row ***************************
      table_id: 10174
  partition_id: 10170
   column_name: is_minor
         db_id: 10169
    table_name: zj_test.source_wiki_edit
partition_name: p06
     row_count: 2
     data_size: 2
           ndv: NULL
    null_count: 0
           max: 1
           min: 0
```

### ヒストグラム

StarRocks 2.4 では、基本統計情報を補完するためにヒストグラムが導入されました。ヒストグラムはデータ表現の効果的な方法と考えられています。データが偏っているテーブルに対して、ヒストグラムはデータ分布を正確に反映できます。

StarRocks は等高ヒストグラムを使用しており、いくつかのバケットに基づいて構築されています。各バケットには等量のデータが含まれています。選択性に大きな影響を与える頻繁にクエリされるデータ値に対して、StarRocks は個別のバケットを割り当てます。バケットが多いほど推定が正確になりますが、メモリ使用量がわずかに増加する可能性があります。ヒストグラム収集タスクのバケット数と最も一般的な値 (MCV) を調整できます。

**ヒストグラムは、データが非常に偏っていて頻繁にクエリされる列に適用されます。テーブルデータが均一に分布している場合、ヒストグラムを作成する必要はありません。ヒストグラムは、数値型、DATE、DATETIME、または文字列型の列にのみ作成できます。**

現在、StarRocks はヒストグラムの手動収集のみをサポートしています。ヒストグラムは `_statistics_` データベースの `histogram_statistics` テーブルに保存されます。以下はこのテーブルから統計データをクエリする例です。

```sql
SELECT * FROM _statistics_.histogram_statistics\G
*************************** 1. row ***************************
   table_id: 10174
column_name: added
      db_id: 10169
 table_name: zj_test.source_wiki_edit
    buckets: NULL
        mcv: [["23","1"],["5","1"]]
update_time: 2023-12-01 15:17:10.274000
```

## 収集タイプと方法

テーブル内のデータサイズとデータ分布は常に変化します。統計情報はデータの変化を表すために定期的に更新する必要があります。統計収集タスクを作成する前に、ビジネス要件に最適な収集タイプと方法を選択する必要があります。

StarRocks は完全収集とサンプル収集をサポートしており、どちらも自動および手動で実行できます。デフォルトでは、StarRocks はテーブルの完全統計情報を自動的に収集します。5 分ごとにデータの更新を確認します。データの変更が検出されると、データ収集が自動的にトリガーされます。自動完全収集を使用したくない場合は、収集タスクをカスタマイズできます。

| **収集タイプ** | **収集方法** | **説明** | **利点と欠点** |
| ------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 完全収集     | 自動/手動      | テーブル全体をスキャンして統計情報を収集します。統計情報はパーティションごとに収集されます。パーティションにデータの変更がない場合、このパーティションからデータは収集されず、リソース消費が削減されます。完全統計情報は `_statistics_.column_statistics` テーブルに保存されます。 | 利点: 統計情報が正確であり、CBO が正確な推定を行うのに役立ちます。欠点: システムリソースを消費し、遅いです。2.5 以降、StarRocks は自動収集期間を指定でき、リソース消費を削減します。 |
| サンプル収集  | 自動/手動      | テーブルの各パーティションから `N` 行のデータを均等に抽出します。統計情報はテーブルごとに収集されます。各列の基本統計情報は 1 つのレコードとして保存されます。列のカーディナリティ情報 (ndv) はサンプルデータに基づいて推定され、正確ではありません。サンプル統計情報は `_statistics_.table_statistic_v1` テーブルに保存されます。 | 利点: システムリソースをあまり消費せず、速いです。欠点: 統計情報が完全ではなく、コスト推定の正確性に影響を与える可能性があります。 |

## 統計情報を収集する

StarRocks は柔軟な統計収集方法を提供します。自動、手動、またはカスタム収集のいずれかを選択して、ビジネスシナリオに適した方法を選択できます。

### 自動収集

基本統計情報については、StarRocks はデフォルトでテーブルの完全統計情報を自動的に収集し、手動操作を必要としません。統計情報が収集されていないテーブルについては、StarRocks はスケジューリング期間内に自動的に統計情報を収集します。統計情報が収集されたテーブルについては、StarRocks はテーブル内の総行数と変更された行数を更新し、この情報を定期的に保持して自動収集をトリガーするかどうかを判断します。

2.4.5 以降、StarRocks は自動完全収集の収集期間を指定でき、これにより自動完全収集によるクラスタパフォーマンスのジッタを防ぎます。この期間は FE パラメータ `statistic_auto_analyze_start_time` と `statistic_auto_analyze_end_time` によって指定されます。

自動収集をトリガーする条件:

- 前回の統計収集以降にテーブルデータが変更された。

- 収集時間が設定された収集期間の範囲内にある。(デフォルトの収集期間は終日です。)

- 前回の収集ジョブの更新時間がパーティションの最新の更新時間よりも早い。

- テーブル統計情報の健康度が指定されたしきい値 (`statistic_auto_collect_ratio`) を下回っている。

> 統計情報の健康度を計算するための公式:
>
> データが更新されたパーティションの数が 10 未満の場合、公式は `1 - (前回の収集以降に更新された行数/総行数)` です。
> データが更新されたパーティションの数が 10 以上の場合、公式は `1 - MIN(前回の収集以降に更新された行数/総行数, 前回の収集以降に更新されたパーティション数/総パーティション数)` です。

さらに、StarRocks はテーブルサイズとテーブル更新頻度に基づいて収集ポリシーを設定できます:

- 小さなデータボリュームのテーブルについては、**統計情報はリアルタイムで制限なく収集されます。たとえテーブルデータが頻繁に更新されても。`statistic_auto_collect_small_table_size` パラメータを使用して、テーブルが小さなテーブルか大きなテーブルかを判断できます。また、`statistic_auto_collect_small_table_interval` を使用して小さなテーブルの収集間隔を設定できます。

- 大きなデータボリュームのテーブルについては、以下の制限が適用されます:

  - デフォルトの収集間隔は 12 時間以上であり、`statistic_auto_collect_large_table_interval` を使用して設定できます。

  - 収集間隔が満たされ、統計情報の健康度が自動サンプル収集のしきい値 (`statistic_auto_collect_sample_threshold`) を下回る場合、サンプル収集がトリガーされます。

  - 収集間隔が満たされ、統計情報の健康度が自動サンプル収集のしきい値 (`statistic_auto_collect_sample_threshold`) を上回り、自動収集のしきい値 (`statistic_auto_collect_ratio`) を下回る場合、完全収集がトリガーされます。

  - データを収集するパーティションのサイズ (`statistic_max_full_collect_data_size`) が 100 GB を超える場合、サンプル収集がトリガーされます。

  - 更新時間が前回の収集タスクの時間よりも遅いパーティションの統計情報のみが収集されます。データに変更がないパーティションの統計情報は収集されません。

:::tip

テーブルのデータが変更された後、このテーブルに対してサンプル収集タスクを手動でトリガーすると、サンプル収集タスクの更新時間がデータ更新時間よりも遅くなり、このスケジューリング期間にこのテーブルの自動完全収集がトリガーされません。
:::

自動完全収集はデフォルトで有効になっており、システムがデフォルト設定を使用して実行します。

以下の表はデフォルト設定を示しています。これらを変更する必要がある場合は、**ADMIN SET CONFIG** コマンドを実行してください。

| **FE** **設定項目**         | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect              | BOOLEAN  | TRUE              | 統計情報を収集するかどうか。このスイッチはデフォルトでオンになっています。 |
| enable_collect_full_statistic         | BOOLEAN  | TRUE              | 自動完全収集を有効にするかどうか。このスイッチはデフォルトでオンになっています。 |
| statistic_collect_interval_sec        | LONG     | 300               | 自動収集中のデータ更新を確認する間隔。単位: 秒。 |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | 自動収集の開始時間。値の範囲: `00:00:00` - `23:59:59`。 |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | 自動収集の終了時間。値の範囲: `00:00:00` - `23:59:59`。 |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 自動完全収集のためにテーブルが小さなテーブルか大きなテーブルかを判断するしきい値。この値を超えるサイズのテーブルは大きなテーブルと見なされ、この値以下のサイズのテーブルは小さなテーブルと見なされます。単位: バイト。デフォルト値: 5368709120 (5 GB)。                         |
| statistic_auto_collect_small_table_interval | LONG    | 0         | 小さなテーブルの完全統計情報を自動的に収集する間隔。単位: 秒。                              |
| statistic_auto_collect_large_table_interval | LONG    | 43200        | 大きなテーブルの完全統計情報を自動的に収集する間隔。単位: 秒。デフォルト値: 43200 (12 時間)。                               |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 自動収集の統計情報が健康かどうかを判断するしきい値。統計情報の健康度がこのしきい値を下回る場合、自動収集がトリガーされます。 |
| statistic_auto_collect_sample_threshold  | DOUBLE | 0.3   | 自動サンプル収集をトリガーするための統計情報の健康度のしきい値。統計情報の健康度がこのしきい値を下回る場合、自動サンプル収集がトリガーされます。 |
| statistic_max_full_collect_data_size | LONG      | 107374182400      | 自動収集のためにデータを収集するパーティションのデータサイズ。単位: バイト。デフォルト値: 107374182400 (100 GB)。データサイズがこの値を超える場合、完全収集は破棄され、サンプル収集が代わりに実行されます。 |
| statistic_full_collect_buffer | LONG | 20971520 | 自動収集タスクが使用する最大バッファサイズ。単位: バイト。デフォルト値: 20971520 (20 MB)。 |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | 単一の分析タスクでクエリする最大行数。この値を超える場合、分析タスクは複数のクエリに分割されます。 |
| statistic_collect_too_many_version_sleep | LONG | 600000 | 収集タスクが実行されるテーブルにデータバージョンが多すぎる場合の自動収集タスクのスリープ時間。単位: ミリ秒。デフォルト値: 600000 (10 分)。  |

統計情報の収集の大部分は自動ジョブに依存できますが、特定の要件がある場合は、ANALYZE TABLE ステートメントを実行して手動でタスクを作成するか、CREATE ANALYZE ステートメントを実行して自動タスクをカスタマイズできます。

### 手動収集

ANALYZE TABLE を使用して手動収集タスクを作成できます。デフォルトでは、手動収集は同期操作です。非同期操作に設定することもできます。非同期モードでは、ANALYZE TABLE を実行した後、システムはこのステートメントが成功したかどうかを即座に返します。ただし、収集タスクはバックグラウンドで実行され、結果を待つ必要はありません。SHOW ANALYZE STATUS を実行してタスクのステータスを確認できます。非同期収集は大きなデータボリュームのテーブルに適しており、同期収集は小さなデータボリュームのテーブルに適しています。**手動収集タスクは作成後に 1 回だけ実行されます。手動収集タスクを削除する必要はありません。** ANALYZE TABLE 操作を実行するには、対応するテーブルに対する INSERT および SELECT 権限が必要です。

#### 基本統計情報を手動で収集する

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES (property [,property])]
```

パラメータの説明:

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトで完全収集が使用されます。

- `col_name`: 統計情報を収集する列。複数の列はカンマ (`,`) で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。指定しない場合、デフォルトで同期収集が使用されます。

- `PROPERTIES`: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` ファイルのデフォルト設定が使用されます。SHOW ANALYZE STATUS の出力の `Properties` 列を通じて実際に使用されるプロパティを確認できます。

| **PROPERTIES**                | **タイプ** | **デフォルト値** | **説明**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | サンプル収集のために収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、完全収集が実行されます。 |

例

手動完全収集

```SQL
-- デフォルト設定を使用してテーブルの完全統計情報を手動で収集します。
ANALYZE TABLE tbl_name;

-- デフォルト設定を使用してテーブルの完全統計情報を手動で収集します。
ANALYZE FULL TABLE tbl_name;

-- デフォルト設定を使用してテーブル内の指定された列の統計情報を手動で収集します。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

手動サンプル収集

```SQL
-- デフォルト設定を使用してテーブルの部分的な統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name;

-- 収集する行数を指定して、テーブル内の指定された列の統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

#### ヒストグラムを手動で収集する

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

パラメータの説明:

- `col_name`: 統計情報を収集する列。複数の列はカンマ (`,`) で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。ヒストグラムにはこのパラメータが必要です。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。指定しない場合、デフォルトで同期収集が使用されます。

- `WITH N BUCKETS`: ヒストグラム収集のためのバケット数 `N`。指定しない場合、`fe.conf` のデフォルト値が使用されます。

- PROPERTIES: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                 | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | 収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、完全収集が実行されます。 |
| histogram_buckets_size         | LONG     | 64                | ヒストグラムのデフォルトバケット数。                   |
| histogram_mcv_size             | INT      | 100               | ヒストグラムの最も一般的な値 (MCV) の数。      |
| histogram_sample_ratio         | FLOAT    | 0.1               | ヒストグラムのサンプリング比率。                          |
| histogram_max_sample_row_count | LONG     | 10000000          | ヒストグラムのために収集する最大行数。       |

ヒストグラムの収集行数は複数のパラメータによって制御されます。`statistic_sample_collect_rows` とテーブル行数 * `histogram_sample_ratio` の間の大きい方の値です。この数は `histogram_max_sample_row_count` で指定された値を超えることはできません。値を超える場合、`histogram_max_sample_row_count` が優先されます。

SHOW ANALYZE STATUS の出力の `Properties` 列を通じて実際に使用されるプロパティを確認できます。

例

```SQL
-- デフォルト設定を使用して v1 にヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 32 バケット、32 MCV、および 50% のサンプリング比率で v1 と v2 にヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### カスタム収集

#### 自動収集タスクをカスタマイズする

CREATE ANALYZE ステートメントを使用して自動収集タスクをカスタマイズできます。ANALYZE TABLE 操作を実行するには、対応するテーブルに対する INSERT および SELECT 権限が必要です。

```SQL
-- すべてのデータベースの統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- データベース内のすべてのテーブルの統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
[PROPERTIES (property [,property])]

-- テーブル内の指定された列の統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

パラメータの説明:

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトでサンプル収集が使用されます。

- `col_name`: 統計情報を収集する列。複数の列はカンマ (`,`) で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。

- `PROPERTIES`: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                        | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 自動収集の統計情報が健康かどうかを判断するしきい値。統計情報の健康度がこのしきい値を下回る場合、自動収集がトリガーされます。 |
| statistics_max_full_collect_data_size | INT      | 100               | 自動収集のためにデータを収集する最大パーティションサイズ。単位: GB。パーティションがこの値を超える場合、完全収集は破棄され、サンプル収集が代わりに実行されます。 |
| statistic_sample_collect_rows         | INT      | 200000            | 収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、完全収集が実行されます。 |
| statistic_exclude_pattern             | String   | null              | ジョブで除外する必要があるデータベースまたはテーブルの名前。ジョブで統計情報を収集しないデータベースとテーブルを指定できます。これは正規表現パターンであり、マッチする内容は `database.table` です。 |
| statistic_auto_collect_interval       | LONG   |  0      | 自動収集の間隔。単位: 秒。デフォルトでは、StarRocks はテーブルサイズに基づいて `statistic_auto_collect_small_table_interval` または `statistic_auto_collect_large_table_interval` を収集間隔として選択します。分析ジョブを作成する際に `statistic_auto_collect_interval` プロパティを指定した場合、この設定が `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` よりも優先されます。 |

例

自動完全収集

```SQL
-- すべてのデータベースの完全統計情報を自動的に収集します。
CREATE ANALYZE ALL;

-- データベースの完全統計情報を自動的に収集します。
CREATE ANALYZE DATABASE db_name;

-- データベース内のすべてのテーブルの完全統計情報を自動的に収集します。
CREATE ANALYZE FULL DATABASE db_name;

-- テーブル内の指定された列の完全統計情報を自動的に収集します。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 

-- 指定されたデータベース 'db_name' を除外して、すべてのデータベースの統計情報を自動的に収集します。
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);
```

自動サンプル収集

```SQL
-- デフォルト設定でデータベース内のすべてのテーブルの統計情報を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 指定されたテーブル 'db_name.tbl_name' を除外して、データベース内のすべてのテーブルの統計情報を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);

-- 統計情報の健康度と収集する行数を指定して、テーブル内の指定された列の統計情報を自動的に収集します。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES (
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);

```

#### カスタム収集タスクを表示する

```SQL
SHOW ANALYZE JOB [WHERE predicate]
```

WHERE 句を使用して結果をフィルタリングできます。このステートメントは以下の列を返します。

| **列名**   | **説明**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | 収集タスクの ID。                               |
| Database     | データベース名。                                           |
| Table        | テーブル名。                                              |
| Columns      | 列名。                                            |
| Type         | 統計情報のタイプ。`FULL` と `SAMPLE` を含みます。       |
| Schedule     | スケジューリングのタイプ。自動タスクの場合は `SCHEDULE` です。 |
| Properties   | カスタムパラメータ。                                           |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED を含みます。 |
| LastWorkTime | 最後の収集時間。                             |
| Reason       | タスクが失敗した理由。タスクの実行が成功した場合は NULL が返されます。 |

例

```SQL
-- すべてのカスタム収集タスクを表示します。
SHOW ANALYZE JOB

-- データベース `test` のカスタム収集タスクを表示します。
SHOW ANALYZE JOB where `database` = 'test';
```

#### カスタム収集タスクを削除する

```SQL
DROP ANALYZE <ID>
```

タスク ID は SHOW ANALYZE JOB ステートメントを使用して取得できます。

例

```SQL
DROP ANALYZE 266030;
```

## 収集タスクのステータスを表示する

SHOW ANALYZE STATUS ステートメントを実行して、すべての現在のタスクのステータスを表示できます。このステートメントはカスタム収集タスクのステータスを表示するためには使用できません。カスタム収集タスクのステータスを表示するには、SHOW ANALYZE JOB を使用してください。

```SQL
SHOW ANALYZE STATUS [WHERE predicate];
```

`LIKE または WHERE` を使用して返す情報をフィルタリングできます。

このステートメントは以下の列を返します。

| **リスト名** | **説明**                                              |
| ------------- | ------------------------------------------------------------ |
| Id            | 収集タスクの ID。                               |
| Database      | データベース名。                                           |
| Table         | テーブル名。                                              |
| Columns       | 列名。                                            |
| Type          | 統計情報のタイプ。FULL、SAMPLE、HISTOGRAM を含みます。 |
| Schedule      | スケジューリングのタイプ。`ONCE` は手動、`SCHEDULE` は自動を意味します。 |
| Status        | タスクのステータス。                                      |
| StartTime     | タスクの実行開始時間。                     |
| EndTime       | タスクの実行終了時間。                       |
| Properties    | カスタムパラメータ。                                           |
| Reason        | タスクが失敗した理由。実行が成功した場合は NULL が返されます。 |

## 統計情報を表示する

### 基本統計情報のメタデータを表示する

```SQL
SHOW STATS META [WHERE predicate]
```

このステートメントは以下の列を返します。

| **列名** | **説明**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | データベース名。                                           |
| Table      | テーブル名。                                              |
| Columns    | 列名。                                            |
| Type       | 統計情報のタイプ。`FULL` は完全収集、`SAMPLE` はサンプル収集を意味します。 |
| UpdateTime | 現在のテーブルの最新の統計情報更新時間。     |
| Properties | カスタムパラメータ。                                           |
| Healthy    | 統計情報の健康度。                       |

### ヒストグラムのメタデータを表示する

```SQL
SHOW HISTOGRAM META [WHERE predicate]
```

このステートメントは以下の列を返します。

| **列名** | **説明**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | データベース名。                                           |
| Table      | テーブル名。                                              |
| Column     | 列。                                                 |
| Type       | 統計情報のタイプ。ヒストグラムの場合は `HISTOGRAM` です。 |
| UpdateTime | 現在のテーブルの最新の統計情報更新時間。     |
| Properties | カスタムパラメータ。                                           |

## 統計情報を削除する

不要な統計情報を削除できます。統計情報を削除すると、統計情報のデータとメタデータ、および期限切れキャッシュ内の統計情報が削除されます。自動収集タスクが進行中の場合、以前に削除された統計情報が再度収集される可能性があることに注意してください。収集タスクの履歴を表示するには、`SHOW ANALYZE STATUS` を使用できます。

### 基本統計情報を削除する

```SQL
DROP STATS tbl_name
```

### ヒストグラムを削除する

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## 収集タスクをキャンセルする

KILL ANALYZE ステートメントを使用して、**実行中の**収集タスク (手動およびカスタムタスクを含む) をキャンセルできます。

```SQL
KILL ANALYZE <ID>
```

手動収集タスクのタスク ID は SHOW ANALYZE STATUS から取得できます。カスタム収集タスクのタスク ID は SHOW ANALYZE JOB から取得できます。

## その他の FE 設定項目

| **FE 設定項目**        | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_manager_sleep_time_sec     | LONG     | 60                | メタデータがスケジュールされる間隔。単位: 秒。システムはこの間隔に基づいて以下の操作を実行します: 統計情報を保存するためのテーブルを作成します。削除された統計情報を削除します。期限切れの統計情報を削除します。 |
| statistic_analyze_status_keep_second | LONG     | 259200            | 収集タスクの履歴を保持する期間。単位: 秒。デフォルト値: 259200 (3 日)。 |

## セッション変数

`statistic_collect_parallel`: BEs で実行できる統計収集タスクの並行性を調整するために使用されます。デフォルト値: 1。この値を増やすことで収集タスクの速度を上げることができます。

## Hive/Iceberg/Hudi テーブルの統計情報を収集する

v3.2.0 以降、StarRocks は Hive、Iceberg、および Hudi テーブルの統計情報の収集をサポートしています。構文は StarRocks 内部テーブルの収集と似ています。**ただし、手動完全収集、手動ヒストグラム収集 (v3.2.7 以降)、および自動完全収集のみがサポートされています。サンプル収集はサポートされていません。** v3.3.0 以降、StarRocks は STRUCT のサブフィールドの統計情報の収集をサポートしています。

収集された統計情報は `default_catalog` の `_statistics_` 内の `external_column_statistics` テーブルに保存されます。これらは Hive Metastore に保存されず、他の検索エンジンと共有することはできません。Hive/Iceberg/Hudi テーブルの統計情報が収集されているかどうかを確認するために、`default_catalog._statistics_.external_column_statistics` テーブルからデータをクエリできます。

以下は `external_column_statistics` から統計データをクエリする例です。

```sql
SELECT * FROM _statistics_.external_column_statistics\G
*************************** 1. row ***************************
    table_uuid: hive_catalog.tn_test.ex_hive_tbl.1673596430
partition_name: 
   column_name: k1
  catalog_name: hive_catalog
       db_name: tn_test
    table_name: ex_hive_tbl
     row_count: 3
     data_size: 12
           ndv: NULL
    null_count: 0
           max: 3
           min: 2
   update_time: 2023-12-01 14:57:27.137000
```

### 制限

Hive、Iceberg、Hudi テーブルの統計情報を収集する際に適用される制限は次のとおりです:

1. Hive、Iceberg、および Hudi テーブルの統計情報のみを収集できます。
2. 手動完全収集、手動ヒストグラム収集 (v3.2.7 以降)、および自動完全収集のみがサポートされています。サンプル収集はサポートされていません。
3. システムが完全統計情報を自動的に収集するためには、Analyze ジョブを作成する必要があります。これは、システムがデフォルトでバックグラウンドでこれを行う StarRocks 内部テーブルの統計情報の収集とは異なります。
4. 自動収集タスクでは、特定のテーブルの統計情報のみを収集できます。データベース内のすべてのテーブルの統計情報や外部カタログ内のすべてのデータベースの統計情報を収集することはできません。
5. 自動収集タスクでは、StarRocks は Hive および Iceberg テーブル内のデータが更新されたかどうかを検出し、更新されたパーティションの統計情報のみを収集します。StarRocks は Hudi テーブル内のデータが更新されたかどうかを認識できず、定期的な完全収集のみを実行できます。

以下の例は Hive 外部カタログ内のデータベースで発生します。`default_catalog` から Hive テーブルの統計情報を収集したい場合は、テーブルを `[catalog_name.][database_name.]<table_name>` 形式で参照してください。

### 手動収集

必要に応じて Analyze ジョブを作成でき、ジョブは作成後すぐに実行されます。

#### 手動収集タスクを作成する

構文:

```sql
-- 手動完全収集
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]

-- 手動ヒストグラム収集 (v3.3.0 以降)
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

以下は手動完全収集の例です:

```sql
ANALYZE TABLE ex_hive_tbl(k1);
+----------------------------------+---------+----------+----------+
| Table                            | Op      | Msg_type | Msg_text |
+----------------------------------+---------+----------+----------+
| hive_catalog.tn_test.ex_hive_tbl | analyze | status   | OK       |
+----------------------------------+---------+----------+----------+
```

#### タスクのステータスを表示する

構文:

```sql
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

例:

```sql
SHOW ANALYZE STATUS where `table` = 'ex_hive_tbl';
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| Id    | Database             | Table       | Columns | Type | Schedule | Status  | StartTime           | EndTime             | Properties | Reason |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| 16400 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:31:42 | 2023-12-04 16:31:42 | {}         |        |
| 16465 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:35 | 2023-12-04 16:37:35 | {}         |        |
| 16467 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:46 | 2023-12-04 16:37:46 | {}         |        |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
```

#### 統計情報のメタデータを表示する

構文:

```sql
SHOW STATS META [WHERE predicate]
```

例:

```sql
SHOW STATS META where `table` = 'ex_hive_tbl';
+----------------------+-------------+---------+------+---------------------+------------+---------+
| Database             | Table       | Columns | Type | UpdateTime          | Properties | Healthy |
+----------------------+-------------+---------+------+---------------------+------------+---------+
| hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | 2023-12-04 16:37:46 | {}         |         |
+----------------------+-------------+---------+------+---------------------+------------+---------+
```

#### 収集タスクをキャンセルする

実行中の収集タスクをキャンセルします。

構文:

```sql
KILL ANALYZE <ID>
```

タスク ID は SHOW ANALYZE STATUS の出力で確認できます。

### 自動収集

外部データソース内のテーブルの統計情報をシステムが自動的に収集するためには、Analyze ジョブを作成できます。StarRocks はデフォルトのチェック間隔である 5 分ごとにタスクを実行するかどうかを自動的に確認します。Hive および Iceberg テーブルの場合、StarRocks はテーブル内のデータが更新された場合にのみ収集タスクを実行します。

ただし、Hudi テーブル内のデータ変更は認識できず、StarRocks は指定したチェック間隔と収集間隔に基づいて定期的に統計情報を収集します。収集動作を制御するために次の FE 設定項目を指定できます:

- statistic_collect_interval_sec

  自動収集中のデータ更新を確認する間隔。単位: 秒。デフォルト: 5 分。

- statistic_auto_collect_small_table_rows (v3.2 以降)

  自動収集中に外部データソース (Hive、Iceberg、Hudi) 内のテーブルが小さなテーブルかどうかを判断するためのしきい値。デフォルト: 10000000。

- statistic_auto_collect_small_table_interval

  小さなテーブルの統計情報を収集する間隔。単位: 秒。デフォルト: 0。

- statistic_auto_collect_large_table_interval

  大きなテーブルの統計情報を収集する間隔。単位: 秒。デフォルト: 43200 (12 時間)。

自動収集スレッドは `statistic_collect_interval_sec` で指定された間隔でデータ更新を確認します。テーブル内の行数が `statistic_auto_collect_small_table_rows` より少ない場合、`statistic_auto_collect_small_table_interval` に基づいてそのようなテーブルの統計情報を収集します。

テーブル内の行数が `statistic_auto_collect_small_table_rows` を超える場合、`statistic_auto_collect_large_table_interval` に基づいてそのようなテーブルの統計情報を収集します。大きなテーブルの統計情報は `Last table update time + Collection interval > Current time` の場合にのみ更新されます。これにより、大きなテーブルの頻繁な分析タスクが防止されます。

#### 自動収集タスクを作成する

構文:

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

自動収集タスク専用の収集間隔を設定するためにプロパティ `statistic_auto_collect_interval` を指定できます。このタスクには FE 設定項目 `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` は適用されません。

例:

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

#### 自動収集タスクのステータスを表示する

手動収集と同じです。

#### 統計情報のメタデータを表示する

手動収集と同じです。

#### 自動収集タスクを表示する

構文:

```sql
SHOW ANALYZE JOB [WHERE predicate]
```

例:

```sql
SHOW ANALYZE JOB WHERE `id` = '17204';

Empty set (0.00 sec)
```

#### 収集タスクをキャンセルする

手動収集と同じです。

#### 統計情報を削除する

```sql
DROP STATS tbl_name
```

## 参考文献

- FE 設定項目をクエリするには、[ADMIN SHOW CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を実行します。

- FE 設定項目を変更するには、[ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を実行します。