---
displayed_sidebar: docs
sidebar_position: 10
---

# CBO の統計収集

このトピックでは、StarRocks コストベースオプティマイザ (CBO) の基本概念と、最適なクエリプランを選択するために CBO の統計を収集する方法について説明します。StarRocks 2.4 では、正確なデータ分布統計を収集するためにヒストグラムが導入されました。

v3.2.0 以降、StarRocks は Hive、Iceberg、Hudi テーブルからの統計収集をサポートしており、他のメタストアシステムへの依存を減らしています。構文は StarRocks 内部テーブルの収集と似ています。

## CBO とは

CBO はクエリ最適化において重要です。SQL クエリが StarRocks に到着すると、論理実行プランに解析されます。CBO は論理プランを複数の物理実行プランに書き換え、変換します。その後、CBO はプラン内の各オペレーターの実行コスト（CPU、メモリ、ネットワーク、I/O など）を推定し、最もコストの低いクエリパスを最終的な物理プランとして選択します。

StarRocks CBO は StarRocks 1.16.0 で導入され、1.19 以降はデフォルトで有効になっています。Cascades フレームワークに基づいて開発された StarRocks CBO は、さまざまな統計情報に基づいてコストを推定します。数万の実行プランの中から最もコストの低い実行プランを選択することができ、複雑なクエリの効率とパフォーマンスを大幅に向上させます。

統計は CBO にとって重要です。統計がコスト推定の正確性と有用性を決定します。以下のセクションでは、統計情報の種類、収集ポリシー、統計の収集方法、および統計情報の表示方法について詳しく説明します。

## 統計情報の種類

StarRocks はコスト推定の入力としてさまざまな統計を収集します。

### 基本統計

デフォルトで、StarRocks は定期的にテーブルとカラムの以下の基本統計を収集します。

- row_count: テーブル内の総行数

- data_size: カラムのデータサイズ

- ndv: カラムのカーディナリティ（カラム内の異なる値の数）

- null_count: カラム内の NULL 値を持つデータの量

- min: カラム内の最小値

- max: カラム内の最大値

完全な統計は `_statistics_` データベースの `column_statistics` に保存されます。このテーブルを表示してテーブル統計をクエリできます。以下はこのテーブルから統計データをクエリする例です。

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

StarRocks 2.4 では、基本統計を補完するためにヒストグラムが導入されました。ヒストグラムはデータ表現の効果的な方法と考えられています。データが偏っているテーブルに対して、ヒストグラムはデータ分布を正確に反映できます。

StarRocks は等高ヒストグラムを使用しており、いくつかのバケットで構成されています。各バケットには同量のデータが含まれます。頻繁にクエリされ、選択性に大きな影響を与えるデータ値には、StarRocks は個別のバケットを割り当てます。バケットが多いほど推定が正確になりますが、メモリ使用量がわずかに増加する可能性があります。ヒストグラム収集タスクのバケット数と最も一般的な値 (MCV) を調整できます。

**ヒストグラムは、データが非常に偏っており、頻繁にクエリされるカラムに適用されます。テーブルデータが均等に分布している場合、ヒストグラムを作成する必要はありません。ヒストグラムは、数値、DATE、DATETIME、または文字列型のカラムにのみ作成できます。**

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

テーブル内のデータサイズとデータ分布は常に変化します。統計はデータの変化を表すために定期的に更新する必要があります。統計収集タスクを作成する前に、ビジネス要件に最適な収集タイプと方法を選択する必要があります。

StarRocks はフル収集とサンプル収集をサポートしており、どちらも自動および手動で実行できます。デフォルトで、StarRocks はテーブルのフル統計を自動的に収集します。5 分ごとにデータ更新をチェックします。データの変更が検出されると、データ収集が自動的にトリガーされます。自動フル収集を使用したくない場合は、FE 設定項目 `enable_collect_full_statistic` を `false` に設定し、収集タスクをカスタマイズできます。

| **収集タイプ** | **収集方法** | **説明** | **利点と欠点** |
| ------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| フル収集     | 自動/手動      | テーブル全体をスキャンして統計を収集します。統計はパーティションごとに収集されます。パーティションにデータの変更がない場合、そのパーティションからデータは収集されず、リソース消費が削減されます。完全な統計は `_statistics_.column_statistics` テーブルに保存されます。 | 利点: 統計が正確であり、CBO が正確な推定を行うのに役立ちます。欠点: システムリソースを消費し、遅いです。2.5 以降、StarRocks は自動収集期間を指定でき、リソース消費を削減します。 |
| サンプル収集  | 自動/手動      | テーブルの各パーティションから `N` 行のデータを均等に抽出します。統計はテーブルごとに収集されます。各カラムの基本統計は 1 レコードとして保存されます。カラムのカーディナリティ情報 (ndv) はサンプルデータに基づいて推定され、正確ではありません。サンプル統計は `_statistics_.table_statistic_v1` テーブルに保存されます。 | 利点: システムリソースを消費せず、迅速です。欠点: 統計が完全ではなく、コスト推定の正確性に影響を与える可能性があります。 |

## 統計の収集

StarRocks は柔軟な統計収集方法を提供します。ビジネスシナリオに応じて、自動、手動、またはカスタム収集を選択できます。

### 自動フル収集

基本統計については、StarRocks はデフォルトでテーブルのフル統計を自動的に収集し、手動操作を必要としません。統計が収集されていないテーブルに対して、StarRocks はスケジューリング期間内に自動的に統計を収集します。統計が収集されたテーブルに対して、StarRocks はテーブル内の総行数と変更された行数を更新し、この情報を定期的に保存して自動収集をトリガーするかどうかを判断します。

2.4.5 以降、StarRocks は自動フル収集の収集期間を指定でき、自動フル収集によるクラスターのパフォーマンスの揺れを防ぎます。この期間は FE パラメータ `statistic_auto_analyze_start_time` と `statistic_auto_analyze_end_time` によって指定されます。

自動収集をトリガーする条件:

- `enable_statistic_collect` が `true` である

- 収集時間が設定された収集期間の範囲内である（デフォルトの収集期間は終日です）

- 前回の収集ジョブの更新時間がパーティションの最新の更新時間よりも早い

- テーブル統計の健康状態が指定されたしきい値（`statistic_auto_collect_ratio`）を下回っている

> 統計の健康状態を計算するための公式: 1 - 前回の統計収集以降に追加された行数/最小パーティションの総行数

さらに、StarRocks はテーブルサイズとテーブル更新頻度に基づいて収集ポリシーを設定できます。

- データ量が少ないテーブルに対して、**統計はリアルタイムで制限なく収集されます。たとえテーブルデータが頻繁に更新されていてもです。`statistic_auto_collect_small_table_size` パラメータを使用して、テーブルが小さいか大きいかを判断できます。また、`statistic_auto_collect_small_table_interval` を使用して小さいテーブルの収集間隔を設定できます。

- データ量が多いテーブルに対して、以下の制限が適用されます。

  - デフォルトの収集間隔は 12 時間以上であり、`statistic_auto_collect_large_table_interval` を使用して設定できます。

  - 収集間隔が満たされ、統計の健康状態が自動サンプル収集のしきい値（`statistic_auto_collect_sample_threshold`）を下回っている場合、サンプル収集がトリガーされます。

  - 収集間隔が満たされ、統計の健康状態が自動サンプル収集のしきい値（`statistic_auto_collect_sample_threshold`）を上回り、自動収集のしきい値（`statistic_auto_collect_ratio`）を下回っている場合、フル収集がトリガーされます。

  - 収集データのパーティションサイズ（`statistic_max_full_collect_data_size`）が 100 GB を超える場合、サンプル収集がトリガーされます。

  - 更新時間が前回の収集タスクの時間よりも遅いパーティションの統計のみが収集されます。データ変更のないパーティションの統計は収集されません。

:::tip

テーブルのデータが変更された後、このテーブルに対してサンプル収集タスクを手動でトリガーすると、サンプル収集タスクの更新時間がデータ更新時間よりも遅くなり、このスケジューリング期間内に自動フル収集がトリガーされません。
:::

自動フル収集はデフォルトで有効になっており、システムがデフォルト設定を使用して実行します。

以下の表はデフォルト設定を示しています。これらを変更する必要がある場合は、**ADMIN SET CONFIG** コマンドを実行してください。

| **FE** **設定項目**         | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect              | BOOLEAN  | TRUE              | 統計を収集するかどうか。このスイッチはデフォルトでオンになっています。 |
| enable_collect_full_statistic         | BOOLEAN  | TRUE              | 自動フル収集を有効にするかどうか。このスイッチはデフォルトでオンになっています。 |
| statistic_collect_interval_sec        | LONG     | 300               | 自動収集中のデータ更新をチェックする間隔。単位: 秒。 |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | 自動収集の開始時間。値の範囲: `00:00:00` - `23:59:59`。 |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | 自動収集の終了時間。値の範囲: `00:00:00` - `23:59:59`。 |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 自動フル収集のためにテーブルが小さいか大きいかを判断するしきい値。この値を超えるテーブルは大きいテーブルと見なされ、この値以下のテーブルは小さいテーブルと見なされます。単位: バイト。デフォルト値: 5368709120 (5 GB)。                         |
| statistic_auto_collect_small_table_interval | LONG    | 0         | 小さいテーブルのフル統計を自動収集する間隔。単位: 秒。                              |
| statistic_auto_collect_large_table_interval | LONG    | 43200        | 大きいテーブルのフル統計を自動収集する間隔。単位: 秒。デフォルト値: 43200 (12 時間)。                               |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 自動収集のための統計が健康かどうかを判断するしきい値。統計の健康状態がこのしきい値を下回る場合、自動収集がトリガーされます。 |
| statistic_auto_collect_sample_threshold  | DOUBLE | 0.3   | 自動サンプル収集をトリガーするための統計の健康しきい値。統計の健康値がこのしきい値を下回る場合、自動サンプル収集がトリガーされます。 |
| statistic_max_full_collect_data_size | LONG      | 107374182400      | 自動収集のためにデータを収集するパーティションのデータサイズ。単位: バイト。デフォルト値: 107374182400 (100 GB)。データサイズがこの値を超える場合、フル収集は破棄され、サンプル収集が実行されます。 |
| statistic_full_collect_buffer | LONG | 20971520 | 自動収集タスクが使用する最大バッファサイズ。単位: バイト。デフォルト値: 20971520 (20 MB)。 |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | 単一の分析タスクでクエリする最大行数。この値を超える場合、分析タスクは複数のクエリに分割されます。 |
| statistic_collect_too_many_version_sleep | LONG | 600000 | 収集タスクが実行されるテーブルに多くのデータバージョンがある場合の自動収集タスクのスリープ時間。単位: ミリ秒。デフォルト値: 600000 (10 分)。  |

統計収集の大部分は自動ジョブに依存できますが、特定の要件がある場合は、ANALYZE TABLE ステートメントを実行してタスクを手動で作成するか、CREATE ANALYZE ステートメントを実行して自動タスクをカスタマイズできます。

### 手動収集

ANALYZE TABLE を使用して手動収集タスクを作成できます。デフォルトでは、手動収集は同期操作です。非同期操作に設定することもできます。非同期モードでは、ANALYZE TABLE を実行した後、システムはこのステートメントが成功したかどうかを即座に返します。ただし、収集タスクはバックグラウンドで実行され、結果を待つ必要はありません。SHOW ANALYZE STATUS を実行してタスクのステータスを確認できます。非同期収集はデータ量が多いテーブルに適しており、同期収集はデータ量が少ないテーブルに適しています。**手動収集タスクは作成後に 1 回だけ実行されます。手動収集タスクを削除する必要はありません。** ANALYZE TABLE 操作を実行するには、対応するテーブルに対する INSERT および SELECT 権限が必要です。

#### 基本統計を手動で収集する

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES (property [,property])]
```

パラメータの説明:

- 収集タイプ
  - FULL: フル収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトでフル収集が使用されます。

- `col_name`: 統計を収集するカラム。複数のカラムをカンマ（`,`）で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。デフォルトで同期収集が使用されます。

- `PROPERTIES`: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` ファイルのデフォルト設定が使用されます。SHOW ANALYZE STATUS の出力の `Properties` 列を介して実際に使用されるプロパティを表示できます。

| **PROPERTIES**                | **タイプ** | **デフォルト値** | **説明**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | サンプル収集のために収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、フル収集が実行されます。 |

例

手動フル収集

```SQL
-- デフォルト設定を使用してテーブルのフル統計を手動で収集します。
ANALYZE TABLE tbl_name;

-- デフォルト設定を使用してテーブルのフル統計を手動で収集します。
ANALYZE FULL TABLE tbl_name;

-- デフォルト設定を使用してテーブル内の指定されたカラムの統計を手動で収集します。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

手動サンプル収集

```SQL
-- デフォルト設定を使用してテーブルの部分統計を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name;

-- 収集する行数を指定して、テーブル内の指定されたカラムの統計を手動で収集します。
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

- `col_name`: 統計を収集するカラム。複数のカラムをカンマ（`,`）で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。ヒストグラムにはこのパラメータが必要です。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。デフォルトで同期収集が使用されます。

- `WITH N BUCKETS`: ヒストグラム収集のためのバケット数 `N`。指定されていない場合、`fe.conf` のデフォルト値が使用されます。

- PROPERTIES: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                 | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | 収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、フル収集が実行されます。 |
| histogram_buckets_size         | LONG     | 64                | ヒストグラムのデフォルトバケット数。                   |
| histogram_mcv_size             | INT      | 100               | ヒストグラムの最も一般的な値 (MCV) の数。      |
| histogram_sample_ratio         | FLOAT    | 0.1               | ヒストグラムのサンプリング比率。                          |
| histogram_max_sample_row_count | LONG     | 10000000          | ヒストグラムのために収集する最大行数。       |

ヒストグラムの収集行数は複数のパラメータによって制御されます。`statistic_sample_collect_rows` とテーブル行数 * `histogram_sample_ratio` の大きい方の値が使用されます。この数は `histogram_max_sample_row_count` で指定された値を超えることはできません。値が超えた場合、`histogram_max_sample_row_count` が優先されます。

SHOW ANALYZE STATUS の出力の `Properties` 列を介して実際に使用されるプロパティを表示できます。

例

```SQL
-- デフォルト設定を使用して v1 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 32 バケット、32 MCV、および 50% のサンプリング比率で v1 と v2 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### カスタム収集

#### 自動収集タスクをカスタマイズする

CREATE ANALYZE ステートメントを使用して自動収集タスクをカスタマイズできます。ANALYZE TABLE 操作を実行するには、対応するテーブルに対する INSERT および SELECT 権限が必要です。

カスタム自動収集タスクを作成する前に、自動フル収集を無効にする必要があります（`enable_collect_full_statistic = false`）。そうしないと、カスタムタスクが有効になりません。

```SQL
-- すべてのデータベースの統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- データベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
[PROPERTIES (property [,property])]

-- テーブル内の指定されたカラムの統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

パラメータの説明:

- 収集タイプ
  - FULL: フル収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトでサンプル収集が使用されます。

- `col_name`: 統計を収集するカラム。複数のカラムをカンマ（`,`）で区切ります。このパラメータが指定されていない場合、テーブル全体が収集されます。

- `PROPERTIES`: カスタムパラメータ。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                        | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 自動収集のための統計が健康かどうかを判断するしきい値。統計の健康状態がこのしきい値を下回る場合、自動収集がトリガーされます。 |
| statistics_max_full_collect_data_size | INT      | 100               | 自動収集のためにデータを収集する最大パーティションサイズ。単位: GB。パーティションがこの値を超える場合、フル収集は破棄され、サンプル収集が実行されます。 |
| statistic_sample_collect_rows         | INT      | 200000            | 収集する最小行数。このパラメータ値がテーブルの実際の行数を超える場合、フル収集が実行されます。 |
| statistic_exclude_pattern             | String   | null              | ジョブで除外する必要があるデータベースまたはテーブルの名前。ジョブで統計を収集しないデータベースとテーブルを指定できます。これは正規表現パターンであり、マッチする内容は `database.table` です。 |
| statistic_auto_collect_interval       | LONG   |  0      | 自動収集の間隔。単位: 秒。デフォルトでは、StarRocks はテーブルサイズに基づいて `statistic_auto_collect_small_table_interval` または `statistic_auto_collect_large_table_interval` を収集間隔として選択します。分析ジョブを作成する際に `statistic_auto_collect_interval` プロパティを指定した場合、この設定が `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` よりも優先されます。 |

例

自動フル収集

```SQL
-- すべてのデータベースのフル統計を自動的に収集します。
CREATE ANALYZE ALL;

-- データベースのフル統計を自動的に収集します。
CREATE ANALYZE DATABASE db_name;

-- データベース内のすべてのテーブルのフル統計を自動的に収集します。
CREATE ANALYZE FULL DATABASE db_name;

-- テーブル内の指定されたカラムのフル統計を自動的に収集します。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 

-- 指定されたデータベース 'db_name' を除外して、すべてのデータベースの統計を自動的に収集します。
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);
```

自動サンプル収集

```SQL
-- デフォルト設定でデータベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 指定されたテーブル 'db_name.tbl_name' を除外して、データベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);

-- 統計の健康状態と収集する行数を指定して、テーブル内の指定されたカラムの統計を自動的に収集します。
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
| Columns      | カラム名。                                            |
| Type         | 統計のタイプ。`FULL` と `SAMPLE` を含みます。       |
| Schedule     | スケジューリングのタイプ。自動タスクの場合は `SCHEDULE` です。 |
| Properties   | カスタムパラメータ。                                           |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED を含みます。 |
| LastWorkTime | 最後の収集の時間。                             |
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

SHOW ANALYZE STATUS ステートメントを実行して、現在のすべてのタスクのステータスを表示できます。このステートメントはカスタム収集タスクのステータスを表示するために使用できません。カスタム収集タスクのステータスを表示するには、SHOW ANALYZE JOB を使用してください。

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
| Columns       | カラム名。                                            |
| Type          | 統計のタイプ。FULL、SAMPLE、HISTOGRAM を含みます。 |
| Schedule      | スケジューリングのタイプ。`ONCE` は手動、`SCHEDULE` は自動を意味します。 |
| Status        | タスクのステータス。                                      |
| StartTime     | タスクが実行を開始した時間。                     |
| EndTime       | タスクの実行が終了した時間。                       |
| Properties    | カスタムパラメータ。                                           |
| Reason        | タスクが失敗した理由。実行が成功した場合は NULL が返されます。 |

## 統計を表示する

### 基本統計のメタデータを表示する

```SQL
SHOW STATS META [WHERE predicate]
```

このステートメントは以下の列を返します。

| **列名** | **説明**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | データベース名。                                           |
| Table      | テーブル名。                                              |
| Columns    | カラム名。                                            |
| Type       | 統計のタイプ。`FULL` はフル収集、`SAMPLE` はサンプル収集を意味します。 |
| UpdateTime | 現在のテーブルの最新の統計更新時間。     |
| Properties | カスタムパラメータ。                                           |
| Healthy    | 統計情報の健康状態。                       |

### ヒストグラムのメタデータを表示する

```SQL
SHOW HISTOGRAM META [WHERE predicate]
```

このステートメントは以下の列を返します。

| **列名** | **説明**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | データベース名。                                           |
| Table      | テーブル名。                                              |
| Column     | カラム。                                                 |
| Type       | 統計のタイプ。ヒストグラムの場合は `HISTOGRAM` です。 |
| UpdateTime | 現在のテーブルの最新の統計更新時間。     |
| Properties | カスタムパラメータ。                                           |

## 統計を削除する

不要な統計情報を削除できます。統計を削除すると、統計のデータとメタデータ、および期限切れキャッシュ内の統計も削除されます。自動収集タスクが進行中の場合、以前に削除された統計が再度収集される可能性があります。収集タスクの履歴を表示するには、`SHOW ANALYZE STATUS` を使用できます。

### 基本統計を削除する

```SQL
DROP STATS tbl_name
```

### ヒストグラムを削除する

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## 収集タスクをキャンセルする

KILL ANALYZE ステートメントを使用して、**実行中の**収集タスクをキャンセルできます。これには手動およびカスタムタスクが含まれます。

```SQL
KILL ANALYZE <ID>
```

手動収集タスクのタスク ID は SHOW ANALYZE STATUS から取得できます。カスタム収集タスクのタスク ID は SHOW ANALYZE JOB から取得できます。

## その他の FE 設定項目

| **FE 設定項目**        | **タイプ** | **デフォルト値** | **説明**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_collect_concurrency        | INT      | 3                 | 並行して実行できる手動収集タスクの最大数。デフォルト値は 3 で、最大 3 つの手動収集タスクを並行して実行できます。この値を超えると、受信タスクは PENDING 状態になり、スケジュールを待ちます。 |
| statistic_manager_sleep_time_sec     | LONG     | 60                | メタデータがスケジュールされる間隔。単位: 秒。システムはこの間隔に基づいて以下の操作を実行します: 統計を保存するテーブルを作成します。削除された統計を削除します。期限切れの統計を削除します。 |
| statistic_analyze_status_keep_second | LONG     | 259200            | 収集タスクの履歴を保持する期間。単位: 秒。デフォルト値: 259200 (3 日)。 |

## セッション変数

`statistic_collect_parallel`: BE で実行できる統計収集タスクの並行性を調整するために使用されます。デフォルト値: 1。この値を増やすことで収集タスクを高速化できます。

## Hive/Iceberg/Hudi テーブルの統計を収集する

v3.2.0 以降、StarRocks は Hive、Iceberg、Hudi テーブルの統計収集をサポートしています。構文は StarRocks 内部テーブルの収集と似ています。**ただし、手動フル収集、手動ヒストグラム収集（v3.2.7 以降）、および自動フル収集のみがサポートされています。サンプル収集はサポートされていません。** v3.3.0 以降、StarRocks は STRUCT のサブフィールドの統計収集をサポートしています。

収集された統計は `default_catalog` の `_statistics_` 内の `external_column_statistics` テーブルに保存されます。これらは Hive Metastore に保存されず、他の検索エンジンと共有できません。`default_catalog._statistics_.external_column_statistics` テーブルからデータをクエリして、Hive/Iceberg/Hudi テーブルの統計が収集されているかどうかを確認できます。

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

### 制限事項

Hive、Iceberg、Hudi テーブルの統計を収集する際に適用される制限は次のとおりです。

1. Hive、Iceberg、Hudi テーブルの統計のみを収集できます。
2. 手動フル収集、手動ヒストグラム収集（v3.2.7 以降）、および自動フル収集のみがサポートされています。サンプル収集はサポートされていません。
3. システムが自動的にフル統計を収集するには、Analyze ジョブを作成する必要があります。これは、システムがデフォルトでバックグラウンドで行う StarRocks 内部テーブルの統計収集とは異なります。
4. 自動収集タスクでは、特定のテーブルの統計のみを収集できます。データベース内のすべてのテーブルの統計や外部カタログ内のすべてのデータベースの統計を収集することはできません。
5. 自動収集タスクでは、StarRocks は Hive および Iceberg テーブルのデータが更新されたかどうかを検出し、更新されたパーティションの統計のみを収集します。StarRocks は Hudi テーブルのデータが更新されたかどうかを検出できず、指定されたチェック間隔と収集間隔に基づいて定期的にフル収集を実行します。

以下の例は Hive 外部カタログのデータベースで発生します。`default_catalog` から Hive テーブルの統計を収集する場合、テーブルを `[catalog_name.][database_name.]<table_name>` 形式で参照します。

### 手動収集

必要に応じて Analyze ジョブを作成でき、ジョブは作成後すぐに実行されます。

#### 手動収集タスクを作成する

構文:

```sql
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]
```

例:

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

#### 統計のメタデータを表示する

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

システムが外部データソースのテーブルの統計を自動的に収集するには、Analyze ジョブを作成します。StarRocks はデフォルトのチェック間隔である 5 分ごとにタスクを実行するかどうかを自動的にチェックします。Hive および Iceberg テーブルの場合、StarRocks はテーブル内のデータが更新された場合にのみ収集タスクを実行します。

ただし、Hudi テーブルのデータ変更は検出できず、StarRocks は指定されたチェック間隔と収集間隔に基づいて定期的に統計を収集します。収集動作を制御するために以下の FE 設定項目を指定できます:

- statistic_collect_interval_sec

  自動収集中のデータ更新をチェックする間隔。単位: 秒。デフォルト: 5 分。

- statistic_auto_collect_small_table_rows (v3.2 以降)

  自動収集中に外部データソース（Hive、Iceberg、Hudi）のテーブルが小さいかどうかを判断するしきい値。デフォルト: 10000000。

- statistic_auto_collect_small_table_interval

  小さいテーブルの統計を収集する間隔。単位: 秒。デフォルト: 0。

- statistic_auto_collect_large_table_interval

  大きいテーブルの統計を収集する間隔。単位: 秒。デフォルト: 43200 (12 時間)。

自動収集スレッドは `statistic_collect_interval_sec` で指定された間隔でデータ更新をチェックします。テーブル内の行数が `statistic_auto_collect_small_table_rows` より少ない場合、そのようなテーブルの統計を `statistic_auto_collect_small_table_interval` に基づいて収集します。

テーブル内の行数が `statistic_auto_collect_small_table_rows` を超える場合、そのようなテーブルの統計を `statistic_auto_collect_large_table_interval` に基づいて収集します。大きいテーブルの統計は `Last table update time + Collection interval > Current time` の場合にのみ更新されます。これにより、大きいテーブルの頻繁な分析タスクが防止されます。

#### 自動収集タスクを作成する

構文:

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

自動収集タスク専用の収集間隔を設定するために、プロパティ `statistic_auto_collect_interval` を指定できます。このタスクには FE 設定項目 `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` は適用されません。

例:

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

#### 自動収集タスクのステータスを表示する

手動収集と同様です。

#### 統計のメタデータを表示する

手動収集と同様です。

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

手動収集と同様です。

#### 統計を削除する

```sql
DROP STATS tbl_name
```

## 参考文献

- FE 設定項目をクエリするには、[ADMIN SHOW CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を実行します。

- FE 設定項目を変更するには、[ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を実行します。