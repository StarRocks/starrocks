---
displayed_sidebar: docs
sidebar_position: 10
---

# CBOの統計情報を収集する

このトピックでは、StarRocksコストベースオプティマイザ (CBO) の基本的な概念と、CBOが最適なクエリプランを選択するために統計情報を収集する方法について説明します。StarRocks 2.4では、正確なデータ分布統計を収集するためにヒストグラムが導入されました。

v3.2.0以降、StarRocksはHive、Iceberg、Hudiテーブルからの統計情報の収集をサポートしており、他のメタストアシステムへの依存を軽減しています。構文はStarRocksの内部テーブルを収集する場合と似ています。

## CBOとは

CBOはクエリの最適化に不可欠です。SQLクエリがStarRocksに到着すると、論理実行プランにパースされます。CBOは論理プランを書き換え、複数の物理実行プランに変換します。その後、CBOはプラン内の各オペレータ (CPU、メモリ、ネットワーク、I/Oなど) の実行コストを推定し、最終的な物理プランとして最もコストの低いクエリパスを選択します。

StarRocks CBOはStarRocks 1.16.0で導入され、1.19以降ではデフォルトで有効になっています。Cascadesフレームワークに基づいて開発されたStarRocks CBOは、さまざまな統計情報に基づいてコストを推定します。数万もの実行プランの中から最もコストの低い実行プランを選択する能力があり、複雑なクエリの効率とパフォーマンスを大幅に向上させます。

統計情報はCBOにとって重要です。それらはコスト推定が正確で有用であるかどうかを決定します。以下のセクションでは、統計情報の種類、収集ポリシー、統計情報の収集方法、および統計情報の表示方法について詳しく説明します。

## 統計情報の種類

StarRocksは、コスト推定の入力としてさまざまな統計情報を収集します。

### 基本統計情報

デフォルトでは、StarRocksはテーブルとカラムの以下の基本統計情報を定期的に収集します。

- `row_count`: テーブルの総行数
- `data_size`: カラムのデータサイズ
- `ndv`: カラムカーディナリティ、つまりカラム内の異なる値の数
- `null_count`: カラム内のNULL値を持つデータの量
- `min`: カラムの最小値
- `max`: カラムの最大値

完全な統計情報は、`_statistics_` データベースの `column_statistics` テーブルに保存されます。このテーブルを参照してテーブル統計をクエリできます。このテーブルから統計データをクエリする例を以下に示します。

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

StarRocks 2.4では、基本統計情報を補完するためにヒストグラムが導入されました。ヒストグラムはデータの効果的な表現方法と考えられています。データが偏っているテーブルの場合、ヒストグラムはデータ分布を正確に反映できます。

StarRocksは等高ヒストグラムを使用します。これはいくつかのバケットで構成され、各バケットには同量のデータが含まれます。頻繁にクエリされ、選択度に大きな影響を与えるデータ値については、StarRocksはそれらのために個別のバケットを割り当てます。バケットが多いほど推定は正確になりますが、メモリ使用量がわずかに増加する可能性があります。ヒストグラム収集タスクのバケット数と最も一般的な値 (MCV) を調整できます。

**ヒストグラムは、データが高度に偏っており、クエリが頻繁に行われるカラムに適用されます。テーブルデータが均一に分布している場合、ヒストグラムを作成する必要はありません。ヒストグラムは、数値、DATE、DATETIME、または文字列型のカラムにのみ作成できます。**

ヒストグラムは `_statistics_` データベースの `histogram_statistics` テーブルに保存されます。このテーブルから統計データをクエリする例を以下に示します。

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

### 多カラム結合統計

v3.5.0以降、StarRocksは多カラム結合統計の収集をサポートしています。現在、StarRocksがカーディナリティ推定を実行する際、ほとんどのシナリオでオプティマイザは複数のカラムが完全に互いに独立している、つまり相関がないと仮定しています。しかし、カラム間に相関がある場合、現在の推定方法では誤った結果につながる可能性があります。これにより、オプティマイザが誤った実行プランを生成してしまうことがあります。現在、多カラム結合NDVのみがサポートされており、主に以下のシナリオでカーディナリティ推定に使用されます。

- 複数のAND結合された等価述語の評価。
- Aggノードの評価。
- 集約プッシュダウン戦略への適用。

現在、多カラム結合統計の収集は手動収集のみをサポートしています。デフォルトのタイプはサンプリング収集です。多カラム統計は、各StarRocksクラスターの `_statistics_` データベースにある `multi_column_statistics` テーブルに保存されます。クエリは次のような情報を返します。

```sql
mysql> select * from _statistics_.multi_column_statistics \G
*************************** 1. row ***************************
    table_id: 1695021
  column_ids: 0#1
       db_id: 110099
  table_name: db.test_multi_col_stats
column_names: id,name
         ndv: 11
 update_time: 2025-04-11 15:09:50
```

### 仮想カラム統計
StarRocks 4.0以降、仮想カラム統計がサポートされています。仮想統計は、物理カラムだけでなく、式に対しても計算できる統計です。

現時点では、実装されている唯一の仮想統計は `unnest` (配列の) です。仮想統計の計算は、クエリパターンに依存するため、デフォルトでは無効になっています。
テーブルを分析する際に、次のようにプロパティを渡すことで `unnest` 仮想統計の計算を有効にできます。
```sql
ANALYZE FULL TABLE `TABLE1` PROPERTIES("unnest_virtual_statistics" = "true");
```

サンプリング統計と完全統計の両方がサポートされていることに注意してください。

例えば、`col1` という配列カラムの `unnest(col1)` という式を使用した統計は、有効にすると計算されます。
セッション変数 `enable_unnest_virtual_statistics` が有効になっている場合、オプティマイザはクエリで `unnest(col1)` 式に遭遇すると、計算された仮想統計を使用し、伝播します。
これにより、配列カラムを定期的にアンネストしてそれらに対してクエリ（結合など）を発行する場合に役立ちます。オプティマイザは統計を使用して、例えば偏った結合を考慮に入れることができます。

## 収集の種類と方法

テーブルのデータサイズとデータ分布は常に変化します。データ変更を反映するために、統計情報は定期的に更新する必要があります。統計収集タスクを作成する前に、ビジネス要件に最適な収集の種類と方法を選択する必要があります。

StarRocksは完全収集とサンプリング収集をサポートしており、どちらも自動的および手動で実行できます。デフォルトでは、StarRocksはテーブルの完全統計を自動的に収集します。5分ごとにデータ更新をチェックし、データ変更が検出されると、データ収集が自動的にトリガーされます。自動完全収集を使用しない場合は、収集タスクをカスタマイズできます。

| **収集タイプ**    | **収集方法**    | **説明**                                                     | **利点と欠点**                                              |
| :---------------- | :---------------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| 完全収集          | 自動/手動         | テーブル全体をスキャンして統計情報を収集します。統計情報はパーティションごとに収集されます。パーティションにデータ変更がない場合、そのパーティションからはデータは収集されず、リソース消費が削減されます。完全な統計情報は `_statistics_.column_statistics` テーブルに保存されます。 | 利点：統計は正確であり、CBOが正確な推定を行うのに役立ちます。欠点：システムリソースを消費し、処理が遅くなります。2.5以降、StarRocksは自動収集期間を指定できるようになり、リソース消費が削減されます。 |
| サンプリング収集  | 自動/手動         | テーブルの各パーティションから `N` 行のデータを均等に抽出します。統計情報はテーブルごとに収集されます。各カラムの基本統計情報は1つのレコードとして保存されます。カラムのカーディナリティ情報 (ndv) はサンプリングされたデータに基づいて推定されるため、正確ではありません。サンプリングされた統計情報は `_statistics_.table_statistic_v1` テーブルに保存されます。 | 利点：システムリソースの消費が少なく、処理が高速です。欠点：統計が完全ではないため、コスト推定の精度に影響を与える可能性があります。 |

v3.5.0以降、サンプリング収集と完全収集の両方の統計情報は `_statistics_.column_statistics` テーブルに保存されます。これは、現在のオプティマイザがカーディナリティ推定において最も最近収集された統計情報に焦点を当てるためであり、バックグラウンドでの自動統計情報収集は、テーブルの異なる健全性状態により異なる収集方法を使用する可能性があります。データスキューがある場合、テーブル全体のサンプリングされた統計情報の誤差率が高くなる可能性があり、同じクエリが異なる収集方法のために異なる統計情報を使用する可能性があり、これがオプティマイザが誤った実行プランを生成する原因となる可能性があります。したがって、サンプリング収集と完全収集の両方は、統計情報がパーティションレベルで収集されると仮定しています。FE設定項目 `statistic_use_meta_statistics` を `false` に変更することで、以前の収集および保存方法に調整できます。

## Predicate Column

v3.5.0以降、StarRocksはPredicate Column統計情報の収集をサポートしています。

Predicate Columnは、クエリでフィルター条件 (WHERE句、JOIN条件、GROUP BYカラム、DISTINCTカラム) として頻繁に使用されるカラムです。StarRocksは、クエリに含まれるテーブルの各Predicate Columnを自動的に記録し、`_statistics_.predicate_columns` テーブルに保存します。クエリは以下を返します。

```sql
select * from _statistics_.predicate_columns \G
*************************** 1. row ***************************
      fe_id: 127.0.0.1_9011_1735874871718
      db_id: 1684773
   table_id: 1685786
  column_id: 1
      usage: normal,predicate,join,group_by
  last_used: 2025-04-11 20:39:32
    created: 2025-04-11 20:39:53
```

より観測可能な情報を得るには、`information_schema.column_stats_usage` ビューをクエリすることもできます。極端なシナリオ (多数のカラムがある場合) では、完全収集には大きなオーバーヘッドがありますが、実際には安定したワークロードの場合、すべてのカラムの統計は必要なく、一部の主要なFilter、Join、Aggregationに関わるカラムのみが必要なことがよくあります。したがって、コストと精度を両立させるために、StarRocksはPredicate Column統計の手動収集をサポートしており、自動的に統計を収集する際にはポリシーに従ってPredicate Column統計を収集することで、テーブル内のすべてのカラム統計を収集することを避けます。クラスター内の各FEノードは、Predicate Column情報を定期的にFEキャッシュに同期および永続化し、利用を加速します。

## 統計情報の収集

StarRocksは柔軟な統計情報収集方法を提供しています。ビジネスシナリオに合わせて、自動、手動、またはカスタム収集を選択できます。

デフォルトでは、StarRocksはテーブルの完全統計情報を定期的に自動収集します。デフォルトの間隔は10分です。システムは、データの更新比率が条件を満たしていると判断した場合、収集を自動的にトリガーします。**完全統計情報の収集は、多くのシステムリソースを消費する可能性があります**。自動完全統計情報収集を使用しない場合は、FE設定項目 `enable_collect_full_statistic` を `false` に設定でき、定期収集タスクは完全統計情報収集からサンプリング収集に変更されます。

v3.5.0以降、StarRocksは、自動収集中にテーブルのデータが前回の収集から現在のテーブルまで大幅に変化していることを検出した場合、完全収集をサンプリング収集に自動的に変更します。サンプリング収集が使用される場合、テーブルにPredicate Column (PREDICATE/JOIN/GROUP BY/DISTINCT) がある場合、StarRocksはサンプリングタスクを完全収集に変換し、Predicate Columnの統計を収集して統計の正確性を確保し、テーブル内のすべてのカラムの統計を収集することはありません。FE設定項目 `statistic_auto_collect_use_full_predicate_column_for_sample` を使用してこれを設定できます。さらに、StarRocksは、完全収集が使用される際にテーブル内のカラム数がFE設定項目 `statistic_auto_collect_predicate_columns_threshold` を超える場合、すべてのカラムの完全収集からPredicate Columnsの完全収集に切り替わります。

### 自動収集

基本統計情報の場合、StarRocksはデフォルトでテーブルの完全統計情報を自動的に収集するため、手動操作は不要です。統計情報が収集されていないテーブルの場合、StarRocksはスケジューリング期間内に自動的に統計情報を収集します。統計情報が収集済みのテーブルの場合、StarRocksはテーブルの総行数と変更された行数を更新し、この情報を定期的に永続化して自動収集をトリガーするかどうかを判断します。ヒストグラムと多カラム結合統計は、現在の自動収集タスクでは取得されません。

2.4.5以降、StarRocksは自動完全収集の収集期間を指定できるようになり、自動完全収集によって引き起こされるクラスターパフォーマンスの揺らぎを防ぎます。この期間は、FEパラメーター `statistic_auto_analyze_start_time` と `statistic_auto_analyze_end_time` で指定されます。

自動収集をトリガーする条件：

- 前回の統計情報収集以降、テーブルデータが変更された。

- 収集時間が設定された収集期間の範囲内である。（デフォルトの収集期間は終日です。）

- 前回の収集ジョブの更新時間がパーティションの最新更新時間よりも早い。

- テーブル統計の健全性が指定されたしきい値 (`statistic_auto_collect_ratio`) を下回っている。

> 統計健全性の計算式:
>
> 1. データが更新されたパーティション数が10未満の場合、式は `1 - (前回の収集以降に更新された行数 / 総行数)` です。
> 2. データが更新されたパーティション数が10以上の場合、式は `1 - MIN(前回の収集以降に更新された行数 / 総行数, 前回の収集以降に更新されたパーティション数 / 総パーティション数)` です。
> 3. v3.5.0以降、パーティションが健全であるかどうかを判断するために、StarRocksは統計情報収集時間とデータ更新時間を比較するのではなく、パーティション更新行の比率を比較します。これはFE設定項目 `statistic_partition_health_v2_threshold` を介して設定できます。また、FE設定項目 `statistic_partition_healthy_v2` を `false` に設定して、以前の健全性チェック動作を使用することもできます。

さらに、StarRocksはテーブルサイズとテーブル更新頻度に基づいて収集ポリシーを設定できます。

- データ量が少ないテーブルの場合、**テーブルデータが頻繁に更新されても、リアルタイムで無制限に統計が収集されます。`statistic_auto_collect_small_table_size` パラメーターを使用して、テーブルが小規模テーブルか大規模テーブルかを判断できます。`statistic_auto_collect_small_table_interval` を使用して、小規模テーブルの収集間隔を設定することもできます。

- データ量が多いテーブルの場合、以下の制限が適用されます。

  - デフォルトの収集間隔は12時間以上であり、`statistic_auto_collect_large_table_interval` を使用して設定できます。

  - 収集間隔に達し、統計健全性が自動サンプリング収集のしきい値より低い場合、サンプリング収集がトリガーされます。この動作はFE設定項目 `statistic_auto_collect_sample_threshold` を使用して設定できます。v3.5.0以降、以下のすべての条件が満たされた場合、サンプリング収集はPredicate Columnsの完全収集に変換されます。
    - テーブルにPredicate Columnが存在し、Predicate Columnsの数が `statistic_auto_collect_max_predicate_column_size_on_sample_strategy` より少ない場合。
    - FE設定 `statistic_auto_collect_use_full_predicate_column_for_sample` が `true` に設定されている場合。

  - 収集間隔に達し、統計健全性が自動サンプリング収集のしきい値 (`statistic_auto_collect_sample_threshold`) より高く、自動収集しきい値 (`statistic_auto_collect_ratio`) より低い場合、完全収集がトリガーされます。

  - データを収集するパーティションのサイズ (`statistic_max_full_collect_data_size`) が100 GBを超える場合、サンプリング収集がトリガーされます。

  - 更新時間が前回の収集タスクの時間より遅いパーティションの統計のみが収集されます。データ変更がないパーティションの統計は収集されません。

- Predicate Columnがあり、テーブル内のカラムの総数が `statistic_auto_collect_predicate_columns_threshold` を超えるテーブルの場合、テーブル内のPredicate Columnの統計のみが収集されます。

:::tip

テーブルのデータが変更された後、このテーブルのサンプリング収集タスクを手動でトリガーすると、サンプリング収集タスクの更新時間がデータ更新時間よりも遅くなり、このスケジューリング期間中にこのテーブルの自動完全収集がトリガーされなくなります。
:::

自動完全収集はデフォルトで有効になっており、システムによってデフォルト設定を使用して実行されます。

以下の表はデフォルト設定について説明しています。変更する必要がある場合は、**ADMIN SET CONFIG** コマンドを実行します。

| **FE設定項目**                                        | **タイプ** | **デフォルト値** | **説明**                                                     |
| :---------------------------------------------------- | :-------- | :---------------- | :----------------------------------------------------------- |
| `enable_statistic_collect`                            | BOOLEAN   | TRUE              | デフォルトおよびユーザー定義の自動収集タスクをオンにするかどうか。このスイッチはデフォルトでオンになっています。 |
| `enable_collect_full_statistic`                       | BOOLEAN   | TRUE              | デフォルトの自動収集を有効にするかどうか。このスイッチはデフォルトでオンになっています。 |
| `statistic_collect_interval_sec`                      | LONG      | 600               | 自動収集中にデータ更新をチェックする間隔。単位：秒。       |
| `statistic_auto_analyze_start_time`                   | STRING    | 00:00:00          | 自動収集の開始時間。値の範囲：`00:00:00` - `23:59:59`。     |
| `statistic_auto_analyze_end_time`                     | STRING    | 23:59:59          | 自動収集の終了時間。値の範囲：`00:00:00` - `23:59:59`。     |
| `statistic_auto_collect_small_table_size`             | LONG      | 5368709120        | 自動完全収集でテーブルが小規模テーブルであるかを判断するしきい値。この値より大きいサイズのテーブルは大規模テーブルと見なされ、この値以下のサイズのテーブルは小規模テーブルと見なされます。単位：バイト。デフォルト値：5368709120 (5 GB)。 |
| `statistic_auto_collect_small_table_interval`         | LONG      | 0                 | 小規模テーブルの完全統計を自動収集する間隔。単位：秒。     |
| `statistic_auto_collect_large_table_interval`         | LONG      | 43200             | 大規模テーブルの完全統計を自動収集する間隔。単位：秒。デフォルト値：43200 (12時間)。 |
| `statistic_auto_collect_ratio`                        | FLOAT     | 0.8               | 自動収集の統計が健全であるかを判断するしきい値。統計の健全性がこのしきい値を下回ると、自動収集がトリガーされます。 |
| `statistic_auto_collect_sample_threshold`             | DOUBLE    | 0.3               | 自動サンプリング収集をトリガーするための統計健全性しきい値。統計の健全性値がこのしきい値より低い場合、自動サンプリング収集がトリガーされます。 |
| `statistic_max_full_collect_data_size`                | LONG      | 107374182400      | 自動収集でデータを収集するパーティションのデータサイズ。単位：バイト。デフォルト値：107374182400 (100 GB)。データサイズがこの値を超えると、完全収集は破棄され、代わりにサンプリング収集が実行されます。 |
| `statistic_full_collect_buffer`                       | LONG      | 20971520          | 自動収集タスクが取得する最大バッファサイズ。単位：バイト。デフォルト値：20971520 (20 MB)。 |
| `statistic_collect_max_row_count_per_query`           | INT       | 5000000000        | 単一の分析タスクでクエリする最大行数。この値を超えると、分析タスクは複数のクエリに分割されます。 |
| `statistic_collect_too_many_version_sleep`            | LONG      | 600000            | 収集タスクが実行されるテーブルのデータバージョンが多すぎる場合の自動収集タスクのスリープ時間。単位：ミリ秒。デフォルト値：600000 (10分)。 |
| `statistic_auto_collect_use_full_predicate_column_for_sample` | BOOLEAN   | TRUE              | 自動完全収集タスクがサンプリング収集ポリシーにヒットしたときに、Predicate Columnの完全収集に変換するかどうか。 |
| `statistic_auto_collect_max_predicate_column_size_on_sample_strategy` | INT       | 16                | 自動完全収集タスクがサンプリング収集ポリシーにヒットしたときに、テーブルに異常に多数のPredicate Columnがあり、この設定項目を超える場合、タスクはPredicate Columnの完全収集に切り替わらず、すべてのカラムのサンプリング収集を維持します。この設定項目は、この動作のPredicate Columnの最大値を制御します。 |
| `statistic_auto_collect_predicate_columns_threshold`  | INT       | 32                | 自動収集中にテーブル内のカラム数がこの設定を超える場合、Predicate Columnのカラム統計のみが収集されます。 |
| `statistic_predicate_columns_persist_interval_sec`    | LONG      | 60                | FEがPredicate Columnの統計情報を同期および永続化する間隔。 |
| `statistic_predicate_columns_ttl_hours`               | LONG      | 24                | FEにキャッシュされたPredicate Column統計情報の有効期限。 |
| `enable_predicate_columns_collection`                 | BOOLEAN   | TRUE              | Predicate Columnの収集を有効にするかどうか。無効にすると、クエリ最適化中にPredicate Columnは記録されません。 |
| `enable_manual_collect_array_ndv`                     | BOOLEAN   | FALSE             | ARRAY型のNDV情報の手動収集を有効にするかどうか。           |
| `enable_auto_collect_array_ndv`                       | BOOLEAN   | FALSE             | ARRAY型のNDV情報の自動収集を有効にするかどうか。           |

ほとんどの統計収集は自動ジョブに依存できますが、特定の要件がある場合は、ANALYZE TABLEステートメントを実行して手動でタスクを作成するか、CREATE ANALYZEステートメントを実行して自動タスクをカスタマイズできます。

### 手動収集

ANALYZE TABLEを使用して手動収集タスクを作成できます。デフォルトでは、手動収集は同期操作です。非同期操作に設定することもできます。非同期モードでは、ANALYZE TABLEを実行すると、システムはすぐにこのステートメントが成功したかどうかを返します。ただし、収集タスクはバックグラウンドで実行され、結果を待つ必要はありません。SHOW ANALYZE STATUSを実行してタスクのステータスを確認できます。非同期収集はデータ量が多いテーブルに適しており、同期収集はデータ量が少ないテーブルに適しています。**手動収集タスクは作成後に一度だけ実行されます。手動収集タスクを削除する必要はありません。** ANALYZE TABLE操作を実行するには、対応するテーブルに対するINSERTおよびSELECT権限が必要です。

#### 基本統計情報の手動収集

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name 
    [( col_name [, col_name]... )
    | col_name [, col_name]...
    | ALL COLUMNS
    | PREDICATE COLUMNS
    | MULTIPLE COLUMNS ( col_name [, col_name]... )]
[PARTITION (partition_name [, partition_name]...)]
[WITH [SYNC | ASYNC] MODE]
[PROPERTIES (property [, property]...)]
```

パラメーターの説明：

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプリング収集を示します。
  - 収集タイプが指定されていない場合、デフォルトで完全収集が使用されます。

- 統計を収集するカラムのタイプ：
  - `col_name`: 統計を収集するカラム。複数のカラムはカンマ (`,`) で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。
  - `ALL COLUMNS`: すべてのカラムから統計を収集します。v3.5.0以降サポート。
  - `PREDICATE COLUMNS`: Predicate Columnのみから統計を収集します。v3.5.0以降サポート。
  - `MULTIPLE COLUMNS`: 指定された複数のカラムから結合統計を収集します。現在、複数カラムの手動同期収集のみがサポートされています。手動統計収集のカラム数は `statistics_max_multi_column_combined_num` を超えることはできません。デフォルト値は `10` です。v3.5.0以降サポート。

- `[WITH SYNC | ASYNC MODE]`: 手動収集タスクを同期モードで実行するか、非同期モードで実行するか。このパラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `PROPERTIES`: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` ファイルのデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE STATUSの出力の `Properties` カラムで確認できます。

| **PROPERTIES**                | **タイプ** | **デフォルト値** | **説明**                                                     |
| :---------------------------- | :-------- | :---------------- | :----------------------------------------------------------- |
| `statistic_sample_collect_rows` | INT       | 200000            | サンプリング収集で収集する最小行数。パラメーター値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |
| `unnest_virtual_statistics`   | BOOL      | false             | 適用可能なカラムの仮想アンネスト統計を計算するかどうか。   |

例

手動完全収集

```SQL
-- デフォルト設定を使用してテーブルの完全統計を手動で収集します。
ANALYZE TABLE tbl_name;

-- デフォルト設定を使用してテーブルの完全統計を手動で収集します。
ANALYZE FULL TABLE tbl_name;

-- デフォルト設定を使用してテーブル内の指定されたカラムの統計を手動で収集します。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

手動サンプリング収集

```SQL
-- デフォルト設定を使用してテーブルの部分統計を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name;

-- 収集する行数を指定して、テーブル内の指定されたカラムの統計を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

- 多カラム結合統計の手動収集

```sql
-- 多カラム結合統計の手動サンプリング収集
ANALYZE SAMPLE TABLE tbl_name MULTIPLE COLUMNS (v1, v2);

-- 多カラム結合統計の手動完全収集
ANALYZE FULL TABLE tbl_name MULTIPLE COLUMNS (v1, v2);
```

- Predicate Columnの手動収集

```sql
-- Predicate Columnの手動サンプリング収集
ANALYZE SAMPLE TABLE tbl_name PREDICATE COLUMNS

-- Predicate Columnの手動完全収集
ANALYZE FULL TABLE tbl_name PREDICATE COLUMNS
```

#### ヒストグラムの手動収集

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

パラメーターの説明：

- `col_name`: 統計を収集するカラム。複数のカラムはカンマ (`,`) で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。このパラメーターはヒストグラムに必須です。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードで実行するか、非同期モードで実行するか。このパラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `WITH N BUCKETS`: `N` はヒストグラム収集のバケット数です。指定しない場合、`fe.conf` のデフォルト値が使用されます。

- PROPERTIES: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                    | **タイプ** | **デフォルト値** | **説明**                                                     |
| :-------------------------------- | :-------- | :---------------- | :----------------------------------------------------------- |
| `statistic_sample_collect_rows`   | INT       | 200000            | 収集する最小行数。パラメーター値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |
| `histogram_buckets_size`          | LONG      | 64                | ヒストグラムのデフォルトのバケット数。                       |
| `histogram_mcv_size`              | INT       | 100               | ヒストグラムの最も一般的な値 (MCV) の数。                    |
| `histogram_sample_ratio`          | FLOAT     | 0.1               | ヒストグラムのサンプリング比率。                             |
| `histogram_max_sample_row_count`  | LONG      | 10000000          | ヒストグラムで収集する最大行数。                             |
| `histogram_collect_bucket_ndv_mode` | STRING    | none              | ヒストグラムのバケットごとの異なる値の数 (NDV) を推定するモード。`none` (デフォルト、異なるカウントは収集されません)、`hll` (正確な推定のためにHyperLogLogを使用)、または `sample` (低オーバーヘッドのサンプルベース推定器を使用)。 |
| `unnest_virtual_statistics`       | BOOL      | false             | 適用可能なカラムの仮想アンネスト統計を計算するかどうか。   |

ヒストグラムの収集行数は複数のパラメーターによって制御されます。`statistic_sample_collect_rows` とテーブル行数 * `histogram_sample_ratio` の大きい方の値です。この数値は `histogram_max_sample_row_count` で指定された値を超えることはできません。値を超過した場合、`histogram_max_sample_row_count` が優先されます。

実際に使用されるプロパティは、SHOW ANALYZE STATUSの出力の `Properties` カラムで確認できます。

例

```SQL
-- デフォルト設定を使用してv1のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 32バケット、32 MCV、50%サンプリング比率でv1とv2のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);

-- バケットごとの正確な異なるカウント推定のために「hll」モードを使用してv3のヒストグラムを収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v3
PROPERTIES(
   "histogram_collect_bucket_ndv_mode" = "hll"
);
```

### カスタム収集

#### 自動収集タスクのカスタマイズ

StarRocksが提供するデフォルトの収集タスクは、ポリシーに従ってすべてのデータベースとすべてのテーブルの統計情報を自動的に収集するため、デフォルトではカスタム収集タスクを作成する必要はありません。

自動収集タスクをカスタマイズしたい場合は、CREATE ANALYZEステートメントを使用して作成する必要があります。収集タスクを作成するには、収集対象テーブルのINSERTおよびSELECT権限が必要です。

```SQL
-- すべてのデータベースの統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- データベース内のすべてのテーブルの統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
[PROPERTIES (property [,property])]

-- テーブル内の指定されたカラムの統計情報を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]

-- テーブル内の指定されたカラムのヒストグラムを自動的に収集します。
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

パラメーターの説明：

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプリング収集を示します。
  - 収集タイプが指定されていない場合、デフォルトでサンプリング収集が使用されます。

- `col_name`: 統計を収集するカラム。複数のカラムはカンマ (`,`) で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。

- `PROPERTIES`: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。

| **PROPERTIES**                        | **タイプ** | **デフォルト値** | **説明**                                                     |
| :------------------------------------ | :-------- | :---------------- | :----------------------------------------------------------- |
| `statistic_auto_collect_ratio`          | FLOAT     | 0.8               | 自動収集の統計が健全であるかを判断するしきい値。統計の健全性がこのしきい値を下回ると、自動収集がトリガーされます。 |
| `statistic_sample_collect_rows`         | INT       | 200000            | 収集する最小行数。パラメーター値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |
| `statistic_exclude_pattern`             | String    | null              | ジョブで除外する必要があるデータベースまたはテーブルの名前。ジョブで統計を収集しないデータベースとテーブルを指定できます。これは正規表現パターンであり、一致する内容は `database.table` であることに注意してください。 |
| `statistic_auto_collect_interval`       | LONG      | 0                 | 自動収集の間隔。単位：秒。デフォルトでは、StarRocksはテーブルサイズに基づいて `statistic_auto_collect_small_table_interval` または `statistic_auto_collect_large_table_interval` を収集間隔として選択します。分析ジョブを作成する際に `statistic_auto_collect_interval` プロパティを指定した場合、この設定は `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` よりも優先されます。 |

例

自動完全収集

```SQL
-- すべてのデータベースの完全統計を自動的に収集します。
CREATE ANALYZE ALL;

-- データベースの完全統計を自動的に収集します。
CREATE ANALYZE DATABASE db_name;

-- データベース内のすべてのテーブルの完全統計を自動的に収集します。
CREATE ANALYZE FULL DATABASE db_name;

-- テーブル内の指定されたカラムの完全統計を自動的に収集します。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 

-- 指定されたデータベース「db_name」を除外し、すべてのデータベースの統計を自動的に収集します。
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);

-- 指定されたデータベース、テーブル、またはカラムのヒストグラムを自動的に収集します。
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON c1,c2;
```

自動サンプリング収集

```SQL
-- デフォルト設定を使用してデータベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 指定されたテーブル「db_name.tbl_name」を除外し、データベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);

-- 統計の健全性と収集する行数を指定して、テーブル内の指定されたカラムの統計を自動的に収集します。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES (
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);

```

**StarRocksが提供する自動収集タスクではなく、`db_name.tbl_name` テーブルを収集しないユーザー定義収集タスクを使用します。**

```sql
ADMIN SET FRONTEND CONFIG("enable_auto_collect_statistics"="false");
DROP ALL ANALYZE JOB;
CREATE ANALYZE FULL ALL db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);
```

### データロード中の統計収集

データロード直後のクエリの良好な実行プランを保証するため、StarRocksはINSERT INTO/OVERWRITE DMLステートメントの終了時に非同期統計収集タスクをトリガーします。これはデフォルトでDMLが終了してから30秒間待機します。統計収集タスクが30秒以内に終了しない場合、DMLの実行結果が返されます。

#### INSERT INTO

- 統計は、パーティションへのデータ初回インポート時にのみ収集されます。
- このインポートの行数が `statistic_sample_collect_rows` より大きい場合、サンプリング収集タスクがトリガーされ、それ以外の場合は完全収集が使用されます。

#### INSERT OVERWRITE

- OVERWRITE前後の行数の変化の比率が `statistic_sample_collect_ratio_threshold_of_first_load` より小さい場合、統計収集タスクはトリガーされません。
- このOVERWRITE操作の行数が `statistic_sample_collect_rows` より大きい場合、サンプリング収集タスクがトリガーされ、それ以外の場合は完全収集が使用されます。

以下のプロパティ (PROPERTIES) は、データロード用のカスタマイズされた収集タスクを作成するために使用されます。設定されていない場合、対応するFE設定項目の値が使用されます。

| **PROPERTIES**                                            | **タイプ** | **デフォルト** | **説明**                                                                                             |
| :-------------------------------------------------------- | :-------- | :------------- | :--------------------------------------------------------------------------------------------------- |
| `enable_statistic_collect_on_first_load`                  | BOOLEAN   | TRUE           | INSERT INTO/OVERWRITE実行後に統計収集タスクをトリガーするかどうか。                                 |
| `semi_sync_collect_statistic_await_seconds`               | LONG      | 30             | 結果を返すまでに統計が収集されるのを待つ最大時間。                                                 |
| `statistic_sample_collect_ratio_threshold_of_first_load`  | DOUBLE    | 0.1            | OVERWRITE操作におけるデータ変更の比率がこの値より小さい場合、統計収集タスクはトリガーされません。 |
| `statistic_sample_collect_rows`                           | LONG      | 200000         | DMLステートメントを介してロードされた合計データ行がこの値を超えると、統計収集にサンプリング収集が使用されます。 |

#### カスタム収集タスクの表示

```SQL
SHOW ANALYZE JOB [WHERE predicate][ORDER BY columns][LIMIT num]
```

WHERE句を使用して結果をフィルタリングできます。このステートメントは以下のカラムを返します。

| **カラム**   | **説明**                                                     |
| :----------- | :----------------------------------------------------------- |
| Id           | 収集タスクのID。                                             |
| Database     | データベース名。                                             |
| Table        | テーブル名。                                                 |
| Columns      | カラム名。                                                   |
| Type         | 統計情報のタイプ。`FULL` と `SAMPLE` があります。            |
| Schedule     | スケジューリングのタイプ。自動タスクの場合は `SCHEDULE` です。 |
| Properties   | カスタムパラメーター。                                       |
| Status       | タスクのステータス。PENDING、RUNNING、SUCCESS、FAILED があります。 |
| LastWorkTime | 最後の収集時間。                                             |
| Reason       | タスクが失敗した理由。タスク実行が成功した場合はNULLが返されます。 |

例

```SQL
-- すべてのカスタム収集タスクを表示します。
SHOW ANALYZE JOB

-- データベース `test` のカスタム収集タスクを表示します。
SHOW ANALYZE JOB where `database` = 'test';
```

#### カスタム収集タスクの削除

```SQL
DROP ANALYZE <ID>
| DROP ALL ANALYZE JOB
```

タスクIDはSHOW ANALYZE JOBステートメントを使用して取得できます。

例

```SQL
DROP ANALYZE 266030;
```

```SQL
DROP ALL ANALYZE JOB;
```

## 収集タスクのステータスの表示

SHOW ANALYZE STATUSステートメントを実行することで、現在のすべてのタスクのステータスを確認できます。このステートメントはカスタム収集タスクのステータスを表示するために使用できません。カスタム収集タスクのステータスを表示するには、SHOW ANALYZE JOBを使用します。

```SQL
SHOW ANALYZE STATUS [WHERE predicate];
```

`LIKE` または `WHERE` を使用して、返される情報をフィルタリングできます。

このステートメントは以下のカラムを返します。

| **リスト名** | **説明**                                                     |
| :----------- | :----------------------------------------------------------- |
| Id           | 収集タスクのID。                                             |
| Database     | データベース名。                                             |
| Table        | テーブル名。                                                 |
| Columns      | カラム名。                                                   |
| Type         | 統計情報のタイプ。FULL、SAMPLE、HISTOGRAM があります。       |
| Schedule     | スケジューリングのタイプ。`ONCE` は手動、`SCHEDULE` は自動です。 |
| Status       | タスクのステータス。                                         |
| StartTime    | タスクの実行開始時刻。                                       |
| EndTime      | タスクの実行終了時刻。                                       |
| Properties   | カスタムパラメーター。                                       |
| Reason       | タスクが失敗した理由。実行が成功した場合はNULLが返されます。 |

## 統計情報の表示

### 基本統計情報のメタデータの表示

```SQL
SHOW STATS META [WHERE predicate][ORDER BY columns][LIMIT num]
```

このステートメントは以下のカラムを返します。

| **カラム**          | **説明**                                                     |
| :------------------ | :----------------------------------------------------------- |
| Database            | データベース名。                                             |
| Table               | テーブル名。                                                 |
| Columns             | カラム名。                                                   |
| Type                | 統計情報のタイプ。`FULL` は完全収集、`SAMPLE` はサンプリング収集を示します。 |
| UpdateTime          | 現在のテーブルの最新統計情報更新時刻。                     |
| Properties          | カスタムパラメーター。                                       |
| Healthy             | 統計情報の健全性。                                           |
| ColumnStats         | カラムANALZEタイプ。                                       |
| TabletStatsReportTime | テーブルのTabletメタデータがFEで更新された時刻。         |
| TableHealthyMetrics | 統計情報の健全性のメトリック。                             |
| TableUpdateTime     | テーブルが更新された時刻。                                   |

### ヒストグラムのメタデータの表示

```SQL
SHOW HISTOGRAM META [WHERE predicate]
```

このステートメントは以下のカラムを返します。

| **カラム** | **説明**                                                     |
| :--------- | :----------------------------------------------------------- |
| Database   | データベース名。                                             |
| Table      | テーブル名。                                                 |
| Column     | カラム。                                                     |
| Type       | 統計情報のタイプ。ヒストグラムの場合は `HISTOGRAM` です。    |
| UpdateTime | 現在のテーブルの最新統計情報更新時刻。                     |
| Properties | カスタムパラメーター。                                       |

## 統計情報の削除

不要な統計情報を削除できます。統計情報を削除すると、統計情報のデータとメタデータの両方が削除され、期限切れのキャッシュ内の統計情報も削除されます。自動収集タスクが進行中の場合、以前に削除された統計情報が再び収集される可能性があることに注意してください。`SHOW ANALYZE STATUS` を使用して収集タスクの履歴を表示できます。

### 基本統計情報の削除

以下のステートメントは `default_catalog._statistics_.column_statistics` テーブルに保存されている統計情報を削除し、FEによってキャッシュされている対応するテーブル統計情報も無効になります。v3.5.0以降、このステートメントは、このテーブルの多カラム結合統計情報も削除します。

```SQL
DROP STATS tbl_name
```

以下のステートメントは `default_catalog._statistics_.multi_column_statistics` テーブルに保存されている多カラム結合統計情報を削除し、FEによってキャッシュされている対応するテーブルの多カラム結合統計情報も無効になります。このステートメントはテーブルの基本統計情報を削除しません。

```SQL
DROP MULTIPLE COLUMNS STATS tbl_name
```

### ヒストグラムの削除

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## 収集タスクのキャンセル

KILL ANALYZEステートメントを使用して、手動タスクとカスタムタスクを含む**実行中**の収集タスクをキャンセルできます。

```SQL
KILL ANALYZE <ID>
```

手動収集タスクのタスクIDはSHOW ANALYZE STATUSから取得できます。カスタム収集タスクのタスクIDはSHOW ANALYZE JOBから取得できます。

## その他のFE設定項目

| **FE設定項目**                     | **タイプ** | **デフォルト値** | **説明**                                                     |
| :----------------------------------- | :-------- | :---------------- | :----------------------------------------------------------- |
| `statistic_manager_sleep_time_sec`   | LONG      | 60                | メタデータがスケジューリングされる間隔。単位：秒。システムは、この間隔に基づいて以下の操作を実行します。統計情報を保存するテーブルを作成します。削除された統計情報を削除します。期限切れの統計情報を削除します。 |
| `statistic_analyze_status_keep_second` | LONG      | 259200            | 収集タスクの履歴を保持する期間。単位：秒。デフォルト値：259200 (3日)。 |

## セッション変数

`statistic_collect_parallel`: BEで実行できる統計収集タスクの並行度を調整するために使用されます。デフォルト値：1。この値を増やすと、収集タスクの速度を上げることができます。

## 外部テーブルの統計情報の収集

v3.2.0以降、StarRocksはHive、Iceberg、Hudiテーブルの統計情報の収集をサポートしています。構文はStarRocksの内部テーブルの収集と似ています。**ただし、手動完全収集、手動ヒストグラム収集 (v3.2.7以降)、および自動完全収集のみがサポートされており、サンプリング収集はサポートされていません。** v3.3.0以降、StarRocksはDelta Lakeテーブルの統計情報とSTRUCT内のサブフィールドの統計情報の収集をサポートしています。v3.4.0以降、StarRocksはクエリによってトリガーされるANALYZEタスクを介した自動統計情報収集をサポートしています。

収集された統計情報は、`default_catalog` 内の `_statistics_` データベースの `external_column_statistics` テーブルに保存されます。これらはHive Metastoreには保存されず、他の検索エンジンと共有できません。`default_catalog._statistics_.external_column_statistics` テーブルからデータをクエリして、Hive/Iceberg/Hudiテーブルの統計情報が収集されたかどうかを確認できます。

`external_column_statistics` から統計データをクエリする例を以下に示します。

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

外部テーブルの統計情報を収集する際には、以下の制限が適用されます。

- Hive、Iceberg、Hudi、およびDelta Lake (v3.3.0以降) テーブルの統計情報のみを収集できます。
- 手動完全収集、手動ヒストグラム収集 (v3.2.7以降)、および自動完全収集のみがサポートされており、サンプリング収集はサポートされていません。
- システムが完全統計情報を自動的に収集するには、Analyzeジョブを作成する必要があります。これは、システムがデフォルトでバックグラウンドでこれを行うStarRocks内部テーブルの統計情報を収集する場合とは異なります。
- 自動収集タスクの場合：
  - 特定のテーブルの統計情報のみを収集できます。データベース内のすべてのテーブルの統計情報や、外部カタログ内のすべてのデータベースの統計情報を収集することはできません。
  - StarRocksはHiveおよびIcebergテーブルのデータが更新されたかどうかを検出し、更新されたパーティションの統計情報のみを収集できます。StarRocksはHudiテーブルのデータが更新されたかどうかを認識できず、定期的な完全収集のみを実行できます。
- クエリによってトリガーされる収集タスクの場合：
  - 現在、Leader FEノードのみがANALYZEタスクをトリガーできます。
  - システムはHiveおよびIcebergテーブルのパーティション変更のチェックのみをサポートし、データが変更されたパーティションの統計のみを収集します。Delta Lake/Hudiテーブルの場合、システムはテーブル全体の統計を収集します。
  - IcebergテーブルにPartition Transformsが適用されている場合、統計の収集は `identity`、`year`、`month`、`day`、`hour` タイプのTransformsに対してのみサポートされます。
  - IcebergテーブルのPartition Evolutionの統計収集はサポートされていません。

以下の例は、Hive外部カタログの下のデータベースで発生します。`default_catalog` からHiveテーブルの統計情報を収集したい場合は、`[catalog_name.][database_name.]<table_name>` 形式でテーブルを参照してください。

### クエリによってトリガーされる収集

v3.4.0以降、システムはクエリによってトリガーされるANALYZEタスクによって外部テーブルの統計情報を自動収集する機能をサポートしています。Hive、Iceberg、Hudi、またはDelta Lakeテーブルをクエリする際、システムはバックグラウンドで対応するテーブルとカラムの統計情報を収集するためにANALYZEタスクを自動的にトリガーし、それを後続のクエリプラン最適化に利用します。

ワークフロー：

1. オプティマイザがFEにキャッシュされた統計情報をクエリする際、クエリされたテーブルとカラムに基づいてANALYZEタスクのオブジェクトを決定します（ANALYZEタスクはクエリに含まれるカラムの統計情報のみを収集します）。
2. システムはタスクオブジェクトをANALYZEタスクとしてカプセル化し、PendingTaskQueueに追加します。
3. スケジューラスレッドはPendingTaskQueueからタスクを定期的にフェッチし、RunningTasksQueueに追加します。
4. ANALYZEタスクの実行中、統計情報を収集してBEに書き込み、FEにキャッシュされた期限切れの統計情報をクリアします。

この機能はデフォルトで有効になっています。以下のシステム変数と設定項目で上記のプロセスを制御できます。

#### システム変数

##### `enable_query_trigger_analyze`

- デフォルト: true
- タイプ: Boolean
- 説明: クエリによってトリガーされるANALYZEタスクを有効にするかどうか。
- 導入バージョン: v3.4.0

#### FE設定

##### `connector_table_query_trigger_analyze_small_table_rows`

- デフォルト: 10000000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: クエリによってトリガーされるANALYZEタスクで、テーブルが小規模テーブルであるかを判断するためのしきい値。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 小規模テーブルのクエリによってトリガーされるANALYZEタスクの間隔。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- デフォルト: 12 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 大規模テーブルのクエリによってトリガーされるANALYZEタスクの間隔。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE上でPending状態のクエリによってトリガーされるANALYZEタスクの最大数。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_schedule_interval`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スケジューラスレッドがクエリによってトリガーされるANALYZEタスクをスケジューリングする間隔。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE上でRunning状態のクエリによってトリガーされるANALYZEタスクの最大数。
- 導入バージョン: v3.4.0

### 手動収集

オンデマンドでAnalyzeジョブを作成でき、ジョブは作成後すぐに実行されます。

#### 手動収集タスクの作成

構文：

```sql
-- 手動完全収集
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]

-- 手動ヒストグラム収集 (v3.3.0以降)
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

手動完全収集の例を以下に示します。

```sql
ANALYZE TABLE ex_hive_tbl(k1);
+----------------------------------+---------+----------+----------+
| Table                            | Op      | Msg_type | Msg_text |
+----------------------------------+---------+----------+----------+
| hive_catalog.tn_test.ex_hive_tbl | analyze | status   | OK       |
+----------------------------------+---------+----------+----------+
```

#### タスクステータスの表示

構文：

```sql
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

例：

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

#### 統計情報のメタデータの表示

構文：

```sql
SHOW STATS META [WHERE predicate]
```

例：

```sql
SHOW STATS META where `table` = 'ex_hive_tbl';
+----------------------+-------------+---------+------+---------------------+------------+---------+
| Database             | Table       | Columns | Type | UpdateTime          | Properties | Healthy |
+----------------------+-------------+---------+------+---------------------+------------+---------+
| hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | 2023-12-04 16:37:46 | {}         |         |
+----------------------+-------------+---------+------+---------------------+------------+---------+
```

#### 収集タスクのキャンセル

実行中の収集タスクをキャンセルします。

構文：

```sql
KILL ANALYZE <ID>
```

タスクIDはSHOW ANALYZE STATUSの出力で確認できます。

### 自動収集

外部データソースのテーブルの統計情報をシステムが自動的に収集するようにするには、Analyzeジョブを作成します。StarRocksは、デフォルトのチェック間隔である5分ごとにタスクを実行するかどうかを自動的にチェックします。HiveおよびIcebergテーブルの場合、StarRocksはテーブルのデータが更新された場合にのみ収集タスクを実行します。

ただし、Hudiテーブルのデータ変更は認識できないため、StarRocksは指定されたチェック間隔と収集間隔に基づいて定期的に統計情報を収集します。以下のFE設定項目を指定して収集動作を制御できます。

- `statistic_collect_interval_sec`

  自動収集中にデータ更新をチェックする間隔。単位：秒。デフォルト：5分。

- `statistic_auto_collect_small_table_rows` (v3.2以降)

  自動収集中に外部データソース (Hive、Iceberg、Hudi) のテーブルが小規模テーブルであるかを判断するしきい値。デフォルト：10000000。

- `statistic_auto_collect_small_table_interval`

  小規模テーブルの統計情報を収集する間隔。単位：秒。デフォルト：0。

- `statistic_auto_collect_large_table_interval`

  大規模テーブルの統計情報を収集する間隔。単位：秒。デフォルト：43200 (12時間)。

自動収集スレッドは `statistic_collect_interval_sec` で指定された間隔でデータ更新をチェックします。テーブルの行数が `statistic_auto_collect_small_table_rows` より少ない場合、`statistic_auto_collect_small_table_interval` に基づいてそれらのテーブルの統計情報を収集します。

テーブルの行数が `statistic_auto_collect_small_table_rows` を超える場合、`statistic_auto_collect_large_table_interval` に基づいてそれらのテーブルの統計情報を収集します。大規模テーブルの統計情報は、`最後のテーブル更新時刻 + 収集間隔 > 現在時刻` の場合にのみ更新されます。これにより、大規模テーブルに対する頻繁な分析タスクが防止されます。

#### 自動収集タスクの作成

構文：

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

`statistic_auto_collect_interval` プロパティを指定して、自動収集タスク専用の収集間隔を設定できます。FE設定項目 `statistic_auto_collect_small_table_interval` および `statistic_auto_collect_large_table_interval` はこのタスクには影響しません。

例：

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

#### 自動収集タスクのステータスの表示

手動収集と同様です。

#### 統計情報のメタデータの表示

手動収集と同様です。

#### 自動収集タスクの表示

構文：

```sql
SHOW ANALYZE JOB [WHERE predicate]
```

例：

```sql
SHOW ANALYZE JOB WHERE `id` = '17204';

Empty set (0.00 sec)
```

#### 収集タスクのキャンセル

手動収集と同様です。

#### 統計情報の削除

```sql
DROP STATS tbl_name
```

## 参考資料

- FE設定項目をクエリするには、[ADMIN SHOW CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を実行します。

- FE設定項目を変更するには、[ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を実行します。
