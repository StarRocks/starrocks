---
displayed_sidebar: docs
---

# CREATE ANALYZE

## 説明

CBO 統計を収集するための自動収集タスクをカスタマイズします。

デフォルトでは、StarRocks はテーブルの完全な統計を自動的に収集します。5 分ごとにデータの更新をチェックします。データの変更が検出されると、データ収集が自動的にトリガーされます。自動の完全収集を使用したくない場合は、FE の設定項目 `enable_collect_full_statistic` を `false` に設定し、収集タスクをカスタマイズできます。

カスタムの自動収集タスクを作成する前に、自動の完全収集を無効にする必要があります (`enable_collect_full_statistic = false`)。そうしないと、カスタムタスクが有効になりません。

このステートメントは v2.4 からサポートされています。

## 構文

```SQL
-- すべてのデータベースの統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] ALL PROPERTIES (property [,property])

-- データベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
PROPERTIES (property [,property])

-- テーブル内の指定された列の統計を自動的に収集します。
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
PROPERTIES (property [,property])
```

## パラメーターの説明

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトでサンプル収集が使用されます。

- `col_name`: 統計を収集する列。複数の列はカンマ（`,`）で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。

- `PROPERTIES`: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE JOB の出力の `Properties` 列で確認できます。

| **PROPERTIES**                        | **Type** | **Default value** | **Description**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 自動収集の統計が健全かどうかを判断するための閾値。この閾値を下回ると、自動収集がトリガーされます。 |
| statistics_max_full_collect_data_size | INT      | 100               | 自動収集がデータを収集するための最大パーティションサイズ。単位: GB。この値を超えるパーティションは、完全収集が破棄され、サンプル収集が代わりに行われます。 |
| statistic_sample_collect_rows         | INT      | 200000            | 収集する最小行数。このパラメーター値がテーブルの実際の行数を超える場合、完全収集が行われます。 |

## 例

例 1: 自動完全収集

```SQL
-- すべてのデータベースの完全な統計を自動的に収集します。
CREATE ANALYZE ALL;

-- データベースの完全な統計を自動的に収集します。
CREATE ANALYZE DATABASE db_name;

-- データベース内のすべてのテーブルの完全な統計を自動的に収集します。
CREATE ANALYZE FULL DATABASE db_name;

-- テーブル内の指定された列の完全な統計を自動的に収集します。
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 
```

例 2: 自動サンプル収集

```SQL
-- デフォルト設定を使用してデータベース内のすべてのテーブルの統計を自動的に収集します。
CREATE ANALYZE SAMPLE DATABASE db_name;

-- 統計の健全性と収集する行数を指定して、テーブル内の指定された列の統計を自動的に収集します。
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES(
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);
```

## 参考資料

[SHOW ANALYZE JOB](SHOW_ANALYZE_JOB.md): カスタム収集タスクのステータスを表示します。

[DROP ANALYZE](DROP_ANALYZE.md): カスタム収集タスクを削除します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中のカスタム収集タスクをキャンセルします。

CBO の統計収集について詳しくは、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。