---
displayed_sidebar: docs
---

# ANALYZE TABLE

## 説明

CBO 統計情報を収集するための手動収集タスクを作成します。デフォルトでは、手動収集は同期操作です。非同期操作に設定することもできます。非同期モードでは、ANALYZE TABLE を実行した後、システムはこのステートメントが成功したかどうかをすぐに返します。ただし、収集タスクはバックグラウンドで実行され、結果を待つ必要はありません。タスクのステータスは SHOW ANALYZE STATUS を実行して確認できます。非同期収集はデータ量の多いテーブルに適しており、同期収集はデータ量の少ないテーブルに適しています。

**手動収集タスクは作成後に一度だけ実行されます。手動収集タスクを削除する必要はありません。**

このステートメントは v2.4 からサポートされています。

### 基本統計情報を手動で収集する

基本統計情報の詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#basic-statistics) を参照してください。

#### 構文

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property])
```

#### パラメーターの説明

- 収集タイプ
  - FULL: 完全収集を示します。
  - SAMPLE: サンプル収集を示します。
  - 収集タイプが指定されていない場合、デフォルトで完全収集が使用されます。

- `col_name`: 統計情報を収集する列。複数の列はカンマ（`,`）で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。パラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `PROPERTIES`: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` ファイルのデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE STATUS の出力の `Properties` 列で確認できます。

| **PROPERTIES**                | **Type** | **Default value** | **Description**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | サンプル収集のために収集する最小行数。このパラメーターの値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |

#### 例

例 1: 手動での完全収集

```SQL
-- デフォルト設定を使用してテーブルの完全な統計情報を手動で収集します。
ANALYZE TABLE tbl_name;

-- デフォルト設定を使用してテーブルの完全な統計情報を手動で収集します。
ANALYZE FULL TABLE tbl_name;

-- デフォルト設定を使用してテーブル内の指定された列の統計情報を手動で収集します。
ANALYZE TABLE tbl_name(c1, c2, c3);
```

例 2: 手動でのサンプル収集

```SQL
-- デフォルト設定を使用してテーブルの部分的な統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name;

-- 収集する行数を指定して、テーブル内の指定された列の統計情報を手動で収集します。
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

### ヒストグラムを手動で収集する

ヒストグラムの詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md#histogram) を参照してください。

#### 構文

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

#### パラメーターの説明

- `col_name`: 統計情報を収集する列。複数の列はカンマ（`,`）で区切ります。このパラメーターが指定されていない場合、テーブル全体が収集されます。ヒストグラムにはこのパラメーターが必要です。

- [WITH SYNC | ASYNC MODE]: 手動収集タスクを同期モードまたは非同期モードで実行するかどうか。パラメーターを指定しない場合、デフォルトで同期収集が使用されます。

- `WITH N BUCKETS`: ヒストグラム収集のためのバケット数 `N`。指定されていない場合、`fe.conf` のデフォルト値が使用されます。

- PROPERTIES: カスタムパラメーター。`PROPERTIES` が指定されていない場合、`fe.conf` のデフォルト設定が使用されます。実際に使用されるプロパティは、SHOW ANALYZE STATUS の出力の `Properties` 列で確認できます。

| **PROPERTIES**                 | **Type** | **Default value** | **Description**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | 収集する最小行数。このパラメーターの値がテーブル内の実際の行数を超える場合、完全収集が実行されます。 |
| histogram_buckets_size         | LONG     | 64                | ヒストグラムのデフォルトのバケット数。                       |
| histogram_mcv_size             | INT      | 100               | ヒストグラムの最も一般的な値 (MCV) の数。                    |
| histogram_sample_ratio         | FLOAT    | 0.1               | ヒストグラムのサンプリング比率。                             |
| histogram_max_sample_row_count | LONG     | 10000000          | ヒストグラムのために収集する最大行数。                       |

ヒストグラムのために収集する行数は、複数のパラメーターによって制御されます。それは `statistic_sample_collect_rows` とテーブル行数 * `histogram_sample_ratio` の間の大きい方の値です。この数は `histogram_max_sample_row_count` で指定された値を超えることはできません。値が超えた場合、`histogram_max_sample_row_count` が優先されます。

#### 例

```SQL
-- デフォルト設定を使用して v1 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- 32 バケット、32 MCV、および 50% のサンプリング比率で v1 および v2 のヒストグラムを手動で収集します。
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

## 参考文献

[SHOW ANALYZE STATUS](SHOW_ANALYZE_STATUS.md): 手動収集タスクのステータスを表示します。

[KILL ANALYZE](KILL_ANALYZE.md): 実行中の手動収集タスクをキャンセルします。

CBO の統計情報収集の詳細については、[Gather statistics for CBO](../../../using_starrocks/Cost_based_optimizer.md) を参照してください。