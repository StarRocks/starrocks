---
displayed_sidebar: docs
sidebar_position: 70
description: "How to use StarRocks virtual columns such as _tablet_id_, _segment_id_, and _row_id_ to inspect where each row of a native table is physically stored."
keywords: ['virtual column', '仮想カラム', 'tablet_id', 'row_id', 'segment_id', 'rowset_id']
---

# 仮想カラム

仮想カラム (Virtual column) は、StarRocks がネイティブテーブルに対して提供する読み取り専用のメタデータカラムです。各行の**物理的な格納位置**、つまりその行がどの Tablet、どの Rowset、どの Segment、そして Segment 内の何行目から読み取られたのかを示します。

仮想カラムはクエリ実行時に BE (または CN) ノードで計算されます。ディスクには格納されず、ストレージ容量を消費せず、テーブルスキーマの一部でもありません。そのため、仮想カラムは次のように動作します。

- `SELECT *` の結果には含まれません。
- `DESCRIBE <table>` および `SHOW CREATE TABLE <table>` の結果には含まれません。
- カラム名を明示的に指定しないと参照できません。

仮想カラムは主に診断用途で使用します。たとえば、データが各 Tablet にどのように分散しているかを分析したり、特定の行の物理的な位置を特定したりする場合です。

## 仮想カラム一覧

| カラム             | データ型 | 説明                                                                    |
| ----------------- | -------- | ----------------------------------------------------------------------- |
| `_tablet_id_`     | BIGINT   | その行が格納されている Tablet の ID。                                       |
| `_rowset_id_`     | STRING   | その行が格納されている Rowset の ID。                                       |
| `_segment_id_`    | BIGINT   | その行が格納されている Segment の ID。                                      |
| `_rss_id_`        | INT      | Tablet 内における Rowset-Segment ID。Rowset ID と Segment ID から算出されます。 |
| `_dynamic_rssid_` | INT      | その行の動的 Rowset-Segment ID。                                           |
| `_row_id_`        | BIGINT   | Segment 内における行番号 (0 から開始)。                                     |
| `_source_id_`     | INT      | その行を読み取った BE または CN ノードの ID。                                |

仮想カラム名は大文字と小文字を区別しません。`_tablet_id_` と `_TABLET_ID_` は同じ仮想カラムを指します。

`_tablet_id_`、`_rowset_id_` (または `_rss_id_`)、`_segment_id_`、`_row_id_` を組み合わせると、読み取られた時点における行の物理的な位置を特定できます。

## 使用方法

仮想カラムは、Internal Catalog のネイティブテーブル (重複キーテーブル、集計テーブル、ユニークキーテーブル、主キーテーブル) およびマテリアライズドビューで使用できます。External Catalog のテーブルは仮想カラムを提供しません。

クエリ内で通常のカラムを使用できる箇所であれば、`SELECT` リスト、`WHERE`、`GROUP BY`、`ORDER BY`、集計関数、JOIN のいずれでも仮想カラムを参照できます。複数のテーブルを含むクエリでは、`t._tablet_id_` のようにテーブル名またはエイリアスで修飾してください。

以降の例では次のテーブルを使用します。

```sql
CREATE TABLE olap_table (
    k1 INT,
    k2 INT,
    v1 STRING
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;

INSERT INTO olap_table VALUES
(1, 10, 'a'),
(2, 20, 'b'),
(3, 30, 'c'),
(4, 40, 'd'),
(5, 50, 'e');
```

### 仮想カラムをクエリする

```sql
SELECT _tablet_id_, k1, k2 FROM olap_table ORDER BY k1;
+------------+------+------+
| _tablet_id_| k1   | k2   |
+------------+------+------+
|    3619836 |    1 |   10 |
|    3619834 |    2 |   20 |
|    3619836 |    3 |   30 |
|    3619838 |    4 |   40 |
|    3619838 |    5 |   50 |
+------------+------+------+
```

仮想カラムは `SELECT *` の結果には表示されません。

```sql
SELECT * FROM olap_table ORDER BY k1;
+------+------+------+
| k1   | k2   | v1   |
+------+------+------+
|    1 |   10 | a    |
|    2 |   20 | b    |
|    3 |   30 | c    |
|    4 |   40 | d    |
|    5 |   50 | e    |
+------+------+------+
```

### Tablet 間のデータ分散を分析する

`_tablet_id_` でグループ化すると、各 Tablet が保持する行数を確認できます。バケット化キーの選択が適切でないために発生したデータスキューを素早く検出できます。

```sql
SELECT _tablet_id_, COUNT(*) AS row_count, SUM(k2) AS sum_k2
FROM olap_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
+------------+-----------+--------+
| _tablet_id_| row_count | sum_k2 |
+------------+-----------+--------+
|    3619836 |         2 |     40 |
|    3619838 |         2 |     90 |
|    3619834 |         1 |     20 |
+------------+-----------+--------+
```

実際にデータを保持している Tablet の数を数えるには、次のようにします。

```sql
SELECT COUNT(DISTINCT _tablet_id_) FROM olap_table;
```

### 行の物理的な位置を特定する

```sql
SELECT k1, _tablet_id_, _rowset_id_, _segment_id_, _row_id_
FROM olap_table
WHERE k1 = 1;
```

### 仮想カラムでフィルタリングする

```sql
SELECT k1, k2, _tablet_id_
FROM olap_table
WHERE _tablet_id_ = 3619836
ORDER BY k1;
```

:::note
仮想カラムに対する述語はデータのスキャン中に評価されます。クエリが読み取る Tablet の数が減るわけではありません。
:::

## 使用上の注意

- 仮想カラムが示すのは物理的な格納情報であり、業務データではありません。Compaction、新たなロード、スキーマ変更、レプリカの移行などによって基盤データが再編成されると、値が変わる可能性があります。業務上のキーとして保存したり、複数のクエリ間で行を突き合わせる目的で使用したりしないでください。
- 仮想カラムはクエリ専用です。ロード、更新、`CREATE TABLE` での宣言はできません。
- StarRocks は仮想カラムの列統計情報を収集しません。
- 仮想カラムの名前はシステム予約名です。デフォルトでは同名のテーブルカラムを作成できません。同名のテーブルカラムは仮想カラムを隠してしまい、クエリではどちらも参照できなくなるためです。そのようなカラム名がどうしても必要な場合は、FE 動的パラメータ [`allow_system_reserved_names`](../administration/configuration/FE_parameters/shared_lake_other.md#allow_system_reserved_names) を `TRUE` に設定してください。詳細は [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

## 仮想カラムの有効化と無効化

仮想カラムは FE 動的パラメータ [`enable_virtual_columns`](../administration/configuration/FE_parameters/user_query_loading.md#enable_virtual_columns) で制御します。デフォルト値は `true` です。

クラスター全体で無効にするには、次のように実行します。

```sql
ADMIN SET FRONTEND CONFIG ("enable_virtual_columns" = "false");
```

仮想カラムが無効な状態で仮想カラムを参照すると、`Column '_tablet_id_' cannot be resolved` のようなエラーが返されます。
