---
displayed_sidebar: docs
---

# Iceberg Catalog でのタイムトラベル

このトピックでは、Iceberg catalog に対する StarRocks のタイムトラベル機能を紹介します。この機能は v3.4.0 以降でサポートされています。

## 概要

各 Iceberg テーブルはメタデータスナップショットログを保持しており、それに適用された変更を表します。データベースはこれらの履歴スナップショットにアクセスすることで Iceberg テーブルに対してタイムトラベルクエリを実行できます。Iceberg はスナップショットのライフサイクル管理を高度に行うために、スナップショットのブランチングとタグ付けをサポートしており、各ブランチまたはタグはカスタマイズされた保持ポリシーに基づいて独自のライフサイクルを維持できます。Iceberg のブランチングとタグ付け機能の詳細については、[公式ドキュメント](https://iceberg.apache.org/docs/latest/branching/)を参照してください。

Iceberg のスナップショットブランチングとタグ付け機能を統合することで、StarRocks は Iceberg catalog 内でのブランチとタグの作成および管理、そしてテーブルに対するタイムトラベルクエリをサポートしています。

## ブランチ、タグ、スナップショットの管理

### ブランチの作成

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] BRANCH [IF NOT EXISTS] <branch_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
[WITH SNAPSHOT RETENTION 
    { minSnapshotsToKeep | maxSnapshotAge | minSnapshotsToKeep maxSnapshotAge }]

minSnapshotsToKeep ::= <int> SNAPSHOTS

maxSnapshotAge ::= <int> { DAYS | HOURS | MINUTES }
```

**パラメータ**

- `branch_name`: 作成するブランチの名前。
- `AS OF VERSION`: ブランチを作成するスナップショット（バージョン）の ID。
- `RETAIN`: ブランチを保持する期間。形式: `<int> <unit>`。サポートされる単位: `DAYS`、`HOURS`、`MINUTES`。例: `7 DAYS`、`12 HOURS`、`30 MINUTES`。
- `WITH SNAPSHOT RETENTION`: 保持するスナップショットの最小数および/またはスナップショットを保持する最大時間。

**例**

テーブル `iceberg.sales.order` のバージョン（スナップショット ID）`12345` に基づいてブランチ `test-branch` を作成し、ブランチを `7` 日間保持し、ブランチ上に少なくとも `2` つのスナップショットを保持します。

```SQL
ALTER TABLE iceberg.sales.order CREATE BRANCH `test-branch` 
AS OF VERSION 12345
RETAIN 7 DAYS
WITH SNAPSHOT RETENTION 2 SNAPSHOTS;
```

### 特定のブランチにデータをロードする

**構文**

```SQL
INSERT INTO [catalog.][database.]table_name
[FOR] VERSION AS OF <branch_name>
<query_statement>
```

**パラメータ**

- `branch_name`: データをロードするテーブルブランチの名前。
- `query_statement`: 結果が宛先テーブルにロードされるクエリ文。StarRocks がサポートする任意の SQL 文を使用できます。

**例**

クエリの結果をテーブル `iceberg.sales.order` のブランチ `test-branch` にロードします。

```SQL
INSERT INTO iceberg.sales.order
FOR VERSION AS OF `test-branch`
SELECT c1, k1 FROM tbl;
```

### タグの作成

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
CREATE [OR REPLACE] TAG [IF NOT EXISTS] <tag_name>
[AS OF VERSION <snapshot_id>]
[RETAIN <int> { DAYS | HOURS | MINUTES }]
```

**パラメータ**

- `tag_name`: 作成するタグの名前。
- `AS OF VERSION`: タグを作成するスナップショット（バージョン）の ID。
- `RETAIN`: タグを保持する期間。形式: `<int> <unit>`。サポートされる単位: `DAYS`、`HOURS`、`MINUTES`。例: `7 DAYS`、`12 HOURS`、`30 MINUTES`。

**例**

テーブル `iceberg.sales.order` のバージョン（スナップショット ID）`12345` に基づいてタグ `test-tag` を作成し、タグを `7` 日間保持します。

```SQL
ALTER TABLE iceberg.sales.order CREATE TAG `test-tag` 
AS OF VERSION 12345
RETAIN 7 DAYS;
```

### ブランチを別のブランチに高速転送する

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**パラメータ**

- `from_branch`: 高速転送したいブランチ。ブランチ名を引用符で囲みます。
- `to_branch`: `from_branch` を高速転送するブランチ。ブランチ名を引用符で囲みます。

**例**

`main` ブランチを `test-branch` ブランチに高速転送します。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

### スナップショットを選択適用する

特定のスナップショットを選択してテーブルの現在の状態に適用できます。この操作は既存のスナップショットに基づいて新しいスナップショットを作成し、元のスナップショットには影響を与えません。

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**パラメータ**

`snapshot_id`: 選択適用したいスナップショットの ID。

**例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

### スナップショットを期限切れにする

特定の時点より前のスナップショットを期限切れにできます。この操作は期限切れのスナップショットのデータファイルを削除します。

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshot('<datetime>')
```

**例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshot('2023-12-17 00:14:38')
```

### ブランチまたはタグを削除する

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
DROP { BRANCH <branch_name> | TAG <tag_name> }
```

**例**

```SQL
ALTER TABLE iceberg.sales.order
DROP BRANCH `test-branch`;

ALTER TABLE iceberg.sales.order
DROP TAG `test-tag`;
```

## タイムトラベルクエリ

### 特定のブランチまたはタグへのタイムトラベル

**構文**

```SQL
[FOR] VERSION AS OF '<branch_or_tag>'
```

**パラメータ**

`tag_or_branch`: タイムトラベルしたいブランチまたはタグの名前。ブランチ名が指定された場合、クエリはブランチの最新スナップショットにタイムトラベルします。タグ名が指定された場合、クエリはタグが参照するスナップショットにタイムトラベルします。

**例**

```SQL
-- ブランチの最新スナップショットにタイムトラベルします。
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-branch';
-- タグが参照するスナップショットにタイムトラベルします。
SELECT * FROM iceberg.sales.order VERSION AS OF 'test-tag';
```

### 特定のスナップショットへのタイムトラベル

**構文**

```SQL
[FOR] VERSION AS OF '<snapshot_id>'
```

**パラメータ**

`snapshot_id`: タイムトラベルしたいスナップショットの ID。

**例**

```SQL
SELECT * FROM iceberg.sales.order VERSION AS OF 12345;
```

### 特定の日時または日付へのタイムトラベル

**構文**

```SQL
[FOR] TIMESTAMP AS OF { '<datetime>' | '<date>' | date_and_time_function }
```

**パラメータ**

`date_and_time_function`: StarRocks がサポートする任意の[日付と時刻の関数](../../../sql-reference/sql-functions/date-time-functions/now.md)。

**例**

```SQL
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26 01:21:00';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF '1986-10-26';
SELECT * FROM iceberg.sales.order TIMESTAMP AS OF now();
```