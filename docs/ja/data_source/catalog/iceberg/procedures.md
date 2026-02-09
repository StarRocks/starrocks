---
displayed_sidebar: docs
toc_max_heading_level: 5
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files', 'add files', 'register table', 'rollback to snapshot', 'remove orphan files']
---

# Iceberg Procedures

このドキュメントでは、StarRocksにおけるIcebergカタログで利用可能なストアドプロシージャについて説明します。これには、スナップショット管理、ブランチ管理、データメンテナンス、メタデータ管理、およびテーブル管理の操作が含まれます。

プロシージャを実行するには、適切な権限が必要です。権限の詳細については、[権限](../../../administration/user_privs/authorization/privilege_item.md)を参照してください。

## スナップショット管理

### Rollback to snapshot

テーブルを指定されたスナップショットにロールバックします。この操作により、テーブルの現在のスナップショットが指定されたスナップショットIDに設定されます。

**`rollback_to_snapshot` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE rollback_to_snapshot(<snapshot_id>)
```

**パラメータ**

`snapshot_id`: テーブルをロールバックするスナップショットのID。

**例**

IDが98765のスナップショットにテーブルをロールバックします：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE rollback_to_snapshot(98765);
```

### Cherry pick a snapshot

特定のスナップショットを選択し、現在のテーブル状態に適用します。この操作により、既存のスナップショットに基づいて新しいスナップショットが作成され、元のスナップショットは影響を受けません。

**`cherrypick_snapshot` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

**パラメータ**

`snapshot_id`: 選択したいスナップショットのID。

**例**

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

---

## ブランチ管理

### Fast forward a branch to another

1つのブランチを別のブランチの最新スナップショットに高速転送します。この操作は、ソースブランチのスナップショットをターゲットブランチのスナップショットに一致するように更新します。

**`fast_forward` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

**パラメータ**

- `from_branch`: 高速転送したいブランチ。ブランチ名を引用符で囲みます。
- `to_branch`: `from_branch` を高速転送する対象のブランチ。ブランチ名を引用符で囲みます。

**例**

`main` ブランチを `test-branch` ブランチに高速転送します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

---

## データメンテナンス

### Rewrite data files

データファイルのレイアウトを最適化するためにファイルを再書き込みします。このプロシージャは小さなファイルをマージして、クエリパフォーマンスを向上させ、メタデータオーバーヘッドを削減します。

**構文**

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [, "key"=value, ...]) 
[WHERE <predicate>]
```

**パラメータ**

##### `rewrite_data_files` プロパティ

手動コンパクションの動作を宣言する `"key"=value` ペア。キーは二重引用符で囲む必要があることに注意してください。

###### `min_file_size_bytes`

- 説明: 小さいデータファイルの上限サイズ。この値よりサイズの小さいデータファイルは、コンパクション中にマージされます。
- 単位: バイト
- タイプ: Int
- デフォルト: 268,435,456 (256 MB)

###### `batch_size`

- 説明: 各バッチで処理できる最大データサイズ。
- 単位: バイト
- タイプ: Int
- デフォルト: 10,737,418,240 (10 GB)

###### `rewrite_all`

- 説明: 特定の要件でデータファイルをフィルタリングするパラメータを無視して、コンパクション中にすべてのデータファイルを再書き込むかどうか。
- 単位: -
- タイプ: Boolean
- デフォルト: false

###### `batch_parallelism`

- 説明: コンパクション中に処理する並列バッチ数。
- 単位: -
- タイプ: Int
- デフォルト: 1

##### `WHERE` 句

- 説明: コンパクションに関与するパーティションを指定するためのフィルタリング述語。

**例**

以下の例は、Iceberg テーブル `t1` の特定のパーティションで手動コンパクションを実行します。パーティションは、句 `WHERE part_col = 'p1'` で表されます。これらのパーティションでは、134,217,728 バイト (128 MB) より小さいデータファイルがコンパクション中にマージされます。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

---

## メタデータ管理

### Expire snapshots

特定のタイムスタンプより前のスナップショットを期限切れにします。この操作により、期限切れスナップショットのデータファイルが削除され、ストレージ使用量の管理に役立ちます。

**`expire_snapshots` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots('<datetime>')
```

**パラメータ**

`datetime`: スナップショットを期限切れにするタイムスタンプ。形式: 'YYYY-MM-DD HH:MM:SS'。

**例**

'2023-12-17 00:14:38' より前のスナップショットを期限切れにします：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38')
```

### Remove orphan files

有効なスナップショットから参照されておらず、指定されたタイムスタンプより古い孤立ファイルをテーブルから削除します。この操作は、未使用のファイルをクリーンアップし、ストレージ容量を解放するのに役立ちます。

**`remove_orphan_files` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE remove_orphan_files([older_than = '<datetime>'])
```

**パラメータ**

`older_than`（オプション）：孤立ファイルを削除するタイムスタンプ。指定しない場合、デフォルトで7日より前のファイルが削除されます。形式: 'YYYY-MM-DD HH:MM:SS'。

**例**

'2024-01-01 00:00:00' より古い孤立ファイルを削除します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00');
```

デフォルトの保持期間（7日）を使用して孤立ファイルを削除します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files();
```

---

## テーブル管理

### Add files

ソーステーブルまたは特定の場所からIcebergテーブルにデータファイルを追加します。このプロシージャはParquetおよびORCファイル形式をサポートしています。

**`add_files` 構文**

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE add_files(
    [source_table = '<source_table>' | location = '<location>', file_format = '<format>']
    [, recursive = <boolean>]
)
```

**パラメータ**

`source_table` または `location` のいずれかを指定する必要がありますが、両方を同時に指定することはできません。

##### `source_table`（オプション）

- 説明: ファイルを追加するソーステーブル。形式: 'catalog.database.table'。
- タイプ: String

##### `location`（オプション）

- 説明: ファイルを追加するディレクトリパスまたはファイルパス。
- タイプ: String

##### `file_format`（`location`を使用する場合に必須）

- 説明: データファイルの形式。サポートされる値: 'parquet'、'orc'。
- タイプ: String

##### `recursive`（オプション）

- 説明: 場所からファイルを追加する際にサブディレクトリを再帰的にスキャンするかどうか。
- タイプ: Boolean
- デフォルト: true

**例**

ソーステーブルからファイルを追加します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(source_table = 'hive_catalog.sales.source_order');
```

特定の場所からParquet形式のファイルを追加します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/', file_format = 'parquet', recursive = true);
```

単一のファイルから追加します：

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/data.parquet', file_format = 'parquet');
```

### Register table

メタデータファイルを使用してIcebergテーブルを登録します。このプロシージャを使用すると、データを移行せずに既存のIcebergテーブルをカタログに追加できます。

**`register_table` 構文**

```SQL
CALL [catalog.]system.register_table(
    database_name = '<database_name>',
    table_name = '<table_name>',
    metadata_file = '<metadata_file_path>'
)
```

**パラメータ**

##### `database_name`

- 説明: テーブルを登録するデータベースの名前。
- タイプ: String
- 必須: はい

##### `table_name`

- 説明: 登録するテーブルの名前。
- タイプ: String
- 必須: はい

##### `metadata_file`

- 説明: Icebergテーブルのメタデータファイルのパス（例: metadata.json）。
- タイプ: String
- 必須: はい

**例**

メタデータファイルを使用してテーブルを登録します：

```SQL
CALL iceberg_catalog.system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```

または現在のカタログを使用します：

```SQL
CALL system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```
