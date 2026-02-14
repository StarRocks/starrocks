---
displayed_sidebar: docs
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files', 'add files', 'register table', 'rollback to snapshot', 'remove orphan files']
---

# Iceberg Procedures

StarRocks Iceberg catalog は、スナップショット管理、ブランチ管理、データメンテナンス、メタデータ管理、およびテーブル管理を含む、Iceberg テーブルを管理するためのさまざまなプロシージャをサポートしています。

プロシージャを実行するには、適切な権限が必要です。権限の詳細については、[権限](../../../administration/user_privs/authorization/privilege_item.md) を参照してください。

## スナップショット管理

### スナップショットへのロールバック

テーブルを特定のスナップショットにロールバックします。この操作は、テーブルの現在のスナップショットを指定されたスナップショット ID に設定します。

#### `rollback_to_snapshot` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE rollback_to_snapshot(<snapshot_id>)
```

#### パラメータ

`snapshot_id`: テーブルをロールバックするスナップショットの ID。

#### 例

テーブルをスナップショット ID 98765 でロールバックします。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE rollback_to_snapshot(98765);
```

特定のsnapshotをcherry pickし、テーブルの現在の状態に適用します。この操作は、既存のsnapshotに基づいて新しいsnapshotを作成しますが、元のsnapshotは変更されません。

#### `cherrypick_snapshot` の構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

#### Parameters

`snapshot_id`: cherry pick したいスナップショットの ID。

#### 例

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

<chunk_translated>
## ブランチ管理
</chunk_translated>

別のブランチにブランチを高速で進める

あるブランチを別のブランチの最新スナップショットに高速で進めます。この操作は、ソースブランチのスナップショットを更新して、ターゲットブランチのスナップショットと一致させます。

#### `fast_forward` の構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

#### パラメータ

- `from_branch`: 高速で転送したいブランチ。ブランチ名を引用符で囲みます。
- `to_branch`: `from_branch` を高速で転送したいブランチ。ブランチ名を引用符で囲みます。

#### 例

`main` ブランチを `test-branch` ブランチに高速で進める場合:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

## データメンテナンス

### データファイルの書き換え

データファイルを書き換えて、ファイルレイアウトを最適化します。この手順では、クエリパフォーマンスを向上させ、メタデータのオーバーヘッドを削減するために、小さなファイルをマージします。

#### `rewrite_data_files` の構文

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [,"key"=value, ...]) 
[WHERE <predicate>]
```

#### パラメータ

##### `rewrite_data_files` プロパティ

`"key"=value` のペアで、手動 `compaction` の動作を宣言します。キーは二重引用符で囲む必要があります。

###### `min_file_size_bytes`

- 説明: 小さいデータファイルのサイズ上限。サイズがこの値を下回るデータファイルは、`compaction` 時にマージされます。
- 単位: バイト
- タイプ: Int
- デフォルト: 268,435,456 (256 MB)

###### `batch_size`

- 説明: 各バッチで処理できるデータの最大サイズ。
- 単位: バイト
- タイプ: Int
- デフォルト: 10,737,418,240 (10 GB)

###### `rewrite_all`

- 説明: 特定の要件を持つデータファイルをフィルタリングするパラメータを無視して、`compaction` 時にすべてのデータファイルを書き換えるかどうか。
- 単位: -
- タイプ: Boolean
- デフォルト: false

###### `batch_parallelism`

- 説明: `compaction` 時に処理する並列バッチの数。
- 単位: -
- タイプ: Int
- デフォルト: 1

##### `WHERE` 句

- 説明: Compaction に含めるパーティションを指定するために使用されるフィルタ述語。

#### 例

次の例では、Iceberg テーブル `t1` 内の特定のパーティションに対して手動で Compaction を実行します。パーティションは `WHERE part_col = 'p1'` という句で表されます。これらのパーティションでは、134,217,728 バイト (128 MB) 未満のデータファイルは Compaction 中にマージされます。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```

## メタデータ管理

### スナップショットの有効期限

特定の日時より古いスナップショットの有効期限が切れます。この操作により、期限切れのスナップショットのデータファイルが削除され、ストレージの使用状況の管理に役立ちます。

#### `expire_snapshots` の構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots(
    [ [older_than =] '<datetime>' ] [,  [retain_last =] <int> ]
)
```

#### パラメータ

##### `older_than`

- 説明: スナップショットを削除するタイムスタンプの基準となる日時。指定しない場合、デフォルトでは、（現在の時刻から）5 日以上前のファイルが削除されます。形式: 'YYYY-MM-DD HH:MM:SS'。
- タイプ: DATETIME
- 必須: いいえ

##### `retain_last`

- 説明: 保持する最新のスナップショットの最大数。このしきい値に達すると、古いスナップショットは削除されます。指定しない場合、デフォルトでは1つのスナップショットのみが保持されます。
- タイプ: Integer
- 必須: いいえ

#### 例

'2023-12-17 00:14:38' より前のスナップショットを期限切れにし、2つのスナップショットを保持します。

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots(older_than = '2023-12-17 00:14:38', retain_last = 2);

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38', 2);
```

### 不要なファイルを削除する

有効なスナップショットから参照されておらず、指定されたタイムスタンプよりも古い、テーブル内の不要なファイルを削除します。この操作は、未使用のファイルをクリーンアップし、ストレージ容量を再利用するのに役立ちます。

#### `remove_orphan_files` の構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE remove_orphan_files(
    [ [older_than =] '<datetime>' ] [,  [location =] '<string>' ]
)
```

#### パラメータ

##### `older_than`

- 説明: 孤立ファイルを削除するタイムスタンプの期限。指定しない場合、デフォルトでは、（現在の時刻から）7 日以上前のファイルが削除されます。形式: 'YYYY-MM-DD HH:MM:SS'。
- タイプ: DATETIME
- 必須: いいえ

##### `location`

- 説明: 不要なファイルを削除するディレクトリ。テーブルの場所のサブディレクトリである必要があります。指定しない場合、デフォルトでテーブルの場所が使用されます。
- タイプ: STRING
- 必須: いいえ

#### 例

テーブルの場所のサブディレクトリ `sub_dir` から、'2024-01-01 00:00:00' より古い孤立ファイルを削除します。

```SQL
-- With the parameter key specified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files(older_than = '2024-01-01 00:00:00', location = 's3://iceberg-bucket/iceberg_db/iceberg_table/sub_dir');

-- With the parameter key unspecified:
ALTER TABLE iceberg.sales.order
EXECUTE remove_orphan_files('2024-01-01 00:00:00', 's3://bucket-test/iceberg_db/iceberg_table/sub_dir');
```

## テーブル管理

### ファイルの追加

ソーステーブルまたは特定の場所から Iceberg テーブルにデータファイルを追加します。このプロシージャは、Parquet および ORC ファイル形式をサポートしています。

#### `add_files` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE add_files(
    [source_table = '<source_table>' | location = '<location>', file_format = '<format>']
    [, recursive = <boolean>]
)
```

#### パラメータ

`source_table` または `location` のいずれかを指定する必要があります。両方を指定することはできません。

##### `source_table`

- 説明: ファイルを追加する元のテーブル。形式: 'catalog.database.table'。
- タイプ: String
- 必須: いいえ

##### `location`

- 説明: ファイルの追加元となるディレクトリパスまたはファイルパス。
- タイプ: String
- 必須: いいえ

##### `file_format`

- 説明: データファイルの形式。サポートされている値: 'parquet'、'orc'。
- タイプ: String
- 必須: いいえ ( `location` を使用する場合に必須)

##### `recursive`

- 説明: ある場所からファイルを追加する際に、サブディレクトリを再帰的にスキャンするかどうか。
- タイプ: Boolean
- デフォルト: true
- 必須: いいえ

#### 例

ソーステーブルからファイルを追加します。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(source_table = 'hive_catalog.sales.source_order');
```

Parquet 形式で特定の場所からファイルを追加します。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/', file_format = 'parquet', recursive = true);
```

単一のファイルからファイルを追加します。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE add_files(location = 's3://bucket/data/order/data.parquet', file_format = 'parquet');
```

### テーブルの登録

メタデータファイルを使用して Iceberg テーブルを登録します。このプロシージャを使用すると、データを移行せずに既存の Iceberg テーブルを catalog に追加できます。

#### `register_table` の構文

```SQL
CALL [catalog.]system.register_table(
    database_name = '<database_name>',
    table_name = '<table_name>',
    metadata_file = '<metadata_file_path>'
)
```

#### パラメータ

##### `database_name`

- 説明: テーブルを登録するデータベースの名前。
- タイプ: String
- 必須: はい

##### `table_name`

- 説明: 登録するテーブルの名前。
- タイプ: String
- 必須: はい

##### `metadata_file`

- 説明: Iceberg テーブルのメタデータファイルへのパス (例: metadata.json)。
- タイプ: String
- 必須: はい

#### 例

メタデータファイルを使用してテーブルを登録します。

```SQL
CALL iceberg_catalog.system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```

現在の catalog を使用することもできます。

```SQL
CALL system.register_table(
    database_name = 'sales',
    table_name = 'order',
    metadata_file = 's3://bucket/metadata/sales/order/metadata/00001-xxxxx-xxxxx-xxxxx.metadata.json'
);
```