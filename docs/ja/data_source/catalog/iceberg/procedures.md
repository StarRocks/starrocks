---
displayed_sidebar: docs
keywords: ['iceberg', 'procedures', 'fast forward', 'cherry pick', 'expire snapshots', 'rewrite data files']
---

# Iceberg Procedure

StarRocks Iceberg Catalog は、ブランチ管理、スナップショット操作、データ最適化などの操作を含む、様々なストアドプロシージャをサポートしています。

プロシージャを実行するには、適切な権限が必要です。権限の詳細については、[権限](../../../administration/user_privs/authorization/privilege_item.md)を参照してください。

## 別のブランチに早送りする

1つのブランチを別のブランチの最新スナップショットに早送りする。この操作は、ソースブランチのスナップショットをターゲットブランチのスナップショットに一致するように更新します。

### `fast_forward` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE fast_forward('<from_branch>', '<to_branch>')
```

### パラメータ

- `from_branch`: 高速転送したいブランチ。ブランチ名を引用符で囲みます。
- `to_branch`: `from_branch` を高速転送する対象のブランチ。ブランチ名を引用符で囲みます。

### 例

`main` ブランチを `test-branch` ブランチに高速転送します。

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE fast_forward('main', 'test-branch');
```

## スナップショットをピックする

特定のスナップショットを選択し、現在のテーブル状態に適用します。この操作により、既存のスナップショットに基づいて新しいスナップショットが作成され、元のスナップショットは影響を受けません。

### `cherrypick_snapshot` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE cherrypick_snapshot(<snapshot_id>)
```

### パラメータ

`snapshot_id`: 選択したいスナップショットのID。

### 例

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE cherrypick_snapshot(54321);
```

## スナップショットを期限切れにする

特定のタイムスタンプより前のスナップショットを期限切れにします。この操作により、期限切れスナップショットのデータファイルが削除され、ストレージ使用量の管理に役立ちます。

### `expire_snapshots` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name
EXECUTE expire_snapshots('<datetime>')
```

### パラメータ

`datetime`: スナップショットを期限切れにするタイムスタンプ。形式: 'YYYY-MM-DD HH:MM:SS'。

### 例

'2023-12-17 00:14:38' より前のスナップショットを期限切れにします:

```SQL
ALTER TABLE iceberg.sales.order
EXECUTE expire_snapshots('2023-12-17 00:14:38')
```

## データファイルを書き換える

データファイルのレイアウトを最適化するためにファイルを再書き込みします。このプロシージャは小さなファイルをマージして、クエリパフォーマンスを向上させ、メタデータオーバーヘッドを削減します。

### `rewrite_data_files` 構文

```SQL
ALTER TABLE [catalog.][database.]table_name 
EXECUTE rewrite_data_files
("key"=value [, "key"=value, ...]) 
[WHERE <predicate>]
```

### パラメータ

#### `rewrite_data_files` プロパティ

手動コンパクションの動作を宣言する `"key"=value` ペア。キーは二重引用符で囲む必要があることに注意してください。

##### `min_file_size_bytes`

- 説明: 小さいデータファイルの上限サイズ。この値よりサイズの小さいデータファイルは、コンパクション中にマージされます。
- 単位: バイト
- タイプ: Int
- デフォルト: 268,435,456 (256 MB)

##### `batch_size`

- 説明: 各バッチで処理できる最大データサイズ。
- 単位: バイト
- タイプ: Int
- デフォルト: 10,737,418,240 (10 GB)

##### `rewrite_all`

- 説明: 特定の要件でデータファイルをフィルタリングするパラメータを無視して、コンパクション中にすべてのデータファイルを再書き込むかどうか。
- 単位: -
- タイプ: Boolean
- デフォルト: false

#### `WHERE` 句

- 説明: コンパクションに関与するパーティションを指定するためのフィルタリング述語。

### 例

以下の例は、Iceberg テーブル `t1` の特定のパーティションで手動コンパクションを実行します。パーティションは、句 `WHERE part_col = 'p1'` で表されます。これらのパーティションでは、134,217,728 バイト (128 MB) より小さいデータファイルがコンパクション中にマージされます。

```SQL
ALTER TABLE t1 EXECUTE rewrite_data_files("min_file_size_bytes"= 134217728) WHERE part_col = 'p1';
```
