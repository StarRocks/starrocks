---
displayed_sidebar: docs
---

# Iceberg メタデータテーブル

このトピックでは、StarRocks で Iceberg テーブルのメタデータ情報を確認する方法について説明します。

## 概要

バージョン V3.4.1 以降、StarRocks は Iceberg メタデータテーブルをサポートしています。これらのメタデータテーブルには、Iceberg テーブルに関するさまざまな情報が含まれており、テーブルの変更履歴、スナップショット、マニフェストなどがあります。元のテーブル名にメタデータテーブル名を追加することで、各メタデータテーブルをクエリできます。

現在、StarRocks は以下の Iceberg メタデータテーブルをサポートしています。

| メタデータテーブル      | 説明                                                         |
| :--------------------- | :----------------------------------------------------------- |
| `history`              | テーブルに対して行われたメタデータ変更のログを表示します。   |
| `metadata_log_entries` | テーブルのメタデータログエントリを表示します。               |
| `snapshots`            | テーブルスナップショットの詳細を表示します。                 |
| `manifests`            | テーブルのログ内のスナップショットに関連付けられたマニフェストの概要を表示します。 |
| `partitions`           | テーブル内のパーティションの詳細を表示します。               |
| `files`                | テーブルの現在のスナップショット内のデータファイルと削除ファイルの詳細を表示します。 |
| `refs`                 | Iceberg の参照に関する詳細を表示し、ブランチやタグを含みます。 |

## `history` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$history;
```

出力:

| フィールド            | 説明                                                         |
| :------------------ | :----------------------------------------------------------- |
| made_current_at     | スナップショットが現在のスナップショットになった時刻。         |
| snapshot_id         | スナップショットの ID。                                       |
| parent_id           | 親スナップショットの ID。                                     |
| is_current_ancestor | このスナップショットが現在のスナップショットの祖先であるかどうか。 |

## `metadata_log_entries` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$metadata_log_entries;
```

出力:

| フィールド               | 説明                                                         |
| :--------------------- | :----------------------------------------------------------- |
| timestamp              | メタデータが記録された時刻。                                 |
| file                   | メタデータファイルの場所。                                   |
| latest_snapshot_id     | メタデータが更新されたときの最新スナップショットの ID。       |
| latest_schema_id       | メタデータが更新されたときの最新スキーマの ID。               |
| latest_sequence_number | メタデータファイルのデータシーケンス番号。                   |

## `snapshots` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$snapshots;
```

出力:

| フィールド       | 説明                                                         |
| :------------ | :----------------------------------------------------------- |
| committed_at  | スナップショットがコミットされた時刻。                         |
| snapshot_id   | スナップショットの ID。                                       |
| parent_id     | 親スナップショットの ID。                                     |
| operation     | Iceberg テーブルに対して行われた操作の種類。 有効な値:<ul><li>`append`: 新しいデータが追加されます。</li><li>`replace`: テーブル内のデータを変更せずにファイルが削除および置換されます。</li><li>`overwrite`: 古いデータが新しいデータで上書きされます。</li><li>`delete`: テーブルからデータが削除されます。</li></ul> |
| manifest_list | スナップショットの変更に関する詳細情報を含む Avro マニフェストファイルのリスト。 |
| summary       | 前のスナップショットから現在のスナップショットまでに行われた変更の概要。 |

## `manifests` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$manifests;
```

出力:

| フィールド                  | 説明                                                         |
| :------------------------ | :----------------------------------------------------------- |
| path                      | マニフェストファイルの場所。                                 |
| length                    | マニフェストファイルの長さ。                                 |
| partition_spec_id         | マニフェストファイルの書き込みに使用されるパーティション仕様の ID。 |
| added_snapshot_id         | このマニフェストエントリが追加されたときのスナップショットの ID。 |
| added_data_files_count    | マニフェストファイル内で `ADDED` ステータスのデータファイルの数。 |
| added_rows_count          | マニフェストファイル内で `ADDED` ステータスのすべてのデータファイルの総行数。 |
| existing_data_files_count | マニフェストファイル内で `EXISTING` ステータスのデータファイルの数。 |
| existing_rows_count       | マニフェストファイル内で `EXISTING` ステータスのすべてのデータファイルの総行数。 |
| deleted_data_files_count  | マニフェストファイル内で `DELETED` ステータスのデータファイルの数。 |
| deleted_rows_count        | マニフェストファイル内で `DELETED` ステータスのすべてのデータファイルの総行数。 |
| partition_summaries       | パーティション範囲のメタデータ。                             |

## `partitions` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$partitions;
```

出力:

| フィールド                        | 説明                                                         |
| :---------------------------- | :----------------------------------------------------------- |
| partition_value               | パーティション列名とパーティション列値のマッピング。         |
| spec_id                       | ファイルのパーティション Spec ID。                           |
| record_count                  | パーティション内のレコード数。                               |
| file_count                    | パーティションにマッピングされたファイルの数。               |
| total_data_file_size_in_bytes | パーティション内のすべてのデータファイルのサイズ。           |
| position_delete_record_count  | パーティション内の Position Delete ファイルの総行数。       |
| position_delete_file_count    | パーティション内の Position Delete ファイルの数。           |
| equality_delete_record_count  | パーティション内の Equality Delete ファイルの総行数。       |
| equality_delete_file_count    | パーティション内の Position Equality ファイルの数。         |
| last_updated_at               | パーティションが最後に更新された時刻。                       |

## `files` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$files;
```

出力:

| フィールド            | 説明                                                         |
| :----------------- | :----------------------------------------------------------- |
| content            | ファイルに格納されているコンテンツの種類。 有効な値: `DATA(0)`, `POSITION_DELETES(1)`, および `EQUALITY_DELETES(2)`。 |
| file_path          | データファイルの場所。                                       |
| file_format        | データファイルの形式。                                       |
| spec_id            | 行を含むファイルを追跡するために使用される Spec ID。         |
| record_count       | データファイルに含まれるエントリの数。                       |
| file_size_in_bytes | データファイルのサイズ。                                     |
| column_sizes       | Iceberg 列 ID とファイル内の対応するサイズとのマッピング。   |
| value_counts       | Iceberg 列 ID とファイル内の対応するエントリ数とのマッピング。 |
| null_value_counts  | Iceberg 列 ID とファイル内の対応する `NULL` 値の数とのマッピング。 |
| nan_value_counts   | Iceberg 列 ID とファイル内の対応する非数値値の数とのマッピング。 |
| lower_bounds       | Iceberg 列 ID とファイル内の対応する下限とのマッピング。     |
| upper_bounds       | Iceberg 列 ID とファイル内の対応する上限とのマッピング。     |
| split_offsets      | 推奨される分割位置のリスト。                                 |
| sort_id            | このファイルのソート順序を表す ID。                          |
| equality_ids       | Equality Delete ファイルでの等価比較に使用されるフィールド ID のセット。 |
| key_metadata       | このファイルを暗号化するために使用される暗号化キーに関するメタデータ（該当する場合）。 |

## `refs` テーブル

使用法:

```SQL
SELECT * FROM [<catalog>.][<database>.]table$refs;
```

出力:

| フィールド                  | 説明                                                         |
| :---------------------- | :----------------------------------------------------------- |
| name                    | 参照の名前。                                                 |
| type                    | 参照の種類。 有効な値: `BRANCH` または `TAG`。              |
| snapshot_id             | 参照のスナップショット ID。                                  |
| max_reference_age_in_ms | 参照が期限切れになるまでの最大年齢。                         |
| min_snapshots_to_keep   | ブランチのみ、ブランチ内で保持する最小スナップショット数。   |
| max_snapshot_age_in_ms  | ブランチのみ、ブランチ内で許可される最大スナップショット年齢。ブランチ内の古いスナップショットは期限切れになります。 |
