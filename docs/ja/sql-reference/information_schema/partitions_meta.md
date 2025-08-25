---
displayed_sidebar: docs
---

# partitions_meta

`partitions_meta` はテーブルのパーティションに関する情報を提供します。

`partitions_meta` には以下のフィールドが提供されています:

| **フィールド**                      | **説明**                                         |
| ----------------------------- | ------------------------------------------------ |
| DB_NAME                       | パーティションが属するデータベースの名前。       |
| TABLE_NAME                    | パーティションが属するテーブルの名前。           |
| PARTITION_NAME                | パーティションの名前。                           |
| PARTITION_ID                  | パーティションの ID。                            |
| COMPACT_VERSION               | パーティションのコンパクトバージョン。           |
| VISIBLE_VERSION               | ��ーティションの可視バージョン。                 |
| VISIBLE_VERSION_TIME          | パーティションの可視バージョン時刻。             |
| NEXT_VERSION                  | パーティションの次のバージョン。                 |
| DATA_VERSION                  | パーティションのデータバージョン。               |
| VERSION_EPOCH                 | パーティションのバージョンエポック。             |
| VERSION_TXN_TYPE              | パーティションのバージョン Txn タイプ。          |
| PARTITION_KEY                 | パーティションのパーティションキー。             |
| PARTITION_VALUE               | パーティションの値（例: `Range` または `List`）。 |
| DISTRIBUTION_KEY              | パーティションの分散キー。                       |
| BUCKETS                       | パーティション内のバケット数。                   |
| REPLICATION_NUM               | パーティションのレプリケーション数。             |
| STORAGE_MEDIUM                | パーティションのストレージメディア。             |
| COOLDOWN_TIME                 | パーティションのクールダウン時間。               |
| LAST_CONSISTENCY_CHECK_TIME   | パーティションの最終整合性チェック時刻。         |
| IS_IN_MEMORY                  | パーティションがメモリ内にあるかどうか（`true`）またはそうでないか（`false`）。 |
| IS_TEMP                       | パーティションが一時的であるかどうか（`true`）またはそうでないか（`false`）。 |
| DATA_SIZE                     | パーティションのデータサイズ。                   |
| ROW_COUNT                     | パーティション内の行数。                         |
| ENABLE_DATACACHE              | パーティションでデータキャッシュが有効になっているかどうか（`true`）またはそうでないか（`false`）。 |
| AVG_CS                        | パーティションの平均コンパクションスコア。       |
| P50_CS                        | パーティションの 50 パーセンタイルコンパクションスコア。 |
| MAX_CS                        | パーティションの最��コンパクションスコア。       |
| STORAGE_PATH                  | パーティションのストレージパス。                 |
| STORAGE_SIZE                  | パーティションのストレージサイズ。               |
| METADATA_SWITCH_VERSION       | パーティションのメタデータスイッチバージョン。   |
| TABLET_BALANCED               | Tablet の配置がパーティション内で均等に分散されているかどうか。 |
