---
displayed_sidebar: docs
---

# tables_config

`tables_config` は、テーブルの設定に関する情報を提供します。

`tables_config` で提供されるフィールドは次のとおりです:

| **Field**        | **Description**                                              |
| ---------------- | ------------------------------------------------------------ |
| TABLE_SCHEMA     | テーブルを格納するデータベースの名前。                        |
| TABLE_NAME       | テーブルの名前。                                             |
| TABLE_ENGINE     | テーブルのエンジンタイプ。                                   |
| TABLE_MODEL      | テーブルのデータモデル。 有効な値: `DUP_KEYS`, `AGG_KEYS`, `UNQ_KEYS` または `PRI_KEYS`。 |
| PRIMARY_KEY      | 主キーテーブルまたはユニークキーテーブルの主キー。テーブルが主キーテーブルまたはユニークキーテーブルでない場合は空の文字列が返されます。 |
| PARTITION_KEY    | テーブルのパーティション列。                                |
| DISTRIBUTE_KEY   | テーブルのバケット列。                                       |
| DISTRIBUTE_TYPE  | テーブルのデータ分散方法。                                   |
| DISTRUBTE_BUCKET | テーブルのバケット数。                                       |
| SORT_KEY         | テーブルのソートキー。                                       |
| PROPERTIES       | テーブルのプロパティ。                                       |
| TABLE_ID         | テーブルのID。                                               |