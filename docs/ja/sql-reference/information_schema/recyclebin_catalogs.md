---
displayed_sidebar: docs
---

# recyclebin_catalogs

`recyclebin_catalogs` は、FE リサイクル ビンに一時的に保存された削除済みデータベース、テーブル、パーティションのメタデータ情報を提供します。

`recyclebin_catalogs` は以下のフィールドを提供します:

| **フィールド** | **説明**                               |
| ------------ | -------------------------------------- |
| TYPE         | 削除されたメタデータのタイプ（Database、Table、Partition を含む）。 |
| NAME         | 削除されたオブジェクトの名前。              |
| DB_ID        | 削除されたオブジェクトの DB ID。           |
| TABLE_ID     | 削除されたオブジェクトの Table ID。        |
| PARTITION_ID | 削除されたオブジェクトの Partition ID。    |
| DROP_TIME    | オブジェクトが削除された時刻。              |
