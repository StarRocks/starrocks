---
displayed_sidebar: docs
---

# views

`views` は、すべてのユーザー定義ビューに関する情報を提供します。

`views` には次のフィールドが含まれています:

| **Field**            | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| TABLE_CATALOG        | ビューが属する catalog の名前。この値は常に `def` です。 |
| TABLE_SCHEMA         | ビューが属するデータベースの名前。                          |
| TABLE_NAME           | ビューの名前。                                               |
| VIEW_DEFINITION      | ビューの定義を提供する `SELECT` 文。                         |
| CHECK_OPTION         | `CHECK_OPTION` 属性の値。この値は `NONE`、`CASCADE`、`LOCAL` のいずれかです。 |
| IS_UPDATABLE         | ビューが更新可能かどうか。`UPDATE` や `DELETE`（および類似の操作）がビューに対して合法である場合、フラグは `YES`（真）に設定されます。それ以外の場合、フラグは `NO`（偽）に設定されます。ビューが更新不可能な場合、`UPDATE`、`DELETE`、`INSERT` などの文は違法であり、拒否されます。 |
| DEFINER              | ビューを作成したユーザーのユーザー名。                       |
| SECURITY_TYPE        | ビューの `SQL SECURITY` 特性。この値は `DEFINER` または `INVOKER` のいずれかです。 |
| CHARACTER_SET_CLIENT |                                                              |
| COLLATION_CONNECTION |                                                              |