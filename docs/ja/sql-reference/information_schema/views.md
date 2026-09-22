---
displayed_sidebar: docs
description: "ビューは、すべてのユーザー定義ビューに関する情報を提供します。"
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
| CHECK_OPTION         | `CHECK_OPTION` 属性の値。StarRocks のビューは `WITH CHECK OPTION` をサポートしていないため、この値は常に `NONE` です。 |
| IS_UPDATABLE         | ビューが更新可能かどうか。この列は値が設定されないため、常に `NO` です。 |
| DEFINER              | ビューを作成したユーザー。この列は値が設定されないため、常に空文字列です。 |
| SECURITY_TYPE        | ビューの `SQL SECURITY` 特性。この列は値が設定されないため常に空文字列であり、[CREATE VIEW](../sql-statements/View/CREATE_VIEW.md) の `SECURITY` 句は反映されません。 |
| CHARACTER_SET_CLIENT | ビューを作成したクライアント接続の文字セット。この値は常に `utf8` です。 |
| COLLATION_CONNECTION | ビューを作成したクライアント接続の照合順序。この値は常に `utf8_general_ci` です。 |
