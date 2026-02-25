---
displayed_sidebar: docs
---

# schemata

`schemata` はデータベースに関する情報を提供します。

`schemata` で提供されるフィールドは次のとおりです:

| **Field**                  | **Description**                                              |
| -------------------------- | ------------------------------------------------------------ |
| CATALOG_NAME               | スキーマが属する catalog の名前。この値は常に `NULL` です。 |
| SCHEMA_NAME                | スキーマの名前。                                             |
| DEFAULT_CHARACTER_SET_NAME | スキーマのデフォルト文字セット。                            |
| DEFAULT_COLLATION_NAME     | スキーマのデフォルト照合順序。                              |
| SQL_PATH                   | この値は常に `NULL` です。                                  |