---
displayed_sidebar: docs
---

# table_privileges

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`table_privileges` は、テーブルの権限に関する情報を提供します。

`table_privileges` には以下のフィールドが含まれています:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 権限が付与されているユーザーの名前。                        |
| TABLE_CATALOG  | テーブルが属する catalog の名前。この値は常に `def` です。  |
| TABLE_SCHEMA   | テーブルが属するデータベースの名前。                         |
| TABLE_NAME     | テーブルの名前。                                             |
| PRIVILEGE_TYPE | 付与された権限。テーブルレベルで付与可能な任意の権限の値が入ります。 |
| IS_GRANTABLE   | ユーザーが `GRANT OPTION` 権限を持っている場合は `YES`、それ以外は `NO`。出力には `PRIVILEGE_TYPE='GRANT OPTION'` として `GRANT OPTION` を別行でリストしません。 |