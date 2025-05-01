---
displayed_sidebar: docs
---

# schema_privileges

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`schema_privileges` は、データベースの権限に関する情報を提供します。

`schema_privileges` で提供されるフィールドは次のとおりです:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 権限が付与されているユーザーの名前。                         |
| TABLE_CATALOG  | スキーマが属する catalog の名前。この値は常に `def` です。   |
| TABLE_SCHEMA   | スキーマの名前。                                             |
| PRIVILEGE_TYPE | 付与された権限。各行には単一の権限がリストされているため、grantee が持つスキーマ権限ごとに1行ずつあります。 |
| IS_GRANTABLE   | ユーザーが `GRANT OPTION` 権限を持っている場合は `YES`、それ以外の場合は `NO`。出力には `PRIVILEGE_TYPE='GRANT OPTION'` として `GRANT OPTION` が別行でリストされません。 |