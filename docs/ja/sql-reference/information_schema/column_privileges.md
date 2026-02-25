---
displayed_sidebar: docs
---

# column_privileges

:::note

このビューは StarRocks の利用可能な機能には適用されません。

:::

`column_privileges` は、現在有効なロールに付与された、または現在有効なロールによって付与された列に対するすべての権限を識別します。

`column_privileges` には以下のフィールドが提供されています:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 権限が付与されているユーザーの名前。                        |
| TABLE_CATALOG  | 列を含むテーブルが属する catalog の名前。この値は常に `def` です。 |
| TABLE_SCHEMA   | 列を含むテーブルが属するデータベースの名前。                |
| TABLE_NAME     | 列を含むテーブルの名前。                                     |
| COLUMN_NAME    | 列の名前。                                                   |
| PRIVILEGE_TYPE | 付与された権限。値は列レベルで付与可能な任意の権限です。各行には単一の権限がリストされているため、被付与者が持つ列権限ごとに1行あります。 |
| IS_GRANTABLE   | ユーザーが `GRANT OPTION` 権限を持っている場合は `YES`、それ以外の場合は `NO`。出力には `PRIVILEGE_TYPE='GRANT OPTION'` として `GRANT OPTION` を別行でリストしません。 |