---
displayed_sidebar: docs
---

# column_privileges

:::note

このビューは、StarRocks の利用可能な機能には適用されません。

:::

`column_privileges` は、現在有効なロールに付与された、または現在有効なロールによって付与されたカラムに対するすべての権限を識別します。

`column_privileges` には次のフィールドが提供されています。

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 権限が付与されているユーザーの名前。                        |
| TABLE_CATALOG  | カラムを含むテーブルが属するカタログの名前。この値は常に `def` です。 |
| TABLE_SCHEMA   | カラムを含むテーブルが属するデータベースの名前。             |
| TABLE_NAME     | カラムを含むテーブルの名前。                                 |
| COLUMN_NAME    | カラムの名前。                                               |
| PRIVILEGE_TYPE | 付与された権限。カラムレベルで付与できる任意の権限の値です。各行は単一の権限をリストしているため、被付与者が持つカラム権限ごとに1行あります。 |
| IS_GRANTABLE   | ユーザーが `GRANT OPTION` 権限を持っている場合は `YES`、そうでない場合は `NO`。出力には `PRIVILEGE_TYPE='GRANT OPTION'` として `GRANT OPTION` を別行でリストしません。 |