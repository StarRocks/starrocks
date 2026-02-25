---
displayed_sidebar: docs
---

# grants_to_users

ビュー `grants_to_users` をクエリすることで、ユーザーに付与された権限を確認できます。

:::note

デフォルトでは、`user_admin` ロールを持つユーザーまたはロールのみがこのビューにアクセスできます。他のユーザーにこのビューの SELECT 権限を付与するには、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) を使用してください。

:::

`grants_to_users` には以下のフィールドが提供されています:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| GRANTEE         | この権限が付与されているユーザー。                           |
| OBJECT_CATALOG  | オブジェクトが属する catalog。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、または GLOBAL FUNCTION レベルの権限である場合は `NULL` が返されます。 |
| OBJECT_DATABASE | オブジェクトが属するデータベース。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION、または CATALOG レベルの権限である場合は `NULL` が返されます。 |
| OBJECT_NAME     | オブジェクトが属するテーブル。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION、CATALOG、または DATABASE レベルの権限である場合は `NULL` が返されます。 |
| OBJECT_TYPE     | オブジェクトのタイプ。                                       |
| PRIVILEGE_TYPE  | 権限のタイプ。同じオブジェクトに対する異なる権限はマージされ、1行で返されます。以下の例に示すように、`'user1'@'%'` は `default_catalog.db_test.view_test` に対して SELECT および DROP 権限を持っています。 |
| IS_GRANTABLE    | 被付与者が付与オプションを持っているかどうか。               |

例:

```Plain
MySQL > SELECT * FROM sys.grants_to_users LIMIT 5\G
*************************** 1. row ***************************
        GRANTEE: 'user1'@'%'
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: view_test
    OBJECT_TYPE: VIEW
 PRIVILEGE_TYPE: SELECT, DROP
   IS_GRANTABLE: NO
*************************** 2. row ***************************
        GRANTEE: 'user2'@'%'
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: simo
    OBJECT_NAME: view_test
    OBJECT_TYPE: VIEW
 PRIVILEGE_TYPE: SELECT
   IS_GRANTABLE: NO
```