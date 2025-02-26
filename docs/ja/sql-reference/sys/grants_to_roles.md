---
displayed_sidebar: docs
---

# grants_to_roles

`grants_to_roles` ビューをクエリすることで、ユーザー定義のロールに付与された権限を確認できます。

:::note

デフォルトでは、`user_admin` ロールを持つユーザーまたはロールのみがこのビューにアクセスできます。他のユーザーにこのビューの SELECT 権限を付与するには、[GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) を使用してください。

:::

`grants_to_roles` には次のフィールドが提供されています:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| GRANTEE         | この権限が付与されているロール。ユーザー定義のロールのみがこのビューに表示され、システム定義のものは表示されません。 |
| OBJECT_CATALOG  | オブジェクトが所属する catalog。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、または GLOBAL FUNCTION レベルの権限の場合は `NULL` が返されます。 |
| OBJECT_DATABASE | オブジェクトが所属するデータベース。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION、または CATALOG レベルの権限の場合は `NULL` が返されます。 |
| OBJECT_NAME     | オブジェクトが所属するテーブル。権限が SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION、CATALOG、または DATABASE レベルの権限の場合は `NULL` が返されます。 |
| OBJECT_TYPE     | オブジェクトのタイプ。                                      |
| PRIVILEGE_TYPE  | 権限のタイプ。同じオブジェクトに対する異なる権限はマージされ、1行で返されます。以下の例のように、`role_test` は `default_catalog.db_test.tbl1` に対して SELECT および ALTER 権限を持っています。 |
| IS_GRANTABLE    | 被付与者が付与オプションを持っているかどうか。                    |

例:

```Plain
MySQL > SELECT * FROM sys.grants_to_roles LIMIT 5\G
*************************** 1. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: tbl1
    OBJECT_TYPE: TABLE
 PRIVILEGE_TYPE: SELECT, ALTER
   IS_GRANTABLE: NO
*************************** 2. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: tbl2
    OBJECT_TYPE: TABLE
 PRIVILEGE_TYPE: SELECT
   IS_GRANTABLE: YES
*************************** 3. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: default_catalog
OBJECT_DATABASE: db_test
    OBJECT_NAME: mv_test
    OBJECT_TYPE: MATERIALIZED VIEW
 PRIVILEGE_TYPE: SELECT
   IS_GRANTABLE: YES
*************************** 4. row ***************************
        GRANTEE: role_test
 OBJECT_CATALOG: NULL
OBJECT_DATABASE: NULL
    OBJECT_NAME: NULL
    OBJECT_TYPE: SYSTEM
 PRIVILEGE_TYPE: CREATE RESOURCE GROUP
   IS_GRANTABLE: NO
```