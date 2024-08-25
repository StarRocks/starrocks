---
displayed_sidebar: docs
---

# grants_to_roles

您可以通过查询 `grants_to_roles` 视图查看用户自定义角色的权限。

:::note

默认仅拥有 `user_admin` 角色的用户或角色可以查询此视图，或通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 语句将此视图的权限赋予给其他用户。

:::

`grants_to_roles` 提供以下字段：

| **字段**        | **描述**                                                     |
| --------------- | ------------------------------------------------------------ |
| GRANTEE         | 被授予此权限的角色。只有用户自定义角色会出现在此视图中，系统预制角色不会出现在此视图中。 |
| OBJECT_CATALOG  | 权限对象所属的 Catalog。如果权限为 SYSTEM、RESOURCE GROUP、RESOURCE、USER 或 GLOBAL FUNCTION 级别权限，则返回 `NULL`。 |
| OBJECT_DATABASE | 权限对象所属的数据库。如果权限为 SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION 或 CATALOG 级别权限，则返回 `NULL`。 |
| OBJECT_NAME     | 权限对象所属的表。如果权限为 SYSTEM、RESOURCE GROUP、RESOURCE、USER、GLOBAL FUNCTION、CATALOG 或 DATABASE 级别权限，则返回 `NULL`。 |
| OBJECT_TYPE     | 权限对象的类型。                                             |
| PRIVILEGE_TYPE  | 权限的类型。同一个对象上的不同权限会进行合并展示。如示例中，`role_test` 同时拥有 `default_catalog.db_test.tbl1`的`SELECT`和`ALTER`权限，则会合并展示。 |
| IS_GRANTABLE    | 被授予角色是否拥有该权限的 GRANT 权限。                      |

示例：

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
