---
displayed_sidebar: docs
---

# grants_to_roles

You can view the privileges granted to user-defined roles by querying the view `grants_to_roles`.

:::note

By default, only users or roles with the `user_admin` role can access this view. You can grant other users the SELECT privilege on this view using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md).

:::

The following fields are provided in `grants_to_roles`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| GRANTEE         | The role to whom this privilege is granted. Only user-defined roles will be listed in this view, and system-defined ones will not. |
| OBJECT_CATALOG  | The catalog to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, or GLOBAL FUNCTION level privilege. |
| OBJECT_DATABASE | The database to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, GLOBAL FUNCTION, or CATALOG level privilege. |
| OBJECT_NAME     | The table to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, GLOBAL FUNCTION, CATALOG, or DATABASE level privilege. |
| OBJECT_TYPE     | The type of the object.                                      |
| PRIVILEGE_TYPE  | The type of the privilege. Different privileges on the same object will be merged and returned in a single row. As shown in the following example, `role_test` has the SELECT and ALTER privileges on `default_catalog.db_test.tbl1`. |
| IS_GRANTABLE    | Whether the grantee has the grant option.                    |

Example:

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
