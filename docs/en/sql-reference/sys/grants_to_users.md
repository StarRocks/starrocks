---
displayed_sidebar: docs
---

# grants_to_users

You can view the privileges granted to users by querying the view `grants_to_users`.

:::note

By default, only users or roles with the `user_admin` role can access this view. You can grant other users the SELECT privilege on this view using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md).

:::

The following fields are provided in `grants_to_users`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| GRANTEE         | The user to whom this privilege is granted.                  |
| OBJECT_CATALOG  | The catalog to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, or GLOBAL FUNCTION level privilege. |
| OBJECT_DATABASE | The database to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, GLOBAL FUNCTION, or CATALOG level privilege. |
| OBJECT_NAME     | The table to which the object belongs. `NULL` is returned if the privilege is a SYSTEM, RESOURCE GROUP, RESOURCE, USER, GLOBAL FUNCTION, CATALOG, or DATABASE level privilege. |
| OBJECT_TYPE     | The type of the object.                                      |
| PRIVILEGE_TYPE  | The type of the privilege. Different privileges on the same object will be merged and returned in a single row. As shown in the following example, `'user1'@'%'` has the SELECT and DROP privileges on `default_catalog.db_test.view_test`. |
| IS_GRANTABLE    | Whether the grantee has the grant option.                    |

Example:

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

