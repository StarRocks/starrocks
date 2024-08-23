---
displayed_sidebar: docs
---

# System metadatabase

This topic describes how to view the exclusive metadata of your StarRocks cluster via system-defined views.

Each StarRocks cluster maintains a database `sys`, which contains several read-only, system-defined views. These metadata views provide a unified, easy-to-use interface that allows you to have an overall insight into the privilege structure, object dependencies, and other information within your StarRocks cluster.

## View metadata information via `sys`

You can view the exclusive metadata information within a StarRocks instance by querying the content of views in `sys`.

The following example checks the privileges granted to user-defined roles by querying the view `grants_to_roles`.

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

## Views in `sys`

`sys` contains the following metadata views:

| **View**             | **Description**                                                               |
| -------------------- | ----------------------------------------------------------------------------- |
| [grants_to_roles](../sys/grants_to_roles.md)         | Records the information of privileges that are granted to user-defined roles. |
| [grants_to_users](../sys/grants_to_users.md)         | Records the information of privileges that are granted to users.              |
| [role_edges](../sys/role_edges.md)                   | Records the grantees of roles.                                                |
| [object_dependencies](../sys/object_dependencies.md) | Records the dependency relationship of asynchronous materialized views.       |

:::note

As per their scenarios of applications, views in `sys` are only accessible to some `admin` roles by default. Depending on your specific needs, you can grant other users the SELECT privilege on these views.

:::

