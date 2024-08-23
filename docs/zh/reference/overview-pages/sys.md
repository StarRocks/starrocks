---
displayed_sidebar: docs
---

# 系统元数据库

本文介绍如何通过系统定义的视图查看 StarRocks 集群中独有的的元数据。

每个 StarRocks 集群维护一个数据库 `sys`，其中包含数张由系统定义的视图。这些视图提供了一个统一且易于使用的接口，使您可以全面了解 StarRocks 集群中的权限结构和对象依赖关系等信息。

## 通过 `sys` 查看元数据信息

您可以通过查询 `sys` 中的视图来查看 StarRocks 实例中的元数据信息。

以下示例通过查询 `grants_to_roles` 视图查看用户自定义角色的权限。

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

## `sys` 中的视图

`sys` 中包含以下视图：

| **视图名**               | **描述**                                                       |
| ----------------------- | ------------------------------------------------------------- |
| [grants_to_roles](../sys/grants_to_roles.md)     | 记录了授予用户自定义角色的权限信息。       |
| [grants_to_users](../sys/grants_to_users.md)     | 记录了授予用户的权限信息。               |
| [role_edges](../sys/role_edges.md)          | 记录了角色的授予关系。                        |
| [object_dependencies](../sys/object_dependencies.md) | 记录了异步物化视图的依赖关系。        |

:::note

`sys` 库中的视图根据使用场景，默认只有一些管理员角色拥有查询权限。您也可以根据实际情况使用对应管理员角色将系统视图的查询权限赋予给其他用户。

:::
