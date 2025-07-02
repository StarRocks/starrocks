---
displayed_sidebar: docs
---

# SHOW GRANTS

## 功能

查看当前用户，指定用户，或指定角色的权限信息。

:::tip

每个用户都可以查看自己和自己所拥有角色的权限信息。只有拥有 `user_admin` 角色的用户才可以查看指定用户或角色的权限信息。

:::

## 语法

```SQL
SHOW GRANTS; -- 查看当前用户的权限信息。
SHOW GRANTS FOR ROLE <role_name>; -- 查看指定角色的权限信息。
SHOW GRANTS FOR <user_identity>; -- 查看指定用户的权限信息。
```

## 参数说明

- role_name：角色名
- user_identity：用户标识

返回字段说明：

```SQL
-- 用户的授权信息。
+--------------+--------+---------------------------------------------+
|UserIdentity  |Catalog | Grants                                      |
+--------------+--------+---------------------------------------------+

-- 角色的授权信息。
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
```

| **字段**     | **说明**                                                     |
| ------------ | ------------------------------------------------------------ |
| UserIdentity | 用户标识。查询用户授权信息时返回该字段。                     |
| RoleName     | 角色名。查询角色授权信息时返回该字段。                       |
| Catalog      | Catalog 名称。如果是 StarRocks 内部数据目录，则显示为 default。如果是外部数据目录 (external catalog)， 则显示 external catalog 的名称。如果 `Grants` 列显示的是授予角色的操作，则该字段为 NULL。 |
| Grants       | 具体的授权操作。                                             |

## 示例

```SQL
mysql> SHOW GRANTS;
+--------------+---------+----------------------------------------+
| UserIdentity | Catalog | Grants                                 |
+--------------+---------+----------------------------------------+
| 'root'@'%'   | NULL    | GRANT 'root', 'testrole' TO 'root'@'%' |
+--------------+---------+----------------------------------------+

mysql> SHOW GRANTS FOR 'user_g'@'%';
+-------------+-------------+-----------------------------------------------------------------------------------------------+
|UserIdentity |Catalog      |Grants                                                                                         |
+-------------+-------------------------------------------------------------------------------------------------------------+
|'user_g'@'%' |NULL         |GRANT role_g, public to `user_g`@`%`;                                                          | 
|'user_g'@'%' |NULL         |GRANT IMPERSONATE ON USER `user_a`@`%` TO USER `user_g`@`%`;                                | 
|'user_g'@'%' |default      |GRANT CREATE DATABASE ON CATALOG default_catalog TO USER `user_g`@`%`;                         |
|'user_g'@'%' |default      |GRANT ALTER, DROP, CREATE_TABLE ON DATABASE db1 TO USER `user_g`@`%`;                          |
|'user_g'@'%' |default      |GRANT CREATE_VIEW ON DATABASE db1 TO USER `user_g`@`%` WITH GRANT OPTION;                      |
|'user_g'@'%' |Hive_catalog |GRANT USAGE ON CATALOG Hive_catalog TO USER `user_g`@`%`                                       |
+-------------+--------------+-----------------------------------------------------------------------------------------------+

mysql> SHOW GRANTS FOR ROLE role_g;
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
|role_g       |NULL    | GRANT role_p, role_test TO ROLE role_g;               | 
+-------------+--------+--------------------------------------------------------+
```

## 相关文档

[GRANT](GRANT.md)
