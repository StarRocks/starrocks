---
displayed_sidebar: "English"
---

# SHOW GRANTS

## Description

Displays all the privileges that have been granted to a user or role.

For more information about roles and privileges, see [Overview of privileges](../../../administration/user_auth/privilege_overview.md).

> NOTE: All the roles and users can view the privileges granted to them or the roles assigned to them. However, only the `user_admin` role can view the privileges of a specific user or role.

## Syntax

```SQL
SHOW GRANTS; -- View the privileges of the current user.
SHOW GRANTS FOR ROLE <role_name>; -- View the privileges of a specific role.
SHOW GRANTS FOR <user_identity>; -- View the privileges of a specific user.
```

## Parameters

- role_name
- user_identity

Return fields:

```SQL
-- View the privileges of a specific user.
+--------------+--------+---------------------------------------------+
|UserIdentity  |Catalog | Grants                                      |
+--------------+--------+---------------------------------------------+

-- View the privileges of a specific role.
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
```

| **Field**    | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| UserIdentity | The user identity, which is displayed when you query the privileges of a user. |
| RoleName     | The role name, which is displayed when you query the privileges of a role. |
| Catalog      | The catalog name.<br />`default` is returned if the GRANT operation is performed on the StarRocks internal catalog.<br />The name of the external catalog is returned if the GRANT operation is performed on an external catalog.<br />`NULL` is returned if the operation shown in the `Grants` column is assigning roles. |
| Grants       | The specific GRANT operation.                                |

## Examples

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
|'user_g'@'%' |NULL         |GRANT IMPERSONATE ON `user_a`@`%`, `user_b`@`%`TO `user_g`@`%`;                                |    
|'user_g'@'%' |default      |GRANT CREATE_DATABASE ON CATALOG default_catalog TO USER `user_g`@`%`;                         | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, CREATE_TABLE ON DATABASE db1 TO USER `user_g`@`%`;                          | 
|'user_g'@'%' |default      |GRANT CREATE_VIEW ON DATABASE db1 TO USER `user_g`@`%` WITH GRANT OPTION;                      | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, SELECT, INGEST, EXPORT, DELETE, UPDATE ON TABLE db.* TO USER `user_g`@`%`;  | 
|'user_g'@'%' |default      |GRANT ALTER, DROP, SELECT ON VIEW db2.view TO USER `user_g`@`%`;                               | 
|'user_g'@'%' |Hive_catalog |GRANT USAGE ON CATALOG Hive_catalog TO USER `user_g`@`%`                                       |
+-------------+--------------+-----------------------------------------------------------------------------------------------+

mysql> SHOW GRANTS FOR ROLE role_g;
+-------------+--------+-------------------------------------------------------+
|RoleName     |Catalog | Grants                                                |
+-------------+-----------------+----------------------------------------------+
|role_g       |NULL    | GRANT role_p, role_test TO ROLE role_g;               | 
|role_g       |default | GRANT SELECT ON *.* TO ROLE role_g WITH GRANT OPTION; | 
+-------------+--------+--------------------------------------------------------+
```

## References

[GRANT](GRANT.md)
