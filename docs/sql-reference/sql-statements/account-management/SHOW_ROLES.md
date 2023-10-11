# SHOW ROLES

## Description

Displays all roles in the system. You can use `SHOW GRANTS FOR ROLE <role_name>;` to view the privileges of a specific role. For more information, see [SHOW GRANTS](SHOW%20GRANTS.md). This command is supported from v3.0.


> Note: Only the `user_admin` role can execute this statement.

## Syntax

```SQL
SHOW ROLES
```

Return fields:

| **Field** | **Description**       |
| --------- | --------------------- |
| Name      | The name of the role. |

## Examples

Display all roles in the system.

```SQL
mysql> SHOW ROLES;
+---------------+
| Name          |
+---------------+
| root          |
| db_admin      |
| cluster_admin |
| user_admin    |
| public        |
| testrole      |
+---------------+
```

## References

- [CREATE ROLE](CREATE%20ROLE.md)
- [ALTER USER](ALTER%20USER.md)
- [DROP ROLE](DROP%20ROLE.md)
