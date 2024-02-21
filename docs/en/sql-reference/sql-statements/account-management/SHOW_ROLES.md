---
displayed_sidebar: "English"
---

# SHOW ROLES

import UserManagementPriv from '../../../assets/commonMarkdown/userManagementPriv.md'

## Description

Displays all roles in the system. You can use `SHOW GRANTS FOR ROLE <role_name>;` to view the privileges of a specific role. For more information, see [SHOW GRANTS](SHOW_GRANTS.md).

This command is supported from v3.0.

<UserManagementPriv />

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

- [CREATE ROLE](CREATE_ROLE.md)
- [ALTER USER](ALTER_USER.md)
- [DROP ROLE](DROP_ROLE.md)
