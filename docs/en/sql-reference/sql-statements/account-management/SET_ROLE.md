---
displayed_sidebar: "English"
---

# SET ROLE

## Description

Activates roles, along with all of its associated privileges and nested roles, for the current session. After the role is activated, users can use this role to perform operations.

 After running this command, you can run `select is_role_in_session("<role_name>");` to verify whether this role is active in the current session.

This command is supported from v3.0.

## Usage notes

Users can only activate roles that have been assigned to them.

You can query the roles of a user using [SHOW GRANTS](./SHOW_GRANTS.md).

You can query the active roles of the current user using `SELECT CURRENT_ROLE()`. For more information, see [current_role](../../sql-functions/utility-functions/current_role.md).

## Syntax

```SQL
-- Active specific roles and perform operations as this role.
SET ROLE <role_name>[,<role_name>,..];
-- Activate all roles of a user, except for specific roles.
SET ROLE ALL EXCEPT <role_name>[,<role_name>,..]; 
-- Activate all roles of a user.
SET ROLE ALL;
```

## Parameters

`role_name`: the role name

## Examples

Query all the roles of the current user.

```SQL
SHOW GRANTS;
+--------------+---------+----------------------------------------------+
| UserIdentity | Catalog | Grants                                       |
+--------------+---------+----------------------------------------------+
| 'test'@'%'   | NULL    | GRANT 'db_admin', 'user_admin' TO 'test'@'%' |
+--------------+---------+----------------------------------------------+
```

Activate the `db_admin` role.

```SQL
SET ROLE db_admin;
```

 Query active roles of the current user.

```SQL
SELECT CURRENT_ROLE();
+--------------------+
| CURRENT_ROLE()     |
+--------------------+
| db_admin           |
+--------------------+
```

## References

- [CREATE ROLE](CREATE_ROLE.md): creates a role.
- [GRANT](GRANT.md): assigns roles to users or other roles.
- [ALTER USER](ALTER_USER.md): modifies roles.
- [SHOW ROLES](SHOW_ROLES.md): show all roles in the system.
- [current_role](../../sql-functions/utility-functions/current_role.md): show roles of the current user.
- [is_role_in_session](../../sql-functions/utility-functions/is_role_in_session.md): verifies whether a role (or a nested role) is active in the current session.
- [DROP ROLE](DROP_ROLE.md): drops a role.
