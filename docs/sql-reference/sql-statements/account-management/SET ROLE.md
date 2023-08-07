# SET ROLE

## Description

Activates a role, along with all of its associated privileges, for the current session.

This command is supported from v3.0.

## Syntax

```SQL
-- Active specific roles.
SET ROLE <role_name>[,<role_name>,..];
-- Activate all roles of a user, except for specific roles.
SET ROLE ALL EXCEPT <role_name>[,<role_name>,..]; 
-- Activate all roles of a user.
SET ROLE ALL;
```

## Parameters

`role_name`: the role name

## Usage notes

Users can only activate roles that have been assigned to them.

You can query the roles of a user using [SHOW GRANTS](./SHOW%20GRANTS.md).

You can query the active roles of the current user using `SELECT CURRENT_ROLE()`. For more information, see [current_role](../../sql-functions/utility-functions/current_role.md).

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

- [CREATE ROLE](CREATE%20ROLE.md): creates a role.
- [GRANT](GRANT.md): assigns roles to users or other roles.
- [ALTER USER](ALTER%20USER.md): modifies roles.
- [SHOW ROLES](SHOW%20ROLES.md): show all roles in the system.
- [current_role](../../sql-functions/utility-functions/current_role.md): show roles of the current user.
- [DROP ROLE](DROP%20ROLE.md): drops a role.
