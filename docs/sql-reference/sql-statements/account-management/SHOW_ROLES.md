# SHOW ROLES

## Description

<<<<<<< HEAD:docs/sql-reference/sql-statements/account-management/SHOW ROLES.md
This statement is used to show all the information about the created roles, containing roles' names, users included and their permissions.
=======
Displays all roles in the system. You can use `SHOW GRANTS FOR ROLE <role_name>;` to view the privileges of a specific role. For more information, see [SHOW GRANTS](SHOW_GRANTS.md). This command is supported from v3.0.
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525)):docs/sql-reference/sql-statements/account-management/SHOW_ROLES.md

Syntax:

```sql
SHOW ROLES
```

## Examples

View the created roles.

```sql
SHOW ROLES;
```
<<<<<<< HEAD:docs/sql-reference/sql-statements/account-management/SHOW ROLES.md
=======

## References

- [CREATE ROLE](CREATE_ROLE.md)
- [ALTER USER](ALTER_USER.md)
- [DROP ROLE](DROP_ROLE.md)
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525)):docs/sql-reference/sql-statements/account-management/SHOW_ROLES.md
