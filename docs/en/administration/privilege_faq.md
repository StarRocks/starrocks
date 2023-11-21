---
displayed_sidebar: "English"
---

# Privilege FAQ

## Why is the error message "no permission" still reported even after the required role has been assigned to a user?

This error may happen if the role is not activated. You can run `select current_role();` to query the roles that have been activated for the user in the current session. If the required role is not activated, run [SET ROLE](../sql-reference/sql-statements/account-management/SET_ROLE.md) to activate this role and perform operations using this role.

If you want roles to be automatically activated upon login, the `user_admin` role can run [SET DEFAULT ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) or [ALTER USER DEFAULT ROLE](../sql-reference/sql-statements/account-management/ALTER_USER.md) to set a default role for each user. After the default role is set, it will be automatically activated when the user logs in.

If you want all the assigned roles of all users to be automatically activated upon login, you can run the following command. This operation requires the OPERATE permission at the System level.

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

However, we recommend that you follow the principle of "least privilege" by setting default roles with limited privileges to prevent potential risks. For example:

- For common users, you can set the `read_only` role that has only the SELECT privilege as the default role, while avoiding setting roles with privileges like ALTER, DROP, and INSERT as default roles.
- For administrators, you can set the `db_admin` role as the default role, while avoiding setting the `node_admin` role, which has the privilege to add and drop nodes, as the default role.

This approach helps ensure that users are assigned roles with appropriate permissions, reducing the risk of unintended operations.

You can run [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to assign the required privileges or roles to users.

## I have granted a user the privilege on all tables in a database (`GRANT ALL ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;`), but the user still cannot create tables in the database. Why?

Creating tables within a database requires the database-level CREATE TABLE privilege. You need to grant the privilege to the user.

```SQL
GRANT CREATE TABLE ON DATABASE <db_name> TO USER <user_identity>;;
```

## I have granted a user all the privileges on a database using `GRANT ALL ON DATABASE <db_name> TO USER <user_identity>;`, but nothing is returned when the user runs `SHOW TABLES;` in this database. Why?

`SHOW TABLES;` returns only tables on which the user has any privilege. If the user has no privilege on a table, this table will not be returned. You can grant any privilege on all tables in this database (using SELECT for example) to the user:

```SQL
GRANT SELECT ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;
```

The statement above is equivalent to `GRANT select_priv ON db.* TO <user_identity>;` used in versions earlier than v3.0.
