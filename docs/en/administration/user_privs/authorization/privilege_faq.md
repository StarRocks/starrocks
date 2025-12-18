---
displayed_sidebar: docs
sidebar_position: 50
---

# Privilege FAQ

## Why is the error message "no permission" still reported even after the required role has been assigned to a user?

This error may happen if the role is not activated. You can run `select current_role();` to query the roles that have been activated for the user in the current session. If the required role is not activated, run [SET ROLE](../../../sql-reference/sql-statements/account-management/SET_ROLE.md) to activate this role and perform operations using this role.

If you want roles to be automatically activated upon login, the `user_admin` role can run [SET DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) or [ALTER USER DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) to set a default role for each user. After the default role is set, it will be automatically activated when the user logs in.

If you want all the assigned roles of all users to be automatically activated upon login, you can run the following command. This operation requires the OPERATE permission at the System level.

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

However, we recommend that you follow the principle of "least privilege" by setting default roles with limited privileges to prevent potential risks. For example:

- Common users can set the `read_only` role that has only the SELECT privilege as the default role, while avoiding setting roles with privileges like ALTER, DROP, and INSERT as default roles.
- Administrators can set the `db_admin` role as the default role, while avoiding setting the `node_admin` role, which has the privilege to add and drop nodes, as the default role.

This approach helps ensure that users are assigned roles with appropriate permissions, reducing the risk of unintended operations.

You can run [GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) to assign the required privileges or roles to users.

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

## What privileges are required to access the StarRocks Web Console `http://<fe_ip>:<fe_http_port>`?

The user must have the `cluster_admin` role.

## How did the privilege retention mechanism change before and after StarRocks v3.0?

Before v3.0, after a user is granted privileges on a table, the privileges would still remain even if the table was dropped and recreated. Starting from v3.0, privileges will no longer be retained after a table is dropped and recreated.

## How to query users and granted privileges in StarRocks?

You can obtain the full user list via querying the system view `sys.grants_to_users` or executing SHOW USERS, and then query each user individually using `SHOW GRANTS FOR <user_identity>`.

## What is the impact on FE resources when querying system views on privilege metadata of large numbers of users and tables?

When the number of users or tables is very large, queries on system views `sys.grants_to_users`, `sys.grants_to_roles`, and `sys.role_edges` may take a long time. These views are computed in real time, consuming a proportion of FE resources. Therefore, it is not recommended to run such operations frequently at large scale.

## Will recreating a catalog cause permission loss? How should privileges be backed up and restored?

Yes. Recreating a catalog will cause its relative privileges to be lost. You should back up all user privileges first and restore them after the catalog is recreated.

## Is there a tool that supports automatic permission migration?

Not at the moment. Users must manually back up and restore permissions using SHOW GRANTS for each user.

## Are there restrictions on using the KILL command? Can it be restricted to only killing a user’s own queries?

Yes. The KILL command now requires the OPERATE privilege, and a user can only kill queries that were initiated by themselves.

## Why do the granted privileges change after renaming or deleting a table? Can the system retain old permissions while adding permissions for the renamed table?

For native tables, privileges are tied to the table ID, not the table name. This ensures data security, as table names can change arbitrarily. If privileges were to follow table names, it could cause data leakage. Similarly, when a table is dropped, its permissions are removed because the object no longer exists.

For external tables, historical versions behaved the same as internal tables. However, because external table metadata is not managed by StarRocks, delays or privilege loss can occur. To address this, future versions will use table-name–based permission management for external tables, which aligns with the expected behavior.

## How to back up user privileges?

Below is a sample script for backing up the user privilege information in the cluster.

```Bash
#!/bin/bash

# MySQL connection info
HOST=""
PORT="9030"
USER="root"
PASSWORD=""  
OUTPUT_FILE="user_privileges.txt"

# Clear output file
> $OUTPUT_FILE

# Get user list
users=$(mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW USERS;" | sed -e '1d' -e '/^+/d')

# Loop through users and get privileges
for user in $users; do
    echo "Privileges for $user:" >> $OUTPUT_FILE
    mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW GRANTS FOR $user;" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
done

echo "All user privileges have been written to $OUTPUT_FILE"
```

## Why does granting USAGE on a normal function produce the error “Unexpected input 'IN', the most similar input is {'TO'}.”? What is the correct way to grant permissions on functions?

Normal functions cannot be granted using IN ALL DATABASES; they can only be granted within the current database. While global functions are granted on the ALL DATABASES scale.
