# REVOKE

## Description

You can use the REVOKE statement to perform the following operations:

- Revoke specific privileges from a user or a role.
- Revoke the privilege that allows a user to impersonate another user to perform operations. This feature is supported only in StarRock 2.4 and later versions.
- Revoke a role from a user. This feature is supported only in StarRock 2.4 and later versions.

## Syntax

- Revoke specific privileges on a database and a table from a user or a role. The role from which you want to revoke privileges must already exist.

    ```SQL
    REVOKE privilege_list ON db_name[.tbl_name] FROM {user_identity | ROLE 'role_name'}
    ```

- Revoke specific privileges on a resource from a user or a role. The role from which you want to revoke privileges must already exist.

    ```SQL
    REVOKE privilege_list ON RESOURCE 'resource_name' FROM {user_identity | ROLE 'role_name'};
    ```

- Revoke the privilege that allows user `a` to impersonate user `b` to perform operations.

    ```SQL
    REVOKE IMPERSONATE ON user_identity_b FROM user_identity_a;
    ```

- Revoke a role from a user. The role must already exist.

    ```SQL
    REVOKE 'role_name' FROM user_identity;
    ```

## Parameters

### privilege_list

The privileges that can be revoked from a user or a role. If you want to revoke multiple privileges at a time, separate the privileges with commas (`,`). You can revoke the following privileges:

- `NODE_PRIV`: the privilege to manage cluster nodes such as enabling nodes and disabling nodes. This privilege can only be granted to the root user.
- `ADMIN_PRIV`: all privileges except `NODE_PRIV`.
- `GRANT_PRIV`: the privilege of performing operations such as creating users and roles, deleting users and roles, granting privileges, revoking privileges, and setting passwords for accounts.
- `SELECT_PRIV`: the read privilege on databases and tables.
- `LOAD_PRIV`: the privilege to load data into databases and tables.
- `ALTER_PRIV`: the privilege to change schemas of databases and tables.
- `CREATE_PRIV`: the privilege to create databases and tables.
- `DROP_PRIV`: the privilege to delete databases and tables.
- `USAGE_PRIV`: the privilege to use resources.

### db_name[.tbl_name]

The database and table. This parameter supports the following three formats:

- `*.*`: indicates all databases and tables.
- `db.*`: indicates a specific database and all tables in this database.
- `db.tbl`: indicates a specific table in a specific database.

> Note: When you use the `db.*` or `db.tbl` format, you can specify a database or a table that does not exist.

### resource_name

The resource name. This parameter supports the following two formats:

- `*`: indicates all the resources.
- `resource`: indicates a specific resource.

> Note: When you use the `resource` format, you can specify a resource that does not exist.

### user_identity

This parameter contains two parts: `user_name` and `host`. `user_name` indicates the user name. `host` indicates the IP address of the user. You can leave `host` unspecified or you can specify a domain for `host`. If you leave `host` unspecified, `host` defaults to `%`, which means you can access StarRocks from any host. If you specify a domain for `host`, it may take one minute for the privilege to take effect. The `user_identity` parameter must be created by the [CREATE USER](../account-management/CREATE%20USER.md) statement.

## Examples

Example 1: Revoke the read privilege on `testDb` and all tables in this database from user `jack`.

```SQL
REVOKE SELECT_PRIV ON db1.* FROM 'jack'@'192.%';
```

Example 2: Revoke the privilege to use spark_resource from user `jack`.

```SQL
REVOKE USAGE_PRIV ON RESOURCE 'spark_resource' FROM 'jack'@'192.%';
```

Example 3: Revoke `my_role` from user `jack`.

```SQL
REVOKE 'my_role' FROM 'jack'@'%';
```

Example 4: Revoke the privilege that allows user `jack` to impersonate `rose` to perform operations.

```SQL
REVOKE IMPERSONATE ON 'rose'@'%' FROM 'jack'@'%';
```
