# GRANT

## Description

You can use the GRANT statement to grant specific privileges to a user or a role.

## Syntax

- Grant specific privileges on a database and a table to a user or a role. If the role that is granted these privileges does not exist, the system automatically creates the role when you execute this statement.

    ```SQL
    GRANT privilege_list ON db_name[.tbl_name] TO {user_identity | ROLE 'role_name'}
    ```

- Grant specific privileges on a resource to a user or a role. If the role that is granted these privileges does not exist, the system automatically creates the role when you execute this statement.

```SQL
GRANT privilege_list ON RESOURCE 'resource_name' TO {user_identity | ROLE 'role_name'};
```

## Parameters

### privilege_list

The privileges that can be granted to a user or a role. If you want to grant multiple privileges at a time, separate the privileges with commas (`,`). The following privileges are supported:

- `NODE_PRIV`: the privilege to manage cluster nodes such as enabling nodes and disabling nodes.
- `ADMIN_PRIV`: all privileges except `NODE_PRIV`.
- `GRANT_PRIV`: the privilege of performing operations such as creating users and roles, deleting users and roles, granting privileges, revoking privileges, and setting passwords for accounts.
- `SELECT_PRIV`: the read privilege on databases and tables.
- `LOAD_PRIV`: the privilege to load data into databases and tables.
- `ALTER_PRIV`: the privilege to change schemas of databases and tables.
- `CREATE_PRIV`: the privilege to create databases and tables.
- `DROP_PRIV`: the privilege to delete databases and tables.
- `USAGE_PRIV`: the privilege to use resources.

The preceding privileges can be classified into the following three categories:

- Node privilege: `NODE_PRIV`
- Database and table privilege: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, and `DROP_PRIV`
- Resource privilege: `USAGE_PRIV`

### db_name[.tbl_name]

The database and table. This parameter supports the following three formats:

- `*.*`: indicates all databases and tables. If this format is specified, the global privilege is granted.
- `db.*`: indicates a specific database and all tables in this database.
- `db.tbl`: indicates a specific table in a specific database.

> Note: When you use the `db.*` or `db.tbl` format, you can specify a database or a table that does not exist.

### resource_name

 The resource name. This parameter supports the following two formats:

- `*`: indicates all the resources.
- `resource`: indicates a specific resource.

> Note: When you use the `resource` format, you can specify a resource that does not exist.

### user_identity

This parameter contains two parts: `user_name` and `host`. `user_name` indicates the user name. `host` indicates the IP address of the user. You can leave `host` unspecified or you can specify a domain for `host`. If you leave `host` unspecified, `host` defaults to `%`, which means you can access StarRocks from any host. If you specify a domain for `host`, it may take one minute for the privilege to take effect. The `user_identity` parameter must be created by the [CREATE USER](../account-management/CREATE_USER.md) statement.

### role_name

The role name.

## Examples

Example 1: Grant the read privilege on all databases and tables to user `jack`.

```SQL
GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
```

Example 2: Grant the data loading privilege on `db1` and all tables in this database to `my_role`.

```SQL
GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
```

Example 3: Grant the read privilege, schema change privilege, and data loading privilege on `db1` and `tbl1` to user `jack`.

```SQL
GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
```

Example 4: Grant the privilege to use all the resources to user `jack`.

```SQL
GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
```

Example 5: Grant the privilege to use `spark_resource` to user `jack`.

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

Example 6: Grant the privilege to use `spark_resource` to the `my_role`.

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```
