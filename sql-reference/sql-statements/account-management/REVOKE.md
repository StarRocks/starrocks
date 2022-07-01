# REVOKE

## Description

You can use the REVOKE statement to revoke specific privileges from a user or a role. You can also use this statement to revoke a role from a user.

## Syntax

- Revoke specific privileges on a database and a table from a user or a role. The role from which you want to revoke privileges must already exist.

    ```SQL
    REVOKE privilege_list ON db_name[.tbl_name] FROM {user_identity | ROLE 'role_name'};
    ```

- Revoke specific privileges on a resource from a user or a role. The role from which you want to revoke privileges must already exist.

    ```SQL
    REVOKE privilege_list ON RESOURCE 'resource_name' FROM {user_identity | ROLE 'role_name'};
    ```

- Revoke the privilege that allows user `a` to perform operations as user `b`.

    ```SQL
    REVOKE IMPERSONATE ON user_identity_b FROM user_identity_a;
    ```

- Revoke a role from a user. The role must already exist.

    ```SQL
    REVOKE 'role_name' FROM user_identity;
    ```

## Parameters

`user_identity`: This parameter contains two parts: `user_name` and `host`. `user_name` indicates the user name. `host` indicates the IP address of the user. You can leave `host` unspecified or you can specify a domain for `host`. If you leave `host` unspecified, `host` defaults to `%`, which means you can access StarRocks from any host. If you specify a domain for `host`, it may take one minute for the privilege to take effect. The `user_identity` parameter must be created by the [CREATE USER](../account-management/CREATE%20USER.md) statement.

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

Example 4: Revoke the privilege that allows user `jack` to perform operations as user `rose`.

```SQL
REVOKE IMPERSONATE ON 'rose'@'%' FROM 'jack'@'%';
```
