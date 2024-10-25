---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## Description

Creates a StarRocks user. In StarRocks, a "user_identity" uniquely identifies a user. From v3.3.3, StarRocks supports setting user properties when creating a user.

<UserManagementPriv />

### Syntax

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## Parameters

- `user_identity` consists of two parts, "user_name" and "host", in the format of `username@'userhost'`.  For the "host" part, you can use `%` for fuzzy match. If "host" is not specified, "%" is used by default, meaning that the user can connect to StarRocks from any host.

  For the naming conventions of usernames, see [System limits](../../System_limit.md).

- `auth_option` specifies the authentication method. Currently, three authentication methods are supported: StarRocks native password, mysql_native_password, and "authentication_ldap_simple". StarRocks native password is the same as mysql_native_password in logic but slightly differs in syntax. One user identity can use only one authentication method.

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        
    }
    ```

    | **Authentication method**    | **Password for user creation** | **Password for login** |
    | ---------------------------- | ------------------------------ | ---------------------- |
    | Native password              | Plaintext or ciphertext        | Plaintext              |
    | `mysql_native_password BY`   | Plaintext                      | Plaintext              |
    | `mysql_native_password WITH` | Ciphertext                     | Plaintext              |
    | `authentication_ldap_simple` | Plaintext                      | Plaintext              |

> Note: StarRocks encrypts users' passwords before storing them.

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`: If this parameter is specified, the roles are automatically assigned to the user and activated by default when the user logs in. If not specified, this user does not have any privileges. Make sure that all the roles that are specified already exist.

- `PROPERTIES` sets user properties, including the maximum user connection number (`max_user_connections`), catalog, database or session variables on the user level. User-level session variables take effect as the user logs in. This feature is supported from v3.3.3.

  ```SQL
  -- Set the maximum user connection number.
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- Set the catalog.
  PROPERTIES ("catalog" = "<catalog_name>")
  -- Set the database.
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- Set session variables.
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip
  - `PROPERTIES` works on user instead of user identity.
  - Global variables and read-only variables cannot be set for a specific user.
  - Variables take effect in the following order: SET_VAR > Session > User property > Global.
  - You can use [SHOW PROPERTY](./SHOW_PROPERTY.md) to view the properties of a specific user.
  :::

## Examples

Example 1: Create a user using a plaintext password with no host specified, which is equivalent to `jack@'%'`.

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

Example 2: Create a user using a plaintext password and allow the user to log in from  `'172.10.1.10'`.

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

Example 3: Create a user with a ciphertext password and allow the user to log in from  `'172.10.1.10'`.

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> Note: You can get the encrypted password using the password() function.

Example 4: Create a user who is allowed to log in from a domain name 'example_domain'.

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

Example 5: Create a user that uses LDAP authentication.

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

Example 6: Create a user that uses LDAP authentication and specify the distinguished name (DN) of the user in LDAP.

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

Example 7: Create a user who is allowed to log in from the '192.168' subnet and set `db_admin` and `user_admin` as the default roles for the user.

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

Example 8: Create a user and set its maximum user connection number to `600`.

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

Example 9: Create a user and set the catalog of the user to `hive_catalog`.

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

Example 10: Create a user and set the database of the user to `test_db` in the default catalog.

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

Example 11: Create a user and set the session variable `query_timeout` to `600` for the user.

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```
