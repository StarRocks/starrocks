# ALTER USER

## Description

Modifies user information, such as password, authentication method, or default roles.

> Individual users can use this command to modify information for themselves. `user_admin` can use this command to modify information for other users.

## Syntax

```SQL
ALTER USER user_identity [auth_option] [default_role]
```

## Parameters

- `user_identity` consists of two parts, "user_name" and "host", in the format of `username@'userhost'`.  For the "host" part, you can use `%` for fuzzy match. If "host" is not specified, "%" is used by default, meaning that the user can connect to StarRocks from any host.

- `auth_option` specifies the authentication method. Currently, three authentication methods are supported: StarRocks native password, mysql_native_password, and "authentication_ldap_simple". StarRocks native password is the same as mysql_native_password in logic but slightly differs in syntax. One user identity can use only one authentication method. You can use ALTER USER to modify users' passwords and authentication methods.

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

- `DEFAULT ROLE`

   ```SQL
    -- Set specified roles as default roles.
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- Set all roles of the user, including roles that will be assigned to this user, as default roles. 
    DEFAULT ROLE ALL
    -- No default role is set but the public role is still enabled after a user login. 
    DEFAULT ROLE NONE
    ```

  Before you run ALTER USER to set default roles, make sure that all the roles have been assigned to users. The roles are automatically activated after the user logs in again.

## Examples

Example 1: Change user's password to a plaintext password.

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

Example 2: Change user's password to a cyphertext password.

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> You can get the encrypted password using the password() function.

Example 3: Change the authentication method to LDAP.

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

Example 4: Change the authentication method to LDAP and specify the distinguished name (DN) of the user in LDAP.

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

Example 5: Change the default roles of the user to `db_admin` and `user_admin`. Note that the user must have been assigned these two roles.

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

Example 6: Set all roles of the user, including roles that will be assigned to this user as default roles.

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

Example 7: Clear all the default roles of the user.

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> Note: By default, the `public` role is still activated for the user.

## References

- [CREATE USER](CREATE%20USER.md)
- [GRANT](GRANT.md)
- [SHOW USERS](SHOW%20USERS.md)
- [DROP USER](DROP%20USER.md)
