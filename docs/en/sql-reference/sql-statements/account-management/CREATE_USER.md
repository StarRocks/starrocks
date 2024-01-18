---
displayed_sidebar: "English"
---

# CREATE USER

## Description

### Syntax

```SQL
CREATE USER
user_identity [auth_option]
[DEFAULT ROLE 'role_name']

user_identity:
    'user_name'@'host'

auth_option: {
    IDENTIFIED BY 'auth_string'
    IDENTIFIED WITH auth_plugin
    IDENTIFIED WITH auth_plugin BY 'auth_string'
    IDENTIFIED WITH auth_plugin AS 'auth_string'
}
```

1. The command "CREATE USER" is used to create a StarRocks user. In StarRocks, a "user_identity" uniquely identifies a user.  
2. "user_identity" consists of two parts, "user_name" and "host". Here, "user_name" is the username and "host" identifies the host address where the client connects. The "host" part can use % for fuzzy matching. If no host is specified, the default is '%', meaning that the user can connect to StarRocks from any host.
3. "auth_option" specifies user authentication method. It currently supports "mysql_native_password" and "authentication_ldap_simple".

If a role is specified, it will automatically grant all permissions the role owns to this newly created user with the precondition that the role already exists. If a role is not specified, the user defaults to having no permission.  

## Examples

1. Create a passwordless user (without specifying host, it is equivalent to jack@'%')

    ```SQL
    CREATE USER 'jack';
    ```

2. Create a password user that allows login from '172.10.1.10'.

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY '123456';
    ```

    or

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

3. To avoid passing in plaintext, use case 2 can also be created in the following way

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    or

    ```SQL
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    The encrypted content could be obtained though PASSWORD(), for example:

    ```sql
    SELECT PASSWORD('123456');
    ```

4. Create a user authenticated by ldap

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple
    ```

5. Create a user authenticated by ldap, and specify user's DN (Distinguished Name) in ldap

    ```sql
    CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```

6. Create a user who is allowed to log in from '192.168' subnet and meanwhile specify its role as example_role

    ```sql
    CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
    ```

7. Create a user who is allowed to log in from the domain named 'example_domain'

    ```sql
    CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
    ```

8. Create a user and specify a role

    ```sql
    CREATE USER 'jack'@'%' IDENTIFIED BY '12345' DEFAULT ROLE 'my_role';
    ```
