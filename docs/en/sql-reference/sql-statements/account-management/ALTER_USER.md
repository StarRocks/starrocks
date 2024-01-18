---
displayed_sidebar: "English"
---

# ALTER USER

## Description

Changes user information.

## Syntax

```SQL
ALTER USER
user_identity [auth_option]

user_identity:
'user_name'@'host'

auth_option: {
IDENTIFIED BY 'auth_string'
IDENTIFIED WITH auth_plugin
IDENTIFIED  WITH auth_plugin BY 'auth_string'
IDENTIFIED WITH auth_plugin AS 'auth_string'
}
```

```plain text
The command is used to change user information. 
 "auth_option" specifies authentication method. It currently supports "mysql_native_password" and "authentication_ldap_simple".
```

## Examples

1. Change user's password in mysql.

    ```sql
    ALTER USER 'jack' IDENTIFIED BY '123456';
    ```

    or

    ```sql
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

2. To avoid transmitting password in plaintext, the following method can also be used to change password.

    ```SQL
    ALTER USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    or

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    The encrypted content can be obtained by "PASSWORD()", for example:

    ```sql
    SELECT PASSWORD('123456');
    ```

3. Alter user authentication method as "ldap".

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple
    ```

4. Alter user authentication method as "ldap", and specify user's DN (Distinguished Name) in ldap.

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```
