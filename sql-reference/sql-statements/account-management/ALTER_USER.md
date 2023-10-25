# ALTER USER

## description

### Syntax

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
ALTER USER 命令用于更改用户信息

auth_option指定用户的认证方式，目前支持mysql_native_password和authentication_ldap_simple
```

## example

1. 修改用户在mysql中的密码

    ```sql
    ALTER USER 'jack' IDENTIFIED BY '123456';
    ```

    或者

    ```sql
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

2. 为了避免传递明文，也可以使用下面的方式来修改密码

    ```SQL
    ALTER USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    或者

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    后面加密的内容可以通过PASSWORD()获得到,例如：

    ```sql
    SELECT PASSWORD('123456');
    ```

3. 修改用户为ldap认证

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple
    ```

4. 修改用户为ldap认证，并指定用户在ldap中的DN(Distinguished Name)

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```

## keyword

ALTER, USER
