---
displayed_sidebar: docs
---

# ALTER USER

## 説明

ユーザー情報を変更します。

## 構文

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
このコマンドはユーザー情報を変更するために使用されます。
"auth_option" は認証方法を指定します。現在は "mysql_native_password" と "authentication_ldap_simple" をサポートしています。
```

## 例

1. mysql でユーザーのパスワードを変更します。

    ```sql
    ALTER USER 'jack' IDENTIFIED BY '123456';
    ```

    または

    ```sql
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

2. パスワードを平文で送信しないようにするため、次の方法でもパスワードを変更できます。

    ```SQL
    ALTER USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    または

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    暗号化された内容は "PASSWORD()" で取得できます。例えば：

    ```sql
    SELECT PASSWORD('123456');
    ```

3. ユーザーの認証方法を "ldap" に変更します。

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple
    ```

4. ユーザーの認証方法を "ldap" に変更し、ldap でユーザーの DN (Distinguished Name) を指定します。

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com'
    ```