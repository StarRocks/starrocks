# ALTER USER

## 功能

ALTER USER 命令用于更改用户信息。

## 语法

```SQL
-- 命令
ALTER USER user_identity [auth_option]

-- 参数说明
user_identity:'user_name'@'host'

auth_option: {
IDENTIFIED BY 'auth_string'
IDENTIFIED WITH auth_plugin
IDENTIFIED WITH auth_plugin BY 'auth_string'
IDENTIFIED WITH auth_plugin AS 'auth_string'
}
```

1. **user_identity**

    由两部分组成，`user_name` 和 `host`，其中 `user_name` 为用户名。`host` 标识用户端连接所在的主机地址。`host` 部分可以使用 `%` 进行模糊匹配。如果不指定 `host`，默认为 '%'，即表示该用户可以从任意 host 连接到 StarRocks。

2. **auth_option**

    指定用户的认证方式，目前支持 `mysql_native_password` 和 `authentication_ldap_simple`。

## 示例

1. 修改用户在 mysql 中的密码。

    ```sql
    ALTER USER 'jack' IDENTIFIED BY '123456';
    ```

    或者

    ```sql
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password BY '123456';
    ```

2. 为了避免传递明文，也可以使用下面的方式来修改密码。

    ```SQL
    ALTER USER 'jack' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    或者

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
    ```

    后面加密的内容可以通过 PASSWORD()获得到，例如：

    ```sql
    SELECT PASSWORD('123456');
    ```

3. 修改用户为 LDAP 认证。

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple;
    ```

4. 修改用户为 LDAP 认证，并指定用户在 LDAP 中的 DN (Distinguished Name)。

    ```SQL
    ALTER USER 'jack' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
    ```
