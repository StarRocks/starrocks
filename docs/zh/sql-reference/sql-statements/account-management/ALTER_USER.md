---
displayed_sidebar: docs
---

# ALTER USER

ALTER USER 修改用户信息，包括密码、认证方式、默认角色和用户属性（从 v3.3.3 开始支持）。

:::tip
普通用户可以使用此命令修改自己的信息。只有拥有 `user_admin` 角色的用户才能修改其他用户的信息。
:::

## 语法

```SQL
ALTER USER user_identity 
[auth_option] 
[default_role] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[SET PROPERTIES ("key"="value", ...)]
```

## 参数

- `user_identity` 由两部分组成，"user_name" 和 "host"，格式为 `username@'userhost'`。对于 "host" 部分，可以使用 `%` 进行模糊匹配。如果未指定 "host"，则默认使用 "%" ，表示用户可以从任何主机连接到 StarRocks。

- `auth_option` 指定认证方式。目前支持三种认证方式：StarRocks native password、mysql_native_password 和 "authentication_ldap_simple"。StarRocks native password 在逻辑上与 mysql_native_password 相同，但在语法上略有不同。一个用户身份只能使用一种认证方式。可以使用 ALTER USER 修改用户的密码和认证方式。

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        
    }
    ```

    | **认证方式**                 | **用户创建时的密码**           | **登录时的密码**       |
    | ---------------------------- | ------------------------------ | ---------------------- |
    | Native password              | 明文或密文                     | 明文                   |
    | `mysql_native_password BY`   | 明文                           | 明文                   |
    | `mysql_native_password WITH` | 密文                           | 明文                   |
    | `authentication_ldap_simple` | 明文                           | 明文                   |

> 注意：StarRocks 在存储用户密码之前会对其进行加密。

- `DEFAULT ROLE` 设置用户的默认角色。

   ```SQL
    -- 将指定角色设置为默认角色。
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- 将用户的所有角色，包括将分配给该用户的角色，设置为默认角色。
    DEFAULT ROLE ALL
    -- 未设置默认角色，但在用户登录后，公共角色仍然启用。
    DEFAULT ROLE NONE
    ```

  在运行 ALTER USER 设置默认角色之前，请确保所有角色已分配给用户。用户再次登录后，角色会自动激活。

- `SET PROPERTIES` 设置用户属性，包括最大用户连接数 (`max_user_connections`)、catalog、数据库或用户级别的会话变量。用户级别的会话变量在用户登录时生效。此功能从 v3.3.3 开始支持。

  ```SQL
  -- 设置最大用户连接数。
  SET PROPERTIES ("max_user_connections" = "<Integer>")
  -- 设置 catalog。
  SET PROPERTIES ("catalog" = "<catalog_name>")
  -- 设置数据库。
  SET PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- 设置会话变量。
  SET PROPERTIES ("session.<variable_name>" = "<value>", ...)
  -- 清除为用户设置的属性。
  SET PROPERTIES ("catalog" = "", "database" = "", "session.<variable_name>" = "");
  ```

  :::tip
  - 全局变量和只读变量不能为特定用户设置。
  - 变量生效的顺序为：SET_VAR > 会话 > 用户属性 > 全局。
  - 可以使用 [SHOW PROPERTY](./SHOW_PROPERTY.md) 查看特定用户的属性。
  :::

## 示例

示例 1：将用户的密码更改为明文密码。

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

示例 2：将用户的密码更改为密文密码。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 可以使用 password() 函数获取加密后的密码。

示例 3：将认证方式更改为 LDAP。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

示例 4：将认证方式更改为 LDAP 并指定用户在 LDAP 中的专有名称 (DN)。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

示例 5：将用户的默认角色更改为 `db_admin` 和 `user_admin`。注意，用户必须已被分配这两个角色。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

示例 6：将用户的所有角色，包括将分配给该用户的角色，设置为默认角色。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

示例 7：清除用户的所有默认角色。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> 注意：默认情况下，`public` 角色仍然为用户激活。

示例 8：将最大用户连接数设置为 `600`。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ("max_user_connections" = "600");
```
> 注意：如果要修改匹配默认主机 (%) 的用户身份的属性，或者明确的主机不严格要求用于消除歧义，也可以仅指定用户名（例如，`ALTER USER 'jack' SET PROPERTIES...`）。

示例 9：将用户的 catalog 设置为 `hive_catalog`。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = 'hive_catalog');
```

示例 10：将用户的数据库设置为默认 catalog 中的 `test_db`。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

示例 11：为用户设置会话变量 `query_timeout` 为 `600`。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('session.query_timeout' = '600');
```

示例 12：清除为用户设置的属性。

```SQL
ALTER USER 'jack'@'192.168.%' SET PROPERTIES ('catalog' = '', 'database' = '', 'session.query_timeout' = '');
```

## 参考

- [CREATE USER](CREATE_USER.md)
- [GRANT](GRANT.md)
- [SHOW USERS](SHOW_USERS.md)
- [DROP USER](DROP_USER.md)