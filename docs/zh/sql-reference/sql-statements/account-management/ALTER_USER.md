---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER USER

## 功能

更改 StarRocks 用户信息，例如用户密码，认证方式，默认角色，或用户属性（自 v3.3.3 起支持）。

:::tip

所有用户都可以修改自己的信息。只有拥有 `user_admin` 角色的用户才可以修改其他用户的信息。

:::

## 语法

```SQL
ALTER USER user_identity 
[auth_option] 
[default_role] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[SET PROPERTIES ("key"="value", ...)]
```

## 参数说明

- `user_identity`：用户标识。由登录IP（userhost） 和用户名（username）组成，写作：`username@'userhost'`。其中，`userhost` 的部分可以使用 `%` 来进行模糊匹配。如果不指定 `userhost`，默认为 `%`，即表示可以从任意 host 使用`username`连接到 StarRocks 的用户。但是，当您在使用 `SET PROPERTIES` 关键字修改用户属性时，必须指定 `username` 而非 `user_identity`。

- `auth_option`：用户的认证方式。目前，StarRocks 支持原生密码、mysql_native_password 和 LDAP 三种认证方式。其中，原生密码与 mysql_native_password 认证方式的内在逻辑相同，仅在具体设置语法上有轻微差别。同一个 user identity 只能使用一种认证方式。通过 ALTER 语句可以变更用户的认证方式和密码。

    ```SQL
      auth_option: {
          IDENTIFIED BY 'auth_string'
          IDENTIFIED WITH mysql_native_password BY 'auth_string'
          IDENTIFIED WITH mysql_native_password AS 'auth_string'
          IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
          
      }
      ```

      | **认证方式**                 | **创建用户时的密码** | **用户登录时的密码** |
      | ---------------------------- | -------------------- | -------------------- |
      | 原生密码                     | 明文或密文           | 明文                 |
      | `mysql_native_password BY`   | 明文                 | 明文                 |
      | `mysql_native_password WITH` | 密文                 | 明文                 |
      | `authentication_ldap_simple` | 明文                 | 明文                 |

    > 注：在所有认证方式中，StarRocks 均会加密存储用户的密码。

- `DEFAULT ROLE`：设置用户默认角色。

    ```SQL
    -- 将列举的角色设置为用户的默认激活角色。
    DEFAULT ROLE <role_name>[, <role_name>, ...]
    -- 将用户拥有的所有角色（包含未来赋予给用户的角色）设置为用户的默认激活角色。
    DEFAULT ROLE ALL
    -- 清空用户的默认角色。注意：仍然会为用户自动激活 public 角色。
    DEFAULT ROLE NONE
    ```

    通过 ALTER 命令更改用户默认角色前请确保对应角色已经赋予给用户。设置后，用户再次登录时会默认激活对应角色。

- `SET PROPERTIES`：设置用户属性，包括用户最大连接数（`max_user_connections`），Catalog，数据库，或用户级别的 Session 变量。用户级别的 Session 变量在用户登录时生效。该功能自 v3.3.3 起支持。

  ```SQL
  -- 设置用户最大连接数。
  SET PROPERTIES ("max_user_connections" = "<Integer>")
  -- 设置 Catalog。
  SET PROPERTIES ("catalog" = "<catalog_name>")
  -- 设置数据库。
  SET PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- 设置 Session 变量。
  SET PROPERTIES ("session.<variable_name>" = "<value>", ...)
  -- 清空用户所有属性设置。
  SET PROPERTIES ("catalog" = "", "database" = "", "session.<variable_name>" = "");
  ```

  :::tip
  - `SET PROPERTIES` 作用于用户本身而非用户标识。当您在 `ALTER USER` 语句中使用 `SET PROPERTIES` 关键字修改用户属性时，必须指定 `username` 而非 `user_identity`。
  - 全局变量和只读变量无法为单个用户设置。
  - 变量按照以下顺序生效：SET_VAR > Session > 用户属性 > Global。
  - 您可以通过 [SHOW PROPERTY](./SHOW_PROPERTY.md) 查看特定用户的属性。
  :::

## 示例

示例一：使用明文修改用户密码。

```SQL
ALTER USER 'jack' IDENTIFIED BY '123456';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

示例二：使用密文修改用户密码。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 其中，密文密码可以通过 PASSWORD() 函数获得。

示例三：修改用户为 LDAP 认证。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

示例四：修改用户为 LDAP 认证，并指定用户在 LDAP 中的 DN (Distinguished Name)。

```SQL
ALTER USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

示例五：修改用户默认激活角色为 `db_admin` 和 `user_admin`。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

> 注意：该用户需要已经拥有 `db_admin` 和 `user_admin` 角色。

示例六：修改用户默认激活角色为所有角色。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE ALL;
```

> 注意：未来赋予给用户的角色也会默认激活

示例七：修改用户默认激活角色为空。

```SQL
ALTER USER 'jack'@'192.168.%' DEFAULT ROLE NONE;
```

> 注意：用户还将默认激活 `public` 角色。

示例八：设置最大用户连接数为 `600`。

```SQL
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "600");
```

示例九：设置用户的 Catalog 为 `hive_catalog`。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = 'hive_catalog');
```

示例十：设置用户的数据库为 Default Catalog 中的 `test_db`。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

示例十一：设置用户的 Session 变量 `query_timeout` 为 `600`。

```SQL
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

示例十二：清空用户所有属性设置。

```SQL
ALTER USER 'jack' SET PROPERTIES ('catalog' = '', 'database' = '', 'session.query_timeout' = '');
```

## 相关文档

- [CREATE USER](CREATE_USER.md)
- [SHOW USERS](SHOW_USERS.md)
- [DROP USER](DROP_USER.md)
