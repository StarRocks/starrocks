---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.md'

## 功能

创建 StarRocks 用户。自 v3.3.3 起，StarRocks 支持创建用户时指定用户属性。

<UserManagementPriv />

## 语法

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## 参数说明

- `user_identity`：用户标识。由登录IP（userhost）和用户名（username）组成，写作：`username@'userhost'` 。其中，`userhost` 的部分可以使用 `%` 来进行模糊匹配。如果不指定 `userhost`，默认为 `%`，即表示创建一个可以从任意 host 使用 `username` 链接到 StarRocks 的用户。

  有关用户名 (username) 的命名要求，参见[系统限制](../../System_limit.md)。

- `auth_option`：用户的认证方式。目前，StarRocks 支持原生密码、mysql_native_password 和 LDAP 三种认证方式，其中，原生密码与 mysql_native_password 认证方式的内在逻辑相同，仅在具体设置语法上有轻微差别。同一个 user identity 只能使用一种认证方式。

    ```SQL
      auth_option: {
          IDENTIFIED BY 'auth_string'
          IDENTIFIED WITH mysql_native_password BY 'auth_string'
          IDENTIFIED WITH mysql_native_password AS 'auth_string'
          IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
          
      }
      ```

      | **认证方式**                 | **创建用户时的密码** | **用户****登录****时的密码** |
      | ---------------------------- | -------------------- | ---------------------------- |
      | 原生密码                     | 明文或密文           | 明文                         |
      | `mysql_native_password BY`   | 明文                 | 明文                         |
      | `mysql_native_password WITH` | 密文                 | 明文                         |
      | `authentication_ldap_simple` | 明文                 | 明文                         |

    > 注：在所有认证方式中，StarRocks均会加密存储用户的密码。

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`：如果指定了此参数，则会自动将此角色赋予给用户，并且在用户登录后默认激活。如果不指定，则该用户默认没有任何权限。指定的角色必须已经存在。

- `PROPERTIES`：设置用户属性，包括用户最大连接数（`max_user_connections`），Catalog，数据库，或用户级别的 Session 变量。用户级别的 Session 变量在用户登录时生效。该功能自 v3.3.3 起支持。

  ```SQL
  -- 设置用户最大连接数。
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- 设置 Catalog。
  PROPERTIES ("catalog" = "<catalog_name>")
  -- 设置数据库。
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- 设置 Session 变量。
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip
  - `PROPERTIES` 作用于用户本身而非用户标识。
  - 全局变量和只读变量无法为单个用户设置。
  - 变量按照以下顺序生效：SET_VAR > Session > 用户属性 > Global。
  - 您可以通过 [SHOW PROPERTY](./SHOW_PROPERTY.md) 查看特定用户的属性。
  :::

## 示例

示例一：使用明文密码创建一个用户（不指定 host 等价于 jack@'%'）。

```SQL
CREATE USER jack IDENTIFIED BY '123456';
CREATE USER jack IDENTIFIED WITH mysql_native_password BY '123456';
```

示例二：使用密文密码创建一个用户，允许该用户从 '172.10.1.10' 登录。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 其中，密文密码可以通过PASSWORD()函数获得

示例三：创建一个允许从域名 'example_domain' 登录的用户。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

示例四：创建一个 LDAP 认证的用户。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

示例五：创建一个 LDAP 认证的用户，并指定用户在 LDAP 中的 DN (Distinguished Name)。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

示例六：创建一个允许从 '192.168' 子网登录的用户，同时指定其默认角色为 `db_admin` 和 `user_admin`。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

示例七：创建用户，设置最大用户连接数为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

示例八：创建用户，设置用户的 Catalog 为 `hive_catalog`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

示例九：创建用户，设置用户的数据库为 Default Catalog 中的 `test_db`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

示例十：创建用户，设置用户的 Session 变量 `query_timeout` 为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```

## 相关文档

创建用户后，可以为用户授予权限或角色，更改用户信息，或者删除用户。

- [ALTER USER](ALTER_USER.md)
- [GRANT](GRANT.md)
- [DROP USER](DROP_USER.md)
