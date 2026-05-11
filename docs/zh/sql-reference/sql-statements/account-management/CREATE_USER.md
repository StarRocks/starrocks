---
displayed_sidebar: docs
---

# 创建用户

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'
import AuthOption from '../../../_assets/commonMarkdown/AuthOption.mdx'

`CREATE USER` 用于创建 StarRocks 用户。在 StarRocks 中，`user_identity` 唯一标识一个用户。从 v3.3.3 版本起，StarRocks 支持在创建用户时设置用户属性。

<UserManagementPriv />

### 语法

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## 参数

- `user_identity` 由 `user_name` 和 `host` 两部分组成，格式为 `username@'userhost'`。对于 `host` 部分，您可以使用 `%` 进行模糊匹配。如果未指定 `host`，则默认使用 `%`，表示用户可以从任何主机连接到 StarRocks。

  有关用户名的命名规范，请参阅 [系统限制](../../System_limit.md)。

<<<<<<< HEAD
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
=======
<AuthOption />

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`：如果指定此参数，则在用户登录时，角色会自动分配给用户并默认激活。如果未指定，则此用户不具有任何权限。请确保所有指定的角色都已存在。

- `PROPERTIES` 用于设置用户属性，包括最大用户连接数 (`max_user_connections`)、catalog、database 或用户级别的会话变量。用户级别的会话变量在用户登录时生效。此功能从 v3.3.3 版本开始支持。
>>>>>>> a20389f1e6 ([Doc] move to snippet (#73117))

  ```SQL
  -- 设置最大用户连接数。
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- 设置 catalog。
  PROPERTIES ("catalog" = "<catalog_name>")
  -- 设置 database。
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- 设置会话变量。
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip

  - `PROPERTIES` 作用于用户，而不是用户身份。
  - 全局变量和只读变量不能为特定用户设置。
  - 变量按以下顺序生效：SET_VAR > 会话 > 用户属性 > 全局。
  - 您可以使用 [SHOW PROPERTY](./SHOW_PROPERTY.md) 查看特定用户的属性。
:::

## 示例

示例 1：创建一个使用明文密码且未指定主机的用户，这等同于 `jack@'%'`。

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

示例 2：创建一个使用明文密码的用户，并允许该用户从 `'172.10.1.10'` 登录。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

示例 3：创建一个使用密文密码的用户，并允许该用户从 `'172.10.1.10'` 登录。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 注意：您可以使用 password() 函数获取加密密码。

示例 4：创建一个允许从域名 'example_domain' 登录的用户。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

示例 5：创建一个使用 LDAP 认证的用户。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

示例 6：创建一个使用 LDAP 认证的用户，并指定该用户在 LDAP 中的专有名称 (DN)。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

示例 7：创建一个允许从 '192.168' 子网登录的用户，并设置 `db_admin` 和 `user_admin` 作为该用户的默认角色。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

示例 8：创建一个用户，并将其最大用户连接数设置为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

示例 9：创建一个用户，并将其 catalog 设置为 `hive_catalog`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

示例 10：创建一个用户，并将其 database 在默认 catalog 中设置为 `test_db`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

示例 11：创建一个用户，并将其会话变量 `query_timeout` 设置为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```

<<<<<<< HEAD
## 相关文档

创建用户后，可以为用户授予权限或角色，更改用户信息，或者删除用户。

- [ALTER USER](ALTER_USER.md)
- [GRANT](GRANT.md)
- [DROP USER](DROP_USER.md)
=======
示例 12：创建一个使用 JSON Web Token 认证的用户。

```SQL
CREATE USER tom IDENTIFIED WITH authentication_jwt AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/jwt/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "starrocks"
}';
```

示例 13：创建一个使用 OAuth 2.0 认证的用户。

```SQL
CREATE USER tom IDENTIFIED WITH authentication_oauth2 AS 
'{
  "auth_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/auth",
  "token_server_url": "http://localhost:38080/realms/master/protocol/openid-connect/token",
  "client_id": "12345",
  "client_secret": "LsWyD9vPcM3LHxLZfzJsuoBwWQFBLcoR",
  "redirect_url": "http://localhost:8030/api/oauth2",
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```
>>>>>>> a20389f1e6 ([Doc] move to snippet (#73117))
