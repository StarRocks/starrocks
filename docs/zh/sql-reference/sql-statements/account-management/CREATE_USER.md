---
displayed_sidebar: docs
---

# CREATE USER

import UserManagementPriv from '../../../_assets/commonMarkdown/userManagementPriv.mdx'

CREATE USER 用于创建一个 StarRocks 用户。在 StarRocks 中，"user_identity" 唯一标识一个用户。从 v3.3.3 开始，StarRocks 支持在创建用户时设置用户属性。

<UserManagementPriv />

### 语法

```SQL
CREATE USER [IF NOT EXISTS] <user_identity> 
[auth_option] 
[DEFAULT ROLE <role_name>[, <role_name>, ...]]
[PROPERTIES ("key"="value", ...)]
```

## 参数

- `user_identity` 由两部分组成，"user_name" 和 "host"，格式为 `username@'userhost'`。对于 "host" 部分，可以使用 `%` 进行模糊匹配。如果未指定 "host"，则默认使用 "%" ，表示用户可以从任何主机连接到 StarRocks。

  有关用户名命名约定，请参见 [系统限制](../../System_limit.md)。

- `auth_option` 指定认证方法。目前支持五种认证方法：StarRocks 本地密码、`mysql_native_password`、`authentication_ldap_simple`、JSON Web Token (JWT) 认证和 OAuth 2.0 认证。StarRocks 本地密码在逻辑上与 `mysql_native_password` 相同，但在语法上略有不同。一个用户身份只能使用一种认证方法。

    ```SQL
    auth_option: {
        IDENTIFIED BY 'auth_string'
        IDENTIFIED WITH mysql_native_password BY 'auth_string'
        IDENTIFIED WITH mysql_native_password AS 'auth_string'
        IDENTIFIED WITH authentication_ldap_simple AS 'auth_string'
        IDENTIFIED WITH authentication_jwt [AS 'auth_properties']
        IDENTIFIED WITH authentication_oauth2 [AS 'auth_properties']
    }
    ```

    | **认证方法**                  | **用户创建时的密码**           | **登录时的密码**       |
    | ---------------------------- | ------------------------------ | ---------------------- |
    | 本地密码                     | 明文或密文                     | 明文                   |
    | `mysql_native_password BY`   | 明文                           | 明文                   |
    | `mysql_native_password WITH` | 密文                           | 明文                   |
    | `authentication_ldap_simple` | 明文                           | 明文                   |

    :::note
    StarRocks 在存储用户密码之前会对其进行加密。
    :::

    有关 JSON Web Token (JWT) 认证和 OAuth 2.0 认证的 `auth_properties` 详细信息，请参见相应的文档：
    - [JSON Web Token 认证](../../../administration/user_privs/authentication/jwt_authentication.md)
    - [OAuth 2.0 认证](../../../administration/user_privs/authentication/oauth2_authentication.md)

- `DEFAULT ROLE <role_name>[, <role_name>, ...]`：如果指定了此参数，这些角色会自动分配给用户，并在用户登录时默认激活。如果未指定，则该用户没有任何权限。请确保所有指定的角色已经存在。

- `PROPERTIES` 设置用户属性，包括最大用户连接数 (`max_user_connections`)、catalog、数据库或用户级别的会话变量。用户级别的会话变量在用户登录时生效。此功能从 v3.3.3 开始支持。

  ```SQL
  -- 设置最大用户连接数。
  PROPERTIES ("max_user_connections" = "<Integer>")
  -- 设置 catalog。
  PROPERTIES ("catalog" = "<catalog_name>")
  -- 设置数据库。
  PROPERTIES ("catalog" = "<catalog_name>", "database" = "<database_name>")
  -- 设置会话变量。
  PROPERTIES ("session.<variable_name>" = "<value>", ...)
  ```

  :::tip
  - 全局变量和只读变量不能为特定用户设置。
  - 变量生效顺序为：SET_VAR > 会话 > 用户属性 > 全局。
  - 可以使用 [SHOW PROPERTY](./SHOW_PROPERTY.md) 查看特定用户的属性。
  :::

## 示例

示例 1：创建一个使用明文密码的用户，未指定主机，相当于 `jack@'%'`。

```SQL
CREATE USER 'jack' IDENTIFIED BY '123456';
```

示例 2：创建一个使用明文密码的用户，并允许用户从 `'172.10.1.10'` 登录。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
```

示例 3：创建一个使用密文密码的用户，并允许用户从 `'172.10.1.10'` 登录。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH mysql_native_password AS '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
```

> 注意：可以使用 password() 函数获取加密密码。

示例 4：创建一个允许从域名 'example_domain' 登录的用户。

```SQL
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '123456';
```

示例 5：创建一个使用 LDAP 认证的用户。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple;
```

示例 6：创建一个使用 LDAP 认证的用户，并在 LDAP 中指定用户的专有名称 (DN)。

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
```

示例 7：创建一个允许从 '192.168' 子网登录的用户，并将 `db_admin` 和 `user_admin` 设置为用户的默认角色。

```SQL
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE db_admin, user_admin;
```

示例 8：创建一个用户，并将其最大用户连接数设置为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ("max_user_connections" = "600");
```

示例 9：创建一个用户，并将用户的 catalog 设置为 `hive_catalog`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'hive_catalog');
```

示例 10：创建一个用户，并将用户的数据库设置为默认 catalog 中的 `test_db`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('catalog' = 'default_catalog', 'database' = 'test_db');
```

示例 11：创建一个用户，并将会话变量 `query_timeout` 设置为 `600`。

```SQL
CREATE USER 'jack'@'192.168.%' PROPERTIES ('session.query_timeout' = '600');
```

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