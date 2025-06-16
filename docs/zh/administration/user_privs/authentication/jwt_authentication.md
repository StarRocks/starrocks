---
displayed_sidebar: docs
sidebar_position: 40
---

# JSON Web Token 认证

本主题描述如何在 StarRocks 中启用 JSON Web Token 认证。

从 v3.5.0 开始，StarRocks 支持使用 JSON Web Token 进行客户端访问认证。

JSON Web Token（JWT）是一项开放标准（RFC 7519），它定义了一种紧凑、自足的方式，可在各方之间以 JSON 对象的形式安全地传输信息。由于该信息经过数字签名，因此可以验证和信任。JWT 可以使用密文（HMAC 算法）或使用 RSA 或 ECDSA 的公钥/私钥对进行签名。

本主题描述如何在 StarRocks 中使用 JWT 认证手动创建和认证用户。有关如何通过安全集成将 StarRocks 与 JWT 认证集成的说明，请参见 [通过安全集成认证用户](./security_integration.md)。有关如何在使用 JWT 认证用户组的更多信息，请参见 [认证用户组](../group_provider.md)。

## 前提条件

如果您想从 MySQL 客户端连接到 StarRocks，MySQL 客户端版本必须是 9.2 或更高版本。

## 使用 JWT 认证创建用户

创建用户时，通过 `IDENTIFIED WITH authentication_jwt AS '{xxx}'` 指定认证方法为 JWT。`{xxx}` 是用户的 JWT 属性。

语法：

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_jwt AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

属性：

- `jwks_url`: JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下公钥本地文件的路径。
- `principal_field`: 用于标识 JWT 中主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- `required_issuer` (可选): 用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。
- `required_audience` (可选): 用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。

示例：

```SQL
CREATE USER tom IDENTIFIED WITH authentication_jwt AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/jwt/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "starrocks"
}';
```

## 使用 JWT 从 MySQL 客户端连接

要使用 JWT 从 MySQL 客户端连接到 StarRocks，您需要启用 `authentication_openid-connect_client` 插件，并传递必要的令牌（使用令牌文件的路径）来认证映射的用户。

语法：

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

示例：

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```
