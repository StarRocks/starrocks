---
displayed_sidebar: docs
sidebar_position: 30
---

# OpenID Connect 认证

本文介绍如何在 StarRocks 中启用 OpenID Connect 认证。

从 v3.5.0 开始，StarRocks 支持使用 OpenID Connect 进行客户端访问认证。

OpenID Connect (OIDC) 是构建在 OAuth 2.0 框架之上的身份层。它允许第三方应用程序验证终端用户的身份并获取基本的用户资料信息。OIDC 使用 JSON web tokens (JWTs)，可以通过符合 OAuth 2.0 规范的流程获取。JWT 是一个小型的、网络安全的 JSON 对象，包含类似证书的加密信息，包括主体、有效时间段和签名。

本文介绍如何在 StarRocks 中手动创建和认证用户使用 OIDC。有关如何通过安全集成将 StarRocks 与您的 OIDC 服务集成的说明，请参阅 [使用安全集成认证](./security_integration.md)。有关如何在您的 OIDC 服务中认证用户组的更多信息，请参阅 [认证用户组](./group_provider.md)。

## 前提条件

如果您想从 MySQL 客户端连接到 StarRocks，MySQL 客户端版本必须为 9.2 或更高版本。更多信息请参阅 [MySQL 官方文档](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html)。

## 使用 OIDC 创建用户

创建用户时，通过 `IDENTIFIED WITH authentication_openid_connect AS '{xxx}'` 指定认证方法为 OIDC。`{xxx}` 是用户的 OIDC 属性。

语法：

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_openid_connect AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

属性：

- `jwks_url`: JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下本地文件的路径。
- `principal_field`: 用于标识 JWT 中主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- `required_issuer` (可选): 用于标识 JWT 中发行者 (`iss`) 的字符串列表。只有当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。
- `required_audience` (可选): 用于标识 JWT 中受众 (`aud`) 的字符串列表。只有当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。

示例：

```SQL
CREATE USER tom IDENTIFIED WITH authentication_openid_connect AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```

## 使用 OIDC 从 MySQL 客户端连接

要使用 OIDC 从 MySQL 客户端连接到 StarRocks，您需要启用 `authentication_openid_connect_client` 插件，并传递必要的令牌（使用令牌文件的路径）以认证映射的用户。

语法：

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

示例：

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```