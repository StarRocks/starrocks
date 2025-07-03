---
displayed_sidebar: docs
sidebar_position: 50
---

# OAuth 2.0 认证

本文介绍如何在 StarRocks 中启用 OAuth 2.0 认证。

从 v3.5.0 开始，StarRocks 支持使用 OAuth 2.0 对客户端访问进行认证。您可以在 Web UI 和 JDBC 驱动程序上通过 HTTP 启用 OAuth 2.0 认证。

StarRocks 使用 [Authorization Code](https://tools.ietf.org/html/rfc6749#section-1.3.1) 流程，该流程将授权码交换为令牌。通常，该流程包括以下步骤：

1. StarRocks 协调器将用户的浏览器重定向到授权服务器。
2. 用户在授权服务器上进行身份验证。
3. 请求被批准后，浏览器被重定向回 StarRocks FE，并附带授权码。
4. StarRocks 协调器将授权码交换为令牌。

本文介绍如何在 StarRocks 中手动创建和认证用户以使用 OAuth 2.0。有关如何使用安全集成将 StarRocks 与您的 OAuth 2.0 服务集成的说明，请参阅 [Authenticate with Security Integration](./security_integration.md)。有关如何在您的 OAuth 2.0 服务中认证用户组的更多信息，请参阅 [Authenticate User Groups](../group_provider.md)。

## 前提条件

如果您希望从 MySQL 客户端连接到 StarRocks，MySQL 客户端版本必须为 9.2 或更高版本。有关更多信息，请参阅 [MySQL 官方文档](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html)。

## 使用 OAuth 2.0 创建用户

创建用户时，通过 `IDENTIFIED WITH authentication_oauth2 AS '{xxx}'` 指定认证方法为 OAuth 2.0。`{xxx}` 是用户的 OAuth 2.0 属性。

语法：

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_oauth2 AS 
'{
  "auth_server_url": "<auth_server_url>",
  "token_server_url": "<token_server_url>",
  "client_id": "<client_id>",
  "client_secret": "<client_secret>",
  "redirect_url": "<redirect_url>",
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

属性：

- `auth_server_url`: 授权 URL。用户浏览器将被重定向到此 URL，以开始 OAuth 2.0 授权过程。
- `token_server_url`: 授权服务器上用于获取访问令牌的端点 URL。
- `client_id`: StarRocks 客户端的公共标识符。
- `client_secret`: 用于授权 StarRocks 客户端与授权服务器通信的密钥。
- `redirect_url`: OAuth 2.0 认证成功后，用户浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- `jwks_url`: JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
- `principal_field`: 用于标识 JWT 中表示主体 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- `required_issuer` (可选): 用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。
- `required_audience` (可选): 用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。

示例：

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

## 使用 OAuth 2.0 从 JDBC 客户端连接

StarRocks 支持 MySQL 协议。您可以自定义 MySQL 插件以自动启动浏览器登录方法。
可参考官方 JDBC OAuth2 插件示例代码：[starrocks-jdbc-oauth2-plugin](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-jdbc-oauth2-plugin)。

## 使用 OAuth 2.0 从 MySQL 客户端连接

若不适合自动调用浏览器登录（如命令行/服务器环境），StarRocks 也支持原生 MySQL 客户端或 JDBC 驱动接入：
- 初次连接时，StarRocks 返回一个认证 URL；
- 用户需手动访问该 URL，在 Web 浏览器完成认证流程；
- 认证完成即可开始正常交互。