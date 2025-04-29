---
displayed_sidebar: docs
sidebar_position: 40
---

# OAuth 2.0 认证

本主题描述如何在 StarRocks 中启用 OAuth 2.0 认证。

从 v3.5.0 开始，StarRocks 支持使用 OAuth 2.0 对客户端访问进行认证。您可以通过 HTTP 为 Web UI 和 JDBC 驱动启用 OAuth 2.0 认证。

StarRocks 使用 [Authorization Code](https://tools.ietf.org/html/rfc6749#section-1.3.1) 流程，该流程将授权码交换为令牌。通常，该流程包括以下步骤：

1. StarRocks 协调器将用户的浏览器重定向到授权服务器。
2. 用户在授权服务器上进行身份验证。
3. 请求被批准后，浏览器被重定向回 StarRocks FE，并附带授权码。
4. StarRocks 协调器将授权码交换为令牌。

本主题描述如何在 StarRocks 中手动创建和认证用户以使用 OAuth 2.0。有关如何使用安全集成将 StarRocks 与您的 OAuth 2.0 服务集成的说明，请参阅 [使用安全集成认证](./security_integration.md)。有关如何在您的 OAuth 2.0 服务中认证用户组的更多信息，请参阅 [认证用户组](./group_provider.md)。

## 前提条件

如果您想从 MySQL 客户端连接到 StarRocks，MySQL 客户端版本必须为 9.2 或更高版本。有关更多信息，请参阅 [MySQL 官方文档](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html)。

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

- `auth_server_url`: 授权 URL。用户浏览器将被重定向到该 URL 以开始 OAuth 2.0 授权过程。
- `token_server_url`: StarRocks 从授权服务器获取访问令牌的端点 URL。
- `client_id`: StarRocks 客户端的公共标识符。
- `client_secret`: 用于授权 StarRocks 客户端与授权服务器的密钥。
- `redirect_url`: OAuth 2.0 认证成功后，用户浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要将其配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- `jwks_url`: 指向 JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
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

以下是一个 JDBC 客户端的示例：

```Java

/**
 * StarRocks 'authentication_oauth2_client' 认证插件。
 */
public class AuthenticationOAuth2Client implements AuthenticationPlugin<NativePacketPayload> {
    public static String PLUGIN_NAME = "authentication_oauth2_client";

    private Long connectionId = null;
    private String sourceOfAuthData = PLUGIN_NAME;

    @Override
    public void init(Protocol<NativePacketPayload> prot, MysqlCallbackHandler cbh) {
        connectionId = prot.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    @Override
    public void setAuthenticationParameters(String user, String password) {
    }

    @Override
    public void setSourceOfAuthData(String sourceOfAuthData) {
        this.sourceOfAuthData = sourceOfAuthData;
    }

    @Override
    public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
        toServer.clear();

        if (!this.sourceOfAuthData.equals(PLUGIN_NAME) || fromServer.getPayloadLength() == 0) {
            // 无法处理来自服务器的任何有效负载，
            // 因此只需跳过此迭代并等待 Protocol::AuthSwitchRequest 或 Protocol::AuthNextFactor。
            return true;
        }

        // 用户浏览器将被重定向到的 URL，以开始 OAuth2 授权过程
        int authServerUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String authServerUrl =
                fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", authServerUrlLength);

        // StarRocks 客户端的公共标识符。
        int clientIdLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String clientId = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", clientIdLength);

        // OAuth2 认证成功后重定向的 URL。
        int redirectUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String redirectUrl = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", redirectUrlLength);

        // StarRocks 的连接 ID 必须包含在 OAuth2 的回调 URL 中
        long connectionId = this.connectionId;

        String authUrl = authServerUrl +
                "?response_type=code" +
                "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8) +
                "&redirect_uri=" + URLEncoder.encode(redirectUrl, StandardCharsets.UTF_8) + "?connectionId=" + connectionId +
                "&scope=openid";

        Desktop desktop = Desktop.getDesktop();
        try {
            desktop.browse(new URI(authUrl));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

        NativePacketPayload packet = new NativePacketPayload(StringUtils.getBytes(""));
        packet.setPosition(packet.getPayloadLength());
        packet.writeInteger(NativeConstants.IntegerDataType.INT1, 0);
        packet.setPosition(0);

        toServer.add(packet);
        return true;
    }
}

public class OAuth2Main {
    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Properties properties = new Properties();
        properties.setProperty("defaultAuthenticationPlugin", "AuthenticationOAuth2Client");
        properties.setProperty("authenticationPlugins", "AuthenticationOAuth2Client");
    }
}
```

## 使用 OAuth 2.0 从 MySQL 客户端连接

有时不允许直接调用用户的浏览器。在这种情况下，StarRocks 也支持原生 MySQL 客户端或原生 MySQL JDBC 驱动。在此模式下，您可以直接与 StarRocks 建立连接，但不能在 StarRocks 中执行任何操作。执行任何命令将返回一个 URL，您需要主动访问此 URL 以完成 OAuth 2.0 认证过程。