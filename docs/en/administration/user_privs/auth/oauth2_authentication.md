---
displayed_sidebar: docs
sidebar_position: 40
---

# OAuth 2.0 Authentication

This topic describes how to enable OAuth 2.0 authentication in StarRocks.

From v3.5.0 onwards, StarRocks supports authenticating client access using OAuth 2.0. You can enable OAuth 2.0 authentication over HTTP for the Web UI and the JDBC driver.

StarRocks uses the [Authorization Code](https://tools.ietf.org/html/rfc6749#section-1.3.1) flow which exchanges an authorization code for a token. Generally, the flow includes the following steps:

1. The StarRocks coordinator redirects the user’s browser to the Authorization Server.
2. The user authenticates from the Authorization Server.
3. After the request is approved, the browser is redirected back to the StarRocks FE with an authorization code.
4. the StarRocks coordinator exchanges the authorization code for a token.

This topic describes how to manually create and authenticate users using OAuth 2.0 in StarRocks. For instructions on how to integrate StarRocks with your OAuth 2.0 service using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups in your OAuth 2.0 service, see [Authenticate User Groups](./group_provider.md).

## Prerequisites

If you want to connect to StarRocks from a MySQL client, the MySQL client version must be 9.2 or later. For more information, see [MySQL official document](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html).

## Create a user with OAuth 2.0

When creating a user, specify the authentication method as OAuth 2.0 by `IDENTIFIED WITH authentication_oauth2 AS '{xxx}'`. `{xxx}` is the OAuth 2.0 properties of the user.

Syntax:

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

Properties:

- `auth_server_url`: The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process.
- `token_server_url`: The URL of the endpoint on the authorization server from which StarRocks obtains the access token.
- `client_id`: The public identifier of the StarRocks client.
- `client_secret`: The secret used to authorize StarRocks client with the authorization server.
- `redirect_url`: The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`.
- `jwks_url`: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `conf` directory.
- `principal_field`: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- `required_issuer` (Optional): The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- `required_audience` (Optional): The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

Example:

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

## Connect from JDBC client with OAuth 2.0

StarRocks supports the MySQL protocol. You can customize a MySQL plugin to automatically launch the browser login method. 

The following is an example of a JDBC client:

```Java

/**
 * StarRocks 'authentication_oauth2_client' authentication plugin.
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
            // Cannot do anything with whatever payload comes from the server,
            // so just skip this iteration and wait for a Protocol::AuthSwitchRequest or a Protocol::AuthNextFactor.
            return true;
        }

        // The URL a user’s browser will be redirected to in order to begin the OAuth2 authorization process
        int authServerUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String authServerUrl =
                fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", authServerUrlLength);

        // The public identifier of the StarRocks client.
        int clientIdLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String clientId = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", clientIdLength);

        // The URL to redirect to after OAuth2 authentication is successful.
        int redirectUrlLength = (int) fromServer.readInteger(NativeConstants.IntegerDataType.INT2);
        String redirectUrl = fromServer.readString(NativeConstants.StringLengthDataType.STRING_VAR, "ASCII", redirectUrlLength);

        // The connection ID of StarRocks must be included in the callback URL of OAuth2
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

## Connect from MySQL client with OAuth 2.0

Sometimes direct calls to the users' browser is not allowed. In such cases, StarRocks also supports the native MySQL client or native MySQL JDBC Driver. In this mode, you can directly establish a connection with StarRocks, but you cannot perform any operations in StarRocks. Executing any command will return a URL, and the you need to actively access this URL to complete the OAuth 2.0 authentication process.
