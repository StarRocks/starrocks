---
displayed_sidebar: docs
sidebar_position: 50
---

# OAuth 2.0 Authentication

This topic describes how to enable OAuth 2.0 authentication in StarRocks.

From v3.5.0 onwards, StarRocks supports authenticating client access using OAuth 2.0. You can enable OAuth 2.0 authentication over HTTP for the Web UI and the JDBC driver.

StarRocks uses the [Authorization Code](https://tools.ietf.org/html/rfc6749#section-1.3.1) flow which exchanges an authorization code for a token. Generally, the flow includes the following steps:

1. The StarRocks coordinator redirects the user’s browser to the Authorization Server.
2. The user authenticates from the Authorization Server.
3. After the request is approved, the browser is redirected back to the StarRocks FE with an authorization code.
4. the StarRocks coordinator exchanges the authorization code for a token.

This topic describes how to manually create and authenticate users using OAuth 2.0 in StarRocks. For instructions on how to integrate StarRocks with your OAuth 2.0 service using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups in your OAuth 2.0 service, see [Authenticate User Groups](../group_provider.md).

## Prerequisites

If you want to connect to StarRocks from a MySQL client, the MySQL client version must be 9.2 or later. For more information, see [MySQL official document](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html).

## Create a user with OAuth 2.0

When creating a user, specify the authentication method as OAuth 2.0 by `IDENTIFIED WITH authentication_oauth2 [AS '{xxx}']`. `{xxx}` is the OAuth 2.0 properties of the user. In addition to the following method, you can configure the default OAuth 2.0 properties in the FE configuration file. You need to manually modify all **fe.conf** files and restart all FEs for configuration to take effect. After the FE configurations are set, StarRocks will use the default properties specified in your configuration file and you can omit the `AS '{xxx}'` part.

Syntax:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_oauth2 [AS 
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
}']
```

| Property            | Corresponding FE Configuration | Description                                                                                                            |
| ------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `auth_server_url`   | `oauth2_auth_server_url`       | The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process.|
| `token_server_url`  | `oauth2_token_server_url`      | The URL of the endpoint on the authorization server from which StarRocks obtains the access token.                     |
| `client_id`         | `oauth2_client_id`             | The public identifier of the StarRocks client.                                                                         |
| `client_secret`     | `oauth2_client_secret`         | The secret used to authorize StarRocks client with the authorization server.                                           |
| `redirect_url`      | `oauth2_redirect_url`          | The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`. |
| `jwks_url`          | `oauth2_jwks_url`              | The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `conf` directory.               |
| `principal_field`   | `oauth2_principal_field`       | The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks. |
| `required_issuer`   | `oauth2_required_issuer`       | (Optional) The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer. |
| `required_audience` | `oauth2_required_audience`     | (Optional) The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience. |

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

If you have set the OAuth 2.0 properties in the FE configuration files, you can directly execute the following statement:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_oauth2;
```

## Connect from JDBC client with OAuth 2.0

StarRocks supports the MySQL protocol. You can customize a MySQL plugin to automatically launch the browser login method. 

For the example code of the JDBC OAuth2 plugin, see the official document for [starrocks-jdbc-oauth2-plugin](https://github.com/StarRocks/starrocks/tree/main/contrib/starrocks-jdbc-oauth2-plugin).

## Connect from MySQL client with OAuth 2.0

If you cannot access a browser in your environment (such as using terminal or server), you can also access StarRocks via native MySQL client or JDBC driver:
- When you first connect to StarRocks, a URL will be returned.
- You need to access this URL on a browser and complete the authentication.
- After the authentication, you can then interact with StarRocks.
