---
displayed_sidebar: docs
sidebar_position: 30
---

# OpenID Connect Authentication

This topic describes how to enable OpenID Connect authentication in StarRocks.

From v3.5.0 onwards, StarRocks supports authenticating client access using OpenID Connect.

OpenID Connect (OIDC) is an identity layer built on top of the OAuth 2.0 framework. It allows third-party applications to verify the identity of the end-user and to obtain basic user profile information. OIDC uses JSON web tokens (JWTs), which you can obtain using flows conforming to the OAuth 2.0 specifications. A JWT is a small, web-safe JSON object that contains cryptographic information similar to a certificate, including a subject, a valid time period, and a signature.

This topic describes how to manually create and authenticate users using OIDC in StarRocks. For instructions on how to integrate StarRocks with your OIDC service using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups in your OIDC service, see [Authenticate User Groups](./group_provider.md).

## Prerequisites

If you want to connect to StarRocks from a MySQL client, the MySQL client version must be 9.2 or later. For more information, see [MySQL official document](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html).

## Create a user with OIDC

When creating a user, specify the authentication method as OIDC by `IDENTIFIED WITH authentication_openid_connect AS '{xxx}'`. `{xxx}` is the OIDC properties of the user.

Syntax:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_openid_connect AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

Properties:

- `jwks_url`: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `fe/conf` directory.
- `principal_field`: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- `required_issuer` (Optional): The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- `required_audience` (Optional): The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

Example:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_openid_connect AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```

## Connect from MySQL client with OIDC

To connect from a MySQL client to StarRocks using OIDC, you need to enable the `authentication_openid_connect_client` plugin, and pass the necessary token (using the path to the token file) to authenticate the mapped user.

Syntax:

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

Example:

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```
