---
displayed_sidebar: docs
sidebar_position: 40
---

# JSON Web Token Authentication

This topic describes how to enable JSON Web Token authentication in StarRocks.

From v3.5.0 onwards, StarRocks supports authenticating client access using JSON Web Tokens.

JSON Web Token (JWT) is an open standard (RFC 7519) that defines a compact and self-contained way for securely transmitting information between parties as a JSON object. This information can be verified and trusted because it is digitally signed. JWTs can be signed using a secret (with the HMAC algorithm) or a public/private key pair using RSA or ECDSA.

This topic describes how to manually create and authenticate users using JWT in StarRocks. For instructions on how to integrate StarRocks with JWT authentication using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups using JWT, see [Authenticate User Groups](../group_provider.md).

## Prerequisites

If you want to connect to StarRocks from a MySQL client, the MySQL client version must be 9.2 or later.

## Create a user with JWT

When creating a user, specify the authentication method as JWT by `IDENTIFIED WITH authentication_jwt AS '{xxx}'`. `{xxx}` is the JWT properties of the user.

Syntax:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_jwt AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

Properties:

- `jwks_url`: The URL to the JSON Web Key Set (JWKS) service or the path to the public key local file under the `fe/conf` directory.
- `principal_field`: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- `required_issuer` (Optional): The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- `required_audience` (Optional): The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

Example:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_jwt AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/jwt/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "starrocks"
}';
```

## Connect from MySQL client with JWT

To connect from a MySQL client to StarRocks using JWT, you need to enable the `authentication_openid-connect_client` plugin, and pass the necessary token (using the path to the token file) to authenticate the mapped user.

Syntax:

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

Example:

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```
