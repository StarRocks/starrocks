---
displayed_sidebar: docs
sidebar_position: 20
---

# Authenticate with Security Integration

Integrate StarRocks with external authentication systems using security integration.

By creating a security integration within your StarRocks cluster, you can allow access of your external authentication service to StarRocks. With the security integration, you do not need to manually create users within StarRocks. When a user tries to log in using an external identity, StarRocks will use the corresponding security integration according to the configuration in `authentication_chain` to authenticate the user. After the authentication is successful and the user is allowed to log in, StarRocks creates a virtual user in the session for the user to perform subsequent operations.

Please note that if you use the security integration to configure an external authentication method, you must also [integrate StarRocks with Apache Ranger](../authorization/ranger_plugin.md) to enable external authorization. Currently, integrating Security Integration with the StarRocks native authorization is not supported.

You can also enable [Group Provider](../group_provider.md) for StarRocks to access the group information in you external authentication systems, thus allowing creating, authenticating, and authorizing user groups in StarRocks.

Manually creating and managing users with external authentication services are also supported in case of specific corner cases. For more instructions, you can refer to [See also](#see-also).

## Create a security integration

Currently, StarRocks' security integration supports the following authentication systems:
- LDAP
- JSON Web Token (JWT)
- OAuth 2.0

:::note
StarRocks does not offer connectivity checks when you create a security integration.
:::

### Create a security integration with LDAP

#### Syntax

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_ldap_simple",
    "authentication_ldap_simple_server_host" = "",
    "authentication_ldap_simple_server_port" = "",
    "authentication_ldap_simple_bind_base_dn" = "",
    "authentication_ldap_simple_user_search_attr" = ""
    "authentication_ldap_simple_bind_root_dn" = "",
    "authentication_ldap_simple_bind_root_pwd" = "",
    "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
    "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
    "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

#### Parameters

##### security_integration_name

- Required: Yes
- Description: The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`.

##### type

- Required: Yes
- Description: The type of the security integration. Specify it as `authentication_ldap_simple`.

##### authentication_ldap_simple_server_host

- Required: No
- Description: The IP address of your LDAP service. Default: `127.0.0.1`.

##### authentication_ldap_simple_server_port

- Required: No
- Description: The port of your LDAP service. Default: `389`.

##### authentication_ldap_simple_bind_base_dn

- Required: Yes
- Description: The base Distinguished Name (DN) of the LDAP user for which the cluster searches.

##### authentication_ldap_simple_user_search_attr

- Required: Yes
- Description: The user's attribute used to log in to the LDAP service, for example, `uid`.

:::note

**DN Passing Mechanism**: LDAP security integration supports DN passing functionality.

- After successful authentication, the system records both the user's login name and complete DN.
- When combined with Group Provider, DN information is automatically passed to the Group Provider.
- If `ldap_user_search_attr` is not configured for the Group Provider, DN will be used for group matching.
- This mechanism is particularly suitable for complex LDAP environments like Microsoft AD.

For more details, see the DN matching mechanism in [Authenticate User Groups](../group_provider.md).

:::

##### authentication_ldap_simple_bind_root_dn

- Required: Yes
- Description: The admin DN of your LDAP service.

##### authentication_ldap_simple_bind_root_pwd

- Required: Yes
- Description: The admin password of your LDAP service.

##### authentication_ldap_simple_ssl_conn_allow_insecure

- Required: No
- Description: Whether to allow non-encrypted connections to the LDAP server. Default value: `true`. Setting this value to `false` indicates that SSL encryption is required to access LDAP.

##### authentication_ldap_simple_ssl_conn_trust_store_path

- Required: No
- Description: Local path to store the SSL CA certificate of the LDAP server. Supports pem and jks formats. You do not need to set this item if the certificate is issued by a trusted organization.

##### ldap_ssl_conn_trust_store_pwd

- Required: No
- Description: The password used to access the locally stored SSL CA certificate of the LDAP server. pem-formatted certificates do not require a password. Only jsk-formatted certificates do.

##### group_provider

- Required: No
- Description: The name of the group provider(s) to be combined with the security integration. Multiple group providers are separated by commas. Once set, StarRocks will record the user's group information under each specified provider upon login. Supported from v3.5 onwards. For detailed instructions on enabling Group Provider, see [Authenticate User Groups](../group_provider.md).

##### permitted_groups

- Required: No
- Description: The name of group(s) whose members are allowed to log in to StarRocks. Multiple groups are separated by commas. Make sure that the specified groups can be retrieved by the combined group provider(s). Supported from v3.5 onwards.

##### comment

- Required: No
- Description: The description of the security integration.

### Create a security integration with JWT

#### Syntax

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_jwt",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
);
```

#### Parameters

##### security_integration_name

- Required: Yes
- Description: The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`.

##### type

- Required: Yes
- Description: The type of the security integration. Specify it as `jwt`.

##### jwks_url

- Required: Yes
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `fe/conf` directory.

##### principal_field

- Required: Yes
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.

##### required_issuer

- Required: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.

##### required_audience

- Required: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

##### comment

- Required: No
- Description: The description of the security integration.

### Create a security integration with OAuth 2.0

#### Syntax

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_oauth2",
    "auth_server_url" = "",
    "token_server_url" = "",
    "client_id" = "",
    "client_secret" = "",
    "redirect_url" = "",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

#### Parameters

##### security_integration_name

- Required: Yes
- Description: The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`.

##### auth_server_url

- Required: Yes
- Description: The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process.

##### token_server_url

- Required: Yes
- Description: The URL of the endpoint on the authorization server from which StarRocks obtains the access token.

##### client_id

- Required: Yes
- Description: The public identifier of the StarRocks client.

##### client_secret

- Required: Yes
- Description: The secret used to authorize StarRocks client with the authorization server.

##### redirect_url

- Required: Yes
- Description: The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`.

##### type

- Required: Yes
- Description: The type of the security integration. Specify it as `authentication_oauth2`.

##### jwks_url

- Required: Yes
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `fe/conf` directory.

##### principal_field

- Required: Yes
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.

##### required_issuer

- Required: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.

##### required_audience

- Required: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

##### comment

- Required: No
- Description: The description of the security integration.

## Configure authentication chain

After the security integration is created, it is added to your StarRocks cluster as a new authentication method. You must enable the security integration by setting the order of the authentication methods via the FE dynamic configuration item `authentication_chain`.

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>[... ,]"
);
```

:::note
- StarRocks prioritizes native authentication for local users. If a local user with the same username does not exist, authentication is performed in the order configured by `authentication_chain`. If login fails using the native authentication method, the cluster will try the next authentication method in the specified order.
- You can specify multiple security integrations in `authentication_chain` except for OAuth 2.0 security integration. You cannot specify multiple OAuth 2.0 security integrations or one with other security integrations.
:::

You can check the value of `authentication_chain` using the following statement:

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

## Manage security integrations

### Alter security integration

You can alter the configuration of an existing security integration using the following statement:

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
(
    "key"="value"[, ...]
)
```

:::note
You cannot alter the `type` of a security integration.
:::

### Drop security integration

You can drop an existing security integration using the following statement:

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

### View security integration

You can view all security integrations in your cluster using the following statement:

```SQL
SHOW SECURITY INTEGRATIONS;
```

Example:

```Plain
SHOW SECURITY INTEGRATIONS;
+--------+--------+---------+
| Name   | Type   | Comment |
+--------+--------+---------+
| LDAP1  | LDAP   | NULL    |
+--------+--------+---------+
```

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Name          | The name of the security integration.                        |
| Type          | The type of the security integration.                        |
| Comment       | The description of the security integration. `NULL` is returned when no description is specified for the security integration. |

You can check the details of a security integration using the following statement:

```SQL
SHOW CREATE SECURITY INTEGRATION <integration_name>
```

Example:

```Plain
SHOW CREATE SECURITY INTEGRATION LDAP1；

+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Security Integration  | Create Security Integration                                                                                                                                                                                                                                                                                                                                                                              |
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| LDAP1                | CREATE SECURITY INTEGRATION LDAP1
    PROPERTIES (
      "type" = "authentication_ldap_simple",
      "authentication_ldap_simple_server_host" = "",
      "authentication_ldap_simple_server_port" = "",
      "authentication_ldap_simple_bind_base_dn" = "",
      "authentication_ldap_simple_user_search_attr" = ""
      "authentication_ldap_simple_bind_root_dn" = "",
      "authentication_ldap_simple_bind_root_pwd" = "",
      "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
      "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
      "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
      "comment" = ""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

:::note
`ldap_bind_root_pwd` is masked when SHOW CREATE SECURITY INTEGRATION is executed.
:::

## See also

- For instructions on how to manually authenticate users via LDAP in StarRocks, see [LDAP Authentication](./ldap_authentication.md).
- For instructions on how to manually authenticate users via JSON Web Token in StarRocks, see [JSON Web Token Authentication](./jwt_authentication.md).
- For instructions on how to manually authenticate users via OAuth 2.0 in StarRocks, see [OAuth 2.0 Authentication](./oauth2_authentication.md).
- For instructions on how to authenticate user groups, see [Authenticate User Groups](../group_provider.md).
