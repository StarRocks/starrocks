---
displayed_sidebar: docs
sidebar_position: 50
---

# Authenticate with Security Integration

This topic describes how to integrate StarRocks with external authentication systems using security integration.

By creating a security integration within your StarRocks cluster, you can allow access of your external authentication service to StarRocks. StarRocks caches the group membership information of your authentication service, and refreshes the cache whenever needed.

## Create a security integration

Currently, StarRocks' security integration supports the following authentication systems:
- LDAP
- OpenID Connect (OIDC)
- OAuth 2.0

> **CAUTION**
>
> StarRocks does not offer connectivity checks when you create a security integration.

### Create a security integration with LDAP

Syntax:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "ldap",
    "ldap_server_host" = "",
    "ldap_server_port" = "",
    "ldap_bind_base_dn" = "",
    "ldap_user_search_attr" = "",
    "ldap_user_group_match_attr" = "",
    "ldap_bind_root_dn" = "",
    "ldap_bind_root_pwd" = "",
    "ldap_cache_refresh_interval" = "",
    "ldap_ssl_conn_allow_insecure" = "{true | false}",
    "ldap_ssl_conn_trust_store_path" = "",
    "ldap_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

Parameters:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`. |
| type                        | Yes          | The type of the security integration. Specify it as `ldap`.  |
| ldap_server_host            | No           | The IP address of your LDAP service. Default: `127.0.0.1`.   |
| ldap_server_port            | No           | The port of your LDAP service. Default: `389`.               |
| ldap_bind_base_dn           | Yes          | The base Distinguished Name (DN) of the LDAP user for which the cluster searches. |
| ldap_user_search_attr       | Yes          | The user's attribute used to log in to the LDAP service, for example, `uid`. |
| ldap_user_group_match_attr  | No           | If the user's attribute as the member of a group is different from the user's DN, you must specify this parameter. For example, if a user's DN is `uid=bob,ou=people,o=starrocks,dc=com`, but its attribute as a group member is `memberUid=bob,ou=people,o=starrocks,dc=com`, you need to specify `ldap_user_search_attr` as `uid` and `ldap_user_group_match_attr` as `memberUid`. If this parameter is not specified, the value you specified in `ldap_user_search_attr` is used. You can also specify a regular expression to match the members in the group. The regular expression must be prefixed by `regex:`. Suppose a group has a member `CN=Poornima K Hebbar (phebbar),OU=User Policy 0,OU=All Users,DC=SEA,DC=CORP,DC=EXPECN,DC=com`. If you specify this property as `regex:CN=.*\\(([^)]+)\\)`, it will match the member `phebbar`. |
| ldap_bind_root_dn           | Yes          | The admin DN of your LDAP service.                           |
| ldap_bind_root_pwd          | Yes          | The admin password of your LDAP service.                     |
| ldap_cache_refresh_interval | No           | The interval at which the cluster automatically refreshes the cached LDAP group information. Unit: Seconds. Default: `900`. |
| ldap_ssl_conn_allow_insecure | No          | Whether to use the non-SSL connection to the LDAP server. Default: `true`. Setting this value to `false` indicates to enable LDAP over SSL. For detailed instructions on enabling SSL, see [SSL Authentication](./ssl_authentication.md). |
| ldap_ssl_conn_trust_store_path | No        | The local path that stores the LDAP SSL certificate.         |
| ldap_ssl_conn_trust_store_pwd | No         | The password used to access the LDAP SSL certificate that is stored locally. |
| group_provider              | No           | The name of the group provider(s) to be combined with the security integration. Multiple group providers are separated by commas. Once set, StarRocks will record the user's group information under each specified provider upon login. Supported from v3.5 onwards. For detailed instructions on enabling Group Provider, see [Authenticate User Groups](./group_provider.md). |
| authenticated_group_list    | No           | The name of group(s) whose members are allowed to log in to StarRocks. Multiple groups are separated by commas. Make sure that the specified groups can be retrieved by the combined group provider(s). Supported from v3.5 onwards. |
| comment                     | No           | The description of the security integration.                 |

### Create a security integration with OIDC

Syntax:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oidc",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

Parameters:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`. |
| type                        | Yes          | The type of the security integration. Specify it as `oidc`.  |
| jwks_url                    | Yes          | The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `fe/conf` directory. |
| principal_field             | Yes          | The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks. |
| required_issuer             | No           | The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer. |
| required_audience           | No           | The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience. |
| comment                     | No           | The description of the security integration.                 |

### Create a security integration with OAuth 2.0

Syntax:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oauth2",
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

Parameters:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | The name of the security integration.<br />**NOTE**<br />The security integration name is globally unique. You cannot specify this parameter as `native`. |
| auth_server_url             | Yes          | The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process. |
| token_server_url            | Yes          | The URL of the endpoint on the authorization server from which StarRocks obtains the access token. |
| client_id                   | Yes          | The public identifier of the StarRocks client.               |
| client_secret               | Yes          | The secret used to authorize StarRocks client with the authorization server. |
| redirect_url                | Yes          | The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`. |
| type                        | Yes          | The type of the security integration. Specify it as `oauth2`.  |
| jwks_url                    | Yes          | The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `fe/conf` directory. |
| principal_field             | Yes          | The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks. |
| required_issuer             | No           | The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer. |
| required_audience           | No           | The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience. |
| comment                     | No           | The description of the security integration.                 |

## Configure authentication chain

After the security integration is created, it is added to your StarRocks cluster as a new authentication method. You must enable the security integration by setting the order of the authentication methods via the FE dynamic configuration item `authentication_chain`. In this case, you need to set the security integration as the preferred authentication method and then the native authentication of the StarRocks cluster.

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>, native"
);
```

> **NOTE**
>
> - If `authentication_chain` is not specified, only the native authentication is enabled.
> - Once `authentication_chain` is set, StarRocks first verifies the user login with the top preferred authentication method. If a login fails with the preferred authentication method, the cluster follows the specified order to try the next authentication method.

You can check the value of `authentication_chain` using the following statement:

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

## Manage security integrations

You can alter the configuration of an existing security integration using the following statement:

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
PROPERTIES (
    "key"="value"[, ...]
)
```

> **CAUTION**
>
> You cannot alter the `type` of a security integration.

You can drop an existing security integration using the following statement:

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

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
    "type" = "ldap",
    "ldap_server_host"="",
    "ldap_server_port"="",
    "ldap_bind_base_dn"="",
    "ldap_user_search_attr"="",
    "ldap_bind_root_dn"="",
    "ldap_bind_root_pwd"="*****",
    "ldap_cache_refresh_interval"="",
    "comment"=""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

> **NOTE**
>
> `ldap_bind_root_pwd` is masked when SHOW CREATE SECURITY INTEGRATION is executed.

## See also

- For instructions on how to manually authenticate users via LDAP in StarRocks, see [LDAP Authentication](./ldap_authentication.md).
- For instructions on how to manually authenticate users via OpenID Connect in StarRocks, see [OpenID Connect Authentication](./oidc_authentication.md).
- For instructions on how to manually authenticate users via OAuth 2.0 in StarRocks, see [OAuth 2.0 Authentication](./oauth2_authentication.md).