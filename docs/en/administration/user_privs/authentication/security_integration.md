---
displayed_sidebar: docs
sidebar_position: 20
description: "Integrate StarRocks with external authentication systems using security integration."
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
    "authentication_ldap_simple_user_search_attr" = "",
    "authentication_ldap_simple_bind_root_dn" = "",
    "authentication_ldap_simple_bind_root_pwd" = "",
    "authentication_ldap_simple_bind_dn_pattern" = "",
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

- Required: No
- Description: The base Distinguished Name (DN) of the LDAP user for which the cluster searches. Required when using search-and-bind mode. Not needed when using direct bind mode with `authentication_ldap_simple_bind_dn_pattern`.

##### authentication_ldap_simple_user_search_attr

- Required: No
- Description: The attribute of a user entry that carries the login name. It is interpolated into the search filter of search-and-bind mode, so it must be the attribute whose value the user actually types when logging in. Default: `uid`, the convention on OpenLDAP, where it is usually also the entry's RDN (`uid=alice,ou=People,dc=example,dc=com`). Not used in direct bind mode.

:::note On Active Directory, set this to `sAMAccountName`

An AD entry's RDN is the display name (`CN=Dana Scully,OU=People,DC=company,DC=com`), while the name a user types to log in is its `sAMAccountName` (`dscully`). AD's schema does contain `uid`, but it holds no value unless an administrator populated it, so the default finds nobody and every login fails. Two settings follow from this choice:

- **Use search-and-bind, not direct bind.** `sAMAccountName` is not part of an AD entry's DN, so no `authentication_ldap_simple_bind_dn_pattern` can produce one. Leave that property unset and set `authentication_ldap_simple_bind_base_dn`, `authentication_ldap_simple_bind_root_dn` and `authentication_ldap_simple_bind_root_pwd` instead.
- **A Group Provider cannot match members by `sAMAccountName` either.** An AD group lists its members by DN (`member: CN=Dana Scully,OU=People,...`), and that DN carries no `sAMAccountName`, so a group provider configured with `"ldap_user_search_attr" = "sAMAccountName"` matches nobody. Either leave the group provider's own `ldap_user_search_attr` unset, so that it matches by DN, or read the groups from the user's own entry with `authentication_ldap_simple_group_source = memberof`.

:::

:::note

**DN Passing Mechanism**: LDAP security integration supports DN passing functionality.

- After successful authentication, the system records both the user's login name and complete DN.
- When combined with Group Provider, DN information is automatically passed to the Group Provider.
- If `ldap_user_search_attr` is not configured for the Group Provider, DN will be used for group matching.
- This mechanism is particularly suitable for complex LDAP environments like Microsoft AD.

For more details, see the DN matching mechanism in [Authenticate User Groups](../group_provider.md).

:::

##### authentication_ldap_simple_bind_root_dn

- Required: No
- Description: The admin DN of your LDAP service. Required when using search-and-bind mode.

##### authentication_ldap_simple_bind_root_pwd

- Required: No
- Description: The admin password of your LDAP service. Required when using search-and-bind mode.

##### authentication_ldap_simple_bind_dn_pattern

- Required: No
- Description: The DN pattern for direct bind authentication. Use `${USER}` as a placeholder for the username. The pattern must produce a valid LDAP Distinguished Name (DN); UPN-style patterns like `${USER}@domain` are not supported. For example, `uid=${USER},ou=People,dc=example,dc=com`. Multiple patterns can be separated by semicolons, and the system will try each pattern in order until one succeeds. When this parameter is set, the system skips the search step and directly binds with the constructed DN, so `authentication_ldap_simple_bind_base_dn`, `authentication_ldap_simple_user_search_attr`, `authentication_ldap_simple_bind_root_dn`, and `authentication_ldap_simple_bind_root_pwd` are not required.

##### authentication_ldap_simple_group_source

- Required: No
- Description: Where the groups of an LDAP-authenticated user come from. Valid values:
  - `group_provider` (default): only the group providers configured in `group_provider` are used. This is the behavior of earlier versions.
  - `memberof`: only the group membership attribute of the user's own LDAP entry is used. Group providers configured in `group_provider` are ignored, but the configuration is kept, so switching back only takes one `ALTER SECURITY INTEGRATION`.
  - `both`: the union of the two sources.

  With `memberof` or `both`, a group newly created in the directory takes effect on the next login without any configuration change here. The resolved groups take part in role mapping via `GRANT ... TO EXTERNAL GROUP`, in the `permitted_groups` login check, in `current_group()`, and in Apache Ranger authorization, exactly like the groups of a group provider. The cluster-wide default is the FE configuration item of the same name. Supported from v4.2 onwards.

##### authentication_ldap_simple_memberof_attr

- Required: No
- Description: The name of the attribute on the user entry that carries its group membership, used when `authentication_ldap_simple_group_source` is `memberof` or `both`. Matched ignoring case, like every LDAP attribute name. Default: `memberOf`, which fits Active Directory and OpenLDAP with the `memberof` overlay installed. Oracle Directory Server and 389 Directory Server use `isMemberOf`. The cluster-wide default is the FE configuration item of the same name. Supported from v4.2 onwards.

:::note Reading group membership from the user entry

- Only direct membership is resolved. Nested groups, the Active Directory primary group (`Domain Users` by default), and groups from another domain or forest are not included, because they do not appear in the attribute.
- The group name is the value of the first RDN of each group DN. For example, `CN=SR Analysts,OU=Groups,DC=company,DC=com` becomes `SR Analysts`. The original case is kept.
- The attribute is read on the connection that authentication already opened, so no additional LDAP connection is made. In search-and-bind mode the number of requests does not change at all; in direct bind mode (`authentication_ldap_simple_bind_dn_pattern`) one read request is added, because a bind response cannot carry attributes.
- In direct bind mode the user reads its own attribute. If the directory does not allow that and `authentication_ldap_simple_bind_root_dn` and `authentication_ldap_simple_bind_root_pwd` are set, the FE retries once with that account, so there is no need to change the authentication mode. If neither reader can see the attribute, the group set is empty and the login still succeeds.
- If the group set is empty and `permitted_groups` is set, the user is rejected, because the intersection is necessarily empty.
- The group set is computed at login and stored in the session. A membership change in the directory takes effect on the next login, not in a running session.
- The legacy per-user form `CREATE USER ... IDENTIFIED WITH authentication_ldap_simple AS '<dn>'` does not support this feature. Use a security integration instead.
- `EXECUTE AS` resolves the groups of the **target** identity from that identity's own configuration: a native-password user has no LDAP identity and only gets its group providers, while `EXECUTE AS EXTERNAL USER` uses the first `authentication_ldap_simple` integration of `authentication_chain`. Impersonation never presents the target's password, so the attribute is read with `authentication_ldap_simple_bind_root_dn` and `authentication_ldap_simple_bind_root_pwd`; with no service account configured, only the group providers contribute.

:::

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
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
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
- Description: The field in the JWT that identifies the subject. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks. To extract a value from a nested claim, specify an [RFC 6901 JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) (a path starting with `/`). Use `~1` to escape `/` and `~0` to escape `~` in key names. For example, `/https:~1~1sts.amazonaws.com~1/request_tags/starrocks_user` resolves the `starrocks_user` field nested under the URI-namespaced claim `https://sts.amazonaws.com/`.

##### required_issuer

- Required: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.

##### required_audience

- Required: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

##### group_source

- Required: No
- Description: Where the groups of a user authenticated by this integration come from. Valid values:
  - `group_provider` (default): only the group providers listed in `group_provider` are used. This is the behavior of earlier versions.
  - `claim`: only the claim named by `group_claim` of the verified token is used. Group providers listed in `group_provider` are ignored, but their configuration is kept, so switching back takes one `ALTER SECURITY INTEGRATION`.
  - `both`: the union of the two sources.

  Taking the groups from the token is what lets an identity provider with no LDAP directory grant any privilege at all: the resolved groups take part in role mapping via `GRANT ... TO EXTERNAL GROUP`, in the `permitted_groups` login check, in `current_group()`, and in Apache Ranger authorization, exactly like the groups of a group provider. Supported from v4.2 onwards.

##### group_claim

- Required: Yes when `group_source` is `claim` or `both`. Setting it without `group_source` is refused, because it would have no effect; together with an explicit `group_source` of `group_provider` it is accepted and kept, so the claim can be put in place before an `ALTER SECURITY INTEGRATION` switches the source.
- Description: The claim of the token that holds the user's groups, dot-separated for a claim nested inside another object. For example, `groups` for a top-level claim, `realm_access.roles` for the realm roles of Keycloak, and `resource_access.<client_id>.roles` for its client roles. The claim should hold an array of strings; a single string is accepted as one group. Surrounding whitespace is removed from each name; nothing else is rewritten - not the case, not a leading slash - because the names are handed to Apache Ranger, which matches them exactly. Whether a name matches a `GRANT ... TO EXTERNAL GROUP` or `permitted_groups` entry that differs only in case is decided by the FE configuration item `authentication_group_case_insensitive` (ignoring case by default; set it to `false` if your identity provider distinguishes groups by case).

  There is deliberately no default: the claim an identity provider emits differs per provider, and a wrong guess would let users log in with no privileges instead of failing visibly. The claim is read only after the signature, the required issuer and the required audience have been verified, so the holder of a token cannot choose their own groups. If the claim is absent from a token, the login still succeeds with no groups and the reason is logged rather than reported as an authentication failure - Microsoft Entra ID drops the claim entirely for a user in more than 200 groups, so an absent claim is a state a correct configuration produces. Note that most identity providers do not put groups in a token unless asked to: Keycloak needs a group membership mapper on the client, and Entra ID needs the groups claim added to the token configuration. Supported from v4.2 onwards.

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
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
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
- Description: The field in the JWT that identifies the subject. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks. To extract a value from a nested claim, specify an [RFC 6901 JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) (a path starting with `/`). Use `~1` to escape `/` and `~0` to escape `~` in key names. For example, `/https:~1~1sts.amazonaws.com~1/request_tags/starrocks_user` resolves the `starrocks_user` field nested under the URI-namespaced claim `https://sts.amazonaws.com/`.

##### required_issuer

- Required: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.

##### required_audience

- Required: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.

##### group_source

- Required: No
- Description: Where the groups of a user authenticated by this integration come from. Valid values:
  - `group_provider` (default): only the group providers listed in `group_provider` are used. This is the behavior of earlier versions.
  - `claim`: only the claim named by `group_claim` of the verified token is used. Group providers listed in `group_provider` are ignored, but their configuration is kept, so switching back takes one `ALTER SECURITY INTEGRATION`.
  - `both`: the union of the two sources.

  Taking the groups from the token is what lets an identity provider with no LDAP directory grant any privilege at all: the resolved groups take part in role mapping via `GRANT ... TO EXTERNAL GROUP`, in the `permitted_groups` login check, in `current_group()`, and in Apache Ranger authorization, exactly like the groups of a group provider. For an OAuth 2.0 integration this also covers the browser login over the MySQL protocol: there the token reaches StarRocks only after the user has signed in through the browser, so the groups are read from the ID token when it arrives, and `permitted_groups` is checked on the first statement after the browser login rather than when the connection opens. With `group_source` left at `group_provider`, nothing changes: the groups and the check are decided when the connection opens, as before. Supported from v4.2 onwards.

##### group_claim

- Required: Yes when `group_source` is `claim` or `both`; setting it with any other `group_source` is refused, because it would have no effect.
- Description: The claim of the token that holds the user's groups, dot-separated for a claim nested inside another object. For example, `groups` for a top-level claim, `realm_access.roles` for the realm roles of Keycloak, and `resource_access.<client_id>.roles` for its client roles. The claim should hold an array of strings; a single string is accepted as one group. Surrounding whitespace is removed from each name; nothing else is rewritten - not the case, not a leading slash - because the names are handed to Apache Ranger, which matches them exactly. Whether a name matches a `GRANT ... TO EXTERNAL GROUP` or `permitted_groups` entry that differs only in case is decided by the FE configuration item `authentication_group_case_insensitive` (ignoring case by default; set it to `false` if your identity provider distinguishes groups by case).

  There is deliberately no default: the claim an identity provider emits differs per provider, and a wrong guess would let users log in with no privileges instead of failing visibly. The claim is read only after the signature, the required issuer and the required audience have been verified, so the holder of a token cannot choose their own groups. If the claim is absent from a token, the login still succeeds with no groups and the reason is logged rather than reported as an authentication failure - Microsoft Entra ID drops the claim entirely for a user in more than 200 groups, so an absent claim is a state a correct configuration produces. Note that most identity providers do not put groups in a token unless asked to: Keycloak needs a group membership mapper on the client, and Entra ID needs the groups claim added to the token configuration. Supported from v4.2 onwards.

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

<<<<<<< HEAD
<SecurityIntegrationConnectSeeAlso />
=======
## Connect to StarRocks via a security integration

- For instructions on how to connect to StarRocks via LDAP, see [LDAP Authentication - Connect to StarRocks](./ldap_authentication.md#connect-from-mysql-client-with-ldap).
- For instructions on how to connect to StarRocks via JWT, see [JSON Web Token Authentication - Connect to StarRocks](./jwt_authentication.md#connect-from-mysql-client-with-jwt).
- For instructions on how to connect to StarRocks via OAuth 2.0, see [OAuth 2.0 Authentication - Connect to StarRocks](./oauth2_authentication.md#connect-from-jdbc-client-with-oauth-20).

## See also

- For instructions on how to manually authenticate users via LDAP in StarRocks, see [LDAP Authentication](./ldap_authentication.md).
- For instructions on how to manually authenticate users via JSON Web Token in StarRocks, see [JSON Web Token Authentication](./jwt_authentication.md).
- For instructions on how to manually authenticate users via OAuth 2.0 in StarRocks, see [OAuth 2.0 Authentication](./oauth2_authentication.md).
- For instructions on how to authenticate user groups, see [Authenticate User Groups](../group_provider.md).
>>>>>>> 2b39e08 ([Doc] Merge the authentication and authorization partials into their pages (#83146))
