---
displayed_sidebar: docs
sidebar_position: 30
---

# LDAP Authentication

import LDAPSSLLink from '../../../_assets/commonMarkdown/ldap_ssl_link.mdx'

In addition to native password-based authentication, StarRocks also supports the LDAP authentication.

This topic describes how to manually create and authenticate users using LDAP in StarRocks. For instructions on how to integrate StarRocks with your LDAP service using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups in your LDAP service, see [Authenticate User Groups](../group_provider.md).

## Enable LDAD authentication

To use LDAP authentication, you need to add the LDAP service into the FE node configuration first.

```Properties
# Add the LDAP service IP address.
authentication_ldap_simple_server_host =
# Add the LDAP service port, with a default value of 389.
authentication_ldap_simple_server_port =
# Whether to allow non-encrypted connections to the LDAP server. Default value: `true`. Setting this value to `false` indicates that SSL encryption is required to access LDAP.
authentication_ldap_simple_ssl_conn_allow_insecure = 
# Local path to store the SSL CA certificate of the LDAP server. Supports pem and jks formats. You do not need to set this item if the certificate is issued by a trusted organization.
authentication_ldap_simple_ssl_conn_trust_store_path = 
# The password used to access the locally stored SSL CA certificate of the LDAP server. pem-formatted certificates do not require a password. Only jsk-formatted certificates do.
authentication_ldap_simple_ssl_conn_trust_store_pwd = 
```

If you wish to authenticate users by means of StarRocks retrieving them directly in the LDAP system (search-and-bind mode), you will need to **add the following additional configuration items**.

```Properties
# Add the Base DN of the user, specifying the user's retrieval range.
authentication_ldap_simple_bind_base_dn =
# Add the name of the attribute that identifies the user in the LDAP object. Default: uid.
authentication_ldap_simple_user_search_attr =
# Add the admin DN for retrieving users.
authentication_ldap_simple_bind_root_dn =
# Add the admin password for retrieving users.
authentication_ldap_simple_bind_root_pwd =
```

If you wish to use **direct bind mode** (skip the search step and bind directly with a constructed DN), you can configure a DN pattern instead. This is useful when the user DN structure is predictable.

```Properties
# The DN pattern for direct bind authentication.
# Use ${USER} as a placeholder for the username.
# Multiple patterns can be separated by semicolon ';'.
authentication_ldap_simple_bind_dn_pattern =
```

For example: `uid=${USER},ou=People,dc=example,dc=com`

If you have users across multiple OUs, you can specify multiple patterns separated by semicolons:

`uid=${USER},ou=Engineering,dc=example,dc=com;uid=${USER},ou=Marketing,dc=example,dc=com`

The system will try each pattern in order and return the first successful bind.

:::note

The pattern must produce a valid LDAP Distinguished Name (DN). UPN-style patterns like `${USER}@corp.example.com` are not supported, because the result is not a DN and would break downstream group lookups. If your DN contains `@` in an attribute value (e.g., `uid=${USER}@corp.example.com,ou=People,dc=example,dc=com`), that is valid.

:::

## DN Matching Mechanism

Starting from v3.5.0, StarRocks supports recording and passing user Distinguished Name (DN) information during LDAP authentication to provide more accurate group resolution.

### How it Works

1. **Authentication Phase**: LDAPAuthProvider records both pieces of information after successful user authentication:
   - Login username (for traditional group matching)
   - User's complete DN (for DN-based group matching)

2. **Group Resolution Phase**: LDAPGroupProvider determines the matching strategy based on the `ldap_user_search_attr` parameter configuration:
   - **When `ldap_user_search_attr` is configured**, it uses username as the key for group matching.
   - **When `ldap_user_search_attr` is not configured**, it uses DN as the key for group matching.

### Use Cases

- **Traditional LDAP Environment**: Group members use simple usernames (such as `cn` attribute). Administrators need to configure `ldap_user_search_attr`.
- **Microsoft AD Environment**: Group members may lack username attributes. `ldap_user_search_attr` cannot be configured. The system will use DN directly for matching.
- **Mixed Environment**: Flexible switching between both matching methods is supported.

## Authentication priority

When a user logs in with LDAP authentication, StarRocks determines the user's DN using the following priority:

1. **Per-user DN**: If the user was created with an explicit DN (`CREATE USER ... AS 'dn'`), that DN is used directly.
2. **Direct bind via DN pattern**: If `authentication_ldap_simple_bind_dn_pattern` is configured, the system constructs the DN from the pattern and attempts to bind directly. Multiple patterns are tried in order.
3. **Search-and-bind**: If neither of the above applies, the system uses the admin account to search for the user in LDAP, then binds with the found DN.

## Create a user with LDAP

When creating a user, specify the authentication method as LDAP authentication by `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'`. xxx is the DN (Distinguished Name) of the user in LDAP.

Example 1: Create a user with an explicit DN.

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
```

Example 2: Create a user without specifying the DN. The system will resolve the DN at login time using either the DN pattern (direct bind) or search-and-bind, depending on the configuration.

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
```

If using **search-and-bind** mode, the following additional FE configuration is needed:

- `authentication_ldap_simple_bind_base_dn`: The base DN of the user, specifying the retrieval range of the user.
- `authentication_ldap_simple_user_search_attr`: The name of the attribute in the LDAP object that identifies the user, uid by default.
- `authentication_ldap_simple_bind_root_dn`: The DN of the administrator account used to retrieve the user information.
- `authentication_ldap_simple_bind_root_pwd`: The password of the administrator account used when retrieving the user information.

If using **direct bind** mode, configure `authentication_ldap_simple_bind_dn_pattern` instead. This does not require an admin account.

## Authenticate users

LDAP authentication requires the client to pass on a clear-text password to StarRocks. There are three ways to pass on a clear-text password:

### Connect from MySQL client with LDAP

Add `--default-auth mysql_clear_password --enable-cleartext-plugin` when executing:

```sql
mysql -utom -P9030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

### Connect from JDBC/ODBC client with LDAP

- **JDBC**

<LDAPSSLLink />

JDBC 5:

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
```

JDBC 8:

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin");
```

- **ODBC**

Add `default\_auth=mysql_clear_password` and `ENABLE_CLEARTEXT\_PLUGIN=1` in the DSN of ODBC: , along with username and password.
