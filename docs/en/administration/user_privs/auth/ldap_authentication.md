---
displayed_sidebar: docs
sidebar_position: 10
---

# LDAP Authentication

In addition to the authentication method of "username+password", StarRocks also supports LDAP.

This topic describes how to manually create and authenticate users using LDAP in StarRocks. For instructions on how to integrate StarRocks with your LDAP service using security integration, see [Authenticate with Security Integration](./security_integration.md). For more information on how to authenticate user groups in your LDAP service, see [Authenticate User Groups](./group_provider.md).

## Enable LDAD authentication

To use LDAP authentication, you need to add the LDAP service into the FE node configuration first.

```Properties
# Add the LDAP service IP address.
authentication_ldap_simple_server_host =
# Add the LDAP service port, with a default value of 389.
authentication_ldap_simple_server_port =
```

If you wish to authenticate users by means of StarRocks retrieving them directly in the LDAP system, you will need to **add the following additional configuration items**.

```Properties
# Add the Base DN of the user, specifying the user's retrieval range.
authentication_ldap_simple_bind_base_dn =
# Add the name of the attribute that identifies the user in the LDAP object. Default: uid.
authentication_ldap_simple_user_search_attr =
# Add the DN of the administrator account to be used when retrieving users.
authentication_ldap_simple_bind_root_dn =
# Add the password of theAdministrator account to be used when retrieving users.
authentication_ldap_simple_bind_root_pwd =
```

## Create a user with LDAD

When creating a user, specify the authentication method as LDAP authentication by `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'`. xxx is the DN (Distinguished Name) of the user in LDAP.

Example 1:

~~~sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
~~~

It is possible to create a user without specifying the user's DN in LDAP. When the user logs in, StarRocks will go to the LDAP system to retrieve the user information. if there is one and only one match, the authentication is successful.

Example 2:

~~~sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
~~~

In this case, additional configuration needs to be added to the FE

* `authentication_ldap_simple_bind_base_dn`: The base DN of the user, specifying the retrieval range of the user.
* `authentication_ldap_simple_user_search_attr`: The name of the attribute in the LDAP object that identifies the user, uid by default.
* `authentication_ldap_simple_bind_root_dn`: The DN of the administrator account used to retrieve the user information.
* `authentication_ldap_simple_bind_root_pwd`: The password of the administrator account used when retrieving the user information.

## Authenticate users

LDAP authentication requires the client to pass on a clear-text password to StarRocks. There are three ways to pass on a clear-text password:

### Connect from MySQL client with LDAP

Add `--default-auth mysql_clear_password --enable-cleartext-plugin` when executing:

~~~sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
~~~

### Connect from JDBC client with LDAP

* **JDBC**

Since JDBC’s default MysqlClearPasswordPlugin requires  SSL transport, a custom plugin is required.

~~~java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
    @Override  
    public boolean requiresConfidentiality() {
        return false;
    }
}
~~~

Once connected, configure the custom plugin into the property.

~~~java
...
Properties properties = new Properties();// replace xxx.xxx.xxx to your package name
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
~~~

* **ODBC**

Add `default\_auth=mysql_clear_password` and `ENABLE_CLEARTEXT\_PLUGIN=1` in the DSN of ODBC: , along with username and password.
