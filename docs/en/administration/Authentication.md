# Authentication methods

In addition to the authentication method of "username+password", StarRocks also supports LDAP.

## LDAP Authentication

To use LDAP authentication, you need to add the LDAP service into the FE node configuration first.

* `authentication_ldap_simple_server_host`: Specify the service IP.
* `authentication_ldap_simple_server_port`: Specify the service port, with a default value of 389.

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

LDAP authentication requires the client to pass on a clear-text password to StarRocks. There are three ways to pass on a clear-text password:

* **MySQL command line**

Add `--default-auth mysql_clear_password --enable-cleartext-plugin` when executing:

~~~sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
~~~

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
Properties properties = new Properties();// replace xxx.xxx.xxx to your pacakage name
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
~~~

* **ODBC**

Add `default\_auth=mysql_clear_password` and `ENABLE_CLEARTEXT\_PLUGIN=1` in the DSN of ODBC: , along with username and password.
