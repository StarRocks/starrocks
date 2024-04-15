---
displayed_sidebar: "English"
---

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
Properties properties = new Properties();// replace xxx.xxx.xxx to your package name
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
~~~

* **ODBC**

Add `default\_auth=mysql_clear_password` and `ENABLE_CLEARTEXT\_PLUGIN=1` in the DSN of ODBC: , along with username and password.

## Custom Authentication

StarRocks supports custom authentication.

There are 4 steps to use Custom Authentication.

1. Implement Custom abstract class

Custom Authentication require users to implement relevant authentication logic themselves. 
The user needs to implement `AuthenticationProvider` to complete authentication.

example:

```java
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.mysql.privilege.Password;
import com.starrocks.sql.ast.UserIdentity;

public class Test implements AuthenticationProvider {

    /** used when Create or Alter User through SQL, to check the new password valid. */
    @Override
    public UserAuthenticationInfo validAuthenticationInfo(
            UserIdentity userIdentity, String password, String textForAuthPlugin)
            throws AuthenticationException {
        return null;
    }

    /** used when login. */
    @Override
    public void authenticate(
            String name,
            String host,
            byte[] password,
            byte[] randomString,
            UserAuthenticationInfo authenticationInfo)
            throws AuthenticationException {}

    /** used to upgrade from 2.x. */
    @Override
    public UserAuthenticationInfo upgradedFromPassword(UserIdentity userIdentity, Password password)
            throws AuthenticationException {
        return null;
    }
}

```


2. Put your jar in **fe/lib**


3. Add 2 parameters in **fe.conf**

```conf
# set the order for authentication method. The example below means StarRocks will try to check password through your 
custom initialization first, and try native method which setted when creating user if custom check failed.
authentication_chain = custom,native
# add Custom Authentication Implementation Class。
authorization_custom_class = xxx.xxx.xxx
```

4. Add custom property file if needed. 

You can create a new file named **custom_authentication.conf** in the same directory with **fe.conf**. The content 
syntax is also the same as **fe.conf**.

The way to get Properties in your Custom class:

```java
  Properties prop =
        (Properties) authenticationInfo.extraInfo.get(AuthPlugin.AUTHENTICATION_CUSTOM.name());
```

** Notice **
1. *root* is super user in StarRocks now, and has been checked before Custom checking. So you do not need to check 
   this user.
2. If need plain password, you can refer to LDAP content above.
3. If connect StarRocks with normal method, you can refer to `MysqlPassword` class.
4. Custom Authentication is provided for users who want to keep usernames and passwords the same as other platforms. So you
   do not need create users before using it, which is the different from other native authentication method.
