# 认证方式

除“用户名+密码”认证方式外，StarRocks还支持LDAP用户。

## LDAP认证

使用LDAP认证时，首先需要将LDAP服务信息配置到 FE 节点的配置中。

* authentication\_ldap\_simple\_server\_host：指定服务IP。
* authentication\_ldap\_simple\_server\_port：指定服务端口，默认为389。

创建用户时通过`IDENTIFIED WITH authentication_ldap_simple AS 'xxx'` 指定该用户的认证方式为LDAP认证。xxx为用户在LDAP中的DN(Distinguished Name)

例如：

~~~sql
CREATE USER zhangsan IDENTIFIED WITH authentication_ldap_simple AS 'uid=zhansan,ou=company,dc=example,dc=com'
~~~

同时，创建用户时也可以不指定用户在LDAP中的DN

例如：

~~~sql
CREATE USER zhangsan IDENTIFIED WITH authentication_ldap_simple
~~~

不指定DN的用户，登录时StarRocks会去LDAP系统中检索该用户，如果有且仅有一个匹配结果，则认证成功果。这种情况下，需要在FE中添加额外的配置：

* authentication\_ldap\_simple\_bind\_base\_dn：用户的base DN，指定用户的检索范围。
* authentication\_ldap\_simple\_user\_search\_attr：LDAP对象中标识用户的属性名称，默认为uid。
* authentication\_ldap\_simple\_bind\_root\_dn：检索用户时，使用的管理员账号DN。
* authentication\_ldap\_simple\_bind\_root\_pwd：检索用户时，使用的管理员账号密码。

LDAP认证需要客户端传递明文密码给StarRocks。三种典型客户端配置明文密码传递的方式如下。

* **mysql 命令行**

执行时添加 --default-auth mysql\_clear\_password --enable-cleartext-plugin 选项，例如：

~~~sql
mysql -uzhangsan -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
~~~

* **JDBC**

由于JDBC默认的MysqlClearPasswordPlugin需要使用SSL传输，所以需要自定义plugin：

~~~java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
    @Override  
    public boolean requiresConfidentiality() {
        return false;
    }
}
~~~

在获取连接时，将自定义的plugin配置到属性中：

~~~java
...
Properties properties = new Properties();// replace xxx.xxx.xxx to your pacakage name
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
~~~

* **ODBC**

在ODBC的DSN中添加配置：default\_auth=mysql\_clear\_password和ENABLE\_CLEARTEXT\_PLUGIN=1，并配上用户名和密码。
