---
displayed_sidebar: "Chinese"
---

# 设置用户认证

本文介绍如何在 StarRocks 中设置用户认证 (authentication)。

## 设置 LDAP 认证

除传统用户名+密码认证方式外，StarRocks 还支持 Lightweight Directory Access Protocol（LDAP）认证。

### 开启 LDAP 认证

在 FE 节点的配置文件 **fe.conf** 中添加以下配置项。

```conf
# 添加 LDAP 服务 IP 地址。
authentication_ldap_simple_server_host =
# 添加 LDAP 服务端口。
authentication_ldap_simple_server_port =
```

如果您希望通过 StarRocks 直接在 LDAP 系统中检索用户的方式认证登录的用户，您还需要**额外添加以下配置项**。

```conf
# 添加用户的 Base DN，指定用户的检索范围。
authentication_ldap_simple_bind_base_dn =
# 添加 LDAP 对象中标识用户的属性名称，默认为 uid。
authentication_ldap_simple_user_search_attr =
# 添加检索用户时使用的管理员账号 DN。
authentication_ldap_simple_bind_root_dn =
# 添加检索用户时，使用的管理员账号密码。
authentication_ldap_simple_bind_root_pwd =
```

### 创建用户

完成以上配置后，您还需要在 StarRocks 中创建相应用户，并指定其认证方式及认证信息。

```sql
CREATE USER user_identity IDENTIFIED WITH authentication_ldap_simple [AS 'ldap_distinguished_name'];
```

以下示例创建 LDAP 认证用户 zhangsan，LDAP Distinguished Name（DN）为 “uid=zhansan,ou=company,dc=example,dc=com”。

```sql
CREATE USER zhangsan IDENTIFIED WITH authentication_ldap_simple AS 'uid=zhansan,ou=company,dc=example,dc=com'
```

如果您希望通过 StarRocks 直接在 LDAP 系统中检索用户的方式认证登录的用户，则在完成以上**额外配置后**，您无需在创建用户时指定用户在 LDAP 中的 DN。该用户在登录时，StarRocks 将在 LDAP 系统中检索该用户，如果有且仅有一个匹配结果，则认证成功果。

### 认证用户

使用 LDAP 认证时，您需要通过客户端传递明文密码给 StarRocks。

典型客户端配置明文密码传递的方式包括以下三种。

* **MySQL 客户端**

```shell
mysql -u<user_identity> -P<query_port> -h<fe_host> -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

示例：

```shell
mysql -uzhangsan -P9030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

* **JDBC**

由于 JDBC 默认的 MysqlClearPasswordPlugin 需要使用 SSL 传输，所以您需要自定义 plugin。

```java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
    @Override  
    public boolean requiresConfidentiality() {
        return false;
    }
}
```

在获取连接时，您需要将自定义的 plugin 配置到属性中。

```java
...
Properties properties = new Properties();// replace xxx.xxx.xxx to your pacakage name
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
```

* **ODBC**

您需要在 ODBC 的 DSN 中添加以下配置，并配上用户名和密码。

```conf
default_auth = mysql_clear_password
ENABLE_CLEARTEXT_PLUGIN = 1
```

## 设置 自定义方式 认证

StarRocks 提供自定义方式认证。

### 开启 自定义 认证

1. 实现自定义认证抽象类

自定义方式需要用户自己实现相关认证逻辑。用户需实现`AuthenticationProvider`以完成认证。

代码示例：

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

2. 将相关jar包放入 **fe/lib** 下


3. 在 FE 节点的配置文件 **fe.conf** 中添加以下配置项。

```conf
# 添加 自定义 认证方式相关类。
authorization_custom_class = xxx.xxx.xxx
```

### 创建用户

完成以上配置后，您还需要在 StarRocks 中创建相应用户，并指定其认证方式及认证信息。

```sql
CREATE USER user_identity IDENTIFIED WITH authentication_custom;
```


### 认证用户

使用 自定义 认证时，您需要通过客户端传递明文密码给 StarRocks。

传递方式参考上述LDAP认证用户相关内容。