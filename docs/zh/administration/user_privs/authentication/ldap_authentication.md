---
displayed_sidebar: docs
sidebar_position: 30
---

# LDAP 认证

除了原生的基于密码的认证，StarRocks 还支持 LDAP 认证。

本主题描述了如何在 StarRocks 中使用 LDAP 手动创建和认证用户。有关如何使用安全集成将 StarRocks 与您的 LDAP 服务集成的说明，请参见[Authenticate with Security Integration](./security_integration.md)。有关如何在 LDAP 服务中认证用户组的更多信息，请参见[Authenticate User Groups](../group_provider.md)。

## 启用 LDAP 认证

要使用 LDAP 认证，您需要首先将 LDAP 服务添加到 FE 节点配置中。

```Properties
# 添加 LDAP 服务的 IP 地址。
authentication_ldap_simple_server_host =
# 添加 LDAP 服务的端口，默认值为 389。
authentication_ldap_simple_server_port =
# 是否允许使用非加密方式连接到 LDAP 服务器。默认值：`true`。将此值设置为 `false` 表示访问 LDAP 需要使用 SSL 加密。
authentication_ldap_simple_ssl_conn_allow_insecure = 
# 存储 LDAP 服务器的 SSL CA 证书的本地路径。支持 pem 和 jks 格式。如果证书是由受信机构颁发的，则无需配置。
authentication_ldap_simple_ssl_conn_trust_store_path = 
# 访问本地存储的 LDAP 服务器的 SSL CA 证书所用的密码。pem 格式证书不需要密码，只有 jsk 格式证书需要。
authentication_ldap_simple_ssl_conn_trust_store_pwd = 
```

如果希望通过 StarRocks 直接在 LDAP 系统中检索用户进行认证，您需要**添加以下额外的配置项**。

```Properties
# 添加用户的 Base DN，指定用户的检索范围。
authentication_ldap_simple_bind_base_dn =
# 添加在 LDAP 对象中标识用户的属性名称。默认：uid。
authentication_ldap_simple_user_search_attr =
# 添加用于检索用户的 Admin DN。
authentication_ldap_simple_bind_root_dn =
# 添加用于检索用户的 Admin 密码。
authentication_ldap_simple_bind_root_pwd =
```

## DN 匹配机制

自 v3.5.0 起，StarRocks 支持在 LDAP 认证过程中记录和传递用户的 Distinguished Name (DN) 信息，以提供更准确的组解析功能。

### 工作原理

1. **认证阶段**：LDAPAuthProvider 在用户认证成功后会同时记录：
   - 登录用户名（用于传统组匹配）
   - 用户的完整 DN（用于基于 DN 的组匹配）

2. **组解析阶段**：LDAPGroupProvider 根据 `ldap_user_search_attr` 参数的配置决定匹配策略：
   - **如配置了 `ldap_user_search_attr`**，则使用用户名作为组匹配的 Key。
   - **如未配置 `ldap_user_search_attr`**，则使用 DN 作为组匹配的 Key。

### 适用场景

- **传统 LDAP 环境**：组成员使用简单用户名（如 `cn` 属性）。管理员需配置 `ldap_user_search_attr` 参数。
- **Microsoft AD 环境**：组成员可能缺少用户名属性，无法配置 `ldap_user_search_attr` 参数，则直接使用 DN 匹配。
- **混合环境**：支持两种匹配方式的灵活切换。

## 使用 LDAP 创建用户

创建用户时，通过 `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'` 指定认证方式为 LDAP 认证。xxx 是用户在 LDAP 中的 DN（Distinguished Name）。

示例 1：

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
```

可以在不指定用户在 LDAP 中的 DN 的情况下创建用户。当用户登录时，StarRocks 将到 LDAP 系统中检索用户信息。如果只有一个匹配项，认证成功。

示例 2：

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
```

在这种情况下，需要在 FE 中添加额外的配置

- `authentication_ldap_simple_bind_base_dn`: 用户的 Base DN，指定用户的检索范围。
- `authentication_ldap_simple_user_search_attr`: 在 LDAP 对象中标识用户的属性名称，默认为 uid。
- `authentication_ldap_simple_bind_root_dn`: 用于检索用户信息的管理员账户的 DN。
- `authentication_ldap_simple_bind_root_pwd`: 用于检索用户信息的管理员账户的密码。

## 认证用户

LDAP 认证要求客户端将明文密码传递给 StarRocks。有三种方式传递明文密码：

### 从 MySQL 客户端连接 LDAP

执行时添加 `--default-auth mysql_clear_password --enable-cleartext-plugin`：

```sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

### 从 JDBC/ODBC 客户端连接 LDAP

- **JDBC**

注意：使用JDBC链接时，Server 端必须要启用 SSL。SSL 配置请参考 [SSL 认证](../ssl_authentication.md)。

JDBC 5：

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
```

JDBC 8：

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin");
```

- **ODBC**

在 ODBC 的 DSN 中添加 `default\_auth=mysql_clear_password` 和 `ENABLE_CLEARTEXT\_PLUGIN=1`，以及用户名和密码。
