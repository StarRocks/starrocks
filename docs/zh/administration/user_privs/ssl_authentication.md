---
displayed_sidebar: docs
---

# SSL 认证

自 v3.4.1 起，StarRocks 支持由 SSL 加密的安全连接。与传统的 DBMS 明文连接不同，SSL 连接提供端点验证和数据加密，确保客户端与 StarRocks 集群之间传输的数据不会被未经授权的用户读取。

## 启用 SSL 认证

### 配置 FE 节点

要在 StarRocks 中启用 SSL 认证，需要在 FE 配置文件 **fe.conf** 中配置以下参数：

- `ssl_keystore_location`：指定存储 SSL 证书和密钥的 keystore 文件路径。
- `ssl_keystore_password`：keystore 文件的访问密码，StarRocks 读取 keystore 文件时需要提供该密码。
- `ssl_key_password`：密钥的访问密码，StarRocks 读取 keystore 文件中的密钥时需要提供该密码。

示例：

```Properties
ssl_keystore_location = // keystore 文件的路径
ssl_keystore_password = // keystore 文件的密码
ssl_key_password = // 密钥的密码
```

### 生成 SSL 证书

在生产环境中，建议使用认证机构颁发的证书。

在开发环境中，可以使用自定义的 SSL 证书。您可以使用以下命令生成自定义 SSL 证书：

```Bash
keytool -genkeypair -alias starrocks \
    -keypass <ssl_key_password> \
    -keyalg RSA -keysize 1024 -validity 365 \
    -keystore <ssl_keystore_location> \
    -storepass <ssl_keystore_password>
```

参数说明：

- `-keypass`：密钥密码，对应 **fe.conf** 中的 `ssl_key_password`。
- `-keystore`：指定 keystore 文件的存储路径，对应 **fe.conf** 中的 `ssl_keystore_location`。示例：`/path/to/starrocks.jks`。
- `-storepass`：keystore 文件的访问密码，对应 **fe.conf** 中的 `ssl_keystore_password`。

### 客户端开启 SSL 认证

StarRocks 兼容 MySQL 协议。对于 MySQL 客户端，SSL 认证默认启用。

对于 JDBC 连接，需要添加以下选项：

```Properties
useSSL=true
verifyServerCertificate=false
```

## 关闭 SSL 认证

如需关闭 SSL 认证，需执行以下操作：

- **MySQL 客户端**：需添加选项 `--ssl-mode=DISABLED`。
- **JDBC 客户端**：需移除 `useSSL=true` 和 `verifyServerCertificate=false`。

## LDAP 认证

LDAP 认证的配置方法已在官方文档 [设置用户认证](./Authentication.md) 中介绍。

对于 JDBC 连接，由于 StarRocks 已支持 SSL 认证，无需自定义 `AuthPlugin`，可直接使用 JDBC 提供的 `MysqlClearPasswordPlugin`。

- 基于 JDBC 5 版本使用 LDAP 认证时，需要添加以下配置：

  ```Properties
  authenticationPlugins: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.jdbc.authentication.MysqlNativePasswordPlugin
  ```

- 基于 JDBC 8 版本使用 LDAP 认证时，需要添加以下配置：

  ```Properties
  authenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin
  ```

## FAQ

#### Q1: 使用 DBeaver 连接 StarRocks 时返回错误 “Unable to load authentication plugin 'mysql_native_password'”.

A: 需要将 JDBC 5 升级至 5.1.46 及以上版本。
