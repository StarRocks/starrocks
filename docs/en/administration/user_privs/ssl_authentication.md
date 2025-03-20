---
displayed_sidebar: docs
---

# SSL Authentication

From v3.4.1 onwards, StarRocks supports secure connections encrypted by SSL. Unlike the traditional cleartext connections to DBMS, SSL connections provide endpoint verification and data encryption to ensure that the data transmitted between clients and StarRocks cannot be read by unauthorized users.

## Enable SSL authentication

### Configure FE nodes

To enable SSL authentication in StarRocks, configure the following parameters in the FE configuration file **fe.conf**:

- `ssl_keystore_location`: Specifies the path to the keystore file that stores the SSL certificate and key.
- `ssl_keystore_password`: The password for accessing the keystore file. StarRocks requires this password to read the keystore file.
- `ssl_key_password`: The password for accessing the key. StarRocks requires this password to retrieve the key from the keystore.

Example:

```Properties
ssl_keystore_location = // Path to the keystore file  
ssl_keystore_password = // Password for the keystore file  
ssl_key_password = // Password for accessing the key  
```

### Generate SSL certificate

In a production environment, it is recommended to use certificates provided by certificate authorities.

In a development environment, you can generate a custom SSL certificate. Use the following command to generate an SSL certificate:

```Bash
keytool -genkeypair -alias starrocks \
    -keypass <ssl_key_password> \
    -keyalg RSA -keysize 1024 -validity 365 \
    -keystore <ssl_keystore_location> \
    -storepass <ssl_keystore_password>
```

Parameters:

- `-keypass`: The key password, corresponding to `ssl_key_password` in  **fe.conf**.
- `-storepass`: The keystore file password, corresponding to `ssl_keystore_password` in  **fe.conf**.
- `-keystore`: The storage path of the keystore file, corresponding to `ssl_keystore_location` in  **fe.conf**.

### Enable SSL on client

StarRocks is compatible with the MySQL protocol. For MySQL clients, SSL authentication is enabled by default.

For JDBC connections, add the following options:

```Properties
useSSL=true
verifyServerCertificate=false
```

## Disable SSL authentication

To disable SSL authentication, follow these steps:

- **MySQL client**: Add the option `--ssl-mode=DISABLED`.
- **JDBC**: Remove `useSSL=true` and `verifyServerCertificate=false`.

## LDAP authentication

See [Authentication methods](./Authentication.md) for detailed instructions on enabling LDAP authentication.

For JDBC connections, since StarRocks supports SSL authentication, you do not need to customize `AuthPlugin`. You can use the built-in `MysqlClearPasswordPlugin`.

- When using JDBC 5 with LDAP authentication, configure the following settings:

  ```Properties
  authenticationPlugins: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.jdbc.authentication.MysqlNativePasswordPlugin
  ```

- When using JDBC 8 with LDAP authentication, configure the following settings:

  ```Properties
  authenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin
  ```

## FAQ

#### Q1: When I connect to StarRocks using DBeaver, an error is returned "Unable to load authentication plugin 'mysql_native_password'".

A: You need to upgrade JDBC 5 to version 5.1.46 or later.
