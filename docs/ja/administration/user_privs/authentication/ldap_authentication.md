---
displayed_sidebar: docs
sidebar_position: 30
---

# LDAP 認証

StarRocks は、ネイティブなパスワードベースの認証に加えて、LDAP 認証もサポートしています。

このトピックでは、StarRocks で LDAP を使用してユーザーを手動で作成し、認証する方法について説明します。セキュリティインテグレーションを使用して StarRocks を LDAP サービスと統合する方法については、[Authenticate with Security Integration](./security_integration.md) を参照してください。LDAP サービスでユーザーグループを認証する方法については、[Authenticate User Groups](../group_provider.md) を参照してください。

## LDAP 認証を有効にする

LDAP 認証を使用するには、まず LDAP サービスを FE ノードの設定に追加する必要があります。

```Properties
# LDAP サービスの IP アドレスを追加します。
authentication_ldap_simple_server_host =
# LDAP サービスのポートを追加します。デフォルト値は 389 です。
authentication_ldap_simple_server_port =
```

StarRocks が LDAP システム内でユーザーを直接取得する方法でユーザーを認証したい場合は、**以下の追加設定項目を追加する必要があります**。

```Properties
# ユーザーのベース DN を追加し、ユーザーの取得範囲を指定します。
authentication_ldap_simple_bind_base_dn =
# LDAP オブジェクト内でユーザーを識別する属性の名前を追加します。デフォルトは uid です。
authentication_ldap_simple_user_search_attr =
# ユーザーを取得する際に使用する管理者アカウントの DN を追加します。
authentication_ldap_simple_bind_root_dn =
# ユーザーを取得する際に使用する管理者アカウントのパスワードを追加します。
authentication_ldap_simple_bind_root_pwd =
```

## LDAP でユーザーを作成する

ユーザーを作成する際、認証方法を LDAP 認証として `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'` と指定します。xxx は LDAP 内のユーザーの DN (Distinguished Name) です。

例 1:

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
```

LDAP 内でユーザーの DN を指定せずにユーザーを作成することも可能です。ユーザーがログインすると、StarRocks は LDAP システムにアクセスしてユーザー情報を取得します。1 つだけ一致する場合、認証は成功します。

例 2:

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
```

この場合、FE に追加の設定を追加する必要があります。

- `authentication_ldap_simple_bind_base_dn`: ユーザーのベース DN で、ユーザーの取得範囲を指定します。
- `authentication_ldap_simple_user_search_attr`: LDAP オブジェクト内でユーザーを識別する属性の名前で、デフォルトは uid です。
- `authentication_ldap_simple_bind_root_dn`: ユーザー情報を取得する際に使用する管理者アカウントの DN です。
- `authentication_ldap_simple_bind_root_pwd`: ユーザー情報を取得する際に使用する管理者アカウントのパスワードです。

## ユーザーを認証する

LDAP 認証では、クライアントがクリアテキストのパスワードを StarRocks に渡す必要があります。クリアテキストのパスワードを渡す方法は 3 つあります。

### MySQL クライアントから LDAP で接続する

実行時に `--default-auth mysql_clear_password --enable-cleartext-plugin` を追加します。

```sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

### JDBC クライアントから LDAP で接続する

- **JDBC**

JDBC のデフォルトの MysqlClearPasswordPlugin は SSL トランスポートを必要とするため、カスタムプラグインが必要です。

```java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
    @Override  
    public boolean requiresConfidentiality() {
        return false;
    }
}
```

接続後、カスタムプラグインをプロパティに設定します。

```java
...
Properties properties = new Properties();// パッケージ名を xxx.xxx.xxx に置き換えます
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
```

- **ODBC**

ODBC の DSN に `default\_auth=mysql_clear_password` と `ENABLE_CLEARTEXT\_PLUGIN=1` を追加し、ユーザー名とパスワードを指定します。