---
displayed_sidebar: docs
sidebar_position: 10
---

# LDAP 認証

"username+password" の認証方法に加えて、StarRocks は LDAP もサポートしています。

このトピックでは、StarRocks で LDAP を使用してユーザーを手動で作成し、認証する方法について説明します。Security Integration を使用して StarRocks を LDAP サービスと統合する方法については、[セキュリティ統合で認証](./security_integration.md) を参照してください。LDAP サービスでユーザーグループを認証する方法については、[ユーザーグループの認証](./group_provider.md) を参照してください。

## LDAD 認証を有効にする

LDAP 認証を使用するには、まず FE ノードの設定に LDAP サービスを追加する必要があります。

```Properties
# LDAP サービスの IP アドレスを追加する。
authentication_ldap_simple_server_host =
# LDAP サービスのポートを追加します。デフォルト値は 389 です。
authentication_ldap_simple_server_port =
```

StarRocks が LDAP システムから直接ユーザーを取得する方法でユーザー認証を行いたい場合は、**以下の追加設定項目を追加する必要があります**。

```Properties
# ユーザーのベース DN を追加し、ユーザーの検索範囲を指定します。
authentication_ldap_simple_bind_base_dn =
# LDAP オブジェクト内でユーザーを識別する属性名を追加します。デフォルトは uid です。
authentication_ldap_simple_user_search_attr =
# ユーザーを検索する際に使用する管理者アカウントの DN を追加します。
authentication_ldap_simple_bind_root_dn =
# ユーザーを検索する際に使用する管理者アカウントのパスワードを追加する。
authentication_ldap_simple_bind_root_pwd =
```

## LDAP 認証でユーザーを作成する

ユーザーを作成する際、認証方法を LDAP 認証として `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'` と指定します。xxx は LDAP 内のユーザーの DN (Distinguished Name) です。

例 1:

~~~sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
~~~

LDAP 内でユーザーの DN を指定せずにユーザーを作成することも可能です。ユーザーがログインすると、StarRocks は LDAP システムにユーザー情報を取得しに行きます。一致するものが一つだけあれば、認証は成功します。

例 2:

~~~sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
~~~

この場合、FE に追加の設定を追加する必要があります。

* `authentication_ldap_simple_bind_base_dn`: ユーザーのベース DN で、ユーザーの取得範囲を指定します。
* `authentication_ldap_simple_user_search_attr`: LDAP オブジェクト内でユーザーを識別する属性の名前。デフォルトは uid です。
* `authentication_ldap_simple_bind_root_dn`: ユーザー情報を取得するために使用する管理者アカウントの DN。
* `authentication_ldap_simple_bind_root_pwd`: ユーザー情報を取得する際に使用する管理者アカウントのパスワード。

## ユーザーを認証する

LDAP 認証では、クライアントが StarRocks にプレーンテキストのパスワードを渡す必要があります。プレーンテキストのパスワードを渡す方法は3つあります。

### LDAP を使った MySQL クライアントからの接続

実行時に `--default-auth mysql_clear_password --enable-cleartext-plugin` を追加します。

~~~sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
~~~

### LDAP を使った JDBC クライアントからの接続

* **JDBC**

JDBC のデフォルトの MysqlClearPasswordPlugin は SSL トランスポートを必要とするため、カスタムプラグインが必要です。

~~~java
public class MysqlClearPasswordPluginWithoutSSL extends MysqlClearPasswordPlugin {
    @Override  
    public boolean requiresConfidentiality() {
        return false;
    }
}
~~~

接続後、カスタムプラグインをプロパティに設定します。

~~~java
...
Properties properties = new Properties();// パッケージ名を xxx.xxx.xxx に置き換えてください
properties.put("authenticationPlugins", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("defaultAuthenticationPlugin", "xxx.xxx.xxx.MysqlClearPasswordPluginWithoutSSL");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");DriverManager.getConnection(url, properties);
~~~

* **ODBC**

ODBC の DSN に `default\_auth=mysql_clear_password` と `ENABLE_CLEARTEXT\_PLUGIN=1` を追加し、ユーザー名とパスワードを含めます。
