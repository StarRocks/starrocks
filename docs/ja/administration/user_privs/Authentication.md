---
displayed_sidebar: docs
---

# 認証方法

"username+password" の認証方法に加えて、StarRocks は LDAP もサポートしています。

## LDAP 認証

LDAP 認証を使用するには、まず FE ノードの設定に LDAP サービスを追加する必要があります。

* `authentication_ldap_simple_server_host`: サービスの IP を指定します。
* `authentication_ldap_simple_server_port`: サービスのポートを指定します。デフォルト値は 389 です。

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

LDAP 認証では、クライアントが StarRocks にプレーンテキストのパスワードを渡す必要があります。プレーンテキストのパスワードを渡す方法は3つあります。

* **MySQL コマンドライン**

実行時に `--default-auth mysql_clear_password --enable-cleartext-plugin` を追加します。

~~~sql
mysql -utom -P8030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
~~~

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