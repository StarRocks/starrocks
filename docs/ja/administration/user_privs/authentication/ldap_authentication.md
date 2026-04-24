---
displayed_sidebar: docs
sidebar_position: 30
---

# LDAP 認証

import LDAPSSLLink from '../../../_assets/commonMarkdown/ldap_ssl_link.mdx'

StarRocks は、ネイティブなパスワードベースの認証に加えて、LDAP 認証もサポートしています。

このトピックでは、StarRocks で LDAP を使用してユーザーを手動で作成し、認証する方法について説明します。セキュリティインテグレーションを使用して StarRocks を LDAP サービスと統合する方法については、[Authenticate with Security Integration](./security_integration.md) を参照してください。LDAP サービスでユーザーグループを認証する方法については、[Authenticate User Groups](../group_provider.md) を参照してください。

## LDAP 認証を有効にする

LDAP 認証を使用するには、まず LDAP サービスを FE ノードの設定に追加する必要があります。

```Properties
# LDAP サービスの IP アドレスを追加します。
authentication_ldap_simple_server_host =
# LDAP サービスのポートを追加します。デフォルト値は 389 です。
authentication_ldap_simple_server_port =
# LDAP サーバへの暗号化されていない接続を許可するかどうか。デフォルト値: `true`. この値を `false` に設定すると、LDAP へのアクセスに SSL 暗号化が必要であることを示します。
authentication_ldap_simple_ssl_conn_allow_insecure = 
# LDAP サーバーの SSL CA 証明書を格納するローカルパス。pem および jks 形式をサポートします。証明書が信頼できる組織によって発行されている場合は、この項目を設定する必要はありません。
authentication_ldap_simple_ssl_conn_trust_store_path = 
# LDAP サーバーのローカルに保存された SSL CA 証明書にアクセスするために使用されるパスワード。pem 形式の証明書にはパスワードは必要ありません。パスワードが必要なのは jsk 形式の証明書だけです。
authentication_ldap_simple_ssl_conn_trust_store_pwd = 
```

StarRocks が LDAP システム内でユーザーを直接取得する方法でユーザーを認証したい場合（検索バインドモード）は、**以下の追加設定項目を追加する必要があります**。

```Properties
# ユーザーのベース DN を追加し、ユーザーの取得範囲を指定します。
authentication_ldap_simple_bind_base_dn =
# LDAP オブジェクト内でユーザーを識別する属性の名前を追加します。デフォルトは uid です。
authentication_ldap_simple_user_search_attr =
# ユーザーを取得するための管理者 DN を追加します。
authentication_ldap_simple_bind_root_dn =
# ユーザーを取得するための管理者パスワードを追加します。
authentication_ldap_simple_bind_root_pwd =
```

**ダイレクトバインドモード**（検索ステップをスキップし、構築した DN で直接バインドする）を使用する場合は、DN パターンを設定できます。ユーザー DN の構造が予測可能な場合に便利です。

```Properties
# ダイレクトバインド認証の DN パターン。
# ユーザー名のプレースホルダーとして ${USER} を使用します。
# 複数のパターンはセミコロン ';' で区切ります。
authentication_ldap_simple_bind_dn_pattern =
```

例: `uid=${USER},ou=People,dc=example,dc=com`

ユーザーが複数の OU に分散している場合は、セミコロンで区切って複数のパターンを指定できます:

`uid=${USER},ou=Engineering,dc=example,dc=com;uid=${USER},ou=Marketing,dc=example,dc=com`

システムは各パターンを順番に試行し、最初にバインドに成功した結果を返します。

:::note

パターンは有効な LDAP Distinguished Name（DN）を生成する必要があります。`${USER}@corp.example.com` のような UPN 形式のパターンはサポートされていません。結果が DN ではないため、ダウンストリームのグループ検索が失敗します。DN の属性値に `@` が含まれる場合（例: `uid=${USER}@corp.example.com,ou=People,dc=example,dc=com`）は有効です。

:::

## DN マッチングメカニズム

v3.5.0 以降、StarRocks は LDAP 認証時にユーザーの識別名 (DN) 情報を記録および渡す機能をサポートし、より正確なグループ解決を実現します。

### 動作原理

1. **認証フェーズ**: LDAPAuthProviderは、ユーザー認証成功後に以下の2つの情報を記録します：
   - ログインユーザー名（従来のグループマッチング用）
   - ユーザーの完全なDN（DNベースのグループマッチング用）

2. **グループ解決フェーズ**: LDAPGroupProvider は、`ldap_user_search_attr` パラメータの設定に基づいてマッチング戦略を決定します:
   - **`ldap_user_search_attr` が設定されている場合**、グループマッチングのキーとしてユーザー名を使用します。
   - **`ldap_user_search_attr` が設定されていない場合**、グループマッチングのキーとして DN を使用します。

### 使用例

- **従来の LDAP 環境**: グループメンバーは単純なユーザー名（`cn` 属性など）を使用します。管理者は `ldap_user_search_attr` を設定する必要があります。
- **Microsoft AD 環境**: グループメンバーにユーザー名属性が存在しない場合があります。`ldap_user_search_attr` は設定できません。システムは直接 DN を使用して照合を行います。
- **混合環境**: 両方の照合方法を柔軟に切り替えることがサポートされています。

## 認証の優先順位

ユーザーが LDAP 認証でログインする際、StarRocks は以下の優先順位でユーザーの DN を決定します：

1. **ユーザー指定の DN**: ユーザー作成時に明示的な DN が指定されている場合（`CREATE USER ... AS 'dn'`）、その DN が直接使用されます。
2. **DN パターンによるダイレクトバインド**: `authentication_ldap_simple_bind_dn_pattern` が設定されている場合、システムはパターンから DN を構築し、直接バインドを試みます。複数のパターンは順番に試行されます。
3. **検索バインド**: 上記のいずれも該当しない場合、システムは管理者アカウントを使用して LDAP 内のユーザーを検索し、見つかった DN でバインドします。

## LDAP でユーザーを作成する

ユーザーを作成する際、認証方法を LDAP 認証として `IDENTIFIED WITH authentication_ldap_simple AS 'xxx'` と指定します。xxx は LDAP 内のユーザーの DN (Distinguished Name) です。

例 1: 明示的な DN を指定してユーザーを作成する。

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple AS 'uid=tom,ou=company,dc=example,dc=com'
```

例 2: DN を指定せずにユーザーを作成する。設定に応じて、ログイン時に DN パターン（ダイレクトバインド）または検索バインドによって DN が解決されます。

```sql
CREATE USER tom IDENTIFIED WITH authentication_ldap_simple
```

**検索バインド**モードを使用する場合、FE に以下の追加設定が必要です：

- `authentication_ldap_simple_bind_base_dn`: ユーザーのベース DN で、ユーザーの取得範囲を指定します。
- `authentication_ldap_simple_user_search_attr`: LDAP オブジェクト内でユーザーを識別する属性の名前で、デフォルトは uid です。
- `authentication_ldap_simple_bind_root_dn`: ユーザー情報を取得する際に使用する管理者アカウントの DN です。
- `authentication_ldap_simple_bind_root_pwd`: ユーザー情報を取得する際に使用する管理者アカウントのパスワードです。

**ダイレクトバインド**モードを使用する場合は、`authentication_ldap_simple_bind_dn_pattern` を設定するだけで、管理者アカウントは不要です。

## ユーザーを認証する

LDAP 認証では、クライアントがクリアテキストのパスワードを StarRocks に渡す必要があります。クリアテキストのパスワードを渡す方法は 3 つあります。

### MySQL クライアントから LDAP で接続する

実行時に `--default-auth mysql_clear_password --enable-cleartext-plugin` を追加します。

```sql
mysql -utom -P9030 -h127.0.0.1 -p --default-auth mysql_clear_password --enable-cleartext-plugin
```

### JDBC/ODBC クライアントから LDAP で接続する

- **JDBC**

<LDAPSSLLink />

JDBC 5:

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
```

JDBC 8:

```java
Properties properties = new Properties();
properties.put("authenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("defaultAuthenticationPlugin", "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin");
properties.put("disabledAuthenticationPlugins", "com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin");
```

- **ODBC**

ODBC の DSN に `default\_auth=mysql_clear_password` と `ENABLE_CLEARTEXT\_PLUGIN=1` を追加し、ユーザー名とパスワードを指定します。
