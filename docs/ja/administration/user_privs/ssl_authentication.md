---
displayed_sidebar: docs
---

# SSL 認証

v3.4.1以降、StarRocks は SSL で暗号化されたセキュアな接続をサポートしています。従来の DBMS への平文接続とは異なり、SSL 接続はエンドポイント認証とデータ暗号化を提供し、クライアントと StarRocks 間で送信されるデータが不正ユーザーに読み取られないようにします。

## SSL 認証の有効化

### FE ノードの構成

StarRocks で SSL 認証を有効にするには、FE 構成ファイル **fe.conf** で以下のパラメータを構成します：

- `ssl_keystore_location`：SSL 証明書とキーを格納するキーストアファイルへのパスを指定します。
- `ssl_keystore_password`：キーストアファイルにアクセスするためのパスワード。StarRocks は、キーストアファイルを読み取るためにこのパスワードを要求します。
- `ssl_key_password`：キーにアクセスするためのパスワード。StarRocks は、キーストアからキーを取得するためにこのパスワードを要求します。

例：

```Properties
ssl_keystore_location = // キーストアファイルへのパス。
ssl_keystore_password = // キーストアファイルのパスワード
ssl_key_password = // キーにアクセスするためのパスワード
```

### SSL 証明書の生成

本番環境では、認証局が提供する証明書を使用することをお勧めします。

開発環境では、カスタム SSL 証明書を生成できます。SSL 証明書を生成するには、以下のコマンドを使用します：

```Bash
keytool -genkeypair -alias starrocks \
    -keypass <ssl_key_password> \
    -keyalg RSA -keysize 1024 -validity 365 \
    -keystore <ssl_keystore_location> \
    -storepass <ssl_keystore_password>
```

パラメータ：

- `-keypass`：鍵のパスワード。**fe.conf** の `ssl_key_password` に対応します。
- `-storepass`：キーストアファイルのパスワード。**fe.conf** の `ssl_keystore_password` に対応します。
- `-keystore`：**fe.conf** の `ssl_keystore_location` に対応するキーストアファイルの保存パス。

### クライアントで SSL を有効にする

StarRocks は MySQL プロトコルと互換性があります。MySQL クライアントの場合、SSL 認証はデフォルトで有効になっています。

JDBC 接続の場合は、以下のオプションを追加します：

```Properties
useSSL=true
verifyServerCertificate=false
```

## SSL 認証を無効にする

SSL認証を無効にするには、以下の手順に従う：

- **MySQL クライアント**：`--ssl-mode=DISABLED` オプションを追加する。
- **JDBC**：`useSSL=true` および `verifyServerCertificate=false` を削除する。

## LDAP 認証

LDAP 認証を有効にする方法の詳細については、[認証方法](./Authentication.md)を参照してください。

JDBC 接続については、 StarRocks は SSL 認証をサポートしているので、`AuthPlugin` をカスタマイズする必要はありません。組み込みの `MysqlClearPasswordPlugin` を使用できます。

- LDAP 認証で JDBC 5 を使用する場合は、以下の設定を行います：

  ```Properties
  authenticationPlugins: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.jdbc.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.jdbc.authentication.MysqlNativePasswordPlugin
  ```

- LDAP 認証で JDBC 8 を使用する場合は、以下の設定を行います：

  ```Properties
  authenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  defaultAuthenticationPlugin: com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin
  disabledAuthenticationPlugins: com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin
  ```

## FAQ

#### Q1: DBeaver を使って StarRocks に接続すると、"Unable to load authentication plugin 'mysql_native_password'" というエラーが返ってきます。

A: JDBC 5 をバージョン 5.1.46 以降にアップグレードする必要があります。
