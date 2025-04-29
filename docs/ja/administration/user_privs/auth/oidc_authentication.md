---
displayed_sidebar: docs
sidebar_position: 30
---

# OpenID Connect 認証

このトピックでは、StarRocks で OpenID Connect 認証を有効にする方法について説明します。

バージョン 3.5.0 以降、StarRocks は OpenID Connect を使用したクライアントアクセスの認証をサポートしています。

OpenID Connect (OIDC) は、OAuth 2.0 フレームワークの上に構築されたアイデンティティレイヤーです。これにより、サードパーティのアプリケーションがエンドユーザーのアイデンティティを確認し、基本的なユーザープロファイル情報を取得することができます。OIDC は JSON ウェブトークン (JWT) を使用し、OAuth 2.0 の仕様に準拠したフローを使用して取得できます。JWT は、証明書に似た暗号情報を含む小さなウェブセーフな JSON オブジェクトで、サブジェクト、有効期間、署名などが含まれます。

このトピックでは、StarRocks で OIDC を使用してユーザーを手動で作成および認証する方法について説明します。セキュリティ統合を使用して StarRocks を OIDC サービスと統合する方法については、[セキュリティ統合で認証](./security_integration.md) を参照してください。OIDC サービスでユーザーグループを認証する方法については、[ユーザーグループの認証](./group_provider.md) を参照してください。

## 前提条件

MySQL クライアントから StarRocks に接続する場合、MySQL クライアントのバージョンは 9.2 以降である必要があります。詳細については、[MySQL 公式ドキュメント](https://dev.mysql.com/doc/refman/9.2/en/openid-pluggable-authentication.html) を参照してください。

## OIDC でユーザーを作成する

ユーザーを作成する際、認証方法を OIDC として `IDENTIFIED WITH authentication_openid_connect AS '{xxx}'` を指定します。`{xxx}` はユーザーの OIDC プロパティです。

構文:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_openid_connect AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

プロパティ:

- `jwks_url`: JSON Web Key Set (JWKS) サービスの URL または `fe/conf` ディレクトリ内のローカルファイルのパス。
- `principal_field`: JWT 内のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一である必要があります。
- `required_issuer` (オプション): JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT の発行者と一致する場合にのみ、JWT は有効と見なされます。
- `required_audience` (オプション): JWT 内のオーディエンス (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT のオーディエンスと一致する場合にのみ、JWT は有効と見なされます。

例:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_openid_connect AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/openid-connect/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```

## MySQL クライアントから OIDC で接続する

MySQL クライアントから StarRocks に OIDC を使用して接続するには、`authentication_openid_connect_client` プラグインを有効にし、必要なトークン（トークンファイルのパスを使用）を渡してマッピングされたユーザーを認証する必要があります。

構文:

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

例:

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```