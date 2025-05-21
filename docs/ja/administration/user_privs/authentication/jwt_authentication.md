---
displayed_sidebar: docs
sidebar_position: 40
---

# JSON Web Token 認証

このトピックでは、StarRocks で JSON Web Token 認証を有効にする方法について説明します。

v3.5.0 以降、StarRocks は JSON Web Token を使用したクライアントアクセスの認証をサポートしています。

JSON Web Token（JWT）は、当事者間で情報をJSONオブジェクトとして安全に伝送するための、コンパクトで自己完結的な方法を定義したオープンスタンダード（RFC 7519）です。この情報はデジタル署名されているため、検証および信頼することができます。JWTは、（HMACアルゴリズムを使用した）秘密鍵、またはRSAまたはECDSAを使用した公開鍵と秘密鍵のペアを使用して署名することができます。

このトピックでは、StarRocks で JWT 認証を使用してユーザーを手動で作成し、認証する方法について説明します。セキュリティインテグレーションを使用して StarRocks を JWT 認証と統合する方法については、[セキュリティインテグレーションで認証](./security_integration.md) を参照してください。JWT でユーザーグループを認証する方法については、[ユーザーグループの認証](../group_provider.md) を参照してください。

## 前提条件

MySQL クライアントから StarRocks に接続する場合、MySQL クライアントのバージョンは 9.2 以降である必要があります。

## JWT でユーザーを作成する

ユーザーを作成する際、認証方法を JWT として `IDENTIFIED WITH authentication_jwt AS '{xxx}'` を指定します。`{xxx}` はユーザーの JWT プロパティです。

構文:

```SQL
CREATE USER <username> IDENTIFIED WITH authentication_jwt AS 
'{
  "jwks_url": "<jwks_url>",
  "principal_field": "<principal_field>",
  "required_issuer": "<required_issuer>",
  "required_audience": "<required_audience>"
}'
```

プロパティ:

- `jwks_url`: JSON Web Key Set (JWKS) サービスの URL または `fe/conf` ディレクトリ内のローカルファイルへのパス。
- `principal_field`: JWT 内でサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と一致している必要があります。
- `required_issuer` (オプション): JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。
- `required_audience` (オプション): JWT 内の受信者 (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 受信者と一致する場合にのみ、JWT は有効と見なされます。

例:

```SQL
CREATE USER tom IDENTIFIED WITH authentication_jwt AS
'{
  "jwks_url": "http://localhost:38080/realms/master/protocol/jwt/certs",
  "principal_field": "preferred_username",
  "required_issuer": "http://localhost:38080/realms/master",
  "required_audience": "12345"
}';
```

## MySQL クライアントから JWT で接続する

MySQL クライアントから StarRocks に JWT を使用して接続するには、`authentication_openid-connect_client` プラグインを有効にし、必要なトークン（トークンファイルのパスを使用）を渡してマップされたユーザーを認証する必要があります。

構文:

```Bash
mysql -h <hostname> -P <query_port> --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>
```

例:

```Bash
mysql -h 127.0.0.1 -P 9030 --authentication-openid-connect-client-id-token-file=/path/to/token/file -u tom
```