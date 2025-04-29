---
displayed_sidebar: docs
sidebar_position: 50
---

# セキュリティ統合で認証

このトピックでは、StarRocks を外部認証システムと統合する方法について説明します。

StarRocks クラスター内でセキュリティ統合を作成することで、外部認証サービスが StarRocks にアクセスできるようになります。StarRocks は認証サービスのグループメンバーシップ情報をキャッシュし、必要に応じてキャッシュを更新します。

## セキュリティ統合を作成する

現在、StarRocks のセキュリティ統合は以下の認証システムをサポートしています:
- LDAP
- OpenID Connect (OIDC)
- OAuth 2.0

> **注意**
>
> StarRocks は、セキュリティ統合を作成する際に接続性チェックを提供しません。

### LDAP を使用したセキュリティ統合の作成

構文:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "ldap",
    "ldap_server_host" = "",
    "ldap_server_port" = "",
    "ldap_bind_base_dn" = "",
    "ldap_user_search_attr" = "",
    "ldap_user_group_match_attr" = "",
    "ldap_bind_root_dn" = "",
    "ldap_bind_root_pwd" = "",
    "ldap_cache_refresh_interval" = "",
    "ldap_ssl_conn_allow_insecure" = "{true | false}",
    "ldap_ssl_conn_trust_store_path" = "",
    "ldap_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

パラメータ:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | セキュリティ統合の名前。<br />**注意**<br />セキュリティ統合名はグローバルに一意です。このパラメータを `native` として指定することはできません。 |
| type                        | Yes          | セキュリティ統合のタイプ。`ldap` として指定します。          |
| ldap_server_host            | No           | LDAP サービスの IP アドレス。デフォルト: `127.0.0.1`。       |
| ldap_server_port            | No           | LDAP サービスのポート。デフォルト: `389`。                   |
| ldap_bind_base_dn           | Yes          | クラスターが検索する LDAP ユーザーの基本識別名 (DN)。        |
| ldap_user_search_attr       | Yes          | LDAP サービスにログインするために使用されるユーザーの属性。例: `uid`。 |
| ldap_user_group_match_attr  | No           | ユーザーがグループのメンバーとしての属性がユーザーの DN と異なる場合、このパラメータを指定する必要があります。例えば、ユーザーの DN が `uid=bob,ou=people,o=starrocks,dc=com` で、グループメンバーとしての属性が `memberUid=bob,ou=people,o=starrocks,dc=com` の場合、`ldap_user_search_attr` を `uid` として指定し、`ldap_user_group_match_attr` を `memberUid` として指定する必要があります。このパラメータが指定されていない場合、`ldap_user_search_attr` に指定された値が使用されます。また、グループ内のメンバーを一致させるために正規表現を指定することもできます。正規表現は `regex:` で始める必要があります。例えば、グループに `CN=Poornima K Hebbar (phebbar),OU=User Policy 0,OU=All Users,DC=SEA,DC=CORP,DC=EXPECN,DC=com` というメンバーがいる場合、このプロパティを `regex:CN=.*\\(([^)]+)\\)` として指定すると、メンバー `phebbar` に一致します。 |
| ldap_bind_root_dn           | Yes          | LDAP サービスの管理者 DN。                                   |
| ldap_bind_root_pwd          | Yes          | LDAP サービスの管理者パスワード。                             |
| ldap_cache_refresh_interval | No           | クラスターがキャッシュされた LDAP グループ情報を自動的に更新する間隔。単位: 秒。デフォルト: `900`。 |
| ldap_ssl_conn_allow_insecure | No          | LDAP サーバーへの非 SSL 接続を使用するかどうか。デフォルト: `true`。この値を `false` に設定すると、LDAP over SSL が有効になります。SSL を有効にする詳細な手順については、[SSL Authentication](./ssl_authentication.md) を参照してください。 |
| ldap_ssl_conn_trust_store_path | No        | LDAP SSL 証明書を保存するローカルパス。                       |
| ldap_ssl_conn_trust_store_pwd | No         | ローカルに保存された LDAP SSL 証明書にアクセスするために使用されるパスワード。 |
| group_provider              | No           | セキュリティ統合と組み合わせるグループプロバイダーの名前。複数のグループプロバイダーはカンマで区切られます。設定されると、StarRocks はログイン時に各指定されたプロバイダーの下でユーザーのグループ情報を記録します。v3.5 以降でサポートされています。Group Provider を有効にする詳細な手順については、[Authenticate User Groups](./group_provider.md) を参照してください。 |
| authenticated_group_list    | No           | StarRocks にログインを許可されているグループの名前。複数のグループはカンマで区切られます。指定されたグループが組み合わせたグループプロバイダーによって取得できることを確認してください。v3.5 以降でサポートされています。 |
| comment                     | No           | セキュリティ統合の説明。                                     |

### OIDC を使用したセキュリティ統合の作成

構文:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oidc",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

パラメータ:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | セキュリティ統合の名前。<br />**注意**<br />セキュリティ統合名はグローバルに一意です。このパラメータを `native` として指定することはできません。 |
| type                        | Yes          | セキュリティ統合のタイプ。`oidc` として指定します。          |
| jwks_url                    | Yes          | JSON Web Key Set (JWKS) サービスの URL または `fe/conf` ディレクトリ内のローカルファイルへのパス。 |
| principal_field             | Yes          | JWT 内のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。 |
| required_issuer             | No           | JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。 |
| required_audience           | No           | JWT 内のオーディエンス (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT オーディエンスと一致する場合にのみ、JWT は有効と見なされます。 |
| comment                     | No           | セキュリティ統合の説明。                                     |

### OAuth 2.0 を使用したセキュリティ統合の作成

構文:

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oauth2",
    "auth_server_url" = "",
    "token_server_url" = "",
    "client_id" = "",
    "client_secret" = "",
    "redirect_url" = "",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

パラメータ:

| **Parameter**               | **Required** | **Description**                                              |
| --------------------------- | ------------ | ------------------------------------------------------------ |
| security_integration_name   | Yes          | セキュリティ統合の名前。<br />**注意**<br />セキュリティ統合名はグローバルに一意です。このパラメータを `native` として指定することはできません。 |
| auth_server_url             | Yes          | 認可 URL。OAuth 2.0 認可プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。 |
| token_server_url            | Yes          | StarRocks がアクセストークンを取得するための認可サーバーのエンドポイントの URL。 |
| client_id                   | Yes          | StarRocks クライアントの公開識別子。                         |
| client_secret               | Yes          | 認可サーバーで StarRocks クライアントを認可するために使用される秘密。 |
| redirect_url                | Yes          | OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。認可コードはこの URL に送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として設定する必要があります。 |
| type                        | Yes          | セキュリティ統合のタイプ。`oauth2` として指定します。        |
| jwks_url                    | Yes          | JSON Web Key Set (JWKS) サービスの URL または `fe/conf` ディレクトリ内のローカルファイルへのパス。 |
| principal_field             | Yes          | JWT 内のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。 |
| required_issuer             | No           | JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。 |
| required_audience           | No           | JWT 内のオーディエンス (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT オーディエンスと一致する場合にのみ、JWT は有効と見なされます。 |
| comment                     | No           | セキュリティ統合の説明。                                     |

## 認証チェーンの設定

セキュリティ統合が作成されると、新しい認証方法として StarRocks クラスターに追加されます。FE ダイナミック設定項目 `authentication_chain` を介して認証方法の順序を設定することで、セキュリティ統合を有効にする必要があります。この場合、セキュリティ統合を優先認証方法として設定し、その後に StarRocks クラスターのネイティブ認証を設定します。

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>, native"
);
```

> **注意**
>
> - `authentication_chain` が指定されていない場合、ネイティブ認証のみが有効になります。
> - `authentication_chain` が設定されると、StarRocks は最も優先される認証方法でユーザーログインを最初に検証します。優先認証方法でのログインが失敗した場合、クラスターは指定された順序に従って次の認証方法を試みます。

以下のステートメントを使用して `authentication_chain` の値を確認できます:

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

## セキュリティ統合の管理

既存のセキュリティ統合の設定を以下のステートメントを使用して変更できます:

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
PROPERTIES (
    "key"="value"[, ...]
)
```

> **注意**
>
> セキュリティ統合の `type` を変更することはできません。

既存のセキュリティ統合を以下のステートメントを使用して削除できます:

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

クラスター内のすべてのセキュリティ統合を以下のステートメントを使用して表示できます:

```SQL
SHOW SECURITY INTEGRATIONS;
```

例:

```Plain
SHOW SECURITY INTEGRATIONS;
+--------+--------+---------+
| Name   | Type   | Comment |
+--------+--------+---------+
| LDAP1  | LDAP   | NULL    |
+--------+--------+---------+
```

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Name          | セキュリティ統合の名前。                                     |
| Type          | セキュリティ統合のタイプ。                                   |
| Comment       | セキュリティ統合の説明。セキュリティ統合に説明が指定されていない場合、`NULL` が返されます。 |

以下のステートメントを使用してセキュリティ統合の詳細を確認できます:

```SQL
SHOW CREATE SECURITY INTEGRATION <integration_name>
```

例:

```Plain
SHOW CREATE SECURITY INTEGRATION LDAP1；

+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Security Integration  | Create Security Integration                                                                                                                                                                                                                                                                                                                                                                              |
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| LDAP1                | CREATE SECURITY INTEGRATION LDAP1
    PROPERTIES (
    "type" = "ldap",
    "ldap_server_host"="",
    "ldap_server_port"="",
    "ldap_bind_base_dn"="",
    "ldap_user_search_attr"="",
    "ldap_bind_root_dn"="",
    "ldap_bind_root_pwd"="*****",
    "ldap_cache_refresh_interval"="",
    "comment"=""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

> **注意**
>
> `ldap_bind_root_pwd` は、SHOW CREATE SECURITY INTEGRATION が実行されたときにマスクされます。

## 参照

- StarRocks で LDAP を介してユーザーを手動で認証する手順については、[LDAP Authentication](./ldap_authentication.md) を参照してください。
- StarRocks で OpenID Connect を介してユーザーを手動で認証する手順については、[OpenID Connect Authentication](./oidc_authentication.md) を参照してください。
- StarRocks で OAuth 2.0 を介してユーザーを手動で認証する手順については、[OAuth 2.0 Authentication](./oauth2_authentication.md) を参照してください。