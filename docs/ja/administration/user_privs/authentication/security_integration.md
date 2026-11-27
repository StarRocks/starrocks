---
displayed_sidebar: docs
description: "セキュリティインテグレーションで外部認証システム（LDAP・OAuth等）と StarRocks を統合してユーザー認証を行います。"
sidebar_position: 20
products: [oss, enterprise, cloud, anywhere]
---

# セキュリティインテグレーションで認証


StarRocks をセキュリティインテグレーションを使用して外部認証システムと統合します。

StarRocks クラスター内でセキュリティインテグレーションを作成することで、外部認証サービスへのアクセスを StarRocks に許可できます。セキュリティインテグレーションを使用すると、StarRocks 内でユーザーを手動で作成する必要がありません。ユーザーが外部 ID を使用してログインしようとすると、StarRocks は `authentication_chain` の設定に従って対応するセキュリティインテグレーションを使用してユーザーを認証します。認証が成功し、ユーザーがログインを許可された後、StarRocks はセッション内に仮想ユーザーを作成し、そのユーザーが後続の操作を実行できるようにします。

セキュリティインテグレーションを使用して外部認証方法を構成する場合は、外部認可を有効にするために [StarRocks を Apache Ranger と統合](../authorization/ranger_plugin.md) する必要があることに注意してください。現在、セキュリティインテグレーションを StarRocks ネイティブ認可と統合することはサポートされていません。

また、StarRocks に [Group Provider](../group_provider.md) を有効にして、外部認証システムのグループ情報にアクセスし、StarRocks でユーザーグループを作成、認証、および認可することができます。

特定のコーナーケースでは、外部認証サービスを使用してユーザーを手動で作成および管理することもサポートされています。詳細な手順については、[See also](#see-also) を参照してください。

## セキュリティインテグレーションを作成する

現在、StarRocks のセキュリティインテグレーションは以下の認証システムをサポートしています:
- LDAP
- OJSON Web Token（JWT）
- OAuth 2.0

:::note
StarRocks はセキュリティインテグレーションを作成する際に接続性チェックを提供しません。
:::

### LDAP を使用したセキュリティインテグレーションの作成

#### 構文

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_ldap_simple",
    "authentication_ldap_simple_server_host" = "",
    "authentication_ldap_simple_server_port" = "",
    "authentication_ldap_simple_bind_base_dn" = "",
    "authentication_ldap_simple_user_search_attr" = "",
    "authentication_ldap_simple_bind_root_dn" = "",
    "authentication_ldap_simple_bind_root_pwd" = "",
    "authentication_ldap_simple_bind_dn_pattern" = "",
    "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
    "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
    "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

#### パラメータ

##### security_integration_name

- 必須: はい
- 説明: セキュリティインテグレーションの名前。<br />**注意**<br />セキュリティインテグレーション名はグローバルに一意です。このパラメータを `native` として指定することはできません。

##### type

- 必須: はい
- 説明: セキュリティインテグレーションのタイプ。`authentication_ldap_simple` として指定します。

##### authentication_ldap_simple_server_host

- 必須: いいえ
- 説明: LDAP サービスの IP アドレス。デフォルト: `127.0.0.1`。

##### authentication_ldap_simple_server_port

- 必須: いいえ
- 説明: LDAP サービスのポート。デフォルト: `389`。

##### authentication_ldap_simple_bind_base_dn

- 必須: いいえ
- 説明: クラスターが検索する LDAP ユーザーの基本識別名 (DN)。検索バインドモードを使用する場合は必須です。ダイレクトバインドモード（`authentication_ldap_simple_bind_dn_pattern` を設定する場合）では不要です。

##### authentication_ldap_simple_user_search_attr

- 必須: いいえ
- 説明: ユーザーエントリー上でログイン名を保持する属性。検索バインドモードの検索フィルターに埋め込まれるため、ユーザーがログイン時に実際に入力する値を保持する属性である必要があります。デフォルト: `uid`。これは OpenLDAP の慣例で、その場合は通常エントリーの RDN でもあります（`uid=alice,ou=People,dc=example,dc=com`）。ダイレクトバインドモードでは使用されません。

:::note Active Directory では `sAMAccountName` を設定してください

AD のエントリーの RDN は表示名（`CN=Dana Scully,OU=People,DC=company,DC=com`）ですが、ユーザーがログイン時に入力するのはその `sAMAccountName`（`dscully`）です。AD のスキーマに `uid` 属性は存在しますが、管理者が値を設定しない限り空のままなので、デフォルト値では誰も見つからず、すべてのログインが失敗します。この選択に伴い、他に 2 つの設定が決まります。

- **ダイレクトバインドではなく検索バインドを使用してください。** `sAMAccountName` は AD エントリーの DN の一部ではないため、`authentication_ldap_simple_bind_dn_pattern` では有効な DN を生成できません。このプロパティは設定せず、代わりに `authentication_ldap_simple_bind_base_dn`、`authentication_ldap_simple_bind_root_dn`、`authentication_ldap_simple_bind_root_pwd` を設定してください。
- **Group Provider も `sAMAccountName` でメンバーを照合できません。** AD のグループはメンバーを DN で列挙し（`member: CN=Dana Scully,OU=People,...`）、その DN に `sAMAccountName` は含まれません。したがって group provider に `"ldap_user_search_attr" = "sAMAccountName"` を設定しても誰にも一致しません。group provider 側の `ldap_user_search_attr` を未設定のままにして DN で照合させるか、`authentication_ldap_simple_group_source = memberof` でユーザー自身のエントリーからグループを読み取ってください。

:::

:::note

**DN パス機構**: LDAP セキュリティインテグレーションは DN パス機能をサポートします。

- 認証成功後、システムはユーザーのログイン名と完全な DN の両方を記録します。
- Group Provider と組み合わせると、DN 情報が自動的にグループプロバイダに渡されます。
- Group Provider で `ldap_user_search_attr` が設定されていない場合、グループマッチングには DN が使用されます。
- このメカニズムは Microsoft AD のような複雑な LDAP 環境に特に適しています。

詳細は[ユーザーグループの認証](../group_provider.md)の DN マッチングメカニズムを参照してください。

:::

##### authentication_ldap_simple_bind_root_dn

- 必須: いいえ
- 説明: LDAP サービスの管理者 DN。検索バインドモードを使用する場合は必須です。

##### authentication_ldap_simple_bind_root_pwd

- 必須: いいえ
- 説明: LDAP サービスの管理者パスワード。検索バインドモードを使用する場合は必須です。

##### authentication_ldap_simple_bind_dn_pattern

- 必須: いいえ
- 説明: ダイレクトバインド認証の DN パターン。ユーザー名のプレースホルダーとして `${USER}` を使用します。パターンは有効な LDAP Distinguished Name（DN）を生成する必要があります。`${USER}@domain` のような UPN 形式はサポートされていません。例: `uid=${USER},ou=People,dc=example,dc=com`。複数のパターンはセミコロンで区切ることができ、システムは成功するまで各パターンを順番に試行します。このパラメータを設定すると、検索ステップがスキップされ、構築した DN で直接バインドされるため、`authentication_ldap_simple_bind_base_dn`、`authentication_ldap_simple_user_search_attr`、`authentication_ldap_simple_bind_root_dn`、`authentication_ldap_simple_bind_root_pwd` は不要です。

##### authentication_ldap_simple_group_source

- 必須: いいえ
- 説明: LDAP で認証されたユーザーのグループの取得元。有効な値:
  - `group_provider`（デフォルト）: `group_provider` で設定された Group Provider のみを使用します。以前のバージョンと同じ動作です。
  - `memberof`: ユーザー自身の LDAP エントリーのグループメンバーシップ属性のみを使用します。`group_provider` で設定された Group Provider は無視されますが設定は保持されるため、元に戻すには `ALTER SECURITY INTEGRATION` 1 回で済みます。
  - `both`: 両方の和集合。

  `memberof` または `both` の場合、ディレクトリー側で新しく作成されたグループは、ここの設定を変更しなくても次回ログイン時に反映されます。解決されたグループは、Group Provider のグループと同様に、`GRANT ... TO EXTERNAL GROUP` によるロールマッピング、`permitted_groups` のログインチェック、`current_group()`、および Apache Ranger の認可に参加します。クラスター全体のデフォルト値は同名の FE 設定項目です。v4.2 以降でサポートされます。

##### authentication_ldap_simple_memberof_attr

- 必須: いいえ
- 説明: `authentication_ldap_simple_group_source` が `memberof` または `both` の場合に使用される、ユーザーエントリー上でグループメンバーシップを保持する属性の名前。すべての LDAP 属性名と同様、大文字小文字を無視して照合されます。デフォルト: `memberOf`。Active Directory および `memberof` overlay を導入した OpenLDAP に適合します。Oracle Directory Server と 389 Directory Server は `isMemberOf` を使用します。クラスター全体のデフォルト値は同名の FE 設定項目です。v4.2 以降でサポートされます。

:::note ユーザーエントリーからのグループメンバーシップの読み取り

- 直接のメンバーシップのみが解決されます。ネストされたグループ、Active Directory のプライマリーグループ（デフォルトは `Domain Users`）、および他のドメインやフォレストのグループは、この属性に現れないため含まれません。
- グループ名は各グループ DN の最初の RDN の値です。例えば `CN=SR Analysts,OU=Groups,DC=company,DC=com` は `SR Analysts` になり、ディレクトリー上の大文字小文字がそのまま保持されます。
- この属性は認証がすでに確立した接続上で読み取られるため、LDAP 接続は追加されません。検索バインドモードではリクエスト数は変わりません。ダイレクトバインドモード（`authentication_ldap_simple_bind_dn_pattern`）では、bind レスポンスが属性を返せないため、読み取りリクエストが 1 つ増えます。
- ダイレクトバインドモードでは、ユーザー自身がこの属性を読み取ります。ディレクトリーがそれを許可しない場合でも、`authentication_ldap_simple_bind_root_dn` と `authentication_ldap_simple_bind_root_pwd` が設定されていれば、FE はそのアカウントで 1 回だけ再試行するため、認証モードを変更する必要はありません。どちらの読み取り主体も属性を参照できない場合、グループは空になり、ログインは成功します。
- グループが空で `permitted_groups` が設定されている場合、積集合が必ず空になるため、そのユーザーはログインを拒否されます。
- グループはログイン時に一度計算され、セッションに保存されます。ディレクトリー側のメンバーシップ変更は次回ログイン時に反映され、実行中のセッションには影響しません。
- 旧来のユーザー単位の形式 `CREATE USER ... IDENTIFIED WITH authentication_ldap_simple AS '<dn>'` はこの機能をサポートしません。代わりに security integration を使用してください。
- `EXECUTE AS` は**対象**の identity のグループを、その identity 自身の設定に従って解決します。native パスワードのユーザーは LDAP identity を持たないため group provider のグループのみを取得し、`EXECUTE AS EXTERNAL USER` は `authentication_chain` の最初の `authentication_ldap_simple` 統合を使用します。impersonation は対象ユーザーのパスワードを提示しないため、この属性は `authentication_ldap_simple_bind_root_dn` と `authentication_ldap_simple_bind_root_pwd` で読み取られます。サービスアカウントが設定されていない場合は group provider のみが有効です。

:::

##### authentication_ldap_simple_ssl_conn_allow_insecure

- 必須: いいえ
- 説明: LDAP サーバへの暗号化されていない接続を許可するかどうか。デフォルト値: `true`. この値を `false` に設定すると、LDAP へのアクセスに SSL 暗号化が必要であることを示します。

##### authentication_ldap_simple_ssl_conn_trust_store_path

- 必須: いいえ
- 説明: LDAP サーバーの SSL CA 証明書を格納するローカルパス。pem および jks 形式をサポートします。証明書が信頼できる組織によって発行されている場合は、この項目を設定する必要はありません。

##### authentication_ldap_simple_ssl_conn_trust_store_pwd

- 必須: いいえ
- 説明: LDAP サーバーのローカルに保存された SSL CA 証明書にアクセスするために使用されるパスワード。pem 形式の証明書にはパスワードは必要ありません。パスワードが必要なのは jsk 形式の証明書だけです。

##### group_provider

- 必須: いいえ
- 説明: セキュリティインテグレーションと組み合わせる Group Provider の名前。複数の Group Provider はカンマで区切られます。設定されると、StarRocks はログイン時に各指定プロバイダーの下でユーザーのグループ情報を記録します。v3.5 以降でサポートされています。Group Provider を有効にする詳細な手順については、[Authenticate User Groups](../group_provider.md) を参照してください。

##### permitted_groups

- 必須: いいえ
- 説明: StarRocks にログインを許可されるグループの名前。複数のグループはカンマで区切られます。指定されたグループが結合された Group Provider によって取得できることを確認してください。v3.5 以降でサポートされています。

##### comment

- 必須: いいえ
- 説明: セキュリティインテグレーションの説明。

### JWT を使用したセキュリティインテグレーションの作成

#### 構文

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_jwt",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
    "comment" = ""
)
```

#### パラメータ

##### security_integration_name

- 必須: はい
- 説明: セキュリティインテグレーションの名前。<br />**注意**<br />セキュリティインテグレーション名はグローバルに一意です。このパラメータを `native` として指定することはできません。

##### type

- 必須: はい
- 説明: セキュリティインテグレーションのタイプ。`jwt` として指定します。

##### jwks_url

- 必須: はい
- 説明: JSON Web Key Set (JWKS) サービスへの URL または `fe/conf` ディレクトリのローカルファイルへのパス。

##### principal_field

- 必須: はい
- 説明: JWT 内のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。

##### required_issuer

- 必須: いいえ
- 説明: JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。

##### required_audience

- 必須: いいえ
- 説明: JWT 内の受信者 (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 受信者と一致する場合にのみ、JWT は有効と見なされます。

##### group_source

- 必須: いいえ
- 説明: このセキュリティインテグレーションで認証されたユーザーのグループの取得元。有効な値:
  - `group_provider`（デフォルト）: `group_provider` で設定された Group Provider のみを使用します。以前のバージョンと同じ動作です。
  - `claim`: 検証済みトークンのうち `group_claim` で指定した claim のみを使用します。`group_provider` で設定された Group Provider は無視されますが設定は保持されるため、元に戻すには `ALTER SECURITY INTEGRATION` 1 回で済みます。
  - `both`: 両方の和集合。

  トークンからグループを取得できることが、LDAP ディレクトリーを持たない ID プロバイダーでも権限を付与できる前提になります。解決されたグループは、Group Provider のグループと同様に、`GRANT ... TO EXTERNAL GROUP` によるロールマッピング、`permitted_groups` のログインチェック、`current_group()`、および Apache Ranger の認可に参加します。v4.2 以降でサポートされます。

##### group_claim

- 必須: `group_source` が `claim` または `both` の場合は必須です。`group_source` を設定せずにこのパラメータだけを設定すると、効果がないため拒否されます。明示的に `group_source` を `group_provider` に設定した場合は受け付けられ、設定も保持されるため、先に claim を用意してから `ALTER SECURITY INTEGRATION` でソースを切り替えられます。
- 説明: ユーザーのグループを保持するトークンの claim。他のオブジェクトの中にネストされた claim はドット区切りで指定します。たとえば、トップレベルの claim は `groups`、Keycloak の realm ロールは `realm_access.roles`、その client ロールは `resource_access.<client_id>.roles` です。claim は文字列の配列であることが想定されますが、単一の文字列も 1 つのグループとして受け付けます。各グループ名の前後の空白は除去されます。それ以外は書き換えられません。大文字小文字も先頭のスラッシュもそのまま保持されます。グループ名は Apache Ranger にそのまま渡され、Ranger は厳密に照合するためです。大文字小文字だけが異なる `GRANT ... TO EXTERNAL GROUP` や `permitted_groups` のエントリーと一致するかどうかは、FE 設定項目 `authentication_group_case_insensitive` で決まります（デフォルトでは大文字小文字を無視します。ID プロバイダーが大文字小文字の異なるグループを別のグループとして扱う場合は `false` に設定してください）。

  デフォルト値は意図的に設けていません。ID プロバイダーが発行する claim はプロバイダーごとに異なり、推測を誤ると明確なエラーにならずに「ログインできるが権限がない」状態になるためです。claim は署名、`required_issuer`、`required_audience` の検証がすべて通った後にのみ読み取られるため、トークンの保持者が自分でグループを選ぶことはできません。トークンに claim が存在しない場合でもログインは成功し、グループは空になり、その理由はログに記録されます（認証失敗として扱われません）。Microsoft Entra ID は 200 を超えるグループに所属するユーザーに対して claim 全体を省略するため、claim が存在しない状態は正しい設定でも発生します。また、ほとんどの ID プロバイダーは既定ではグループをトークンに含めません。Keycloak ではクライアントに group membership mapper を追加し、Entra ID ではトークン設定に groups claim を追加する必要があります。v4.2 以降でサポートされます。

##### comment

- 必須: いいえ
- 説明: セキュリティインテグレーションの説明。

### OAuth 2.0 を使用したセキュリティインテグレーションの作成

#### 構文

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_oauth2",
    "auth_server_url" = "",
    "token_server_url" = "",
    "client_id" = "",
    "client_secret" = "",
    "redirect_url" = "",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
    "comment" = ""
)
```

#### パラメータ

##### security_integration_name

- 必須: はい
- 説明: セキュリティインテグレーションの名前。<br />**注意**<br />セキュリティインテグレーション名はグローバルに一意です。このパラメータを `native` として指定することはできません。

##### auth_server_url

- 必須: はい
- 説明: 認可 URL。OAuth 2.0 認可プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。

##### token_server_url

- 必須: はい
- 説明: StarRocks がアクセストークンを取得するための認可サーバーのエンドポイントの URL。

##### client_id

- 必須: はい
- 説明: StarRocks クライアントの公開識別子。

##### client_secret

- 必須: はい
- 説明: 認可サーバーで StarRocks クライアントを認可するために使用される秘密。

##### redirect_url

- 必須: はい
- 説明: OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。認可コードはこの URL に送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として構成する必要があります。

##### type

- 必須: はい
- 説明: セキュリティインテグレーションのタイプ。`authentication_oauth2` として指定します。

##### jwks_url

- 必須: はい
- 説明: JSON Web Key Set (JWKS) サービスへの URL または `fe/conf` ディレクトリのローカルファイルへのパス。

##### principal_field

- 必須: はい
- 説明: JWT 内のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。

##### required_issuer

- 必須: いいえ
- 説明: JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。

##### required_audience

- 必須: いいえ
- 説明: JWT 内の受信者 (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 受信者と一致する場合にのみ、JWT は有効と見なされます。

##### group_source

- 必須: いいえ
- 説明: このセキュリティインテグレーションで認証されたユーザーのグループの取得元。有効な値:
  - `group_provider`（デフォルト）: `group_provider` で設定された Group Provider のみを使用します。以前のバージョンと同じ動作です。
  - `claim`: 検証済みトークンのうち `group_claim` で指定した claim のみを使用します。`group_provider` で設定された Group Provider は無視されますが設定は保持されるため、元に戻すには `ALTER SECURITY INTEGRATION` 1 回で済みます。
  - `both`: 両方の和集合。

  トークンからグループを取得できることが、LDAP ディレクトリーを持たない ID プロバイダーでも権限を付与できる前提になります。解決されたグループは、Group Provider のグループと同様に、`GRANT ... TO EXTERNAL GROUP` によるロールマッピング、`permitted_groups` のログインチェック、`current_group()`、および Apache Ranger の認可に参加します。 OAuth 2.0 インテグレーションでは、MySQL プロトコル経由のブラウザーログインにも適用されます。この経路ではユーザーがブラウザーでサインインした後にトークンが StarRocks に届くため、グループは ID トークンの到着時に読み取られ、`permitted_groups` も接続確立時ではなくブラウザーログイン後の最初のステートメントで確認されます。`group_source` が `group_provider` のままの場合は変わらず、グループと確認は接続確立時に決まります。v4.2 以降でサポートされます。

##### group_claim

- 必須: `group_source` が `claim` または `both` の場合は必須です。それ以外の `group_source` と組み合わせて設定すると、効果がないため拒否されます。
- 説明: ユーザーのグループを保持するトークンの claim。他のオブジェクトの中にネストされた claim はドット区切りで指定します。たとえば、トップレベルの claim は `groups`、Keycloak の realm ロールは `realm_access.roles`、その client ロールは `resource_access.<client_id>.roles` です。claim は文字列の配列であることが想定されますが、単一の文字列も 1 つのグループとして受け付けます。各グループ名の前後の空白は除去されます。それ以外は書き換えられません。大文字小文字も先頭のスラッシュもそのまま保持されます。グループ名は Apache Ranger にそのまま渡され、Ranger は厳密に照合するためです。大文字小文字だけが異なる `GRANT ... TO EXTERNAL GROUP` や `permitted_groups` のエントリーと一致するかどうかは、FE 設定項目 `authentication_group_case_insensitive` で決まります（デフォルトでは大文字小文字を無視します。ID プロバイダーが大文字小文字の異なるグループを別のグループとして扱う場合は `false` に設定してください）。

  デフォルト値は意図的に設けていません。ID プロバイダーが発行する claim はプロバイダーごとに異なり、推測を誤ると明確なエラーにならずに「ログインできるが権限がない」状態になるためです。claim は署名、`required_issuer`、`required_audience` の検証がすべて通った後にのみ読み取られるため、トークンの保持者が自分でグループを選ぶことはできません。トークンに claim が存在しない場合でもログインは成功し、グループは空になり、その理由はログに記録されます（認証失敗として扱われません）。Microsoft Entra ID は 200 を超えるグループに所属するユーザーに対して claim 全体を省略するため、claim が存在しない状態は正しい設定でも発生します。また、ほとんどの ID プロバイダーは既定ではグループをトークンに含めません。Keycloak ではクライアントに group membership mapper を追加し、Entra ID ではトークン設定に groups claim を追加する必要があります。v4.2 以降でサポートされます。

##### comment

- 必須: いいえ
- 説明: セキュリティインテグレーションの説明。

## 認証チェーンを構成する

セキュリティインテグレーションが作成されると、新しい認証方法として StarRocks クラスターに追加されます。`authentication_chain` という FE 動的構成項目を設定して、認証方法の順序を設定することでセキュリティインテグレーションを有効にする必要があります。

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>[... ,]"
);
```

:::note
- StarRocks はローカルユーザーのネイティブ認証を優先します。同じユーザー名を持つローカルユーザーが存在しない場合、`authentication_chain` で設定した順序で認証が行われます。ネイティブ認証方式でログインに失敗した場合、クラスタは指定された順序で次の認証方式を試行します。
- OAuth 2.0 セキュリティインテグレーションを除いて、`authentication_chain` に複数のセキュリティインテグレーションを指定できます。複数の OAuth 2.0 セキュリティインテグレーションを指定することや、他のセキュリティインテグレーションと一緒に指定することはできません。
- `authentication_chain` には LDAP と JWT のセキュリティインテグレーションを同時に含められますが、MySQL プロトコルでは部分的なサポートにとどまります。[LDAP と JWT のセキュリティインテグレーションを混在させる](#ldap-と-jwt-のセキュリティインテグレーションを混在させる)を参照してください。
:::

`authentication_chain` の値を確認するには、次のステートメントを使用します:

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

### LDAP と JWT のセキュリティインテグレーションを混在させる

HTTP と Arrow Flight では、同じ `authentication_chain` 上の LDAP と JWT のセキュリティインテグレーションは並存できます。認証情報そのものが種類を示すためです。`Basic`（ユーザー名とパスワード）は LDAP セキュリティインテグレーションで、`Bearer`（トークン）は JWT と OAuth 2.0 セキュリティインテグレーションで検証されます。[HTTP と Arrow Flight から接続する](#http-と-arrow-flight-から接続する)を参照してください。

MySQL プロトコルでは部分的なサポートにとどまります。StarRocks は認証情報を受け取る前に接続のクライアント認証プラグインを 1 つ決めなければならず、2 種類のセキュリティインテグレーションは異なるプラグインを必要とします。LDAP は `mysql_clear_password`、JWT は `authentication_openid_connect_client` です。StarRocks はクライアントがハンドシェイクで宣言したプラグインによって選びます。

| クライアントが宣言するプラグイン | 使われるセキュリティインテグレーション |
| --- | --- |
| `authentication_openid_connect_client` | チェーン上の最初の JWT セキュリティインテグレーション |
| `mysql_clear_password` | チェーン上の最初の LDAP セキュリティインテグレーション |
| どちらも宣言しない（多くのクライアントはデフォルトで `mysql_native_password` を送る） | チェーン上の最初のセキュリティインテグレーション |

したがって:

- 追加の設定なしで大多数のクライアントが使うセキュリティインテグレーションを先頭に置いてください。
- もう一方の種類のクライアントはプラグインを宣言する必要があります。MySQL クライアントでは `--default-auth=<plugin>`、MySQL Connector/J では `defaultAuthenticationPlugin=<plugin>` を使います。MySQL クライアントはバージョン 9.1 から `authentication_openid_connect_client` を同梱しており、コミュニティ版にも含まれます。それより古い MySQL クライアントと MariaDB クライアントにはこのプラグインがありません。
- プラグインを宣言できないクライアント（PyMySQL など）は、常にチェーン上の最初のセキュリティインテグレーションを使います。

```Bash
# JWT セキュリティインテグレーションのユーザー（チェーンの先頭は LDAP セキュリティインテグレーション）
mysql -h <fe_host> -P <query_port> --default-auth=authentication_openid_connect_client \
    --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>

# LDAP セキュリティインテグレーションのユーザー（チェーンの先頭は JWT セキュリティインテグレーション）
mysql -h <fe_host> -P <query_port> --default-auth=mysql_clear_password --enable-cleartext-plugin -u <username> -p
```

JWT クライアントが OpenID Connect プラグインを読み込めない場合は、FE の設定項目 `authentication_chain_accept_jwt_as_password` を `true` にし、LDAP セキュリティインテグレーションを先頭に置いてください。JWT クライアントはトークンをパスワードとして渡します。MySQL ログインのパスワードが JWT の形をしている場合、StarRocks はまず JWT と OAuth 2.0 セキュリティインテグレーションで検証し、どれも受け入れなかった場合にのみパスワードとして LDAP セキュリティインテグレーションに渡します。トークンは平文パスワードと同じように送られるため、SSL 接続を使ってください。この設定項目は MySQL プロトコルにのみ適用されます。HTTP と Arrow Flight ではトークンを `Bearer` で送ってください。

```Bash
mysql -h <fe_host> -P <query_port> --ssl-mode=REQUIRED --enable-cleartext-plugin -u <username> -p"$(cat <path_to_token_file>)"
```

## HTTP と Arrow Flight から接続する

`authentication_chain` に登録したセキュリティインテグレーションのユーザーは、MySQL プロトコルだけでなく StarRocks のすべてのエントリポイントからログインできます: HTTP SQL API、Stream Load（FE 経由または BE に直接）、FE Web UI、Arrow Flight SQL。クライアントが送る認証情報はインテグレーションのタイプによって決まります。

| インテグレーションのタイプ | HTTP | Arrow Flight SQL |
| --- | --- | --- |
| LDAP（`authentication_ldap_simple`） | HTTP Basic: `-u <username>:<password>` | ユーザー名とパスワード |
| JWT（`authentication_jwt`）、<br />OAuth 2.0（`authentication_oauth2`） | `Authorization: Bearer <token>` | `Authorization: Bearer <token>` ヘッダー。<br />例: ADBC の `adbc.flight.sql.authorization_header` オプション |

Bearer トークンにはユーザー名は不要です。StarRocks は `principal_field` で指定したクレームからユーザー名を読み取り、`jwks_url` で署名を検証し、チェーン上の JWT および OAuth 2.0 インテグレーションのうち `required_issuer` と `required_audience`（設定時）がトークンに一致するものすべてにトークンを渡します。複数のインテグレーションが同じ issuer を共有する場合は、それぞれに異なる `required_audience` を設定してください。トークンの `aud` クレームがどのインテグレーション、つまりどの Group Provider とロールを使うかを決めます。すべてのインテグレーションに `required_issuer` を設定することを強く推奨します。

例:

```Bash
# LDAP インテグレーションのユーザーで SQL API を呼ぶ
curl -u alice:alice_password -H 'Content-Type: application/json' -XPOST \
    http://<fe_host>:<fe_http_port>/api/v1/catalogs/default_catalog/databases/<db>/sql \
    -d '{"query": "select current_user()"}'

# JWT で BE に直接 Stream Load する（BE 側で enable_http_auth = true が必要）
curl -H "Authorization: Bearer ${TOKEN}" -H "label:load_1" -H "column_separator:," \
    -T data.csv -XPUT http://<be_host>:<be_http_port>/api/<db>/<table>/_stream_load
```

次の点に注意してください:

- セキュリティインテグレーションのユーザーの権限は [Group Provider](../group_provider.md) と `GRANT ... TO EXTERNAL GROUP` からのみ付与されます。ユーザーが解決されたグループはすべてのエントリポイントで有効です。
- Arrow Flight SQL では、JWT はユーザー名とパスワードとまったく同じ方法で認証されます。サーバーは自身が発行するセッショントークンを返し、クライアントは接続の残りの間それを使います。セッションは JWT より長く生きられません。トークンの `exp` を過ぎると接続は拒否され、クライアントは新しい JWT で再認証する必要があります。
- FE の設定項目 `enable_rest_bearer_authentication`（デフォルト `true`）は、HTTP と Arrow Flight エントリポイントの Bearer 認証を無効にします。MySQL プロトコルには影響せず、MySQL プロトコルのトークンログインは引き続き `authentication_openid_connect_client` と `authentication_oauth2_client` プラグインを使います。
- MySQL プロトコルはクライアントに一度に 1 つのプラグインしか提示できません。クライアントが宣言したプラグインが対応するセキュリティインテグレーションを選び、何も宣言しないクライアントは `authentication_chain` の最初のセキュリティインテグレーションを使います（[LDAP と JWT のセキュリティインテグレーションを混在させる](#ldap-と-jwt-のセキュリティインテグレーションを混在させる)を参照）。HTTP と Arrow Flight にはこの制限はありません。トークン自体が issuer と audience でルーティングされるためです。
- HTTP 認証の失敗はすべて `401` を返し、`WWW-Authenticate` に `Basic` と `Bearer` の両方のチャレンジを含み、どのインテグレーションがなぜ拒否したかは示しません。原因の調査には FE のメトリクス `authentication_failure_total` と `authentication_failure_reason_total` を使ってください。

## セキュリティインテグレーションを管理する

### セキュリティインテグレーションを変更する

既存のセキュリティインテグレーションの構成を変更するには、次のステートメントを使用します:

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
(
    "key"="value"[, ...]
)
```

:::note
セキュリティインテグレーションの `type` を変更することはできません。
:::

### セキュリティインテグレーションを削除する

既存のセキュリティインテグレーションを削除するには、次のステートメントを使用します:

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

### セキュリティインテグレーションを表示する

クラスター内のすべてのセキュリティインテグレーションを表示するには、次のステートメントを使用します:

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
| Name          | セキュリティインテグレーションの名前。                                      |
| Type          | セキュリティインテグレーションのタイプ。                                    |
| Comment       | セキュリティインテグレーションの説明。セキュリティインテグレーションに説明が指定されていない場合、`NULL` が返されます。 |

セキュリティインテグレーションの詳細を確認するには、次のステートメントを使用します:

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
      "type" = "authentication_ldap_simple",
      "authentication_ldap_simple_server_host" = "",
      "authentication_ldap_simple_server_port" = "",
      "authentication_ldap_simple_bind_base_dn" = "",
      "authentication_ldap_simple_user_search_attr" = ""
      "authentication_ldap_simple_bind_root_dn" = "",
      "authentication_ldap_simple_bind_root_pwd" = "",
      "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
      "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
      "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
      "comment" = ""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

:::note
`ldap_bind_root_pwd` は SHOW CREATE SECURITY INTEGRATION が実行されたときにマスクされます。
:::

## セキュリティ統合によるStarRocksへの接続

- LDAP経由でStarRocksに接続する方法については、[LDAP認証 - StarRocksへの接続](./ldap_authentication.md#mysql-クライアントから-ldap-で接続する)を参照してください。
- JWT 経由で StarRocks に接続する方法については、[JSON Web Token 認証 - StarRocks への接続](./jwt_authentication.md#mysql-クライアントから-jwt-で接続する) を参照してください。
- OAuth 2.0 経由で StarRocks に接続する方法については、[OAuth 2.0 認証 - StarRocks への接続](./oauth2_authentication.md#jdbc-クライアントからの-oauth-20-接続) を参照してください。

## See also

- StarRocks で LDAP を使用してユーザーを手動で認証する方法については、[LDAP 認証](./ldap_authentication.md) を参照してください。
- StarRocks で JSON Web Token を使用してユーザーを手動で認証する方法については、[JSON Web Token 認証](./jwt_authentication.md) を参照してください。
- StarRocks で OAuth 2.0 を使用してユーザーを手動で認証する方法については、[OAuth 2.0 認証](./oauth2_authentication.md) を参照してください。
- ユーザーグループを認証する方法については、[ユーザーグループの認証](../group_provider.md) を参照してください。

