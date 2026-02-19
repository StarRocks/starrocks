---
displayed_sidebar: docs
sidebar_position: 20
---

# セキュリティインテグレーションで認証

import SecurityIntegrationRangerLink from '../../../_assets/user_priv/security_integration_ranger_link.mdx'
import SecurityIntegrationIntro from '../../../_assets/user_priv/security_integration_intro.mdx'
import SecurityIntegrationJWT from '../../../_assets/user_priv/security_integration_jwt.mdx'
import SecurityIntegrationOAuth from '../../../_assets/user_priv/security_integration_oauth.mdx'
import SecurityIntegrationConnectSeeAlso from '../../../_assets/user_priv/security_integration_connect_see_also.mdx'

StarRocks をセキュリティインテグレーションを使用して外部認証システムと統合します。

StarRocks クラスター内でセキュリティインテグレーションを作成することで、外部認証サービスへのアクセスを StarRocks に許可できます。セキュリティインテグレーションを使用すると、StarRocks 内でユーザーを手動で作成する必要がありません。ユーザーが外部 ID を使用してログインしようとすると、StarRocks は `authentication_chain` の設定に従って対応するセキュリティインテグレーションを使用してユーザーを認証します。認証が成功し、ユーザーがログインを許可された後、StarRocks はセッション内に仮想ユーザーを作成し、そのユーザーが後続の操作を実行できるようにします。

<SecurityIntegrationRangerLink />

また、StarRocks に [Group Provider](../group_provider.md) を有効にして、外部認証システムのグループ情報にアクセスし、StarRocks でユーザーグループを作成、認証、および認可することができます。

特定のコーナーケースでは、外部認証サービスを使用してユーザーを手動で作成および管理することもサポートされています。詳細な手順については、[See also](#see-also) を参照してください。

## セキュリティインテグレーションを作成する

<SecurityIntegrationIntro />

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
    "authentication_ldap_simple_user_search_attr" = ""
    "authentication_ldap_simple_bind_root_dn" = "",
    "authentication_ldap_simple_bind_root_pwd" = "",
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

- 必須: はい
- 説明: クラスターが検索する LDAP ユーザーの基本識別名 (DN)。

##### authentication_ldap_simple_user_search_attr

- 必須: はい
- 説明: LDAP サービスにログインするために使用されるユーザーの属性。例: `uid`。

:::note

**DN パス機構**: LDAP セキュリティインテグレーションは DN パス機能をサポートします。

- 認証成功後、システムはユーザーのログイン名と完全な DN の両方を記録します。
- Group Provider と組み合わせると、DN 情報が自動的にグループプロバイダに渡されます。
- Group Provider で `ldap_user_search_attr` が設定されていない場合、グループマッチングには DN が使用されます。
- このメカニズムは Microsoft AD のような複雑な LDAP 環境に特に適しています。

詳細は[ユーザーグループの認証](../group_provider.md)の DN マッチングメカニズムを参照してください。

:::

##### authentication_ldap_simple_bind_root_dn

- 必須: はい
- 説明: LDAP サービスの管理者 DN。

##### authentication_ldap_simple_bind_root_pwd

- 必須: はい
- 説明: LDAP サービスの管理者パスワード。

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

<SecurityIntegrationJWT />

<SecurityIntegrationOAuth />

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
:::

`authentication_chain` の値を確認するには、次のステートメントを使用します:

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

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

<SecurityIntegrationConnectSeeAlso />

