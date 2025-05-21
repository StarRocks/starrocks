---
displayed_sidebar: docs
sidebar_position: 30
---

# ユーザーグループの認証

StarRocks で Group Provider を有効にして、外部認証システムからユーザーグループを認証し、認可します。

v3.5.0 以降、StarRocks は Group Provider をサポートしており、外部認証システムからグループ情報を収集してユーザーグループ管理を行います。

## 概要

LDAP、OpenID Connect、OAuth 2.0、Apache Ranger などの外部ユーザー認証および認可システムとの統合を深めるために、StarRocks はユーザーグループ情報を収集し、集団的なユーザー管理をより良い体験にします。

グループプロバイダーを使用すると、外部ユーザーシステムから異なる目的でグループ情報を取得できます。グループ情報は独立しており、認証、認可、またはその他のプロセスに柔軟に統合でき、特定のワークフローに厳密に結びつけられることはありません。

Group Provider は、ユーザーとグループの間のマッピングです。グループ情報を必要とするプロセスは、このマッピングを必要に応じてクエリできます。

### ワークフロー

以下のフローチャートは、LDAP と Apache Ranger を例にして、Group Provider のワークフローを説明しています。

![Group Provider](../../_assets/group_provider.png)

## グループプロバイダーを作成する

StarRocks は 3 種類のグループプロバイダーをサポートしています:
- **LDAP group provider**: LDAP サービス内のユーザーとグループを検索して一致させます
- **Unix group provider**: オペレーティングシステム内のユーザーとグループを検索して一致させます
- **File group provider**: ファイルで定義されたユーザーとグループを検索して一致させます

### 構文

```SQL
-- LDAP group provider
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "ldap",
    ldap_info,
    ldap_search_group_arg,
    ldap_search_attr,
    [ldap_cache_attr]
)

ldap_info ::=
    "ldap_conn_url" = "",
    "ldap_bind_root_dn" = "",
    "ldap_bind_root_pwd" = "",
    "ldap_bind_base_dn" = "",
    ["ldap_conn_timeout" = "",]
    ["ldap_conn_read_timeout" = ""]

ldap_search_group_arg ::= 
    { "ldap_group_dn" = "" 
    | "ldap_group_filter" = "" }, 
    "ldap_group_identifier_attr" = ""

ldap_search_user_arg ::=
    "ldap_group_member_attr" = "",
    "ldap_user_search_attr" = ""

ldap_cache_arg ::= 
    "ldap_cache_refresh_interval" = ""

-- Unix group provider
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "unix"
)

-- File group provider
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "file",
    "group_file_url" = ""
)
```

### パラメータ

#### `type`

作成するグループプロバイダーのタイプ。 有効な値:
- `ldap`: LDAP group provider を作成します。この値が設定されている場合、`ldap_info`、`ldap_search_group_arg`、`ldap_search_user_arg`、およびオプションで `ldap_cache_arg` を指定する必要があります。
- `unix`: Unix group provider を作成します。
- `file`: File group provider を作成します。この値が設定されている場合、`group_file_url` を指定する必要があります。

#### `ldap_info`

LDAP サービスに接続するために使用される情報。

##### `ldap_conn_url`

LDAP サーバーの URL。形式: `ldap://<ldap_server_host>:<ldap_server_port>)`。

##### `ldap_bind_root_dn`

LDAP サービスの管理者の Distinguished Name (DN)。

##### `ldap_bind_root_pwd`

LDAP サービスの管理者パスワード。

##### `ldap_bind_base_dn`

クラスターが検索する LDAP ユーザーのベース DN。

##### `ldap_conn_timeout`

LDAP サービスへの接続のタイムアウト時間。

##### `ldap_conn_read_timeout`

オプション。LDAP サービスへの接続での読み取り操作のタイムアウト時間。

#### `ldap_search_group_arg`

StarRocks がグループを検索する方法を制御するために使用される引数。

:::note
`ldap_group_dn` または `ldap_group_filter` のいずれかのみを指定できます。両方を指定することはサポートされていません。
:::

##### `ldap_group_dn`

検索対象のグループの DN。この DN を使用してグループが直接クエリされます。例: `"cn=ldapgroup1,ou=Group,dc=starrocks,dc=com;cn=ldapgroup2,ou=Group,dc=starrocks,dc=com"`。

##### `ldap_group_filter`

LDAP サーバーによって認識されるカスタマイズされたグループフィルター。グループを検索するために直接 LDAP サーバーに送信されます。例: `(&(objectClass=groupOfNames)(cn=testgroup))`。

##### `ldap_group_identifier_attr`

グループ名の識別子として使用される属性。

#### `ldap_search_user_arg`

StarRocks がグループ内のユーザーを識別する方法を制御するために使用される引数。

##### `ldap_group_member_attr`

グループメンバーを表す属性。有効な値: `member` と `memberUid`。

##### `ldap_user_search_attr`

メンバー属性値からユーザー識別子を抽出する方法を指定します。明示的に属性を定義することも（例: `cn` または `uid`）、正規表現を使用することもできます。

#### `ldap_cache_arg`

LDAP グループ情報のキャッシュ動作を定義するために使用される引数。

##### `ldap_cache_refresh_interval`

オプション。StarRocks がキャッシュされた LDAP グループ情報を自動的に更新する間隔。単位: 秒。デフォルト: `900`。

#### `group_file_url`

ユーザーグループを定義するファイルへの URL または相対パス（`fe/conf` 以下）。

:::note

グループファイルには、グループとそのメンバーのリストが含まれています。各行でグループ名とメンバーをコロンで区切って定義できます。複数のユーザーはカンマで区切られます。例: `group_name:user_1,user_2,user_3`。

:::

### 例

LDAP サーバーに次のグループとメンバー情報が含まれているとします。

```Plain
-- Group information
# testgroup, Group, starrocks.com
dn: cn=testgroup,ou=Group,dc=starrocks,dc=com
objectClass: groupOfNames
cn: testgroup
member: uid=test,ou=people,dc=starrocks,dc=com
member: uid=tom,ou=people,dc=starrocks,dc=com

-- User information
# test, People, starrocks.com
dn: cn=test,ou=People,dc=starrocks,dc=com
objectClass: inetOrgPerson
cn: test
uid: test
sn: FTE
userPassword:: 
```

`testgroup` のメンバーのために `ldap_group_provider` を作成します:

```SQL
CREATE GROUP PROVIDER ldap_group_provider 
PROPERTIES(
    "type"="ldap", 
    "ldap_conn_url"="ldap://xxxx:xxx",
    "ldap_bind_root_dn"="cn=admin,dc=starrocks,dc=com",
    "ldap_bind_root_pwd"="123456",
    "ldap_bind_base_dn"="dc=starrocks,dc=com",
    "ldap_group_filter"="(&(objectClass=groupOfNames)(cn=testgroup))",
    "ldap_group_identifier_attr"="cn",
    "ldap_group_member_attr"="member",
    "ldap_user_search_attr"="uid=([^,]+)"
)
```

上記の例では、`ldap_group_filter` を使用して `groupOfNames` objectClass と `cn` が `testgroup` のグループを検索します。したがって、`cn` はグループを識別するために `ldap_group_identifier_attr` に指定されます。`ldap_group_member_attr` は `member` に設定されており、`groupOfNames` objectClass でメンバーを識別するために `member` 属性が使用されます。`ldap_user_search_attr` は `uid=([^,]+)` という式に設定されており、`member` 属性内のユーザーを識別するために使用されます。

## グループプロバイダーをセキュリティインテグレーションと組み合わせる

グループプロバイダーを作成した後、セキュリティインテグレーションと組み合わせて、グループプロバイダーで指定されたユーザーが StarRocks にログインできるようにすることができます。セキュリティインテグレーションの作成に関する詳細は、[Authenticate with Security Integration](./authentication/security_integration.md) を参照してください。

### 構文

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
(
    "group_provider" = "",
    "authenticated_group_list" = ""
)
```

### パラメータ

#### `group_provider`

セキュリティインテグレーションと組み合わせるグループプロバイダーの名前。複数のグループプロバイダーはカンマで区切られます。設定されると、StarRocks はログイン時に各指定されたプロバイダーの下でユーザーのグループ情報を記録します。

#### `authenticated_group_list`

オプション。StarRocks にログインすることが許可されているグループの名前。複数のグループはカンマで区切られます。指定されたグループが組み合わせたグループプロバイダーによって取得できることを確認してください。

### 例

```SQL
ALTER SECURITY INTEGRATION LDAP SET
PROPERTIES(
        "group_provider"="ldap_group_provider",
        "authenticated_group_list"="testgroup"
);
```

## グループプロバイダーを外部認可システム (Apache Ranger) と組み合わせる

セキュリティインテグレーションで関連するグループプロバイダーを構成すると、StarRocks はログイン時にユーザーのグループ情報を記録します。このグループ情報は、Ranger との認可プロセスに自動的に含まれ、追加の構成が不要になります。

StarRocks と Ranger の統合に関する詳細な手順については、[Manage permissions with Apache Ranger](./authorization/ranger_plugin.md) を参照してください。