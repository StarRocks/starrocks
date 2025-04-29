---
displayed_sidebar: docs
sidebar_position: 60
---

# ユーザーグループの認証

このトピックでは、StarRocks でユーザーグループを作成し、認証し、認可する方法について説明します。

v3.5.0 以降、StarRocks はユーザーグループの作成と管理をサポートしています。

## 概要

LDAP、OpenID Connect、OAuth 2.0、Apache Ranger などの外部ユーザー認証および認可システムとの統合を深めるために、StarRocks はユーザーグループをサポートし、集団的なユーザー管理の体験を向上させます。

:::note

**StarRocks におけるユーザーグループとロールの違い**

- **ユーザーグループはユーザーの集合です**。ロールと権限はユーザーグループに付与でき、ユーザーグループのすべてのメンバーに効果を及ぼします。
- **ロールは権限の集合です**。ロール内の権限は、特定のユーザーまたはユーザーグループに付与されない限り効果を発揮しません。

例えば、ロール `analyst_role` は `table_a` に対する INSERT および SELECT 権限を含み、ユーザーグループ `analyst_group` は `tom`、`chelsea`、`sophie` の3人のユーザーで構成されています。`analyst_role` を `analyst_group` に付与することで、`tom`、`chelsea`、`sophie` はすべて `table_a` に対する INSERT および SELECT 権限を持つことができます。

:::

StarRocks では、グループプロバイダーを介して外部ユーザーシステムからグループの情報を取得することで、ユーザーグループを作成できます。グループ情報は独立しており、認証、認可、その他のプロセスに柔軟に統合でき、特定のワークフローに密接に結びつけられることはありません。

グループプロバイダーは、ユーザーとグループの間のマッピングです。グループ情報を必要とするプロセスは、このマッピングを必要に応じて照会できます。

### ワークフロー

次のフローチャートは、LDAP と Apache Ranger を例にして、グループプロバイダーのワークフローを説明しています。

![Group Provider](../../../_assets/group_provider.png)

## グループプロバイダーの作成

StarRocks は3種類のグループプロバイダーをサポートしています:
- **LDAP グループプロバイダー**: LDAP サービスでユーザーとグループを検索して一致させます
- **Unix グループプロバイダー**: オペレーティングシステムでユーザーとグループを検索して一致させます
- **ファイルグループプロバイダー**: ファイルで指定されたユーザーとグループを検索して一致させます

### 構文

```SQL
-- LDAP グループプロバイダー
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

-- Unix グループプロバイダー
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "unix"
)

-- ファイルグループプロバイダー
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "file",
    "group_file_url" = ""
)
```

### パラメータ

#### `type`

作成するグループプロバイダーのタイプ。 有効な値:
- `ldap`: LDAP グループプロバイダーを作成します。この値を設定する場合、`ldap_info`、`ldap_search_group_arg`、`ldap_search_user_arg`、およびオプションで `ldap_cache_arg` を指定する必要があります。
- `unix`: Unix グループプロバイダーを作成します。
- `file`: ファイルグループプロバイダーを作成します。この値を設定する場合、`group_file_url` を指定する必要があります。

#### `ldap_info`

LDAP サービスに接続するために使用される情報。

##### `ldap_conn_url`

LDAP サーバーの URL。形式: `ldap://<ldap_server_host>:<ldap_server_port>)`。

##### `ldap_bind_root_dn`

LDAP サービスの管理者識別名 (DN)。

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

LDAP サーバーで認識されるカスタマイズされたグループフィルター。グループを検索するために直接 LDAP サーバーに送信されます。例: `(&(objectClass=groupOfNames)(cn=testgroup))`。

##### `ldap_group_identifier_attr`

グループ名の識別子として使用される属性。

#### `ldap_search_user_arg`

StarRocks がグループ内のユーザーを識別する方法を制御するために使用される引数。

##### `ldap_group_member_attr`

グループメンバーを表す属性。有効な値: `member` および `memberUid`。

##### `ldap_user_search_attr`

メンバー属性値からユーザー識別子を抽出する方法を指定します。明示的に属性を定義することも（例: `cn` または `uid`）、正規表現を使用することもできます。

#### `ldap_cache_arg`

LDAP グループ情報のキャッシュ動作を定義するために使用される引数。

##### `ldap_cache_refresh_interval`

オプション。StarRocks がキャッシュされた LDAP グループ情報を自動的に更新する間隔。単位: 秒。デフォルト: `900`。

#### `group_file_url`

ユーザーグループを定義するファイルへの URL または相対パス (`fe/conf` の下)。

:::note

グループファイルには、グループとそのメンバーのリストが含まれています。各行にコロンで区切ってグループを定義できます。複数のユーザーはカンマで区切られます。例: `group_name:user_1,user_2,user_3`。

:::

### 例

LDAP サーバーに次のグループとメンバー情報が含まれているとします。

```Plain
-- グループ情報
# testgroup, Group, starrocks.com
dn: cn=testgroup,ou=Group,dc=starrocks,dc=com
objectClass: groupOfNames
cn: testgroup
member: uid=test,ou=people,dc=starrocks,dc=com
member: uid=tom,ou=people,dc=starrocks,dc=com

-- ユーザー情報
# test, People, starrocks.com
dn: cn=test,ou=People,dc=starrocks,dc=com
objectClass: inetOrgPerson
cn: test
uid: test
sn: FTE
userPassword:: 
```

`testgroup` のメンバーのために `ldap_group_provider` というグループプロバイダーを作成します:

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

上記の例では、`ldap_group_filter` を使用して `groupOfNames` オブジェクトクラスと `cn` が `testgroup` のグループを検索します。そのため、`cn` が `ldap_group_identifier_attr` に指定され、グループを識別します。`ldap_group_member_attr` は `member` に設定され、`groupOfNames` オブジェクトクラスの `member` 属性を使用してメンバーを識別します。`ldap_user_search_attr` は `member` 属性内のユーザーを識別するために使用される式 `uid=([^,]+)` に設定されています。

## グループプロバイダーをセキュリティ統合と組み合わせる

グループプロバイダーを作成した後、セキュリティ統合と組み合わせて、グループプロバイダーで指定されたユーザーが StarRocks にログインできるようにすることができます。セキュリティ統合の作成に関する詳細は、[Authenticate with Security Integration](./security_integration.md) を参照してください。

### 構文

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
PROPERTIES(
        "group_provider" = "",
        "authenticated_group_list" = ""
)
```

### パラメータ

#### `group_provider`

セキュリティ統合と組み合わせるグループプロバイダーの名前。複数のグループプロバイダーはカンマで区切られます。設定すると、StarRocks はログイン時に各指定されたプロバイダーの下でユーザーのグループ情報を記録します。

#### `authenticated_group_list`

オプション。StarRocks にログインすることを許可されているグループの名前。複数のグループはカンマで区切られます。指定されたグループが組み合わせたグループプロバイダーによって取得できることを確認してください。

### 例

```SQL
ALTER SECURITY INTEGRATION LDAP SET
PROPERTIES(
        "group_provider"="ldap_group_provider",
        "authenticated_group_list"="testgroup"
);
```

## グループプロバイダーを外部認可システム (Apache Ranger) と組み合わせる

セキュリティ統合で関連するグループプロバイダーを設定すると、StarRocks はログイン時にユーザーのグループ情報を記録します。このグループ情報は、その後、Ranger との認可プロセスに自動的に含まれ、追加の設定が不要になります。

StarRocks を Ranger と統合するための詳細な手順については、[Manage permissions with Apache Ranger](../ranger_plugin.md) を参照してください。