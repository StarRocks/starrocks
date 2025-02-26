---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# GRANT

import UserPrivilegeCase from '../../../_assets/commonMarkdown/userPrivilegeCase.md'
import MultiServiceAccess from '../../../_assets/commonMarkdown/multi-service-access.mdx'

## 説明

特定のオブジェクトに対する1つ以上の権限をユーザーまたはロールに付与します。

ロールをユーザーまたは他のロールに付与します。

付与可能な権限の詳細については、[Privilege items](../../../administration/user_privs/privilege_overview.md) を参照してください。

GRANT 操作が実行された後、[SHOW GRANTS](./SHOW_GRANTS.md) を実行して詳細な権限情報を表示するか、[REVOKE](REVOKE.md) を実行して権限またはロールを取り消すことができます。

GRANT 操作を実行する前に、関連するユーザーまたはロールが作成されていることを確認してください。詳細については、[CREATE USER](./CREATE_USER.md) および [CREATE ROLE](./CREATE_ROLE.md) を参照してください。

:::tip

- `user_admin` ロールを持つユーザーのみが、他のユーザーやロールに任意の権限を付与できます。
- ロールがユーザーに付与された後、このロールとして操作を実行する前に [SET ROLE](SET_ROLE.md) を実行してこのロールを有効化する必要があります。すべてのデフォルトロールをログイン時に有効化したい場合は、[ALTER USER](ALTER_USER.md) または [SET DEFAULT ROLE](SET_DEFAULT_ROLE.md) を実行してください。システム内のすべての権限をすべてのユーザーにログイン時に有効化したい場合は、グローバル変数 `SET GLOBAL activate_all_roles_on_login = TRUE;` を設定してください。
- 一般ユーザーは、`WITH GRANT OPTION` キーワードを持つ権限のみを他のユーザーやロールに付与できます。
:::

## 構文

### ロールまたはユーザーに権限を付与

#### システム

```SQL
GRANT
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE | CREATE STORAGE VOLUME } 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### リソースグループ

```SQL
GRANT
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_group_name> [, <resource_group_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### リソース

```SQL
GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### グローバル UDF

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name>(input_data_type) [, <function_name>(input_data_type),...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

例: `GRANT usage ON GLOBAL FUNCTION a(string) to kevin;`

#### Internal catalog

```SQL
GRANT
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### External catalog

```SQL
GRANT
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### データベース

```SQL
GRANT
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* このコマンドを実行する前に SET CATALOG を実行する必要があります。
* External Catalog 内のデータベースに対しては、CREATE TABLE 権限を Hive (v3.1以降) および Iceberg データベース (v3.2以降) にのみ付与できます。

#### テーブル

```SQL
-- 特定のテーブルに対する権限を付与します。
  GRANT
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]}
    ON TABLE <table_name> [, < table_name >,...]
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

-- 特定のデータベースまたはすべてのデータベース内のすべてのテーブルに対する権限を付与します。
  GRANT
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]}
    ON ALL TABLES IN { { DATABASE <database_name> } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* このコマンドを実行する前に SET CATALOG を実行する必要があります。
* `<db_name>.<table_name>` を使用してテーブルを表すこともできます。
* Internal および External Catalogs 内のすべてのテーブルに対して SELECT 権限を付与して、これらのテーブルからデータを読み取ることができます。Hive および Iceberg Catalogs 内のテーブルに対しては、INSERT 権限を付与してデータを書き込むことができます (Iceberg は v3.1以降、Hive は v3.2以降でサポート)。

  ```SQL
  GRANT <priv> ON TABLE <db_name>.<table_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### ビュー

```SQL
GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           { { DATABASE <database_name> } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* このコマンドを実行する前に SET CATALOG を実行する必要があります。
* `<db_name>.<view_name>` を使用してビューを表すこともできます。
* External Catalog 内のテーブルに対しては、Hive テーブルビューにのみ SELECT 権限を付与できます (v3.1以降)。

  ```SQL
  GRANT <priv> ON VIEW <db_name>.<view_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### マテリアライズドビュー

```SQL
GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { { DATABASE <database_name> } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* このコマンドを実行する前に SET CATALOG を実行する必要があります。
* `<db_name>.<mv_name>` を使用して mv を表すこともできます。

  ```SQL
  GRANT <priv> ON MATERIALIZED VIEW <db_name>.<mv_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### 関数

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name>(input_data_type) [, < function_name >(input_data_type),...]
       ｜ ALL FUNCTIONS} IN 
           { { DATABASE <database_name> } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

- このコマンドを実行する前に SET CATALOG を実行する必要があります。
- `<db_name>.<function_name>` を使用して関数を表すこともできます。

  ```SQL
  GRANT <priv> ON FUNCTION <db_name>.<function_name>(input_data_type) TO {ROLE <role_name> | USER <user_name>}
  ```

#### ユーザー

```SQL
GRANT IMPERSONATE
ON USER <user_identity>
TO USER <user_identity_1> [ WITH GRANT OPTION ]
```

#### ストレージボリューム

```SQL
GRANT  
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUMES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

### ロールをロールまたはユーザーに付与

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>
```

## 例

例 1: すべてのデータベース内のすべてのテーブルからデータを読み取る権限をユーザー `jack` に付与します。

```SQL
GRANT SELECT ON *.* TO 'jack'@'%';
```

例 2: データベース `db1` のすべてのテーブルにデータをロードする権限をロール `my_role` に付与します。

```SQL
GRANT INSERT ON db1.* TO ROLE 'my_role';
```

例 3: データベース `db1` のテーブル `tbl1` に対してデータの読み取り、更新、およびロードする権限をユーザー `jack` に付与します。

```SQL
GRANT SELECT,ALTER,INSERT ON db1.tbl1 TO 'jack'@'192.8.%';
```

例 4: すべてのリソースを使用する権限をユーザー `jack` に付与します。

```SQL
GRANT USAGE ON RESOURCE * TO 'jack'@'%';
```

例 5: リソース `spark_resource` を使用する権限をユーザー `jack` に付与します。

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

例 6: リソース `spark_resource` を使用する権限をロール `my_role` に付与します。

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

例 7: テーブル `sr_member` からデータを読み取る権限をユーザー `jack` に付与し、この権限を他のユーザーやロールに付与できるようにします (WITH GRANT OPTION を指定)。

```SQL
GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
```

例 8: システム定義のロール `db_admin`、`user_admin`、および `cluster_admin` をユーザー `user_platform` に付与します。

```SQL
GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
```

例 9: ユーザー `jack` がユーザー `rose` として操作を実行できるようにします。

```SQL
GRANT IMPERSONATE ON USER 'rose'@'%' TO USER 'jack'@'%';
```

## ベストプラクティス

### シナリオに基づいてロールをカスタマイズ

<UserPrivilegeCase />

<MultiServiceAccess />