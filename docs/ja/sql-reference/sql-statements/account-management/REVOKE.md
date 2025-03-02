---
displayed_sidebar: docs
---

# REVOKE

## 説明

特定の権限またはロールをユーザーまたはロールから取り消します。StarRocks がサポートする権限については、[Privileges supported by StarRocks](../../../administration/user_privs/privilege_overview.md) を参照してください。

:::tip

- 一般ユーザーは、他のユーザーやロールから `WITH GRANT OPTION` キーワードを持つ自分の権限のみを取り消すことができます。`WITH GRANT OPTION` については、[GRANT](./GRANT.md) を参照してください。
- `user_admin` ロールを持つユーザーのみが、他のユーザーから権限を取り消す権限を持っています。

:::

## 構文

### 権限を取り消す

取り消せる権限はオブジェクトに依存します。以下はオブジェクトに基づく構文の説明です。

#### システム

```SQL
REVOKE
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE } 
    ON SYSTEM
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### リソースグループ

```SQL
REVOKE
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resourcegroup_name> [, <resourcegroup_name>,...] ｜ ALL RESOURCE GROUPS} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### リソース

```SQL
REVOKE
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, <resource_name>,...] ｜ ALL RESOURCES} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### ユーザー

```SQL
REVOKE IMPERSONATE ON USER <user_identity> FROM USER <user_identity>;
```

#### グローバル UDF

```SQL
REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name>(input_data_type) [, <function_name>(input_data_type),...]    
       | ALL GLOBAL FUNCTIONS }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### 内部カタログ

```SQL
REVOKE 
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### 外部カタログ

```SQL
REVOKE  
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   FROM { ROLE | USER} {<role_name>|<user_identity>}
```

#### データベース

```SQL
REVOKE 
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON {{ DATABASE <database_name> [, <database_name>,...]} | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* このコマンドを実行する前に、SET CATALOG を実行する必要があります。

#### テーブル

```SQL
REVOKE  
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           { { DATABASE <database_name> [, <database_name>,...]} | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* このコマンドを実行する前に、SET CATALOG を実行する必要があります。
* db.tbl を使用してテーブルを表すこともできます。

  ```SQL
  REVOKE <priv> ON TABLE db.tbl FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### ビュー

```SQL
REVOKE  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...]}  | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* このコマンドを実行する前に、SET CATALOG を実行する必要があります。
* db.view を使用してビューを表すこともできます。

  ```SQL
  REVOKE <priv> ON VIEW db.view FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### マテリアライズドビュー

```SQL
REVOKE
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL [DATABASES] }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* このコマンドを実行する前に、SET CATALOG を実行する必要があります。
* db.mv を使用して mv を表すこともできます。

  ```SQL
  REVOKE <priv> ON MATERIALIZED VIEW db.mv FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### 関数

```SQL
REVOKE
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name>(input_data_type) [, <function_name>(input_data_type),...]
       ｜ ALL FUNCTIONS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

* このコマンドを実行する前に、SET CATALOG を実行する必要があります。
* db.function を使用して関数を表すこともできます。

  ```SQL
  REVOKE <priv> ON FUNCTION <db_name>.<function_name>(input_data_type) FROM {ROLE <role_name> | USER <user_identity>}
  ```

#### ストレージボリューム

```SQL
REVOKE
    CREATE STORAGE VOLUME 
    ON SYSTEM
    FROM { ROLE | USER} {<role_name>|<user_identity>}

REVOKE
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUME} 
    FROM { ROLE | USER} {<role_name>|<user_identity>}
```

### ロールを取り消す

```SQL
REVOKE <role_name> [,<role_name>, ...] FROM ROLE <role_name>
REVOKE <role_name> [,<role_name>, ...] FROM USER <user_identity>
```

## パラメータ

| **パラメータ**     | **説明**                                         |
| ------------------ | ----------------------------------------------- |
| role_name          | ロール名。                                      |
| user_identity      | ユーザー識別子。例: 'jack'@'192.%'.             |
| resourcegroup_name | リソースグループ名。                            |
| resource_name      | リソース名。                                    |
| function_name      | 関数名。                                        |
| catalog_name       | 外部カタログの名前。                            |
| database_name      | データベース名。                                |
| table_name         | テーブル名。                                    |
| view_name          | ビュー名。                                      |
| mv_name            | マテリアライズドビューの名前。                  |

## 例

### 権限を取り消す

ユーザー `jack` からテーブル `sr_member` の SELECT 権限を取り消します:

```SQL
REVOKE SELECT ON TABLE sr_member FROM USER 'jack'@'192.%'
```

ロール `test_role` からリソース `spark_resource` の USAGE 権限を取り消します:

```SQL
REVOKE USAGE ON RESOURCE 'spark_resource' FROM ROLE 'test_role';
```

### ロールを取り消す

ユーザー `jack` からロール `example_role` を取り消します:

```SQL
REVOKE example_role FROM 'jack'@'%';
```

ロール `test_role` からロール `example_role` を取り消します:

```SQL
REVOKE example_role FROM ROLE 'test_role';
```

## 参照

[GRANT](GRANT.md)