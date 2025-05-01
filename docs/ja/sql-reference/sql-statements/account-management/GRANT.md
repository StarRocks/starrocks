---
displayed_sidebar: docs
---

# GRANT

## 説明

GRANT ステートメントを使用して、次の操作を実行できます。

- 特定の権限をユーザーまたはロールに付与します。
- ロールをユーザーに付与します。この機能は StarRocks 2.4 以降のバージョンでサポートされています。
- ユーザー `a` にユーザー `b` を偽装する権限を付与します。その後、ユーザー `a` は [EXECUTE AS](../account-management/EXECUTE_AS.md) ステートメントを使用してユーザー `b` として操作を実行できます。この機能は StarRocks 2.4 以降のバージョンでサポートされています。

## 構文

- データベースおよびテーブルに対する特定の権限をユーザーまたはロールに付与します。ロールまたはユーザーは存在している必要があります。

    ```SQL
    GRANT privilege_list ON db_name.tbl_name TO {user_identity | ROLE 'role_name'}
    ```

- リソースに対する特定の権限をユーザーまたはロールに付与します。ロールまたはユーザーは存在している必要があります。

    ```SQL
    GRANT privilege_list ON RESOURCE 'resource_name' TO {user_identity | ROLE 'role_name'};
    ```

- ユーザー `a` にユーザー `b` を偽装して操作を実行する権限を付与します。

    ```SQL
    GRANT IMPERSONATE ON user_identity_b TO user_identity_a;
    ```

- ユーザーにロールを付与します。付与されるロールは存在している必要があります。

    ```SQL
    GRANT 'role_name' TO user_identity;
    ```

## パラメータ

### privilege_list

ユーザーまたはロールに付与できる権限です。複数の権限を一度に付与したい場合は、カンマ（`,`）で権限を区切ります。例 3 を参照してください。次の権限がサポートされています。

- `NODE_PRIV`: ノードの有効化や無効化など、クラスターノードを管理する権限。
- `ADMIN_PRIV`: `NODE_PRIV` を除くすべての権限。
- `GRANT_PRIV`: ユーザーやロールの作成、削除、権限の付与や取り消し、アカウントのパスワード設定などの操作を行う権限。
- `SELECT_PRIV`: データベースおよびテーブルの読み取り権限。
- `LOAD_PRIV`: データベースおよびテーブルへのデータロード権限。
- `ALTER_PRIV`: データベースおよびテーブルのスキーマを変更する権限。
- `CREATE_PRIV`: データベースおよびテーブルを作成する権限。
- `DROP_PRIV`: データベースおよびテーブルを削除する権限。
- `USAGE_PRIV`: リソースを使用する権限。

これらの権限は次の3つのカテゴリに分類できます。

- ノード権限: `NODE_PRIV`
- データベースおよびテーブル権限: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, `DROP_PRIV`
- リソース権限: `USAGE_PRIV`

### db_name.tbl_name

データベースとテーブル。このパラメータは次の3つの形式をサポートしています。

- `*.*`: クラスター内のすべてのデータベースとテーブルを示します。この形式が指定された場合、グローバル権限が付与されます。
- `db.*`: 特定のデータベースとそのデータベース内のすべてのテーブルを示します。
- `db.tbl`: 特定のデータベース内の特定のテーブルを示します。

> 注: `db.*` または `db.tbl` 形式を使用する場合、存在しないデータベースまたはテーブルを指定することができます。

### resource_name

リソース名。このパラメータは次の2つの形式をサポートしています。

- `*`: すべてのリソースを示します。
- `resource`: 特定のリソースを示します。

> 注: `resource` 形式を使用する場合、存在しないリソースを指定することができます。

### user_identity

このパラメータは `user_name` と `host` の2つの部分で構成されています。`user_name` はユーザー名を示します。`host` はユーザーのIPアドレスを示します。`host` を指定しないか、`host` にドメインを指定することができます。`host` を指定しない場合、`host` はデフォルトで `%` となり、任意のホストから StarRocks にアクセスできます。`host` にドメインを指定した場合、権限が有効になるまでに1分かかることがあります。`user_identity` パラメータは CREATE USER ステートメントによって作成される必要があります。

### role_name

ロール名。

## 例

例 1: すべてのデータベースとテーブルに対する読み取り権限をユーザー `jack` に付与します。

```SQL
GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
```

例 2: `db1` およびこのデータベース内のすべてのテーブルに対するデータロード権限を `my_role` に付与します。

```SQL
GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
```

例 3: `db1` の `tbl1` に対する読み取り権限、スキーマ変更権限、およびデータロード権限をユーザー `jack` に付与します。

```SQL
GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
```

例 4: すべてのリソースを使用する権限をユーザー `jack` に付与します。

```SQL
GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
```

例 5: `spark_resource` を使用する権限をユーザー `jack` に付与します。

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

例 6: `spark_resource` を使用する権限をロール `my_role` に付与します。

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

例 7: `my_role` をユーザー `jack` に付与します。

```SQL
GRANT 'my_role' TO 'jack'@'%';
```

例 8: ユーザー `jack` にユーザー `rose` を偽装して操作を実行する権限を付与します。

```SQL
GRANT IMPERSONATE ON 'rose'@'%' TO 'jack'@'%';
```

## ベストプラクティス - シナリオに基づいてロールをカスタマイズ

権限とユーザーを管理するためにロールをカスタマイズすることをお勧めします。以下の例では、いくつかの一般的なシナリオに対する権限の組み合わせを分類しています。

### StarRocks テーブルに対するグローバルな読み取り専用権限を付与

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_only;
   -- すべてのカタログに対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- すべてのテーブルをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- すべてのビューをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- すべてのマテリアライズドビューをクエリし、それらを使用してクエリを加速する権限をロールに付与します。
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   さらに、クエリで UDF を使用する権限を付与することができます。

   ```SQL
   -- すべてのデータベースレベルの UDF に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- グローバル UDF に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

### StarRocks テーブルに対するグローバルな書き込み権限を付与

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_only;
   -- すべてのカタログに対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   -- すべてのテーブルに対する INSERT および UPDATE 権限をロールに付与します。
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   -- すべてのマテリアライズドビューに対する REFRESH 権限をロールに付与します。
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

### 特定の external catalog に対する読み取り専用権限を付与

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG hive_catalog;
   -- すべてのデータベース内のすべてのテーブルおよびすべてのビューをクエリする権限を付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   注: Hive テーブルビューのみをクエリできます（v3.1 以降）。

### 特定の external catalog に対する書き込み専用権限を付与

Iceberg テーブルにのみデータを書き込むことができます（v3.1 以降）。

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG iceberg_catalog;
   -- Iceberg テーブルにデータを書き込む権限を付与します。
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

### グローバル、データベース、テーブル、パーティションレベルでのバックアップおよびリストア操作を実行する権限を付与

- グローバルなバックアップおよびリストア操作を実行する権限を付与:

     グローバルなバックアップおよびリストア操作を実行する権限は、任意のデータベース、テーブル、またはパーティションをバックアップおよびリストアすることを許可します。これには、SYSTEM レベルでの REPOSITORY 権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、任意のテーブルでのデータロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- default catalog でのデータベース作成権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- 任意のデータベースでのテーブル作成権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover;
     -- 任意のテーブルでのデータロードおよびエクスポート権限を付与します。
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- データベースレベルでのバックアップおよびリストア操作を実行する権限を付与:

     データベースレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、任意のテーブルへのデータロード権限、およびバックアップ対象のデータベース内の任意のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_db;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- データベース作成権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- テーブル作成権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover_db;
     -- 任意のテーブルへのデータロード権限を付与します。
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- バックアップ対象のデータベース内の任意のテーブルからデータをエクスポートする権限を付与します。
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

- テーブルレベルでのバックアップおよびリストア操作を実行する権限を付与:

     テーブルレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、対応するデータベースでのテーブル作成権限、データベース内の任意のテーブルへのデータロード権限、およびバックアップ対象のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_tbl;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- 対応するデータベースでのテーブル作成権限を付与します。
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- データベース内の任意のテーブルへのデータロード権限を付与します。
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- バックアップ対象のテーブルからデータをエクスポートする権限を付与します。
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- パーティションレベルでのバックアップおよびリストア操作を実行する権限を付与:

     パーティションレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、および対応するテーブルでのデータロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_par;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- 対応するテーブルでのデータロードおよびエクスポート権限を付与します。
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```

マルチサービスアクセス制御のベストプラクティスについては、[Multi-service access control](../../../administration/User_privilege.md#scenario-2-multiple-lines-of-business) を参照してください。