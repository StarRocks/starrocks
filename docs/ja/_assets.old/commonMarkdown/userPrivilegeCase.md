ロールをカスタマイズして権限とユーザーを管理することをお勧めします。以下の例では、一般的なシナリオに対するいくつかの権限の組み合わせを分類しています。

#### StarRocks テーブルに対するグローバルな読み取り専用権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_only;
   -- すべてのカタログに対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- すべてのテーブルをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- すべてのビューをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- すべてのマテリアライズドビューをクエリし、それらを使用してクエリを高速化する権限をロールに付与します。
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   さらに、クエリで UDF を使用する権限を付与できます。

   ```SQL
   -- すべてのデータベースレベルの UDF に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- グローバル UDF に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

#### StarRocks テーブルに対するグローバルな書き込み権限を付与する

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

#### 特定の external catalog に対する読み取り専用権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_catalog_only;
   -- 対象の catalog に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   -- 対応する catalog に切り替えます。
   SET CATALOG hive_catalog;
   -- external catalog 内のすべてのテーブルとすべてのビューをクエリする権限を付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   :::tip
   external catalog のビューについては、Hive テーブルビューのみをクエリできます (v3.1 以降)。
   :::

#### 特定の external catalog に対する書き込み専用権限を付与する

Iceberg テーブル (v3.1 以降) および Hive テーブル (v3.2 以降) にのみデータを書き込むことができます。

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_catalog_only;
   -- 対象の catalog に対する USAGE 権限をロールに付与します。
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- 対応する catalog に切り替えます。
   SET CATALOG iceberg_catalog;
   -- Iceberg テーブルにデータを書き込む権限を付与します。
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

#### 特定のデータベースに対する管理者権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE db1_admin;
   -- 対象のデータベースに対するすべての権限をロールに付与します。このロールは、このデータベース内でテーブル、ビュー、マテリアライズドビュー、および UDF を作成できます。また、このデータベースを削除または変更することもできます。
   GRANT ALL ON DATABASE db1 TO ROLE db1_admin;
   -- 対応する catalog に切り替えます。
   SET CATALOG iceberg_catalog;
   -- このデータベース内のテーブル、ビュー、マテリアライズドビュー、および UDF に対するすべての権限をロールに付与します。
   GRANT ALL ON ALL TABLES IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL MATERIALIZED VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL FUNCTIONS IN DATABASE db1 TO ROLE db1_admin;
   ```

#### グローバル、データベース、テーブル、およびパーティションレベルでのバックアップおよびリストア操作を実行する権限を付与する

- グローバルなバックアップおよびリストア操作を実行する権限を付与する:

     グローバルなバックアップおよびリストア操作を実行する権限により、ロールは任意のデータベース、テーブル、またはパーティションをバックアップおよびリストアできます。これには、SYSTEM レベルでの REPOSITORY 権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、および任意のテーブルでのデータのロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- default catalog でのデータベース作成権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- 任意のデータベースでのテーブル作成権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover;
     -- 任意のテーブルでのデータのロードおよびエクスポート権限を付与します。
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- データベースレベルでのバックアップおよびリストア操作を実行する権限を付与する:

     データベースレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、任意のテーブルへのデータロード権限、およびバックアップ対象のデータベース内の任意のテーブルからのデータエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_db;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- データベース作成権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- テーブル作成権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover_db;
     -- 任意のテーブルへのデータロード権限を付与します。
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- バックアップ対象のデータベース内の任意のテーブルからのデータエクスポート権限を付与します。
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

- テーブルレベルでのバックアップおよびリストア操作を実行する権限を付与する:

     テーブルレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、対応するデータベースでのテーブル作成権限、データベース内の任意のテーブルへのデータロード権限、およびバックアップ対象のテーブルからのデータエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_tbl;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- 対応するデータベースでのテーブル作成権限を付与します。
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- データベース内の任意のテーブルへのデータロード権限を付与します。
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- バックアップ対象のテーブルからのデータエクスポート権限を付与します。
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- パーティションレベルでのバックアップおよびリストア操作を実行する権限を付与する:

     パーティションレベルでのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、および対応するテーブルでのデータのロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_par;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- 対応するテーブルでのデータのロードおよびエクスポート権限を付与します。
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```