ロールをカスタマイズして、権限とユーザーを管理することをお勧めします。以下の例では、一般的なシナリオに対するいくつかの権限の組み合わせを分類しています。

#### StarRocks テーブルに対するグローバルな読み取り専用権限を付与

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

#### StarRocks テーブルに対するグローバルな書き込み権限を付与

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

#### 特定の external catalog に対する読み取り専用権限を付与

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_catalog_only;
   -- 対象のカタログに対する USAGE 権限をロールに付与します。
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG hive_catalog;
   -- external catalog 内のすべてのテーブルとビューをクエリする権限を付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   :::tip
   external catalog のビューについては、Hive テーブルビューのみクエリできます (v3.1 以降)。
   :::

#### 特定の external catalog に対する書き込み専用権限を付与

Iceberg テーブルにのみデータを書き込むことができます (v3.1 以降)。

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_catalog_only;
   -- 対象のカタログに対する USAGE 権限をロールに付与します。
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG iceberg_catalog;
   -- Iceberg テーブルにデータを書き込む権限を付与します。
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

#### 特定のデータベースに対する管理者権限を付与

   ```SQL
   -- ロールを作成します。
   CREATE ROLE db1_admin;
   -- 対象のデータベースに対するすべての権限をロールに付与します。このロールは、このデータベース内でテーブル、ビュー、マテリアライズドビュー、および UDF を作成できます。また、このデータベースを削除または変更することもできます。
   GRANT ALL ON DATABASE db1 TO ROLE db1_admin;
   -- 対応するカタログに切り替えます。
   SET CATALOG iceberg_catalog;
   -- このデータベース内のテーブル、ビュー、マテリアライズドビュー、および UDF に対するすべての権限をロールに付与します。
   GRANT ALL ON ALL TABLES IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL MATERIALIZED VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL FUNCTIONS IN DATABASE db1 TO ROLE db1_admin;
   ```

#### グローバル、データベース、テーブル、およびパーティションレベルでバックアップおよびリストア操作を実行する権限を付与

- グローバルなバックアップおよびリストア操作を実行する権限を付与:

     グローバルなバックアップおよびリストア操作を実行する権限は、任意のデータベース、テーブル、またはパーティションをバックアップおよびリストアすることを許可します。これには、SYSTEM レベルでの REPOSITORY 権限、default catalog でデータベースを作成する権限、任意のデータベースでテーブルを作成する権限、および任意のテーブルでデータをロードおよびエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- default catalog でデータベースを作成する権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- 任意のデータベースでテーブルを作成する権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover;
     -- 任意のテーブルでデータをロードおよびエクスポートする権限を付与します。
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- データベースレベルでバックアップおよびリストア操作を実行する権限を付与:

     データベースレベルでバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、default catalog でデータベースを作成する権限、任意のデータベースでテーブルを作成する権限、任意のテーブルにデータをロードする権限、およびバックアップ対象のデータベース内の任意のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_db;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- データベースを作成する権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- テーブルを作成する権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover_db;
     -- 任意のテーブルにデータをロードする権限を付与します。
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- バックアップ対象のデータベース内の任意のテーブルからデータをエクスポートする権限を付与します。
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

- テーブルレベルでバックアップおよびリストア操作を実行する権限を付与:

     テーブルレベルでバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、対応するデータベースでテーブルを作成する権限、データベース内の任意のテーブルにデータをロードする権限、およびバックアップ対象のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_tbl;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- 対応するデータベースでテーブルを作成する権限を付与します。
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- データベース内の任意のテーブルにデータをロードする権限を付与します。
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- バックアップ対象のテーブルからデータをエクスポートする権限を付与します。
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- パーティションレベルでバックアップおよびリストア操作を実行する権限を付与:

     パーティションレベルでバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでの REPOSITORY 権限、および対応するテーブルでデータをロードおよびエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_par;
     -- SYSTEM レベルでの REPOSITORY 権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- 対応するテーブルでデータをロードおよびエクスポートする権限を付与します。
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```