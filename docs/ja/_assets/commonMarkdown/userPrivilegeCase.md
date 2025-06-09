ロールをカスタマイズして、権限とユーザーを管理することをお勧めします。以下の例は、一般的なシナリオにおけるいくつかの権限の組み合わせを分類しています。

#### StarRocks テーブルに対するグローバルな読み取り専用権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_only;
   -- すべてのカタログに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- すべてのテーブルをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- すべてのビューをクエリする権限をロールに付与します。
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- すべてのマテリアライズドビューをクエリし、それらを使用してクエリを高速化する権限をロールに付与します。
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   さらに、クエリでUDFを使用する権限を付与できます。

   ```SQL
   -- すべてのデータベースレベルのUDFに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- グローバルUDFに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

#### StarRocks テーブルに対するグローバルな書き込み権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_only;
   -- すべてのカタログに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   -- すべてのテーブルに対するINSERTおよびUPDATE権限をロールに付与します。
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   -- すべてのマテリアライズドビューに対するREFRESH権限をロールに付与します。
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

#### 特定の external catalog に対する読み取り専用権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE read_catalog_only;
   -- 対象のカタログに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG hive_catalog;
   -- external catalog 内のすべてのテーブルとすべてのビューをクエリする権限を付与します。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   :::tip
   external catalog のビューについては、Hive テーブルビューのみをクエリできます (v3.1以降)。
   :::

#### 特定の external catalog に対する書き込み専用権限を付与する

Iceberg テーブル (v3.1以降) および Hive テーブル (v3.2以降) にのみデータを書き込むことができます。

   ```SQL
   -- ロールを作成します。
   CREATE ROLE write_catalog_only;
   -- 対象のカタログに対するUSAGE権限をロールに付与します。
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- 対応するカタログに切り替えます。
   SET CATALOG iceberg_catalog;
   -- Iceberg テーブルにデータを書き込む権限を付与します。
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

#### 特定のデータベースに対する管理者権限を付与する

   ```SQL
   -- ロールを作成します。
   CREATE ROLE db1_admin;
   -- 対象のデータベースに対するすべての権限をロールに付与します。このロールは、このデータベースでテーブル、ビュー、マテリアライズドビュー、およびUDFを作成できます。また、このデータベースを削除または変更することもできます。
   GRANT ALL ON DATABASE db1 TO ROLE db1_admin;
   -- 対応するカタログに切り替えます。
   SET CATALOG iceberg_catalog;
   -- このデータベース内のテーブル、ビュー、マテリアライズドビュー、およびUDFに対するすべての権限をロールに付与します。
   GRANT ALL ON ALL TABLES IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL MATERIALIZED VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL FUNCTIONS IN DATABASE db1 TO ROLE db1_admin;
   ```

#### グローバル、データベース、テーブル、およびパーティションレベルでバックアップおよびリストア操作を実行する権限を付与する

- グローバルバックアップおよびリストア操作を実行する権限を付与する:

     グローバルバックアップおよびリストア操作を実行する権限は、任意のデータベース、テーブル、またはパーティションをバックアップおよびリストアすることを許可します。これには、SYSTEM レベルでのREPOSITORY権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、および任意のテーブルでのデータのロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover;
     -- SYSTEM レベルでのREPOSITORY権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- default catalog でデータベースを作成する権限を付与します。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- 任意のデータベースでテーブルを作成する権限を付与します。
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover;
     -- 任意のテーブルでデータをロードおよびエクスポートする権限を付与します。
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- データベースレベルのバックアップおよびリストア操作を実行する権限を付与する:

     データベースレベルのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでのREPOSITORY権限、default catalog でのデータベース作成権限、任意のデータベースでのテーブル作成権限、任意のテーブルへのデータロード権限、およびバックアップ対象のデータベース内の任意のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_db;
     -- SYSTEM レベルでのREPOSITORY権限を付与します。
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

- テーブルレベルのバックアップおよびリストア操作を実行する権限を付与する:

     テーブルレベルのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでのREPOSITORY権限、対応するデータベースでのテーブル作成権限、データベース内の任意のテーブルにデータをロードする権限、およびバックアップ対象のテーブルからデータをエクスポートする権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_tbl;
     -- SYSTEM レベルでのREPOSITORY権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- 対応するデータベースでテーブルを作成する権限を付与します。
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- データベース内の任意のテーブルにデータをロードする権限を付与します。
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- バックアップしたいテーブルからデータをエクスポートする権限を付与します。
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- パーティションレベルのバックアップおよびリストア操作を実行する権限を付与する:

     パーティションレベルのバックアップおよびリストア操作を実行する権限には、SYSTEM レベルでのREPOSITORY権限、および対応するテーブルでのデータのロードおよびエクスポート権限が必要です。

     ```SQL
     -- ロールを作成します。
     CREATE ROLE recover_par;
     -- SYSTEM レベルでのREPOSITORY権限を付与します。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- 対応するテーブルでデータをロードおよびエクスポートする権限を付与します。
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```