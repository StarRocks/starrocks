---
---

We recommend you customize roles to manage privileges and users. The following examples classify a few combinations of privileges for some common scenarios.

#### Grant global read-only privileges on StarRocks tables

   ```SQL
   -- Create a role.
   CREATE ROLE read_only;
   -- Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- Grant the privilege to query all tables to the role.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- Grant the privilege to query all views to the role.
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- Grant the privilege to query all materialized views and the privilege to accelerate queries with them to the role.
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   And you can further grant the privilege to use UDFs in queries:

   ```SQL
   -- Grant the USAGE privilege on all database-level UDF to the role.
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- Grant the USAGE privilege on global UDF to the role.
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

#### Grant global write privileges on StarRocks tables

   ```SQL
   -- Create a role.
   CREATE ROLE write_only;
   -- Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   -- Grant the INSERT and UPDATE privileges on all tables to the role.
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   -- Grant the REFRESH privilege on all materialized views to the role.
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

#### Grant read-only privileges on a specific external catalog

   ```SQL
   -- Create a role.
   CREATE ROLE read_catalog_only;
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   -- Switch to the corresponding catalog.
   SET CATALOG hive_catalog;
   -- Grant the privileges to query all tables and all views in the external catalog.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   :::tip
   For views in external catalogs, you can query only Hive table views (since v3.1).
   :::

#### Grant write-only privileges on a specific external catalog

You can only write data into Iceberg tables (since v3.1) and Hive tables (since v3.2).

   ```SQL
   -- Create a role.
   CREATE ROLE write_catalog_only;
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- Switch to the corresponding catalog.
   SET CATALOG iceberg_catalog;
   -- Grant the privilege to write data into Iceberg tables.
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

#### Grant admin privileges on a specific database

   ```SQL
   -- Create a role.
   CREATE ROLE db1_admin;
   -- Grant ALL privileges on the destination database to the role. This role can create tables, views, materialized views, and UDFs in this database. And it also can drop or modify this database.
   GRANT ALL ON DATABASE db1 TO ROLE db1_admin;
   -- Switch to the corresponding catalog.
   SET CATALOG iceberg_catalog;
   -- Grant all privileges on tables, views, materialized views, and UDFs in this database to the role.
   GRANT ALL ON ALL TABLES IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL MATERIALIZED VIEWS IN DATABASE db1 TO ROLE db1_admin;
   GRANT ALL ON ALL FUNCTIONS IN DATABASE db1 TO ROLE db1_admin;
   ```

#### Grant privileges to perform backup and restore operations on global, database, table, and partition levels

- Grant privileges to perform global backup and restore operations:

     The privileges to perform global backup and restore operations allow the role to back up and restore any database, table, or partition. It requires the REPOSITORY privilege on the SYSTEM level, the privileges to create databases in the default catalog, to create tables in any database, and to load and export data on any table.

     ```SQL
     -- Create a role.
     CREATE ROLE recover;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- Grant the privilege to create databases in the default catalog.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- Grant the privilege to create tables in any database.
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover;
     -- Grant the privilege to load and export data on any table.
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- Grant the privileges to perform database-level backup and restore operations:

     The privileges to perform database-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create databases in the default catalog, the privilege to create tables in any database, the privilege to load data into any table, and the privilege export data from any table in the database to be backed up.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_db;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- Grant the privilege to create databases.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- Grant the privilege to create tables.
     GRANT CREATE TABLE ON ALL DATABASES TO ROLE recover_db;
     -- Grant the privilege to load data into any table.
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- Grant the privilege to export data from any table in the database to be backed up.
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

- Grant the privileges to perform table-level backup and restore operations:

     The privileges to perform table-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create tables in corresponding databases, the privilege to load data into any table in the database, and the privilege to export data from the table to be backed up.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_tbl;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- Grant the privilege to create tables in corresponding databases.
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- Grant the privilege to load data into any table in a database.
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- Grant the privilege to export data from the table you want to back up.
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- Grant the privileges to perform partition-level backup and restore operations:

     The privileges to perform partition-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, and the privilege to load and export data on the corresponding table.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_par;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- Grant the privilege to load and export data on the corresponding table.
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```
