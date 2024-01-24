
建议您通过自定义角色管理权限和用户。以下梳理了一些常见场景所需的权限项。

1. StarRocks 内表全局查询权限

   ```SQL
   -- 创建自定义角色。
   CREATE ROLE read_only;
   -- 赋予角色所有 Catalog 的使用权限。
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- 赋予角色所有表的查询权限。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- 赋予角色所有视图的查询权限。
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- 赋予角色所有物化视图的查询和加速权限。
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   您还可以进一步授予角色在查询中使用 UDF 的权限：

   ```SQL
   -- 赋予角色所有库级别 UDF 的使用权限。
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- 赋予角色所有全局 UDF 的使用权限。
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

2. StarRocks 内表全局写权限

   ```SQL
   -- 创建自定义角色。
   CREATE ROLE write_only;
   -- 赋予角色所有 Catalog 的使用权限。
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   -- 赋予角色所有表的导入、更新权限。
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   -- 赋予角色所有物化视图的更新权限。
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

3. 指定外部数据目录（External Catalog）下的查询权限

   ```SQL
   -- 创建自定义角色。
   CREATE ROLE read_catalog_only;
   -- 赋予角色目标 Catalog 的 USAGE 权限。
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   -- 切换到对应数据目录。
   SET CATALOG hive_catalog;
   -- 赋予角色所有表的查询权限。注意当前仅支持查询 Hive 表的视图 (自 3.1 版本起)。
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   ```

4. 指定外部数据目录（External Catalog）下的写权限

   当前仅支持写入数据到 Iceberg 表 (自 3.1 版本起) 和 Hive 表（自 3.2 版本起）。

   ```SQL
   -- 创建自定义角色。
   CREATE ROLE write_catalog_only;
   -- 赋予角色目标 Catalog 的 USAGE 权限。
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- 切换到对应数据目录。
   SET CATALOG iceberg_catalog;
   -- 赋予角色所有 Iceberg 表的写入权限。
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

5. 全局、数据库级、表级以及分区级备份恢复权限

   - 全局备份恢复权限

     全局备份恢复权限可以对任意库、表、分区进行备份恢复。需要 SYSTEM 级的 REPOSITORY 权限，在 Default Catalog 下创建数据库的权限，在任意数据库下创建表的权限，以及对任意表进行导入、导出的权限。

     ```SQL
     -- 创建自定义角色。
     CREATE ROLE recover;
     -- 赋予角色 SYSTEM 级的 REPOSITORY 权限。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- 赋予角色创建数据库的权限。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- 赋予角色创建任意表的权限。
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover;
     -- 赋予角色向任意表导入、导出数据的权限。
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

   - 数据库级备份恢复权限

     数据库级备份恢复权限可以对整个数据库进行备份恢复，需要 SYSTEM 级的 REPOSITORY 权限，在 Default Catalog 下创建数据库的权限，在任意数据库下创建表的权限，以及待备份数据库下所有表的导出权限。

     ```SQL
     -- 创建自定义角色。
     CREATE ROLE recover_db;
     -- 赋予角色 SYSTEM 级的 REPOSITORY 权限。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- 赋予角色创建数据库的权限。
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- 赋予角色创建任意表的权限。
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover_db;
     -- 赋予角色向任意表导入数据的权限。
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- 赋予角色向待备份数据库下所有表的导出权限。
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

   - 表级备份恢复权限

     表级备份恢复权限需要 SYSTEM 级的 REPOSITORY 权限，在待备份数据库下创建表及导入数据的权限，以及待备份表的导出权限。

     ```SQL
     -- 创建自定义角色。
     CREATE ROLE recover_tbl;
     -- 赋予角色 SYSTEM 级的 REPOSITORY 权限。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- 赋予角色在对应数据库下创建表的权限。
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- 赋予角色向任意表导入数据的权限。
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- 赋予角色导出待备份表数据的权限。
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

   - 分区级备份恢复权限

     分区级备份恢复权限需要 SYSTEM 级的 REPOSITORY 权限，以及对待备份表的导入、导出权限。

     ```SQL
     -- 创建自定义角色。
     CREATE ROLE recover_par;
     -- 赋予角色 SYSTEM 级的 REPOSITORY 权限。
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- 赋予角色对对应表进行导入的权限。
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```
