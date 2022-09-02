# 查询外部数据

本文介绍如何通过 external catalog 查询外部数据。

## 前提条件

根据数据源已创建不同类型 external catalog。关于当前支持的 external catalog 类型，请参见 [Catalog](../catalog/catalog_overview.md#catalog)。

## 操作步骤

1. 连接 StarRocks。
   - 如从 MySQL 客户端连接到 StarRocks。连接后，默认进入到 `default_catalog`。
   - 如使用 JDBC 连接到 StarRocks，连接时即可通过`default_catalog.db_name`的方式指定要连接的数据库。

2. （可选）执行以下语句查看当前 StarRocks 集群中的所有 catalog 并找到指定的 external catalog。有关返回值说明，请参见 [SHOW CATALOGS](/sql-reference/sql-statements/data-manipulation/SHOW%20CATALOGS.md)。

    ```SQL
    SHOW CATALOGS;
    ```

3. （可选）执行以下语句查看指定 external catalog 中的数据库。有关参数和返回值说明，请参见 [SHOW DATABASES](/sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md)。

    ```SQL
    SHOW DATABASES FROM catalog_name;
    ```

4. （可选）执行以下语句将当前会话切换到指定 external catalog 的指定数据库。

    ```SQL
    USE catalog_name.db_name;
    ```

5. 使用 [SELECT](/sql-reference/sql-statements/data-manipulation/SELECT.md) 语句查询外部数据。

## 示例

创建一个名为`hive1`的 Hive catalog 。如需通过`hive1`查询 Apache Hive™ 集群中`hive_db.hive_table`的数据，操作如下：

1. 将当前会话切换到`hive1.hive_db`。

    ```SQL
    USE hive1.hive_db;
    ```

2. 查询`hive_tabel`表中的数据。

    ```SQL
    SELECT * FROM hive_table limit 1;
    ```

## 更多操作

如要查询 StarRocks 的内部数据，请参见[查询内部数据](../catalog/default_catalog.md#查询内部数据)。
