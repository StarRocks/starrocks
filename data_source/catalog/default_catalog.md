# Default catalog

本文介绍什么是 default catalog，以及如何使用 default catalog 查询 StarRocks 内部数据。

StarRocks 2.3 及以上版本提供了 internal catalog（内部数据目录），用于管理 StarRocks 的[内部数据](../catalog/catalog_overview.md#基本概念)。每个 StarRocks 集群都有且只有一个 internal catalog，名为 `default_catalog`。StarRocks 暂不支持修改 internal catalog 的名称，也不支持创建新的 internal catalog。

## 查询内部数据

1. 连接 StarRocks。
   - 如从 MySQL 客户端连接到 StarRocks。连接后，默认进入到 `default_catalog`。
   - 如使用 JDBC 连接到 StarRocks，连接时即可通过`default_catalog.db_name`的方式指定要连接的数据库。
2. （可选）执行以下语句查看当前 StarRocks 集群中的所有数据库。关于返回值说明，请参见 [SHOW DATABASES](/sql-reference/sql-statements/data-manipulation/SHOW%20DATABASES.md)。

    ```SQL
    SHOW DATABASES;
    ```

    或

    ```SQL
    SHOW DATABASES FROM default_catalog;
    ```

3. （可选）执行如下语句切换到指定数据库。

    ```SQL
    USE db_name;
    ```

    或

    ```SQL
    USE default_catalog.db_name；
    ```

4. 使用 [SELECT](/sql-reference/sql-statements/data-manipulation/SELECT.md) 语句查询内部数据。

## 示例

如要查询`olap_db.olap_table`中的数据，操作如下：

1. 使用`olap_db`作为当前数据库。

    ```SQL
    USE olap_db;
    ```

2. 查询`olap_table`表中的数据。

    ```SQL
    SELECT * FROM olap_table limit 1;
    ```

## 更多操作

如要查询外部数据，请参见[查询外部数据](/data_source/catalog/query_external_data.md)。
