---
displayed_sidebar: "Chinese"
---

# Default catalog

本文介绍什么是 Default Catalog，以及如何使用 Default Catalog 查询 StarRocks 内部数据。

StarRocks 2.3 及以上版本提供了 Internal Catalog（内部数据目录），用于管理 StarRocks 的[内部数据](../catalog/catalog_overview.md#基本概念)。每个 StarRocks 集群都有且只有一个 Internal Catalog，名为 `default_catalog`。StarRocks 暂不支持修改 Internal Catalog 的名称，也不支持创建新的 Internal Catalog。

## 查询内部数据

1. 连接 StarRocks。
   - 如从 MySQL 客户端连接到 StarRocks。连接后，默认进入到 `default_catalog`。
   - 如使用 JDBC 连接到 StarRocks，连接时即可通过 `default_catalog.db_name` 的方式指定要连接的数据库。
2. （可选）通过 [SHOW DATABASES](../../sql-reference/sql-statements/data-manipulation/SHOW_DATABASES.md) 查看数据库：

   ```SQL
   SHOW DATABASES;
   ```

   或

   ```SQL
   SHOW DATABASES FROM default_catalog;
   ```

3. （可选）通过 [SET CATALOG](../../sql-reference/sql-statements/data-definition/SET_CATALOG.md) 切换当前会话生效的 Catalog：

   ```SQL
   SET CATALOG <catalog_name>;
   ```

   再通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 指定当前会话生效的数据库：

   ```SQL
   USE <db_name>;
   ```

   或者，也可以通过 [USE](../../sql-reference/sql-statements/data-definition/USE.md) 直接将会话切换到目标 Catalog 下的指定数据库：

   ```SQL
   USE <catalog_name>.<db_name>;
   ```

4. 通过 [SELECT](../../sql-reference/sql-statements/data-manipulation/SELECT.md) 查询内部数据：

   ```SQL
   SELECT * FROM <table_name>;
   ```

   如在以上步骤中未指定数据库，则可以在查询语句中直接指定。

   ```SQL
   SELECT * FROM <db_name>.<table_name>;
   ```

   或

   ```SQL
   SELECT * FROM default_catalog.<db_name>.<table_name>;
   ```

## 示例

如要查询 `olap_db.olap_table` 中的数据，操作如下：

 ```SQL
USE olap_db;
SELECT * FROM olap_table limit 1;
```

或

```SQL
SELECT * FROM olap_db.olap_table limit 1;   
```

或

```SQL
SELECT * FROM default_catalog.olap_db.olap_table limit 1;
```

## 更多操作

如要查询外部数据，请参见[查询外部数据](./query_external_data.md)。
