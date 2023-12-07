---
displayed_sidebar: "Chinese"
---

# SHOW CREATE VIEW

## 功能

查看指定逻辑视图的创建语句 CREATE VIEW。只有拥有该视图和视图对应基表的 `SELECT_PRIV` 权限的用户才可以查看。视图创建语句可以帮助您理解视图定义，作为后续修改视图或重建视图的参考。

从 2.5.4 版本开始，为了兼容 MySQL 标准语法，支持使用 SHOW CREATE VIEW 来查看异步物化视图的创建语句。该语句将物化视图当做普通视图来处理。

## 语法

```SQL
SHOW CREATE VIEW [<db_name>.]<view_name>
```

## 参数说明

| **参数**  | **必选** | **说明**                                                     |
| --------- | -------- | ------------------------------------------------------------ |
| db_name   | 否       | 数据库名称。如不指定，则默认查看当前数据库中指定视图的创建语句。 |
| view_name | 是       | 视图名称。                                                   |

## 返回结果说明

```plain
+---------+--------------+----------------------+----------------------+
| View    | Create View  | character_set_client | collation_connection |
+---------+--------------+----------------------+----------------------+
```

返回结果中的参数说明如下：

| **参数**             | **说明**                                  |
| -------------------- | ----------------------------------------- |
| View                 | 视图名称。                                |
| Create View          | 视图的创建语句。                          |
| character_set_client | 客户端连接 StarRocks 服务端使用的字符集。 |
| collation_connection | 字符集的校对规则。                        |

## 示例

### 查看逻辑视图创建语句

创建表 `base`。

```SQL
CREATE TABLE base (
    k1 date,
    k2 int,
    v1 int sum)
PARTITION BY RANGE(k1)
(
    PARTITION p1 values less than('2020-02-01'),
    PARTITION p2 values less than('2020-03-01')
)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES( "replication_num"  = "1");
```

在表 `base` 上创建视图 `example_view`。

```SQL
CREATE VIEW example_view (k1, k2, v1)
AS SELECT k1, k2, v1 FROM base;
```

查看视图 `example_view` 的创建语句。

```Plain
SHOW CREATE VIEW example_view;

MySQL [yn_db]> SHOW CREATE VIEW example_view;
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
| View         | Create View                                                                                                                                         | character_set_client | collation_connection |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
| example_view | CREATE VIEW `example_view` (k1, k2, v1) COMMENT "VIEW" AS SELECT `yn_db`.`base`.`k1`, `yn_db`.`base`.`k2`, `yn_db`.`base`.`v1`
FROM `yn_db`.`base`; | utf8                 | utf8_general_ci      |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+

```

### 查看物化视图创建语句

在表 `base` 上创建物化视图 `example_mv`。

```SQL
CREATE MATERIALIZED VIEW example_mv distributed by hash(k1)
AS SELECT k1 FROM base;
```

查看物化视图 `example_mv` 的创建语句。

```Plain
SHOW CREATE VIEW example_mv;
+------------+----------------------------------------------------------------------------+----------------------+----------------------+
| View       | Create View                                                                | character_set_client | collation_connection |
+------------+----------------------------------------------------------------------------+----------------------+----------------------+
| example_mv | CREATE VIEW `example_mv` AS SELECT `yn_db`.`base`.`k1`
FROM `yn_db`.`base` | utf8                 | utf8_general_ci      |
+------------+------------------------------------------------
```
