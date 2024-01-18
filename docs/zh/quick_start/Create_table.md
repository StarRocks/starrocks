---
displayed_sidebar: "Chinese"
---

# 创建表

本文介绍如何在 StarRocks 中创建表以及进行相关操作。

> **注意**
>
> 只有拥有 default_catalog 下 [CREATE DATABASE](../administration/privilege_item.md) 权限的用户才可以创建数据库。只有拥有该数据库 CREATE TABLE 权限的用户才可以在该数据库下创建表。

## 连接 StarRocks

在成功 [部署 StarRocks 集群](../quick_start/deploy_with_docker.md) 后，您可以通过 MySQL 客户端连接任意一个 FE 节点的 `query_port`（默认为 `9030`）以连接 StarRocks。StarRocks 内置 `root` 用户，密码默认为空。

```shell
mysql -h <fe_host> -P9030 -u root
```

## 创建数据库

创建 `example_db` 数据库。

> **注意**
>
> 在指定数据库名、表名和列名等变量时，如果使用了保留关键字，必须使用反引号 (`) 包裹，否则可能会产生报错。有关 StarRocks 的保留关键字列表，请参见[关键字](../sql-reference/sql-statements/keywords.md#保留关键字)。

```sql
CREATE DATABASE example_db;
```

您可以通过 `SHOW DATABASES;` 命令查看当前 StarRocks 集群中所有数据库。

```Plain Text
MySQL [(none)]> SHOW DATABASES;

+--------------------+
| Database           |
+--------------------+
| _statistics_       |
| example_db         |
| information_schema |
+--------------------+
3 rows in set (0.00 sec)
```

> 说明：与 MySQL 的表结构类似，`information_schema` 包含当前 StarRocks 集群的元数据信息，但是部分统计信息还不完善。推荐您通过 `DESC table_name` 等命令来获取数据库元数据信息。

## 创建表

在新建的数据库中创建表。

StarRocks 支持 [多种表类型](../table_design/table_types/table_types.md)，以适用不同的应用场景。以下示例基于 [明细表](../table_design/table_types/duplicate_key_table.md) 编写建表语句。

更多建表语法，参考 [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 。

```sql
use example_db;
CREATE TABLE IF NOT EXISTS `detailDemo` (
    `recruit_date`  DATE           NOT NULL COMMENT "YYYY-MM-DD",
    `region_num`    TINYINT        COMMENT "range [-128, 127]",
    `num_plate`     SMALLINT       COMMENT "range [-32768, 32767] ",
    `tel`           INT            COMMENT "range [-2147483648, 2147483647]",
    `id`            BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    `password`      LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    `name`          CHAR(20)       NOT NULL COMMENT "range char(m),m in (1-255)",
    `profile`       VARCHAR(500)   NOT NULL COMMENT "upper limit value 1048576 bytes",
    `hobby`         STRING         NOT NULL COMMENT "upper limit value 65533 bytes",
    `leave_time`    DATETIME       COMMENT "YYYY-MM-DD HH:MM:SS",
    `channel`       FLOAT          COMMENT "4 bytes",
    `income`        DOUBLE         COMMENT "8 bytes",
    `account`       DECIMAL(12,4)  COMMENT "",
    `ispass`        BOOLEAN        COMMENT "true/false"
) ENGINE=OLAP
DUPLICATE KEY(`recruit_date`, `region_num`)
PARTITION BY RANGE(`recruit_date`)
(
    PARTITION p20220311 VALUES [('2022-03-11'), ('2022-03-12')),
    PARTITION p20220312 VALUES [('2022-03-12'), ('2022-03-13')),
    PARTITION p20220313 VALUES [('2022-03-13'), ('2022-03-14')),
    PARTITION p20220314 VALUES [('2022-03-14'), ('2022-03-15')),
    PARTITION p20220315 VALUES [('2022-03-15'), ('2022-03-16'))
)
DISTRIBUTED BY HASH(`recruit_date`, `region_num`);
```

> 注意
>
> * 在 StarRocks 中，字段名不区分大小写，表名区分大小写。
> * 自 3.1 版本起，您在建表时可以不设置分桶键（即 DISTRIBUTED BY 子句）。StarRocks 默认使用随机分桶，将数据随机地分布在分区的所有分桶中。更多信息，请参见[随机分桶](../table_design/Data_distribution.md#随机分桶自-v31)。

### 建表语句说明

#### 排序键

StarRocks 表内部组织存储数据时会按照指定列排序，这些列为排序列（Sort Key）。明细表中由 `DUPLICATE KEY` 指定排序列。以上示例中的 `recruit_date` 以及 `region_num` 两列为排序列。

> 注意：排序列在建表时应定义在其他列之前。排序键详细描述以及不同类型表中排序键的设置方法请参考 [排序键](../table_design/Sort_key.md)。

#### 字段类型

StarRocks 表中支持多种字段类型，除以上示例中已经列举的字段类型，还支持 [BITMAP 类型](../using_starrocks/Using_bitmap.md)，[HLL 类型](../using_starrocks/Using_HLL.md)，[ARRAY 类型](../sql-reference/sql-statements/data-types/Array.md)，字段类型介绍详见 [数据类型章节](../sql-reference/sql-statements/data-types/BIGINT.md)。

> 注意：在建表时，您应尽量使用精确的类型。例如，整型数据不应使用字符串类型，INT 类型即可满足的数据不应使用 BIGINT 类型。精确的数据类型能够更好的发挥数据库的性能。

#### 分区分桶

`PARTITION` 关键字用于给表 [创建分区](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#partition_desc)。以上示例中使用 `recruit_date` 进行范围分区，从 11 日到 15 日每天创建一个分区。StarRocks 支持动态生成分区，详见 [动态分区管理](../table_design/dynamic_partitioning.md)。**为了优化生产环境的查询性能，我们强烈建议您为表制定合理的数据分区计划。**

`DISTRIBUTED` 关键字用于给表 [创建分桶](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#distribution_desc)，以上示例中使用 `recruit_date` 以及 `region_num` 两个字段作为分桶列。并且自 2.5.7 起 StarRocks 支持自动设置分桶数量，您无需手动设置分桶数量，详见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

创建表时合理的分区和分桶设计可以优化表的查询性能。有关分区分桶列如何选择，详见 [数据分布](../table_design/Data_distribution.md)。

#### 表类型

`DUPLICATE` 关键字表示当前表为明细表，`KEY` 中的列表示当前表的排序列。StarRocks 支持多种表类型，分别为 [明细表](../table_design/table_types/duplicate_key_table.md)，[聚合表](../table_design/table_types/aggregate_table.md)，[更新表](../table_design/table_types/unique_key_table.md)，[主键表](../table_design/table_types/primary_key_table.md)。不同模型的适用于多种业务场景，合理选择可优化查询效率。

#### 索引

StarRocks 默认会给 Key 列创建稀疏索引加速查询，具体规则见 [排序键](../table_design/Sort_key.md)。支持的索引类型有 [Bitmap 索引](../using_starrocks/Bitmap_index.md)，[Bloomfilter 索引](../using_starrocks/Bloomfilter_index.md) 等。

> 注意：索引创建对表模型和列有要求，详细说明见对应索引介绍章节。

#### ENGINE 类型

默认 ENGINE 类型为 `olap`，对应 StarRocks 集群内部表。其他可选项包括 `mysql`，`elasticsearch`，`hive`，`jdbc`（2.3 及以后），`hudi`（2.2 及以后）以及 `iceberg`，分别代表所创建的表为相应类型的 [外部表](../data_source/External_table.md)。

## 查看表信息

您可以通过 SQL 命令查看表的相关信息。

* 查看当前数据库中所有的表

```sql
SHOW TABLES;
```

* 查看表的结构

```sql
DESC table_name;
```

示例：

```sql
DESC detailDemo;
```

* 查看建表语句

```sql
SHOW CREATE TABLE table_name;
```

示例：

```sql
SHOW CREATE TABLE detailDemo;
```

<br/>

## 修改表结构

StarRocks 支持多种 DDL 操作。

您可以通过 [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md#schema-change) 命令可以修改表的 Schema，包括增加列，删除列，修改列类型（暂不支持修改列名称），改变列顺序。

### 增加列

例如，在以上创建的表中，在 `ispass` 列后新增一列 `uv`，类型为 BIGINT，默认值为 `0`。

```sql
ALTER TABLE detailDemo ADD COLUMN uv BIGINT DEFAULT '0' after ispass;
```

### 删除列

删除以上步骤新增的列。

> 注意
>
> 如果您通过上述步骤添加了 `uv`，请务必删除此列以保证后续 Quick Start 内容可以执行。

```sql
ALTER TABLE detailDemo DROP COLUMN uv;
```

### 查看修改表结构作业状态

修改表结构为异步操作。提交成功后，您可以通过以下命令查看作业状态。

```sql
SHOW ALTER TABLE COLUMN\G;
```

当作业状态为 FINISHED，则表示作业完成，新的表结构修改已生效。

修改 Schema 完成之后，您可以通过以下命令查看最新的表结构。

```sql
DESC table_name;
```

示例如下：

```Plain Text
MySQL [example_db]> desc detailDemo;

+--------------+-----------------+------+-------+---------+-------+
| Field        | Type            | Null | Key   | Default | Extra |
+--------------+-----------------+------+-------+---------+-------+
| recruit_date | DATE            | No   | true  | NULL    |       |
| region_num   | TINYINT         | Yes  | true  | NULL    |       |
| num_plate    | SMALLINT        | Yes  | false | NULL    |       |
| tel          | INT             | Yes  | false | NULL    |       |
| id           | BIGINT          | Yes  | false | NULL    |       |
| password     | LARGEINT        | Yes  | false | NULL    |       |
| name         | CHAR(20)        | No   | false | NULL    |       |
| profile      | VARCHAR(500)    | No   | false | NULL    |       |
| hobby        | VARCHAR(65533)  | No   | false | NULL    |       |
| leave_time   | DATETIME        | Yes  | false | NULL    |       |
| channel      | FLOAT           | Yes  | false | NULL    |       |
| income       | DOUBLE          | Yes  | false | NULL    |       |
| account      | DECIMAL64(12,4) | Yes  | false | NULL    |       |
| ispass       | BOOLEAN         | Yes  | false | NULL    |       |
| uv           | BIGINT          | Yes  | false | 0       |       |
+--------------+-----------------+------+-------+---------+-------+
15 rows in set (0.00 sec)
```

### 取消修改表结构

您可以通过以下命令取消当前正在执行的作业。

```sql
CANCEL ALTER TABLE COLUMN FROM table_name\G;
```

## 创建用户并授权

`example_db` 数据库创建完成之后，您可以创建 `test` 用户，并授予其 `example_db` 的读写权限。

```sql
CREATE USER 'test' IDENTIFIED by '123456';
GRANT ALL on example_db.* to test;
```

通过登录被授权的 `test` 账户，就可以操作 `example_db` 数据库。

```bash
mysql -h 127.0.0.1 -P9030 -utest -p123456
```

<br/>

## 下一步

表创建成功后，您可以 [导入并查询数据](../quick_start/Import_and_query.md)。
