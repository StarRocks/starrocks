# 创建表

## 使用 MySQL 客户端访问 StarRocks

安装部署好 StarRocks 集群后，可使用 MySQL 客户端连接某一个 FE 实例的 query_port(默认 9030)连接 StarRocks， StarRocks 内置 root 用户，密码默认为空：

```shell
mysql -h fe_host -P9030 -u root
```

## 创建数据库

使用 root 用户建立 example\_db 数据库:

```sql
create database example_db;
```

通过 `show databases;` 查看数据库信息：

```Plain Text
show databases;

+--------------------+
| Database           |
+--------------------+
| example_db         |
| information_schema |
+--------------------+
2 rows in set (0.00 sec)
```

Information_schema 的表结构类似 MySQL，但是部分统计信息还不完善，当前推荐通过 `desc tablename` 等命令来获取数据库元数据信息。
<br/>

## 建表

StarRocks 支持 [多种数据模型](../table_design/Data_model.md)，分别适用于不同的应用场景，以 [明细表](../table_design/Data_model.md#明细模型) 为例书写建表语句：

```sql
use example_db;
CREATE TABLE IF NOT EXISTS detailDemo (
    make_time     DATE           NOT NULL COMMENT "YYYY-MM-DD",
    mache_verson  TINYINT        COMMENT "range [-128, 127]",
    mache_num     SMALLINT       COMMENT "range [-32768, 32767] ",
    de_code       INT            COMMENT "range [-2147483648, 2147483647]",
    saler_id      BIGINT         COMMENT "range [-2^63 + 1 ~ 2^63 - 1]",
    pd_num        LARGEINT       COMMENT "range [-2^127 + 1 ~ 2^127 - 1]",
    pd_type       CHAR(20)        NOT NULL COMMENT "range char(m),m in (1-255) ",
    pd_desc       VARCHAR(500)   NOT NULL COMMENT "upper limit value 65533 bytes",
    us_detail     STRING         NOT NULL COMMENT "upper limit value 65533 bytes",
    relTime       DATETIME       COMMENT "YYYY-MM-DD HH:MM:SS",
    channel       FLOAT          COMMENT "4 bytes",
    income        DOUBLE         COMMENT "8 bytes",
    account       DECIMAL(12,4)  COMMENT "",
    ispass        BOOLEAN        COMMENT "true/false"
) ENGINE=OLAP
DUPLICATE KEY(make_time, mache_verson)
PARTITION BY RANGE (make_time) (
    START ("2022-03-11") END ("2022-03-15") EVERY (INTERVAL 1 day)
)
DISTRIBUTED BY HASH(make_time, mache_verson) BUCKETS 8
PROPERTIES(
    "replication_num" = "3",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-3",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);
```

可以通过 `show tables;` 命令查看当前库的所有表，通过 `desc table_name;` 命令可以查看表结构。通过 `show create table table_name;` 可查看建表语句。请注意：在 StarRocks 中字段名不区分大小写，表名区分大小写。

表创建成功后，可以参考 [导入查询](/quick_start/Import_and_query.md) 章节 [Stream load Demo](/quick_start/Import_and_query.md#stream-load%E5%AF%BC%E5%85%A5demo) 进行数据导入及查询操作。

更多建表语法详见 [CREATE TABLE](/sql-reference/sql-statements/data-definition/CREATE_TABLE.md) 章节。

### 建表语句说明

#### 排序键

StarRocks 表内部组织存储数据时会按照指定列排序，这些列为排序列（Sort Key），明细模型中由 `DUPLICATE KEY` 指定排序列，以上 demo 中的 `make_time, mache_verson` 两列为排序列。注意排序列在建表时应定义在其他列之前。排序键详细描述以及不同数据模型的表的设置方法请参考 [排序键](../table_design/Sort_key.md)。

#### 字段类型

StarRocks 表中支持多种字段类型，除 demo 中已经列举的字段类型，还支持 [BITMAP 类型](/using_starrocks/Using_bitmap.md)，[HLL 类型](../using_starrocks/Using_HLL.md)，[Array 类型](../using_starrocks/Array.md)，字段类型介绍详见 [数据类型章节](/sql-reference/sql-statements/data-types/)。

建表时尽量使用精确的类型。例如整形就不要用字符串类型，INT 类型满足则不要使用 BIGINT，精确的数据类型能够更好的发挥数据库的性能。

#### 分区，分桶

`PARTITION` 关键字用于给表 [创建分区](/sql-reference/sql-statements/data-definition/CREATE_TABLE.md#Syntax)，当前 demo 中使用 `make_time` 进行范围分区，从 11 日到 15 日每天创建一个分区。StarRocks 支持动态生成分区，`PROPERTIES` 中的 `dynamic_partition` 开头的相关属性配置都是为表设置动态分区。详见 [动态分区管理](/table_design/Data_distribution.md#动态分区管理)。

`DISTRIBUTED` 关键字用于给表 [创建分桶](/sql-reference/sql-statements/data-definition/CREATE_TABLE.md#distribution_desc)，以上示例中使用 `recruit_date` 以及 `region_num` 两个字段通过 Hash 算法创建 8 个桶。

创建表时合理的分区和分桶设计可以优化表的查询性能，分区分桶列如何选择详见 [数据分布章节](/table_design/Data_distribution.md)。

#### 数据模型

`DUPLICATE` 关键字表示当前表为明细模型，`KEY` 中的列表示当前表的排序列。StarRocks 支持多种数据模型，分别为 [明细模型](/table_design/Data_model.md#明细模型)，[聚合模型](/table_design/Data_model.md#聚合模型)，[更新模型](/table_design/Data_model.md#更新模型)，[主键模型](/table_design/Data_model.md#主键模型)。不同模型的适用于多种业务场景，合理选择可优化查询效率。

#### 索引

StarRocks 默认会给 Key 列创建稀疏索引加速查询，具体规则见 [排序键和 shortke index](/table_design/Sort_key.md#排序列的原理) 章节。支持的索引类型有 [Bitmap 索引](/table_design/Bitmap_index.md#原理)，[Bloomfilter 索引](/table_design/Bloomfilter_index.md#原理) 等。

注意：索引创建对表模型和列有要求，详细说明见对应索引介绍章节。

#### ENGINE 类型

默认为 olap。可选 mysql，elasticsearch，hive，ICEBERG 代表创建表为 [外部表](/data_source/External_table.md#外部表)。

<br/>

## 其他操作

表创建成功后即可进行[数据导入查询](/quick_start/Import_and_query.md)。

StarRocks支持[用户创建授权](/sql-reference/sql-statements/account-management)及多种[DDL操作](/sql-reference/sql-statements/data-definition)，此处仅简单介绍部分Schema Change 操作和如何创建用户并授权。

### Schema 修改

使用 [ALTER TABLE](/sql-reference/sql-statements/data-definition/ALTER_TABLE.md) 命令可以修改表的 Schema，包括增加列，删除列，修改列类型（暂不支持修改列名称），改变列顺序。

以下举例说明。

原表 table1 的 Schema 如下:

```Plain Text
+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | Yes  | true  | 10      |       |
| citycode | smallint(6) | Yes  | true  | N/A     |       |
| username | varchar(32) | Yes  | true  |         |       |
| pv       | bigint(20)  | Yes  | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
```

新增一列 uv，类型为 BIGINT，聚合类型为 SUM，默认值为 0:

```sql
ALTER TABLE table1 ADD COLUMN uv BIGINT SUM DEFAULT '0' after pv;
```

Schema Change 为异步操作，提交成功后，可以通过以下命令查看:

```sql
SHOW ALTER TABLE COLUMN\G
```

当作业状态为 FINISHED，则表示作业完成。新的 Schema 已生效。

ALTER TABLE 完成之后, 可以通过 desc table 查看最新的 schema：

```Plain Text
mysql> desc table1;

+----------+-------------+------+-------+---------+-------+
| Field    | Type        | Null | Key   | Default | Extra |
+----------+-------------+------+-------+---------+-------+
| siteid   | int(11)     | Yes  | true  | 10      |       |
| citycode | smallint(6) | Yes  | true  | N/A     |       |
| username | varchar(32) | Yes  | true  |         |       |
| pv       | bigint(20)  | Yes  | false | 0       | SUM   |
| uv       | bigint(20)  | Yes  | false | 0       | SUM   |
+----------+-------------+------+-------+---------+-------+
5 rows in set (0.00 sec)
```

可以使用以下命令取消当前正在执行的作业:

```sql
CANCEL ALTER TABLE COLUMN FROM table1\G
```

### 创建用户并授权

StarRocks 中拥有 [Create_priv 权限](../administration/User_privilege.md#权限类型) 的用户才可建立数据库。

example_db 数据库创建完成之后，可以通过 root 账户 example_db 读写权限授权给 test 账户，授权之后采用 test 账户登录就可以操作 example\_db 数据库了：

```sql
mysql > create user 'test' identified by '123456';
mysql > grant all on example_db to test;
```

退出 root 账户，使用 test 登录 StarRocks 集群：

```sql
mysql > exit

mysql -h 127.0.0.1 -P9030 -utest -p123456
```

更多用户权限介绍请参考 [用户权限章节](/administration/User_privilege.md)，创建用户更改用户密码相关命令详见 [用户账户管理章节](/sql-reference/sql-statements/account-management/)。
