---
displayed_sidebar: docs
---

# dict_mapping

## 功能

通过指定字典表和 key，返回该 key 所映射的 value。

此函数的核心目的就为了简化全局字典表的使用：在导入数据到目标表过程中时，StarRocks 会自动根据该函数中的传参从字典表中获取指定 key 所映射的 value，并导入到目标表中。

自 v3.2.5 起，StarRocks 支持该函数。并且注意，StarRocks 存算分离模式暂时不支持该函数。

## 语法

```SQL
dict_mapping([<db_name>.]<dict_table>, key_column_expr_list [, <value_column> ] [, <strict_mode>] )

key_column_expr_list ::= key_column_expr [, key_column_expr ... ]

key_column_expr ::= <column_name> | <expr>
```

## 参数说明

- 必填参数：
  - `[<db_name>.]<dict_table>`:  字典表名，且表必须是主键模型表。支持的数据类型为 VARCHAR。
  - `key_column_expr_list`：指定字典表 key 列的表达式列表，由一个或多个表达式  `key_column_expr` 组成，表达式 `key_column_expr` 可以是字典表中 key 列名，也可以是具体的 key 或 key 表达式。

    该表达式列表必须包括字典表的所有主键列，即表达式的个数必须和字典表中所有主键列的个数相同。所以如果字典表使用联合主键，则该表达式列表中的表达式和字典表表结构中定义的主键列必须按位置一一对应，多个表达式之间用英文逗号分隔（`,`）。并且如果 `key_column_expr`是一个具体的 key 或 key 表达式，则其类型必须和对应的字典表中的列的类型相同。
- 可选参数：
  - `<value_column>`：value 列名，也就是映射列名。如果不指定，则默认为字典表的自增列。value 列也可以定义为字典表中除自增列和主键以外的列，并且对列的数据类型无限制。
  - `<strict_mode>`：是否启用严格模式，即在未找到与该 key 呈映射关系的 value 时，是否返回报错。如果为 `TRUE`，则返回报错。如果为 `FALSE`（默认），则返回 `NULL`。

## 返回值说明

返回值的数据类型与 value 列的数据类型保持一致。如果 value 列为字典表的自增列，则返回值的数据类型为 BIGINT。

然而当未找到与该 key 呈映射关系的 value 时，如果为 `strict_mode` 参数为默认的 `FALSE`，则返回 `NULL`。如果为 `TRUE`，则返回报错 `ERROR 1064 (HY000): In strict mode, query failed if record not exist in dict table.`。

## 示例

**示例一：直接查询字典表中与 key 呈映射关系的 value。**

1. 创建一张字典表并插入模拟的数据。

      ```SQL
      MySQL [test]> CREATE TABLE dict (
          order_uuid STRING,
          order_id_int BIGINT AUTO_INCREMENT 
      )
      PRIMARY KEY (order_uuid)
      DISTRIBUTED BY HASH (order_uuid);
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict (order_uuid) VALUES ('a1'), ('a2'), ('a3');
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      
      MySQL [test]> select * from dict;
      +------------+--------------+
      | order_uuid | order_id_int |
      +------------+--------------+
      | a1         |            1 |
      | a3         |            3 |
      | a2         |            2 |
      +------------+--------------+
      3 rows in set (0.01 sec)
      ```

     > **注意**
     >
     > 当前 INSERT INTO 还没有支持部分列更新（Partial Updates），您必须确保插入到`dict`中的 key 列的值是没有重复的。否则字典表中多次插入相同的 key 列值，导致其映射的 value 列值会变化。

2. 查询字典表中与 key `a1` 呈映射关系的 value。

      ```SQL
      MySQL [test]> SELECT dict_mapping('dict', 'a1');
      +----------------------------+
      | dict_mapping('dict', 'a1') |
      +----------------------------+
      |                          1 |
      +----------------------------+
      1 row in set (0.01 sec)
      ```

**示例二：数据表里的映射列是配置 `dict_mapping` 函数的生成列，导入数据至该表时由 StarRocks 自动获取 key 所映射的 value。**

1. 创建数据表并且使用 `dict_mapping('dict', order_uuid)` 将映射列配置为生成列。

   1. ```SQL
      CREATE TABLE dest_table1 (
          id BIGINT,
          -- 该列记录 STRING 类型订单编号，对应示例一中字典表 dict 的 order_uuid 列。
          order_uuid STRING, 
          batch int comment 'used to distinguish different batch loading',
          -- 该列记录 BIGINT 类型订单编号，与 order_uuid 列映射。
          -- 该列是配置 dict_mapping 的生成列，在导入数据时其列值自动从示例一中的字典表 dict 中获取。
          -- 后续可以直接基于该列进行去重和 JOIN 查询。
          order_id_int BIGINT AS dict_mapping('dict', order_uuid)
      )
      DUPLICATE KEY (id, order_uuid)
      DISTRIBUTED BY HASH(id);
      ```

2. 在导入模拟的数据至该表，因为该表的列 `order_id_int` 是配置 `dict_mapping('dict', 'order_uuid')`  的生成列，所以 StarRocks 会自动根据字典表`dict`中 key 和 value 的映射关系向该 `order_id_int` 列填充值。

      ```SQL
      MySQL [test]> INSERT INTO dest_table1(id,order_uuid,batch) VALUES (1,'a1',1),(2,'a1',1),(3,'a3',1),(4,'a3',1);
      Query OK, 4 rows affected (0.05 sec) 
      {'label':'insert_e191b9e4-8a98-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'72'}
      
      MySQL [test]> SELECT * FROM dest_table1;
      +------+------------+-------+--------------+
      | id   | order_uuid | batch | order_id_int |
      +------+------------+-------+--------------+
      |    1 | a1         |     1 |            1 |
      |    4 | a3         |     1 |            3 |
      |    2 | a1         |     1 |            1 |
      |    3 | a3         |     1 |            3 |
      +------+------------+-------+--------------+
      4 rows in set (0.02 sec)
      ```

    本示例中介绍的 `dict_mapping` 使用方式可以用于加速去重计算和 JOIN 的查询。相对于之前的[构建全局字典加速精确去重](../../../using_starrocks/query_acceleration_with_auto_increment.md)中提供的两种解决方案，使用 `dict_mapping` 的解决方案更加灵活易用。因为使用 `dict_mapping` 的解决方案时，在“将字典的映射关系导入至数据表”这一阶段，是由直接 StarRocks 从字典表获取映射的值，而不再需要您手动编写语句关联字典表，来获取映射的值。并且，该解决方案还支持使用各种导入方式导入数据。<!--详细的使用方式，请参见xxx。-->

**示例三：如果数据表中没有将映射列配置为生成列，则在导入至数据表时您需要显式为映射列配置 `dict _mapping` 函数，从而获取 key 所映射的 value。**

> **注意**
>
> 示例三与示例二的区别在于，导入至数据表时，需要您修改导入命令，显式为映射列配置 `dict_mapping` 的表达式。

1. 创建一张数据表。

      ```SQL
      CREATE TABLE dest_table2 (
          id BIGINT,
          order_uuid STRING, -- 该列记录 STRING 类型订单编号，对应示例一中字典表 dict 的 order_uuid 列。
          order_id_int BIGINT NULL, -- 该列记录 BIGINT 类型订单编号，与前一列相互映射，后续用于精确去重计数和 Join
          batch int comment 'used to distinguish different batch loading'
      )
      DUPLICATE KEY (id, order_uuid, order_id_int)
      DISTRIBUTED BY HASH(id);
      ```

2. 导入模拟数据至该数据表的时候，您通过配置 `dict_mapping` 来从字典表中获取所映射的值。

      ```SQL
      MySQL [test]> INSERT INTO dest_table2 VALUES (1,'a1',dict_mapping('dict', 'a1'),1);
      Query OK, 1 row affected (0.35 sec)
      {'label':'insert_19872ab6-8a96-11ee-b29c-00163e03897d', 'status':'VISIBLE', 'txnId':'42'}
      
      MySQL [test]> SELECT * FROM dest_table2;
      +------+------------+--------------+-------+
      | id   | order_uuid | order_id_int | batch |
      +------+------------+--------------+-------+
      |    1 | a1         |            1 |     1 |
      +------+------------+--------------+-------+
      1 row in set (0.02 sec)
      ```

**示例四：启用严格模式**

启用严格模式，并且使用字典表中不存在的 key，查询与其呈映射关系的 value，此时直接返回报错而不是 `NULL`。从而可以确保导入至数据表之前，相关 key 已经先导入至字典表并生成与其映射的value。

```SQL
MySQL [test]>  SELECT dict_mapping('dict', 'b1', true);
ERROR 1064 (HY000): In strict mode, query failed if record not exist in dict table.
```

**示例五：如果字典表使用联合主键，则查询时候必须指定所有主键**

1. 创建一张具有联合主键的字典表并导入模拟数据。

      ```SQL
      MySQL [test]> CREATE TABLE dict2 (
          order_uuid STRING,
          order_date DATE, 
          order_id_int BIGINT AUTO_INCREMENT
      )
      PRIMARY KEY (order_uuid,order_date)  -- 联合主键
      DISTRIBUTED BY HASH (order_uuid,order_date)
      ;
      Query OK, 0 rows affected (0.02 sec)
      
      MySQL [test]> INSERT INTO dict2 VALUES ('a1','2023-11-22',default), ('a2','2023-11-22',default), ('a3','2023-11-22',default);
      Query OK, 3 rows affected (0.12 sec)
      {'label':'insert_9e60b0e4-89fa-11ee-a41f-b22a2c00f66b', 'status':'VISIBLE', 'txnId':'15029'}
      
      
      MySQL [test]> select * from dict2;
      +------------+------------+--------------+
      | order_uuid | order_date | order_id_int |
      +------------+------------+--------------+
      | a1         | 2023-11-22 |            1 |
      | a3         | 2023-11-22 |            3 |
      | a2         | 2023-11-22 |            2 |
      +------------+------------+--------------+
      3 rows in set (0.01 sec)
      ```

2. 查询字典表中与 Key 呈映射关系的 value。因为字典表具有联合主键，所以在 `dict_mapping` 中需要输入所有主键。

      ```SQL
      SELECT dict_mapping('dict2', 'a1', cast('2023-11-22' as DATE));
      ```

   注意，如果只输入一个主键，则会报错。

      ```SQL
      MySQL [test]> SELECT dict_mapping('dict2', 'a1');
      ERROR 1064 (HY000): Getting analyzing error. Detail message: dict_mapping function param size should be 3 - 5.
      ```
