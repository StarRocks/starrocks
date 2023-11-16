# 生成列

StarRocks 自 3.1 版本起支持生成列（Generated Column）。该特性支持预先计算并存储表达式的结果，从而加速包含复杂表达式的查询，并且支持[查询改写](#查询改写)，因此极大提高了查询性能。

您可以定义一个或者多个生成列来存储表达式的结果。当执行包含相同表达式的查询时，优化器会进行查询改写，用生成列替换表达式。或者您也可以直接查询生成列的数据。

由于在导入阶段计算表达式会占用一定的时间，因此建议您**提前评估生成列对导入性能的影响**。此外，由于建表后新增或者修改生成列比较耗时且开销较大，因此**建议您[在建表时创建生成列](#建表时创建生成列推荐)，而不是建表后新增生成列**。

## 基本用法

### 创建生成列

#### 语法

```SQL
col_name data_type [NULL] AS generation_expr [COMMENT 'string']
```

#### 建表时创建生成列（推荐）

创建表 `test_tbl1`，包含五列，其中列 `newcol1` 和 `newcol2` 是生成列，分别是引用普通列 `data_array` 和 `data_json` 通过计算表达式后生成的列。

```SQL
CREATE TABLE test_tbl1
(
    id INT NOT NULL,
    data_array ARRAY<int> NOT NULL,
    data_json JSON NOT NULL,
    newcol1 DOUBLE AS array_avg(data_array),
    newcol2 String AS json_string(json_query(data_json, "$.a"))
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id);
```

**注意事项：**

- 生成列必须在普通列之后。
- 生成列的表达式不支持使用聚合函数。
- 生成列的表达式中不能引用其他生成列或[自增列](./auto_increment.md)，可以引用多个普通列。
- 生成列的数据类型必须与表达式返回结果的数据类型相匹配。
- 不支持在聚合表创建生成列。
- StarRocks 存算分离模式暂时不支持该功能。

#### 建表后增加生成列

> **注意**
>
> 该操作比较耗时且开销较大。因此建议在建表时新增生成列，如果必须使用 ALTER TABLE 增加生成列，请提前评估开销和时间成本。

1. 创建表 `test_tbl2`，包含三个普通列 `id`、`data_array` 和 `data_json`。建表成功后插入一行数据。

      ```SQL
      -- 建表
      CREATE TABLE test_tbl2
      (
          id INT NOT NULL,
          data_array ARRAY<int> NOT NULL,
          data_json JSON NOT NULL
       )
      PRIMARY KEY (id)
      DISTRIBUTED BY HASH(id);
      
      -- 插入一行数据
      INSERT INTO test_tbl2 VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
      
      -- 查询数据
      MySQL [example_db]> select * from test_tbl2;
      +------+------------+------------------+
      | id   | data_array | data_json        |
      +------+------------+------------------+
      |    1 | [1,2]      | {"a": 1, "b": 2} |
      +------+------------+------------------+
      1 row in set (0.04 sec)
      ```

2. 执行 ALTER TABLE ... ADD COLUMN ... 增加生成列 `newcol1` 和 `newcol2`，分别是引用普通列 `data_array` 和 `data_json` 计算表达式后生成的列。

      ```SQL
      ALTER TABLE test_tbl2
      ADD COLUMN newcol1 DOUBLE AS array_avg(data_array);
      
      ALTER TABLE test_tbl2
      ADD COLUMN newcol2 STRING AS json_string(json_query(data_json, "$.a"));
      ```

    **注意事项：**

    - 不支持在聚合表增加生成列。
    - 普通列一定位于生成列前面，因此使用 ALTER TABLE ... ADD COLUMN ... 增加普通列的时候，如果不指定新增普通列的位置，则系统会自动加在生成列前面。同时不支持使用 AFTER 指定加在生成列后。

3. 查询表的数据。

      ```SQL
      MySQL [example_db]> SELECT * FROM test_tbl2;
      +------+------------+------------------+---------+---------+
      | id   | data_array | data_json        | newcol1 | newcol2 |
      +------+------------+------------------+---------+---------+
      |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
      +------+------------+------------------+---------+---------+
      1 row in set (0.04 sec)
      ```

    返回结果显示，表中已经增加生成列 `newcol1` 和 `newcol2`，并且 StarRocks 通过计算表达式自动得出该生成列的值。

### 导入数据至生成列

导入数据时 StarRocks 通过计算表达式自动得出生成列的值，**您无法指定生成列的值**。本文以使用 [INSERT INTO](../../loading/InsertInto.md) 导入数据为例进行说明。

1. 使用 INSERT INTO 插入一行数据至表 `test_tbl1`，注意不能在 `VALUES ()` 中指定生成列的值 。

      ```SQL
      INSERT INTO test_tbl1 (id, data_array, data_json)
          VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
      ```

2. 查询表的数据。

      ```SQL
      MySQL [example_db]> SELECT * FROM test_tbl1;
      +------+------------+------------------+---------+---------+
      | id   | data_array | data_json        | newcol1 | newcol2 |
      +------+------------+------------------+---------+---------+
      |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
      +------+------------+------------------+---------+---------+
      1 row in set (0.01 sec)
      ```

    返回结果显示，StarRocks 计算表达式自动得出生成列 `newcol1` 和 `newcol2` 的值。

    **注意事项：**<br />导入数据时如果您指定生成列的值，则会返回如下报错：

      ```SQL
      MySQL [example_db]> INSERT INTO test_tbl1 (id, data_array, data_json, newcol1, newcol2) 
      VALUES (2, [3,4], parse_json('{"a" : 3, "b" : 4}'), 3.5, "3");
      ERROR 1064 (HY000): Getting analyzing error. Detail message: materialized column 'newcol1' can not be specified.
      
      MySQL [example_db]> INSERT INTO test_tbl1 VALUES (2, [3,4], parse_json('{"a" : 3, "b" : 4}'), 3.5, "3");
      ERROR 1064 (HY000): Getting analyzing error. Detail message: Column count doesn't match value count.
      ```

### 修改生成列

> **注意**
>
> 该操作比较耗时且开销较大。如果必须修改生成列，请提前评估开销和时间成本。

支持修改生成列的数据类型和表达式。

1. 创建表 `test_tbl3`，包含五列，其中列 `newcol1` 和 `newcol2` 是生成列，分别是引用普通列 `data_array` 和 `data_json` 通过计算表达式后生成的列。建表成功后插入一行数据。

      ```SQL
      -- 建表
      MySQL [example_db]> CREATE TABLE test_tbl3
      (
          id INT NOT NULL,
          data_array ARRAY<int> NOT NULL,
          data_json JSON NOT NULL,
          -- 指定生成列的数据类型和表达式如下
          newcol1 DOUBLE AS array_avg(data_array),
          newcol2  String AS json_string(json_query(data_json, "$.a"))
      )
      PRIMARY KEY (id)
      DISTRIBUTED BY HASH(id);
      
      -- 插入一行数据
      INSERT INTO test_tbl3 (id, data_array, data_json)
          VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
      
      -- 查询表的数据
      MySQL [example_db]> select * from test_tbl3;
      +------+------------+------------------+---------+---------+
      | id   | data_array | data_json        | newcol1 | newcol2 |
      +------+------------+------------------+---------+---------+
      |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
      +------+------------+------------------+---------+---------+
      1 row in set (0.01 sec)
      ```

2. 修改生成列 `newcol1` 和 `newcol2`：
   - 修改生成列 `newcol1` 的数据类型为 `ARRAY<INT>`，表达式改为 `data_array`。

        ```SQL
        ALTER TABLE test_tbl3 
        MODIFY COLUMN newcol1 ARRAY<INT> AS data_array;
        ```

   - 修改生成列 `newcol2` 的表达式，以提取普通列 `data_json` 中字段 `b` 的值。

        ```SQL
        ALTER TABLE test_tbl3
        MODIFY COLUMN newcol2 STRING AS json_string(json_query(data_json, "$.b"));
        ```

3. 查看修改后表结构和表中的数据。

    - 查看修改后的表结构

        ```SQL
        MySQL [example_db]>   SHOW CREATE TABLE test_tbl3\G
        **** 1. row ****
            Table: test_tbl3
        Create Table: CREATE TABLE test_tbl3 (
        id int(11) NOT NULL COMMENT "",
        data_array array<int(11)> NOT NULL COMMENT "",
        data_json json NOT NULL COMMENT "",
        -- 修改后生成列的数据类型和表达式如下
        newcol1 array<int(11)> NULL AS example_db.test_tbl3.data_array COMMENT "",
        newcol2 varchar(65533) NULL AS json_string(json_query(example_db.test_tbl3.data_json, '$.b')) COMMENT ""
        ) ENGINE=OLAP 
        PRIMARY KEY(id)
        DISTRIBUTED BY HASH(id)
        PROPERTIES (...);
        1 row in set (0.00 sec)
        ```

    - 查看修改后的表数据，返回结果显示，StarRocks 根据修改后的表达式重新计算出生成列 `newcol1` 和 `newcol2` 的值。

        ```SQL
        MySQL [example_db]> select * from test_tbl3;
        +------+------------+------------------+---------+---------+
        | id   | data_array | data_json        | newcol1 | newcol2 |
        +------+------------+------------------+---------+---------+
        |    1 | [1,2]      | {"a": 1, "b": 2} | [1,2]   | 2       |
        +------+------------+------------------+---------+---------+
        1 row in set (0.01 sec)
        ```

### 删除生成列

删除表 `test_tbl3` 中的生成列 `newcol1`。

```SQL
ALTER TABLE test_tbl3 DROP COLUMN newcol1;
```

**注意事项：**<br />如果生成列的表达式引用某个普通列，并且您需要删除或修改该普通列，您必须先删除该生成列，才能删除或修改该普通列。

### 查询改写

如果查询中的表达式与某个生成列的表达式匹配，则优化器会自动进行查询改写，直接读取生成列的值。

1. 假设您创建了 `test_tbl4`，表结构如下：

      ```SQL
      CREATE TABLE test_tbl4
      (
          id INT NOT NULL,
          data_array ARRAY<int> NOT NULL,
          data_json JSON NOT NULL,
          newcol1 DOUBLE AS array_avg(data_array),
          newcol2 String AS json_string(json_query(data_json, "$.a"))
      )
      PRIMARY KEY (id) DISTRIBUTED BY HASH(id);
      ```

2. 如果通过 `SELECT array_avg(data_array), json_string(json_query(data_json, "$.a")) FROM test_tbl4;` 查询 `test_tbl4` 的数据，由于查询语句只涉及正常列 `data_array` 和 `data_json` ，但是查询中的表达式与生成列 `newcol1` 和 `newcol2` 的表达式相匹配，则执行计划显示优化器会自动进行查询改写，直接读取生成列  `newcol1` 和 `newcol2` 的值。

      ```SQL
      MySQL [example_db]> EXPLAIN SELECT array_avg(data_array), json_string(json_query(data_json, "$.a")) FROM test_tbl4;
      +---------------------------------------+
      | Explain String                        |
      +---------------------------------------+
      | PLAN FRAGMENT 0                       |
      |  OUTPUT EXPRS:4: newcol1 | 5: newcol2 | -- 查询改写后命中生成列 newcol1 和 newcol2
      |   PARTITION: RANDOM                   |
      |                                       |
      |   RESULT SINK                         |
      |                                       |
      |   0:OlapScanNode                      |
      |      TABLE: test_tbl4                 |
      |      PREAGGREGATION: ON               |
      |      partitions=0/1                   |
      |      rollup: test_tbl4                |
      |      tabletRatio=0/0                  |
      |      tabletList=                      |
      |      cardinality=1                    |
      |      avgRowSize=2.0                   |
      +---------------------------------------+
      15 rows in set (0.00 sec)
      ```

### 部分列更新与生成列

对主键表进行[部分列更新](../../loading/Load_to_Primary_Key_tables.md#部分更新)时，您必须在 `columns` 中指定生成列引用的所有普通列。以下以 Stream Load 进行说明。

1. 创建表 `test_tbl5`，包含五列，其中列 `newcol1` 和 `newcol2` 是生成列，分别是引用普通列 `data_array` 和 `data_json` 通过计算表达式后生成的列。建表成功后插入一行数据。

      ```SQL
      -- 建表
      CREATE TABLE test_tbl5
      (
          id INT NOT NULL,
          data_array ARRAY<int> NOT NULL,
          data_json JSON NULL,
          newcol1 DOUBLE AS array_avg(data_array),
          newcol2 String AS json_string(json_query(data_json, "$.a"))
      )
      PRIMARY KEY (id)
      DISTRIBUTED BY HASH(id);
      
      -- 插入一行数据
      INSERT INTO test_tbl5 (id, data_array, data_json)
          VALUES (1, [1,2], parse_json('{"a" : 1, "b" : 2}'));
      
      -- 查看表的数据
      MySQL [example_db]> select * from test_tbl5;
      +------+------------+------------------+---------+---------+
      | id   | data_array | data_json        | newcol1 | newcol2 |
      +------+------------+------------------+---------+---------+
      |    1 | [1,2]      | {"a": 1, "b": 2} |     1.5 | 1       |
      +------+------------+------------------+---------+---------+
      1 row in set (0.01 sec)
      ```

2. 准备 CSV 文件 `my_data1.csv`，用于更新表 `test_tbl5`的部分列。

      ```csv
      1|[3,4]|{"a": 3, "b": 4}
      2|[3,4]|{"a": 3, "b": 4} 
      ```

3. 通过 [Stream Load](../sql-statements/data-manipulation/STREAM_LOAD.md) 使用 CSV 文件 `my_data1.csv` 更新表 `test_tbl5` 的部分列。您需要设置 `"partial_update:true"`，并且必须在 `columns` 中指定生成列引用的所有普通列。

      ```Bash
      curl --location-trusted -u <username>:<password> -H "label:1" \
          -H "column_separator:|" \
          -H "partial_update:true" \
          -H "columns:id,data_array,data_json" \ 
          -T my_data1.csv -XPUT \
          http://<fe_host>:<fe_http_port>/api/example_db/test_tbl5/_stream_load
      ```

4. 查看表的数据。

      ```SQL
      MySQL [example_db]> select * from test_tbl5;
      +------+------------+------------------+---------+---------+
      | id   | data_array | data_json        | newcol1 | newcol2 |
      +------+------------+------------------+---------+---------+
      |    1 | [3,4]      | {"a": 3, "b": 4} |     3.5 | 3       |
      |    2 | [3,4]      | {"a": 3, "b": 4} |     3.5 | 3       |
      +------+------------+------------------+---------+---------+
      2 rows in set (0.01 sec)
      ```

如果部分列更新时，没有指定生成列引用的所有正常列，则 Stream Load 会返回报错。

1. 准备 CSV 文件 `my_data2.csv`。

      ```csv
      1|[3,4]
      2|[3,4]
      ```

2. 通过 [Stream Load](../sql-statements/data-manipulation/STREAM_LOAD.md) 使用 CSV 文件 `my_data2.csv` 进行部分列更新。`my_data2.csv` 中没有提供  `data_json` 列的值，并且 Stream Load 导入作业中 `columns` 参数没有包含 `data_json` 列，即使`data_json` 列允许为空，但是因为生成列 `newcol2` 引用了 `data_json` 列，则 Stream Load 会返回报错。

      ```SQL
      $ curl --location-trusted -u root: -H "label:2" \
          -H "column_separator:|" \
          -H "partial_update:true" \
          -H "columns:id,data_array" \
          -T my_data2.csv -XPUT \
          http://<fe_host>:<fe_http_port>/api/example_db/test_tbl5/_stream_load
      {
          "TxnId": 3134,
          "Label": "2",
          "Status": "Fail",
          "Message": "column data_json needs to be used for expression evaluation for materialized column newcol2",
          ...
      }
      ```
