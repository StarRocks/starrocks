# AUTO_INCREMENT

StarRocks 自 3.0 版本起支持 `AUTO_INCREMENT` 列属性，可以简化数据管理。本文介绍 `AUTO_INCREMENT` 列属性的应用场景、用法和特性。

## 功能介绍

当插入一条新的记录时，StarRocks 会自动为该记录的自增列分配一个表内全局唯一的整数值作为自增 ID，并且后续值会自动增加。自增列可以简化数据管理，同时可以加速一些查询场景。以下是一些自增列的应用场景：

- 主键：自增列可用于生成主键，确保每条记录都有一个唯一的标识符，方便查询和管理数据。
- 关联表：在多个表之间进行关联时，可以使用自增列作为 Join Key，相比使用如 UUID 等字符串类型的列能够提高查询速度。
- 高基数列的精确去重计数：将自增列的 ID 值作为字典唯一值列，相比用字符串直接精确去重计数，查询速度能提升数倍甚至十数倍。

您需要在 CREATE TABLE 语句中通过 `AUTO_INCREMENT` 属性指定自增列。自增列的数据类型只支持 BIGINT，从 1 开始增加，自增步长为 1。 并且 StarRocks 支持[隐式分配自增列的值和显式指定自增 ID](#分配自增列的值)。

## 基本用法

### 建表指定自增列

创建表 `test_tbl1`，包含两列，分别为 `id` 和 `number`，如下所示，建表时指定 `number` 列为自增列：

```SQL
CREATE TABLE test_tbl1
(
    id BIGINT NOT NULL, 
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

### 分配自增列的值

**隐式分配自增列的值**

导入时，您无需指定自增列的值，StarRocks 会自动为该自增列分配唯一的整数值，并插入到表中。

```SQL
INSERT INTO test_tbl1 (id) VALUES (1);
INSERT INTO test_tbl1 (id) VALUES (2);
INSERT INTO test_tbl1 (id) VALUES (3),(4),(5);
```

查看表的数据。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |      3 |
|    4 |      4 |
|    5 |      5 |
+------+--------+
5 rows in set (0.02 sec)
```

您也可以指定自增列的值为 `DEFAULT`，StarRocks 会自动为该自增列分配唯一的整数值，并插入到表中。

```SQL
INSERT INTO test_tbl1 (id, number) VALUES (6, DEFAULT);
```

查看表的数据。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |      3 |
|    4 |      4 |
|    5 |      5 |
|    6 |      6 |
+------+--------+
6 rows in set (0.02 sec)
```

在实际使用中，您查看表的数据时可能会返回如下结果。这是因为 StarRocks 无法保证自增列的值按照时间顺序严格递增，但是能保证自增列的值大致上是递增的。更多介绍，请参见[单调性保证](#单调性保证)。

```SQL
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
+------+--------+
6 rows in set (0.01 sec)
```

**显式指定自增列的值**

您也可以显式地指定自增列的值，并插入到表中。

```SQL
INSERT INTO test_tbl1 (id, number) VALUES (7, 100);

-- 查看表的数据
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
|    7 |    100 |
+------+--------+
7 rows in set (0.01 sec)
```

并且，后续插入新数据时不会影响 StarRocks 新生成的自增列的值。

```SQL
INSERT INTO test_tbl1 (id) VALUES (8);

-- 查看表的数据
mysql > SELECT * FROM test_tbl1 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 | 200002 |
|    5 | 200003 |
|    6 | 200004 |
|    7 |    100 |
|    8 |      2 |
+------+--------+
8 rows in set (0.01 sec)
```

**注意事项**

因为同时隐式分配和显式指定自增 ID 可能会破坏自增 ID 的[全局唯一性](#唯一性保证)，建议您不要混用。

## 基本特性

### 唯一性保证

在一般情况下，StarRocks 保证自增 ID 在一张表内是全局唯一的。

但是，如果您混用隐式分配和显式指定自增 ID，则可能会破坏自增 ID 的全局唯一性。因此建议您不要同时隐式分配和显式指定自增 ID。以下是一个简单的示例：

创建表 `test_tbl2`，其中列 `number` 为自增列。

```SQL
CREATE TABLE test_tbl2
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
 ) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

往表 `test_tbl2` 中同时隐式分配和显式指定自增 ID。

```SQL
INSERT INTO test_tbl2 (id, number) VALUES (1, DEFAULT);
INSERT INTO test_tbl2 (id, number) VALUES (2, 2);
INSERT INTO test_tbl2 (id) VALUES (3);
```

查询表 `test_tbl2` 的数据。

```SQL
mysql > SELECT * FROM test_tbl2 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 | 100001 |
+------+--------+
3 rows in set (0.08 sec)
```

### 单调性保证

为了提升分配自增 ID 的性能，BE 会本地缓存部分自增 ID。在这种情况下，StarRocks 无法保证自增 ID 按照时间顺序严格递增，只能保证自增 ID 大致上是递增的。
> **说明**
>
> BE 缓存的自增 ID 数量由 FE 动态参数 `auto_increment_cache_size` 决定，默认是 `100000`。您可以使用 `ADMIN SET FRONTEND CONFIG ("auto_increment_cache_size" = "xxx");` 进行修改 。
假设 StarRocks 集群具有一个 FE 节点和两个 BE 节点。创建表 `test_tbl3` 并且插入五行数据，如下所示：

```SQL
CREATE TABLE test_tbl3
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");

INSERT INTO test_tbl3 VALUES (1, DEFAULT);
INSERT INTO test_tbl3 VALUES (2, DEFAULT);
INSERT INTO test_tbl3 VALUES (3, DEFAULT);
INSERT INTO test_tbl3 VALUES (4, DEFAULT);
INSERT INTO test_tbl3 VALUES (5, DEFAULT);
```

表 test_tbl3` 中自增 ID 不是单调递增的。这是因为两个 BE 节点分别缓存了 [1, 100000] 和 [100001, 200000] 范围内的自增 ID，使用多个 INSERT 语句导入数据时，会发送给不同的 BE，由不同 BE 分配自增 ID，因此无法保证自增 ID 的严格单调性。

```SQL
mysql > SELECT * FROM test_tbl3 ORDER BY id;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 | 200001 |
|    4 |      2 |
|    5 | 100002 |
+------+--------+
5 rows in set (0.07 sec)
```

## 部分列更新与自增列

本小节介绍具有自增列的表如何实现部分列更新。

> **说明**
>
> 目前仅主键模型的表支持部分列更新。

### 自增列为主键

如果自增列为主键或主键的一部分，由于部分列更新时您需要指定主键，因此部分列更新的用户行为和没有定义自增列完全一样。

1. 在数据库 `example_db` 中创建表 `test_tbl4`，并且插入一条数据。

    ```SQL
    -- 建表
    CREATE TABLE test_tbl4
    (
        id BIGINT AUTO_INCREMENT,
        name BIGINT NOT NULL,
        job1 BIGINT NOT NULL,
        job2 BIGINT NOT NULL
    ) 
    PRIMARY KEY (id, name)
    DISTRIBUTED BY HASH(id)
    PROPERTIES("replicated_storage" = "true");

    -- 准备数据
    mysql > INSERT INTO test_tbl4 (id, name, job1, job2) VALUES (0, 0, 1, 1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_6af28e77-7d2b-11ed-af6e-02424283676b', 'status':'VISIBLE', 'txnId':'152'}

    -- 查询数据
    mysql > SELECT * FROM test_tbl4 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |    1 |    1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. 准备 CSV 文件 **my_data4.csv**，用于更新表 `test_tbl4`。 CSV 文件包括自增列的 ID 值，不包含列 `job1` 的值，并且第一行数据的主键存在表 `test_tbl4` 中，第二行的主键不存在。

    ```Plaintext
    0,0,99
    1,1,99
    ```

3. 通过 [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md) 将 CSV 文件数据更新至表 `test_tbl4`。

    ```Bash
    curl --location-trusted -u <username>:<password> -H "label:1" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns:id,name,job2" \
        -T my_data4.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/test_tbl4/_stream_load
    ```

4. 查询更新后的表。第一条数据原先已经存在表 `test_tbl4` 中，并且列 `job1` 保持原先的值。第二条数据是新插入的数据，由于列 `job1` 没有定义默认值，因此部分列更新框架会直接将此列的值设置为 `0`。

    ```SQL
    mysql > SELECT * FROM test_tbl4 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |    1 |   99 |
    |    1 |    1 |    0 |   99 |
    +------+------+------+------+
    2 rows in set (0.01 sec)
    ```

### 自增列不为主键

如果自增列不是主键或者主键的一部分，并且 Stream Load 中未给出自增 ID，则会发生以下情况：

- 如果表中已经存在该行，则 StarRocks 不会更新自增ID。
- 如果表中不存在该行，则 StarRocks 会自动生成新的自增 ID。

该特性可以构建字典表的值，用于加速字符串的精确去重计数。

1. 在数据库 `example_db` 中创建表 `test_tbl5`，指定 `job1` 为自增列，并且插入一条数据。

    ```SQL
    -- 建表
    CREATE TABLE test_tbl5
    (
        id BIGINT NOT NULL,
        name BIGINT NOT NULL,
        job1 BIGINT NOT NULL AUTO_INCREMENT,
        job2 BIGINT NOT NULL
    )
    PRIMARY KEY (id, name)
    DISTRIBUTED BY HASH(id)
    PROPERTIES("replicated_storage" = "true");

    -- 准备数据
    mysql > INSERT INTO test_tbl5 VALUES (0, 0, -1, -1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_458d9487-80f6-11ed-ae56-aa528ccd0ebf', 'status':'VISIBLE', 'txnId':'94'}

    mysql > SELECT * FROM test_tbl5 ORDER BY id;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |   -1 |   -1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. 准备 CSV 文件 **my_data5.csv**，用于更新表 `test_tbl5`。CSV 文件不包含自增列 `job1` 的值，并且第一行数据的主键存在于表中，第二、三行数据的主键不存在。

    ```Plaintext
    0,0,99
    1,1,99
    2,2,99
    ```

3. 通过 [Stream Load](../../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md) 将 CSV 文件数据导入至表 `test_tbl5`。

    ```Bash
    curl --location-trusted -u <username>:<password> -H "label:2" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns: id,name,job2" \
        -T my_data5.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/test_tbl5/_stream_load
    ```

4. 查询更新后的表。第一条数据已经存在表 `test_tbl5` 中，自增列 `job1` 保持原先的 ID 值。第二、三条数据是新插入的数据，自增列 `job1` 的 ID 值由 StarRocks 自动生成。

    ```SQL
    mysql > SELECT * FROM test_tbl5 ORDER BY id;
    +------+------+--------+------+
    | id   | name | job1   | job2 |
    +------+------+--------+------+
    |    0 |    0 |     -1 |   99 |
    |    1 |    1 |      1 |   99 |
    |    2 |    2 | 100001 |   99 |
    +------+------+--------+------+
    3 rows in set (0.01 sec)
    ```

## 使用限制

- 创建具有自增列的表时，必须设置 `'replicated_storage' = 'true'`，以确保所有副本具有相同的自增 ID。
- 每个表最多只能有一个自增列。
- 自增列必须是 BIGINT 类型。
- 自增列必须为 `NOT NULL`，并且不支持指定默认值。
- 您可以从具有自增列的主键模型的表中删除数据。但是如果自增列不为 Primary Key，则您在删除数据时，需要注意以下两个场景中的限制：
  - DELETE 操作的同时，还存在一个部分列更新的导入任务，其中只包含 UPSERT 操作。如果 UPSERT 操作和 DELETE 操作命中了同一行数据，并且 UPSERT 操作在 DELETE 操作后执行，则该 UPSERT 操作可能会失效。
  - 存在一个部分列更新的导入任务，其中包含若干个对同一行数据的 UPSERT、DELETE 操作。如果某个 UPSERT 操作在 DELETE 操作后执行，则该 UPSERT 操作可能会失效。
- 不支持使用 ALTER TABLE 添加 `AUTO_INCREMENT` 属性。
- 存算分离模式暂时不支持该功能。
- 不支持设置自增列的起始值和自增步长。

## Keywords

AUTO_INCREMENT, AUTO INCREMENT
