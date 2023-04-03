# AUTO_INCREMENT

StarRocks 自 3.0 版本起支持 `AUTO_INCREMENT` 列属性，可以简化数据管理。本文介绍 `AUTO_INCREMENT` 列属性的应用场景、用法和特性。

## 功能介绍

当插入一条新的记录时，StarRocks 会自动为该记录的自增列分配一个全局唯一的整数值作为自增 ID，并且后续值会自动增加。自增列可以简化数据管理，同时可以加速一些查询场景。以下是一些自增列的应用场景：

- 主键：自增列可用于生成主键，确保每条记录都有一个唯一的标识符，方便查询和管理数据。
- 关联表：在多个表之间进行关联时，可以使用自增列作为 Join Key，相比使用如 UUID、字符串等类型列能够提高查询速度。
- 高基数列的精确去重计数：将自增列的 ID 值作为字典唯一值列，相比用字符串直接精确去重计数，查询速度能提升数倍甚至十数倍。

您需要在 CREATE TABLE 语句中通过 `AUTO_INCREMENT` 属性指定自增列。自增列的数据类型只支持 BIGINT，从 1 开始增加，自增步长为 1。 并且 StarRocks 支持[隐式分配](#隐式分配自增列的值)和[显式指定自增 ID](#显式指定自增列的值)。

## 基本用法

## 建表指定自增列

创建表 `t`，包含两列，分别为 `id` 和 `number`，如下所示，建表时指定 `number` 列为自增列：

```SQL
CREATE TABLE t
(
    id BIGINT NOT NULL, 
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

### 分配自增列的值

#### 隐式分配自增列的值

导入时，您无需指定自增列的值，或者指定自增列的值为 `DEFAULT`，StarRocks 会自动为该自增列分配唯一的整数值，并插入到表中。

```SQL
INSERT INTO t (id) VALUES (1);
INSERT INTO t (id, number) VALUES (2, DEFAULT);

-- 查看表的数据
SELECT * FROM t;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
+------+--------+
2 rows in set (0.08 sec)
```

> **说明**
>
> 您在实际使用时，StarRocks 无法保证自增列的值按照时间顺序严格递增，但是能保证自增列的值大致上是递增的。更多介绍，请参见[单调性保证](#单调性保证)。

#### 显式指定自增列的值

您也可以显式地指定自增列的值，并插入到表中。

```SQL
INSERT INTO t (id, number) VALUES (3, 100);

-- 查看表的数据
SELECT * FROM t;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |    100 |
+------+--------+
3 rows in set (0.01 sec)
```

并且，后续插入新数据时不会影响 StarRocks 新生成的自增列的值。

```SQL
INSERT INTO t (id) VALUES (4);

-- 查看表的数据
SELECT * FROM t;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |    100 |
|    4 |      3 |
+------+--------+
4 rows in set (0.02 sec)
```

## 基本特性

### 唯一性保证

在一般情况下，StarRocks 保证自增 ID 是全局唯一的。

但是，如果您混用隐式分配和显式指定自增 ID，则可能会破坏自增 ID 的全局唯一性。因此建议您不要同时隐式分配和显式指定自增 ID。以下是一个简单的示例：

创建表 `t`，其中列 `number` 为自增列。

```SQL
CREATE TABLE t
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
 ) 
PRIMARY KEY (id) 
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");
```

往表 `t` 中同时隐式分配和显式指定自增 ID。

```SQL
INSERT INTO t (id, number) VALUES (1, DEFAULT);
INSERT INTO t (id, number) VALUES (2, 2);
INSERT INTO t (id, number) VALUES (3);
```

查询表 `t` 的数据。

```SQL
SELECT * FROM t;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 |      2 |
|    3 |      2 |
+------+--------+
3 rows in set (0.08 sec)
```

### 单调性保证

为了提升分配自增 ID 的性能，BE 会本地缓存部分自增 ID。在这种情况下，StarRocks 无法保证自增 ID 按照时间顺序严格递增，只能保证自增 ID 大致上是递增的。

假设 StarRocks 集群具有一个 FE 节点和两个 BE 节点。创建表 `t` 并且插入五行数据，如下所示：

```SQL
CREATE TABLE t
(
    id BIGINT NOT NULL,
    number BIGINT NOT NULL AUTO_INCREMENT
) 
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id)
PROPERTIES("replicated_storage" = "true");

INSERT INTO t VALUES (1, DEFAULT);
INSERT INTO t VALUES (2, DEFAULT);
INSERT INTO t VALUES (3, DEFAULT);
INSERT INTO t VALUES (4, DEFAULT);
INSERT INTO t VALUES (5, DEFAULT);
```

表 `t` 中自增 ID 不是单调递增的。这是因为两个 BE 节点分别缓存了 [1, 100000] 和 [100001, 200000] 范围内的自增 ID，使用多个 INSERT 语句导入数据时，会发送给不同的 BE，由不同 BE 分配自增 ID，因此无法保证自增 ID 的严格单调性。

```SQL
SELECT * FROM t;
+------+--------+
| id   | number |
+------+--------+
|    1 |      1 |
|    2 | 100001 |
|    3 |      2 |
|    4 | 100002 |
|    5 |      3 |
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

1. 在数据库 `example_db` 中创建表 `t`，并且插入一条数据。

    ```SQL
    -- 建表
    CREATE TABLE t
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
    INSERT INTO t (id, name, job1, job2) VALUES (0, 0, 1, 1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_6af28e77-7d2b-11ed-af6e-02424283676b', 'status':'VISIBLE', 'txnId':'152'}

    -- 查询数据
    SELECT * FROM t ORDER BY name;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |    1 |    1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. 准备 CSV 文件 **my_data.csv**，用于更新表 `t`。 CSV 文件包括自增列的 ID 值，不包含列 `job1` 的值，并且第一行数据的主键存在表 `t` 中，第二行的主键不存在。

    ```Plaintext
    0,0,99
    1,1,99
    ```

3. 通过 Stream Load 将 CSV 文件数据更新至表 `t`。

    ```Bash
    curl --location-trusted -u root: -H "label:123" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns:id,name,job2" \
        -T my_data.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/t/_stream_load
    ```

4. 查询更新后的表。第一条数据原先已经存在表 `t` 中，并且列 `job1` 保持原先的值。第二条数据是新插入的数据，由于列 `job1` 没有定义默认值，因此部分列更新框架会直接将此列的值设置为 `0`。

    ```SQL
    SELECT * FROM t ORDER BY name;
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

- 如果表中已经存在该行，则 StarRocks 不会更新自增FID。
- 如果表中不存在该行，则 StarRocks 会自动生成新的自增 ID。

该特性可以构建字典表的值，用于加速字符串的精确去重计数。

1. 在数据库 `example_db` 中创建表 `t`，指定 `job1` 为自增列，并且插入一条数据。

    ```SQL
    -- 建表
    CREATE TABLE t
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
    INSERT INTO t VALUES (0, 0, -1, -1);
    Query OK, 1 row affected (0.04 sec)
    {'label':'insert_458d9487-80f6-11ed-ae56-aa528ccd0ebf', 'status':'VISIBLE', 'txnId':'94'}

    SELECT * FROM t ORDER BY name;
    +------+------+------+------+
    | id   | name | job1 | job2 |
    +------+------+------+------+
    |    0 |    0 |   -1 |   -1 |
    +------+------+------+------+
    1 row in set (0.01 sec)
    ```

2. 准备 CSV 文件 **my_data.csv**，用于更新表 `t`。CSV 文件不包含自增列 `job1` 的值，并且第一行数据的主键存在于表中，第二、三行数据的主键不存在。

    ```Plaintext
    0,0,99
    1,1,99
    2,2,99
    ```

3. 通过 Stream Load 将 CSV 文件数据导入至表 `t`。

    ```Bash
    curl --location-trusted -u root: -H "label:123" \
        -H "column_separator:," \
        -H "partial_update:true" \
        -H "columns: id,name,job2" \
        -T my_data.csv -XPUT \
        http://<fe_host>:<fe_http_port>/api/example_db/t/_stream_load
    ```

4. 查询更新后的表。第一条数据已经存在表 `t` 中，自增列 `job1` 保持原先的 ID 值。第二、三条数据是新插入的数据，自增列 `job1` 的 ID 值由 StarRocks 自动生成。

    ```SQL
    SELECT * FROM t ORDER BY name;
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

- 创建具有自增列的表时，必须设置 `"replicated_storage" = "true"`，以确保所有副本具有相同的自增 ID。
- 每个表最多只能有一个自增列。
- 自增列必须是 BIGINT 类型。
- 自增列必须为 `NOT NULL`，并且不支持指定默认值。
- 如果主键模型的表中自增列为 Primary Key，则支持 DELETE 操作。反之，则不支持 DELETE 操作。
- 不支持使用 ALTER TABLE 添加 `AUTO_INCREMENT` 属性。
- 存算分离模式暂时不支持该功能。
- 不支持设置自增列的起始值和自增步长。
