# 主键模型导入

StarRocks 支持通过导入作业，对主键模型的表进行数据变更（插入、更新和删除数据），并且支持部分更新【公测中】。

## 内部实现

目前支持的导入方式有 Stream Load、Broker Load 和 Routine Load。

> 说明：
>
> - 暂不支持通过 Spark Load 插入、更新或删除数据。
>
> - 暂不支持通过 SQL DML 语句（INSERT 和 UPDATE）插入、更新或删除数据，将在未来版本中支持。

导入时，所有操作默认为 UPSERT 操作，暂不支持区分 INSERT 和 UPDATE 操作。 值得注意的是，为同时支持 UPSERT 和 DELETE 操作，StarRocks 在 Stream Load 和 Broker Load 作业的创建语法中增加 `op` 字段，用于存储操作类型。在导入时，可以新增一列 `__op`，用于存储操作类型，取值为 `0` 时代表 UPSERT 操作，取值为 `1` 时代表 DELETE 操作。

> 说明：建表时无需添加列 `__op`。

## 通过 Stream Load 或 Broker Load 变更数据

Stream Load 和 Broker Load 导入数据的操作方式类似，根据导入的数据文件的操作形式有如下几种情况。这里通过一些例子来展示具体的导入操作：

### 示例 1

当导入的数据文件只有 UPSERT 操作时可以不添加 `__op` 列。可以指定 `__op` 为 UPSERT 操作，也可以不做任何指定，StarRocks 会默认导入为 UPSERT 操作。例如，向表 `t` 中导入如下内容：

```Plain
# 要导入的内容：
0,aaaa
1,bbbb
2,\N
4,dddd
```

- 如果选择 Stream Load 导入方式，执行如下语句：

  ```Bash
  # 不指定 __op 列的操作类型。
  curl --location-trusted -u root: -H "label:lineorder" \
      -H "column_separator:," -T demo.csv \
      http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
  # 指定 __op 列的操作类型。
  curl --location-trusted -u root: -H "label:lineorder" \
      -H "column_separator:," -H "columns:__op ='upsert'" -T demo.csv \
      http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
  ```

- 如果选择 Broker Load 导入方式，执行如下语句：

  ```SQL
  # 不指定 __op 列的操作类型。
  load label demo_db.label1 (
      data infile("hdfs://localhost:9000/demo.csv")
      into table demo_tbl1
      columns terminated by ","
      format as "csv"
  ) with broker "broker1";
    
  # 指定 __op 列的操作类型。
  load label demo_db.label2 (
      data infile("hdfs://localhost:9000/demo.csv")
      into table demo_tbl1
      columns terminated by ","
      format as "csv"
      set (__op ='upsert')
  ) with broker "broker1";
  ```

### 示例 2

当导入的数据文件只有 DELETE 操作时，需指定 `__op` 列为 DELETE 操作。例如，要删除如下内容：

```Plain
# 要删除的内容：
1, bbbb
4, dddd
```

> 说明：DELETE 操作虽然只用到主键列，但同样要提供全部的列，这一点与 UPSERT 操作要求一致。

- 如果选择 Stream Load 导入方式，执行如下语句：
  
  ```Bash
  curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
      -H "columns:__op='delete'" -T demo.csv \
      http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
  ```

- 如果选择 Broker Load 导入方式，执行如下语句：

  ```SQL
  load label demo_db.label3 (
      data infile("hdfs://localhost:9000/demo.csv")
      into table demo_tbl1
      columns terminated by ","
      format as "csv"
      set (__op ='delete')
  ) with broker "broker1";  
  ```

### 示例 3

当导入的数据文件中同时包含 UPSERT 和 DELETE 操作时，需要指定额外的 `__op` 来表明操作类型。例如，想要删除 `id` 为 `1`、`4` 的行，并且添加 `id` 为 `5`、`6` 的行：

```Plain
1,bbbb,1
4,dddd,1
5,eeee,0
6,ffff,0
```

> 说明：DELETE 操作虽然只用到主键列，但同样要提供全部的列，这一点与 UPSERT 操作要求一致。

- 如果选择 Stream Load 导入方式，执行如下语句：
  
  ```Bash
  curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
      -H "columns: c1,c2,c3,pk=c1,col0=c2,__op=c3 " -T demo.csv \
      http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
  ```

  其中，指定了 `__op` 为第三列。

- 如果选择 Broker Load 导入方式，执行如下语句：

  ```Bash
  load label demo_db.label4 (
      data infile("hdfs://localhost:9000/demo.csv")
      into table demo_tbl1
      columns terminated by ","
      format as "csv"
      (c1,c2,c3)
      set (pk=c1,col0=c2,__op=c3)
  ) with broker "broker1";
  ```

  其中，指定了 `__op` 为第三列。

更多关于 Stream Load 和 Broker Load 的使用方法，可参考 [Stream Load](/loading/StreamLoad.md) 和 [Broker Load](/loading/BrokerLoad.md)。

## 通过 Routine Load 变更数据

可以在 Routine Load 的创建语句中，在 `COLUMNS` 关键字的最后增加一列，指定为 `__op`。在实际的导入操作中，`__op` 列的取值为 `0` 时代表 UPSERT 操作，取值为 `1` 时代表 DELETE 操作。

### 示例 1

导入 CSV 数据。

数据样例：

```Bash
2020-06-23  2020-06-23 00: 00: 00 beijing haidian 1   -128    -32768  -2147483648    0
2020-06-23  2020-06-23 00: 00: 01 beijing haidian 0   -127    -32767  -2147483647    1
2020-06-23  2020-06-23 00: 00: 02 beijing haidian 1   -126    -32766  -2147483646    0
2020-06-23  2020-06-23 00: 00: 03 beijing haidian 0   -125    -32765  -2147483645    1
2020-06-23  2020-06-23 00: 00: 04 beijing haidian 1   -124    -32764  -2147483644    0
```

执行语句：

```SQL
CREATE ROUTINE LOAD routine_load_basic_types_1631533306858 on primary_table_without_null 
COLUMNS (k1, k2, k3, k4, k5, v1, v2, v3, __op),
COLUMNS TERMINATED BY '\t' 
PROPERTIES (
    "desired_concurrent_number" = "1",
    "max_error_number" = "1000",
    "max_batch_interval" = "5"
) FROM KAFKA (
    "kafka_broker_list" = "localhgost:9092",
    "kafka_topic" = "starrocks-data"
    "kafka_offsets" = "OFFSET_BEGINNING"
);
```

### 示例 2

导入 JSON 数据，源数据中有字段表示 UPSERT 或者 DELETE 操作。比如下面常见的 Canal 同步到 Apache Kafka® 的数据样例，使用 `type` 字段表示本次操作的类型，支持的取值为 INSERT、UPDATE 和 DELETE。

> 说明：暂不支持同步 DDL 语句。

数据样例：

```JSON
{
    "data": [{
        "query_id": "3c7ebee321e94773-b4d79cc3f08ca2ac",
        "conn_id": "34434",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.578",
        "end_time": "2020-10-19 20:40:10"
    }],
    "database": "center_service_lihailei",
    "es": 1603111211000,
    "id": 122,
    "isDdl": false,
    "mysqlType": {
        "query_id": "varchar(64)",
        "conn_id": "int(11)",
        "user": "varchar(32)",
        "start_time": "datetime(3)",
        "end_time": "datetime"
    },
    "old": null,
    "pkNames": ["query_id"],
    "sql": "",
    "sqlType": {
        "query_id": 12,
        "conn_id": 4,
        "user": 12,
        "start_time": 93,
        "end_time": 93
    },
    "table": "query_record",
    "ts": 1603111212015,
    "type": "INSERT"
}
```

执行语句:

```SQL
CREATE ROUTINE LOAD cdc_db.label5 ON cdc_table
COLUMNS(pk, col0, temp,__op =(CASE temp WHEN "DELETE" THEN 1 ELSE 0 END))
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_error_number" = "1000",
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.data[0].query_id\",\"$.data[0].conn_id\",\"$.data[0].user\",\"$.data[0].start_time\",\"$.data[0].end_time\",\"$.type\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "localhost:9092",
    "kafka_topic" = "cdc-data",
    "property.group.id" = "starrocks-group",
    "property.client.id" = "starrocks-client",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

### 示例 3

导入 JSON 数据，可以不指定 `__op`，源数据中有 `op_type` 字段可以表示 UPSERT 或 DELETE 操作。`op_type` 字段的取值只有 `0` 和 `1`。取值为 `0` 时代表 UPSERT 操作，取值为 `1` 时代表 DELETE 操作。

数据样例：

```JSON
{"pk": 1, "col0": "123", "op_type": 0}
{"pk": 2, "col0": "456", "op_type": 0}
{"pk": 1, "col0": "123", "op_type": 1}
```

建表语句:

```SQL
CREATE TABLE `demo_tbl2` (
  `pk` bigint(20) NOT NULL COMMENT "",
  `col0` varchar(65533) NULL COMMENT ""
) ENGINE = OLAP 
PRIMARY KEY(`pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
```

执行语句：

```SQL
CREATE ROUTINE LOAD demo_db.label6 ON demo_tbl2
COLUMNS(pk,col0,__op)
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_error_number" = "1000",
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.pk\",\"$.col0\",\"$.op_type\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "localhost:9092",
    "kafka_topic" = "pk-data",
    "property.group.id" = "starrocks-group",
    "property.client.id" = "starrocks-client",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
```

查询结果：

```SQL
mysql > select * from demo_db.demo_tbl2;
+------+------+
| pk   | col0 |
+------+------+
|    2 | 456  |
+------+------+
```

更多关于 Routine Load 的使用方法，可参考 [Routine Load](/loading/RoutineLoad.md)。

## 部分更新【公测中】

> 说明：自 StarRocks v2.2 起，主键模型的表支持部分更新，您可以选择只更新部分指定的列。

以表 `demo` 为例，假设表 `demo` 包含 `id`、`name` 和 `age` 三列。

执行如下语句，创建 `demo` 表：

```SQL
create table demo(
    id int not null,
    name string null default '',
    age int not null default '0'
) primary key(id)
```

假设只更新表 `demo` 的 `id` 和 `name` 两列，只需要给出这两列的数据即可。如下所示，表中每行都是用逗号分隔的两列数据：

```Plain
0,aaaa
1,bbbb
2,\N
4,dddd
```

> 说明：
>
> - 所更新的列必须包含主键列，`demo` 表中是指 `id` 列。
>
> - 所有行的列数必须相同，这一点与同普通 CSV 格式文件的要求一致。

根据选择的导入方式，执行相关命令。

- 如果选择 Stream Load 导入方式，执行如下命令：

  ```Bash
  curl --location-trusted -u root: \
      -H "label:lineorder" -H "column_separator:," \
      -H "partial_update:true" -H "columns:id,name" \
      -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
  ```

  > 说明：需要设置 `-H "partial_update:true"`，以指定为部分列更新，并且指定所需更新的列名 `"columns:id,name"`。有关 Stream Load 的具体设置方式，可参考 [Stream Load](/loading/StreamLoad.md)。

- 如果选择 Broker Load 导入方式，执行如下命令：

  ```SQL
  load label demo.demo (
      data infile("hdfs://localhost:9000/demo.csv")
      into table t
      columns terminated by ","
      format as "csv"
      (c1, c2)
      set (id=c1, name=c2)
  ) with broker "broker1"
  properties (
      "partial_update" = "true"
  );
  ```

  > 说明：在 `properties` 中设置 `"partial_update" = "true"`，指定为部分列更新，并且指定所需更新的列名 `set (id=c1, name=c2)`。有关 Broker Load 的具体设置方式，可参考 [Broker Load](/loading/BrokerLoad.md)。

- 如果选择 Routine Load 导入方式，执行如下命令：

  ```SQL
  CREATE ROUTINE LOAD routine_load_demo on demo 
  COLUMNS (id, name),
  COLUMNS TERMINATED BY ','
  PROPERTIES (
      "partial_update" = "true"
  ) FROM KAFKA (
      "kafka_broker_list" = "broker1:9092,broker2:9092,broker3:9092",
      "kafka_topic" = "my_topic",
      "kafka_partitions" = "0,1,2,3",
      "kafka_offsets" = "101,0,0,200"
  );
  ```

  > 说明：在 `properties` 中设置 `"partial_update" = "true"`，指定为部分列更新，并且指定所需更新的列名 `COLUMNS (id, name)`。有关 Routine Load 的具体设置方式，可参考 [Routine Load](/loading/RoutineLoad.md)。

## 参考文档

有关 DELETE 语句在主键模型的更多使用方法，请参见 [DELETE](/sql-reference/sql-statements/data-manipulation/DELETE.md)。
