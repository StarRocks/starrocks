# 主键模型导入

StarRocks 支持通过导入任务，对主键模型的表进行数据变更（插入、更新和删除数据）。

## 内部实现

目前支持的导入数据方式有 Stream Load、Broker Load、Routine Load。

> * 暂不支持通过 Spark Load 插入、更新和删除数据。
> * 暂不支持通过 SQL DML 语句（INSERT、UPDATE、DELETE）插入、更新和删除数据，将在未来版本中支持。

导入时，所有操作默认为 UPSERT 操作，暂不支持区分 INSERT 和 UPDATE 操作。
值得注意的是，导入时，为同时支持 UPSERT 和 DELETE 操作，StarRocks 在 Stream Load、Broker Load 的创建任务语法中增加op字段，用于存储操作类型。在导入时，可以新增一列`__op`，用于存储操作类型，取值为 0 时，代表 UPSERT 操作，取值为 1 时，代表 DELETE 操作。

> 建表时无需添加列`__op`。

## 通过 Stream Load 或 Broker Load 变更数据

Stream Load 和 Broker Load 导入数据的操作方式类似，根据导入的数据文件的操作形式有如下几种情况。这里通过一些例子来展示具体的导入操作：

**1.** 当导入的数据文件只有 UPSERT 操作时可以不添加 `__op` 列。可以指定 `__op` 为 UPSERT 操作，也可以不做任何指定，StarRocks 会默认导入为 UPSERT 操作。例如想要向表 t 中导入如下内容：

~~~text
# 导入内容
0,aaaa
1,bbbb
2,\N
4,dddd
~~~

Stream Load 导入语句：

~~~Bash
#不指定__op
curl --location-trusted -u root: -H "label:lineorder" \
    -H "column_separator:," -T demo.csv \
    http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
#指定__op
curl --location-trusted -u root: -H "label:lineorder" \
    -H "column_separator:," -H " columns:__op ='upsert'" -T demo.csv \
    http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
~~~

Broker Load 导入语句：

~~~sql
#不指定__op
load label demo_db.label1 (
    data infile("hdfs://localhost:9000/demo.csv")
    into table demo_tbl1
    format as "csv"
) with broker "broker1";

#指定__op
load label demo_db.label2 (
    data infile("hdfs://localhost:9000/demo.csv")
    into table demo_tbl1
    format as "csv"
    set (__op ='upsert')
) with broker "broker1";
~~~

**2.** 当导入的数据文件只有 DELETE 操作时，只需指定`__op`为 DELETE 操作。例如想要删除如下内容：

~~~text
#导入内容
1, bbbb
4, dddd
~~~

注意：DELETE 操作虽然只用到 Primary Key 列，但同样要提供全部的列，与 UPSERT 操作保持一致。

Stream Load 导入语句：

~~~bash
curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
    -H "columns:__op='delete'" -T demo.csv \
    http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
~~~

Broker Load 导入语句：

~~~sql
load label demo_db.label3 (
    data infile("hdfs://localhost:9000/demo.csv")
    into table demo_tbl1
    format as "csv"
    set (__op ='delete')
) with broker "broker1";  
~~~

**3.** 当导入的数据文件中同时包含 UPSERT 和 DELETE 操作时，需要指定额外的 `__op` 来表明操作类型。例如想要导入如下内容：

~~~text
1,bbbb,1
4,dddd,1
5,eeee,0
6,ffff,0
~~~

注意：

* DELETE 操作虽然只用到 primary key 列，但同样要提供全部的列，与 UPSERT 操作保持一致。
* 上述导入内容表示删除 id 为 1、4 的行，添加 id 为 5、6 的行。

Stream Load 导入语句：

~~~bash
curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
    -H " columns: c1,c2,c3,pk=c1,col0=c2,__op=c3 " -T demo.csv \
    http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
~~~

其中，指定了 `__op` 为第三列。

Broker Load 导入语句：

~~~bash
load label demo_db.label4 (
    data infile("hdfs://localhost:9000/demo.csv")
    into table demo_tbl1
    format as "csv"
    (c1,c2,c3)
    set (pk=c1,col0=c2,__op=c3)
) with broker "broker1";
~~~

其中，指定了 `__op` 为第三列。

更多关于 Stream Load 和 Broker Load 使用方法，请参考 [STREAM LOAD](../loading/StreamLoad.md) 和 [BROKER LOAD](../loading/BrokerLoad.md)

## 通过 Routine Load 变更数据

可以在创建 Routine Load 的语句中，在 columns 最后增加一列，指定为 `__op`。在真实导入中，`__op` 为 0 则表示 UPSERT 操作，为 1 则表示 DELETE 操作。例如导入如下内容：

**示例 1** 导入 CSV 数据

~~~bash
2020-06-23  2020-06-23 00: 00: 00 beijing haidian 1   -128    -32768  -2147483648    0
2020-06-23  2020-06-23 00: 00: 01 beijing haidian 0   -127    -32767  -2147483647    1
2020-06-23  2020-06-23 00: 00: 02 beijing haidian 1   -126    -32766  -2147483646    0
2020-06-23  2020-06-23 00: 00: 03 beijing haidian 0   -125    -32765  -2147483645    1
2020-06-23  2020-06-23 00: 00: 04 beijing haidian 1   -124    -32764  -2147483644    0
~~~

Routine Load 导入语句：

~~~sql
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
~~~

**示例 2** 导入 JSON 数据，源数据中有字段表示 UPSERT 或者 DELETE 操作，比如下面常见的 Canal 同步到 Kafka 的数据样例，`type` 可以表示本次操作的类型（当前还不支持同步 DDL 语句）。

数据样例：

~~~json
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
~~~

导入语句:

~~~sql
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
~~~

**示例 3** 导入 JSON 数据，源数据中有字段可以分别表示 UPSERT 和 DELETE 操作，可以不指定`__op`。

数据样例：op_type 字段的取值只有 0 和 1，分别表示 UPSERT 和 DELETE 操作。

~~~json
{"pk": 1, "col0": "123", "op_type": 0}
{"pk": 2, "col0": "456", "op_type": 0}
{"pk": 1, "col0": "123", "op_type": 1}
~~~

建表语句:

~~~sql
CREATE TABLE `demo_tbl2` (
  `pk` bigint(20) NOT NULL COMMENT "",
  `col0` varchar(65533) NULL COMMENT ""
) ENGINE = OLAP 
PRIMARY KEY(`pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
~~~

导入语句：

~~~sql
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
~~~

查询结果

~~~sql
mysql > select * from demo_db.demo_tbl2;
+------+------+
| pk   | col0 |
+------+------+
|    2 | 456  |
+------+------+
~~~

Routine Load 更多使用方法请参考 [ROUTINE LOAD](../loading/RoutineLoad.md)。
