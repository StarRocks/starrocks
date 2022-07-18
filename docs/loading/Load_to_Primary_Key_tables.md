# Load data into tables of Primary Key model

StarRocks supports load jobs. You can run load jobs to insert, update, or delete data from tables that use the Primary Key model. You can also run load jobs to update only portions of such tables. The support for partial updates is in preview.

## Internal implementation

StarRocks supports the following data ingestion methods: stream load, broker load, and routine load.

> Note:

- > You cannot insert, update, or delete data by performing a Spark load job.

- > You cannot insert, update, or delete data by executing the SQL DML statements INSERT and UPDATE. These operations will be supported in future StarRocks versions.

All load operations are UPSERT by default. INSERT operations are not distinguished from UPDATE operations. To support both UPSERT and DELETE operations during a load job, StarRocks provides a field named `op` in the syntax that is used to create a stream or broker load job. The `op` field is used to hold operation types. You can create a column named `__op` when you start a load job. A value of `0` indicates an UPSERT operation, and a value of `1` indicates a DELETE operation.

> Note: You do not need to add a column named `__op` when you create a table.

## Update data by running a stream or broker load job

A stream load job runs in a similar way as a broker load job. The operations for a stream or broker load job vary based on the file into which you want to load data.

### Example 1

If you perform only UPSERT operations on a file, you do not need to add the `__op` column to the file. However, if you add the `__op` column to the file, you can specify the operation type as UPSERT or leave the operation type unspecified for the column. Suppose that you want to load the following data into a table named `t` by running a stream or broker load job:

~~~text
# The data that you want to load into the table is as follows:
0,aaaa
1,bbbb
2,\N
4,dddd
~~~

- Run a stream load job.

    ~~~Bash
    # Load the data without an operation type specified for the __op field column.
    curl --location-trusted -u root: -H "label:lineorder" \
        -H "column_separator:," -T demo.csv \
        http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
    # Load the data with an operation type specified for the __op field column.
    curl --location-trusted -u root: -H "label:lineorder" \
        -H "column_separator:," -H "columns:__op ='upsert'" -T demo.csv \
        http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
    ~~~

- Run a broker load job.

    ~~~sql
    # Load the data without an operation type specified for the __op field column.
    load label demo_db.label1 (
        data infile("hdfs://localhost:9000/demo.csv")
        into table demo_tbl1
        columns terminated by ","
        format as "csv"
    ) with broker "broker1";

    # Load the data with an operation type specified for the __op field column.
    load label demo_db.label2 (
        data infile("hdfs://localhost:9000/demo.csv")
        into table demo_tbl1
        columns terminated by ","
        format as "csv"
        set (__op ='upsert')
    ) with broker "broker1";
    ~~~

### Example 2

If you perform only DELETE operations on a file, you only need to specify the operation type as DELETE for the `__op` column. Suppose that you want to delete the following data by running a stream or broker load job:

~~~text
# The data that you want to delete is as follows:
1, bbbb
4, dddd
~~~

> Note: DELETE operations are performed only on the column that is defined as the primary key of the table. However, you still need to provide all columns of the table. This is the same for UPSERT operations.

- Run a stream load job.

    ~~~bash
    curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
        -H "columns:__op='delete'" -T demo.csv \
        http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
    ~~~

- Run a broker load job.

    ~~~sql
    load label demo_db.label3 (
        data infile("hdfs://localhost:9000/demo.csv")
        into table demo_tbl1
        columns terminated by ","
        format as "csv"
        set (__op ='delete')
    ) with broker "broker1";  
    ~~~

### Example 3

If you perform both UPSERT and DELETE operations on a file, you must use the `__op` field to indicate the type of each operation. Suppose that you want to delete columns 1 and 4 and add columns 5 and 6 to a table:

~~~text
1,bbbb,1
4,dddd,1
5,eeee,0
6,ffff,0
~~~

> Note:
> DELETE operations are performed only on the column that is defined as the primary key of the table. However, you still need to provide all columns of the table. This is the same for UPSERT operations.

- Run a stream load job.

    ~~~bash
    curl --location-trusted -u root: -H "label:lineorder" -H "column_separator:," \
        -H "columns: c1,c2,c3,pk=c1,col0=c2,__op=c3 " -T demo.csv \
        http://localhost:8030/api/demo_db/demo_tbl1/_stream_load
    ~~~

    In the job, column `3` is specified as the `__op` column.

- Run a broker load job.

    ~~~bash
    load label demo_db.label4 (
        data infile("hdfs://localhost:9000/demo.csv")
        into table demo_tbl1
        columns terminated by ","
        format as "csv"
        (c1,c2,c3)
        set (pk=c1,col0=c2,__op=c3)
    ) with broker "broker1";
    ~~~

    In the job, column `3` is specified as the `__op` column.

For more information about stream load and broker load, see [Stream Load](./StreamLoad.md) and [Broker Load](./BrokerLoad.md).

## Update data by running a routine load job

You can add a column named `__op` in the `COLUMNS` keyword in the statement that is used to create a routine load job. A value of `0` in the `__op` column indicates an UPSERT operation, and the value of `1` in the `__op` column indicates a DELETE operation.

### Example 1

Load CSV data.

For example, you want to load the following data:

~~~bash
2020-06-23  2020-06-23 00: 00: 00 beijing haidian 1   -128    -32768  -2147483648    0
2020-06-23  2020-06-23 00: 00: 01 beijing haidian 0   -127    -32767  -2147483647    1
2020-06-23  2020-06-23 00: 00: 02 beijing haidian 1   -126    -32766  -2147483646    0
2020-06-23  2020-06-23 00: 00: 03 beijing haidian 0   -125    -32765  -2147483645    1
2020-06-23  2020-06-23 00: 00: 04 beijing haidian 1   -124    -32764  -2147483644    0
~~~

Execute the following statement to create and run a routine load job:

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

### Example 2

Load JSON data, which contains a field to specify whether an operation is UPSERT or DELETE.

For example, you want to synchronize the following data from Canal into Apache Kafka®, with the `type` field specified to indicate the operation type, which can be INSERT, UPDATE, or DELETE.

> Note: Data definition language (DDL) operations cannot be synchronized from Canal to Apache Kafka®.

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

Execute the following statement to create and run a routine load job:

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

### Example 3

Load JSON data, which contains a field to indicate whether an operation is UPSERT or DELETE. You can choose not to specify the `__op` field.

For example, you want to load the following data. The valid values of the `op_type` field are `0` and `1`. A value of `0` indicates an UPSERT operation, and a value of `1` indicates a DELETE operation.

~~~json
{"pk": 1, "col0": "123", "op_type": 0}
{"pk": 2, "col0": "456", "op_type": 0}
{"pk": 1, "col0": "123", "op_type": 1}
~~~

Execute the following statement to create a table:

~~~sql
CREATE TABLE `demo_tbl2` (
  `pk` bigint(20) NOT NULL COMMENT "",
  `col0` varchar(65533) NULL COMMENT ""
) ENGINE = OLAP 
PRIMARY KEY(`pk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
~~~

Execute the following statement to create and run a routine load job:

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

The query result is as follows:

~~~sql
mysql > select * from demo_db.demo_tbl2;
+------+------+
| pk   | col0 |
+------+------+
|    2 | 456  |
+------+------+
~~~

For more information about routine loads, see [Routine Load](./RoutineLoad.md).

## [Preview] Update partial data

> Note: Since v2.2, StarRocks supports updates to only specified columns of tables that use the Primary Key model.

In this section, a table named `demo` is used as an example. The `demo` table consists of three columns: `id`, `name`, and `age`.

Execute the following statement to create a table named `demo`:

~~~SQL
create table demo(
    id int not null,
    name string null default '',
    age int not null default '0'
) primary key(id)
~~~

If you update specified columns, such as columns `id` and `name`, of the `demo` table, you need to provide only the data of columns `id` and `name`.

> Note:

- > The columns that you specify must include the column that is defined as the primary key of the table. In this example, the primary key column is column `id`.

- > All columns of the table must consist of the same number of rows. This requirement is the same for CSV files. In the following example, each line represents two columns, which are separated by a comma (,).

~~~Plain Text
0,aaaa
1,bbbb
2,\N
4,dddd
~~~

Execute the following statements to update the data:

- If you want to run a stream load job, execute the following statement:

    ~~~Bash
    curl --location-trusted -u root: \
        -H "label:lineorder" -H "column_separator:," \
        -H "partial_update:true" -H "columns:id,name" \
        -T demo.csv http://localhost:8030/api/demo/demo/_stream_load
    ~~~

    > Note: In the preceding statement, you must specify the `-H "partial_update:true"` setting, and specify the columns that you want to update in the `"columns:id,name"` format. For more information about the parameter settings for a stream load job, see [Stream Load](./StreamLoad.md).

- If you want to run a broker load job, execute the following statement:

    ~~~SQL
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
    ~~~

    > Note: In the preceding statement, you must specify the `-H "partial_update:true"` setting in the `properties` class. Additionally, you must specify the columns that you want to update. For example, if you want to update two columns, `c1` and `c2`, specify the `set (id=c1, name=c2)` setting in the preceding statement. For more information about the parameter settings for a stream load job, see [Broker Load](./BrokerLoad.md).

- If you want to run a routine load job, execute the following statement:

    ~~~SQL
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
    ~~~

    > Note: In the preceding statement, you must specify the `-H "partial_update:true"` setting, and specify the columns that you want to update in the `COLUMNS (id, name)` format. For more information about the parameter settings for a stream load job, see [Routine Load](./RoutineLoad.md).

## References

For more information about the usage of the DELETE statement in the Primary Key model, see [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md).
