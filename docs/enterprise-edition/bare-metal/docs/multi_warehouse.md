# Manage compute resources using warehouses

This topic describes how to manage compute resources in a share-data StarRocks cluster using the Multi-Warehouse feature. This feature is supported in the StarRocks Enterprise Edition from v3.2.0 onwards.

## Overview

A warehouse in a share-data cluster is a group of Compute Nodes (CNs) that can provide with you the required compute resources (CPU/Memory/Temporary storage) to perform query, ingestion, and data processing tasks. Each warehouse serves as an individual compute resource pool, which allows you to isolate compute resources physically.

In a share-data cluster, data is shared among multiple warehouses, yet distinct warehouses maintain the physical isolation of compute and memory resources. Therefore, you can create multiple warehouses tailored to different business needs, such as ad hoc query warehouse, ETL warehouse, and compaction warehouse, and effortlessly route specific tasks to the respective warehouse.

### Benefits

Multi-Warehouse can bring the following benefits:

- **Resource isolation**
  -  Multi-Warehouse allows for finer-grained scheduling of CNs. You can allocate different tasks to distinct warehouses, ensuring the physical isolation of compute resources.
- **Data sharing**
  -  Multiple warehouses can share a common data storage, empowering authorized users to access cluster data through any warehouse seamlessly.
- **Vertical scalability**
  -  Multi-Warehouse allows you to group CNs as needed, bringing higher flexibility to cope with fluctuations in load. You can dynamically start or stop a warehouse, or scale it up or down by adding or dropping the stateless CNs.
- **Horizontal scalability**
  -  You can easily create a new warehouse to cater to the demand of new business scenarios, without impacting the existing warehouses. Tasks on the existing warehouses will not be disrupted because no data redistribution is needed. You can build a centralized large cluster to support multiple concurrent services, avoiding the increased complexity and costs associated with maintaining multiple independent clusters.

### Use cases

Multi-Warehouse finds applications in various scenarios:

- **Diverse business workloads**
  -  You can assign different types of workloads to distinct warehouses to isolate the compute resources physically. For example, you can allocate one warehouse to perform query analytics and another for ETL processing, optimizing resource utilization for each.
- **Centralized large cluster**
  -  Database administrators can maintain a common large cluster and create separate warehouses for each business unit, effectively mitigating the need to establish numerous smaller StarRocks clusters, thus reducing maintenance overhead.
- **Background task separation**
  -  You can isolate and execute background tasks, such as compaction, within dedicated warehouses to prevent disruption to regular operations. Furthermore, you can adjust warehouse resources as needed to strike a balance between cost and performance.

### Best practices

Multi-Warehouse's capabilities shine in scenarios like:

- **Online-offline hybrid analysis**
  -  Multi-Warehouse offers superior resource isolation and data sharing capabilities for businesses combining online and offline operations. With unified data storage, this approach streamlines storage costs and simplifies cluster management.
- **Ad hoc** **query**
  -  You can flexibly scale up or down the warehouse for ad hoc query requests.
- **Offline tasks**
  -  In cases where an offline task is time-sensitive, Multi-Warehouse enables rapid resource allocation adjustments, ensuring urgent tasks are accomplished within time constraints.

## Create a warehouse

Each time a new StarRocks cluster is deployed, a built-in warehouse `default_warehouse` is created. Any request without specifying a warehouse to use is routed to `default_warehouse`.

Use the following syntax to create a new warehouse:

```SQL
CREATE WAREHOUSE [ IF NOT EXISTS ] <warehouse_name>
[COMMENT <comment>]
```

**Parameter**:

- `warehouse_name` (Required): The name of the warehouse you want to create.
- `comment` (Optional): The comment for the warehouse you want to create.

**Example**:

Create a warehouse `wh1`.

```SQL
CREATE WAREHOUSE wh1;
```

## Add CNs to warehouse

After building a new warehouse, you must add CNs to it to allocate compute resources to it.

Use the following syntax to add CNs to a warehouse:

```SQL
ALTER SYSTEM ADD COMPUTE NODE "<cn_ip_or_fqdn>:<heartbeat_service_port>" [, ...] 
[INTO WAREHOUSE <warehouse_name>]
```

**Parameter**:

- `cn_ip_or_fqdn` (Required): The IP address or FQDN of the CN to add to the warehouse.
- `heartbeat_service_port` (Required): The heartbeat service port of the CN to add to the warehouse.
- `warehouse_name` (Optional): The name of the warehouse you want to add the CN to. If this parameter is not specified, `default_warehouse` is used.

**Example**:

Add a CN to `wh1`.

```SQL
ALTER SYSTEM ADD COMPUTE NODE "xxx.xxx.xx.xxx:9050"
INTO WAREHOUSE wh1;
```

## Set warehouse

After creating a new warehouse and allocating compute resources to it, you can enable it whenever needed by setting the warehouse for the current session, specified users, or specific operations. Thus, each request can only use the compute resources of the specified warehouse.

### Set warehouse for current session

Use the following syntax to set a warehouse for the current session:

```SQL
SET WAREHOUSE = <warehouse_name>
```

**Parameter**:

`warehouse_name` (Required): The name of the warehouse you want to use in the current session.

> **NOTE**
>
> If you do not set the warehouse for the current session, `default_warehouse` is used.

**Example**:

Set `wh1` for the current session.

```SQL
SET WAREHOUSE = wh1;
```

You can view the warehouse that is in use in the current session:

```SQL
SHOW VARIABLES LIKE "%warehouse%";
```

### Set warehouse for specific user

You can set a warehouse for a user as their default warehouse.

```SQL
ALTER USER '<username>' SET PROPERTIES ("warehouse" = "<warehouse_name>")
```

**Parameter**:

- `username` (Required): The username of the user for whom you want to set the default warehouse.
- `warehouse_name` (Required): The name of the warehouse you want to set as the default warehouse for the specified user.

**Example**:

Set `wh1` as the default warehouse of the user `jack`.

```SQL
ALTER USER 'jack' SET PROPERTIES ("warehouse" = "wh1");
```

### Set warehouse for specific SQL

You can use hints to set the warehouse for specific SQL statements.

**Example**:

```SQL
SELECT /*+ SET_VAR(warehouse="wh1") */ * FROM my_db.my_table;
UPDATE /*+ SET_VAR(warehouse="wh1") */ my_db.my_table SET c1 = 2 WHERE c1 = 1;
DELETE /*+ SET_VAR(warehouse="wh1") */ FROM my_db.my_table PARTITION p1 WHERE k1 = 3;
INSERT /*+ SET_VAR(warehouse="wh1") */ INTO my_db.my_table SELECT * FROM my_db.my_table2;
```

### Set warehouse for loading tasks

#### For Broker Load

You can set the warehouse for a specific Broker Load task by declaring it in the property `warehouse`.

**Example**:

```SQL
LOAD LABEL my_db.test_wh
(
    DATA INFILE("hdfs://127.0.0.1:9000/user/data/example1.csv")
    INTO TABLE my_table
)
WITH BROKER
(
    "username" = "jack_hdfs",
    "password" = "hdfs123456"
)
PROPERTIES
(
    "warehouse"="wh1"
);
```

#### For Routine Load

You can set the warehouse for a specific Routine Load task by declaring it in the job property `warehouse`.

**Example**:

```SQL
CREATE ROUTINE LOAD my_db.test_wh
ON my_table 
COLUMNS TERMINATED BY '\t', WHERE v1 != 0 
PROPERTIES (
    "desired_concurrent_number"="1",
    "max_error_number"="1000",
    "max_batch_interval"="7",
    "warehouse"="wh1"
) 
FROM KAFKA (
    "kafka_broker_list"="127.0.0.1:9092",
    "kafka_topic"="xxx",
    "kafka_partitions"="0",
    "kafka_offsets"="OFFSET_BEGINNING"
);
```

#### For Stream Load

You can set the warehouse for a specific Stream Load task by declaring it in the header parameter.

**Example**:

```Bash
curl --location-trusted -u jack:12345 \
    -H "label:stream_load_test_wh"  -H "timeout:100" -H "max_filter_ratio:1" \
    -H "warehouse:wh1" \
    -T example1.csv -XPUT http://127.0.0.1:8030/api/my_db/my_table/_stream_load
```

#### For Stream Load transaction interface

You can set the warehouse when you begin a transaction using the Stream Load transaction interface by declaring the warehouse in the header parameter.

> **NOTE**
>
> You do not need to set the warehouse for `load`, `prepare`, and `commit` operations.

**Example**:

```Bash
curl --location-trusted -u jack:12345 \
    -H "label:stream_load_trans_test_wh" -H "db:my_db" -H "table:my_table" \
    -H "warehouse:wh1" \
    -XPOST http://127.0.0.1:8030/api/transaction/begin
```

#### For Flink connector

If you want to set the warehouse for the Flink connector, you must declare it both in `jdbc-url` and with the connector property `sink.properties.warehouse`.

> **NOTE**
>
> Declare a warehouse in `jdbc-url` in the format `jdbc:mysql://<host>:<port>?sessionVariables=warehouse=<warehouse_name>`.

**Example**:

- Flink SQL example

```SQL
CREATE TABLE `score_board` (
    `id` INT,
    `name` STRING,
    `score` INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030?sessionVariables=warehouse=wh1',
    'load-url' = '127.0.0.1:8030',
    'sink.properties.warehouse' = 'wh1',
    'database-name' = 'test',
    'table-name' = 'score_board'
    'username' = 'root',
    'password' = ''
);

INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
```

- Flink DataStream example

```Java
String[] records = new String[]{
        "1\tstarrocks-csv\t100",
        "2\tflink-csv\t100"
};
DataStream<String> source = env.fromElements(records);

StarRocksSinkOptions options = StarRocksSinkOptions.builder()
        .withProperty("jdbc-url", jdbcUrl)
        .withProperty("load-url", loadUrl)
        .withProperty("database-name", "test")
        .withProperty("table-name", "score_board")
        .withProperty("username", "root")
        .withProperty("password", "")
        .withProperty("sink.properties.warehouse", "wh1")
        .withProperty("sink.properties.format", "csv")
        .withProperty("sink.properties.column_separator", "\t")
        .build();

SinkFunction<String> starRockSink = StarRocksSink.sink(options);
source.addSink(starRockSink);
```

### Set warehouse for materialized view refresh

You can set the warehouse for the refresh task of materialized views by declaring it in the property `warehouse`.

> **NOTE**
>
> If `warehouse` is not specified, the warehouse of the current session will be used. 

**Example**:

```SQL
CREATE MATERIALIZED VIEW order_mv
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC EVERY (interval 1 MINUTE) 
PROPERTIES ("warehouse" = "wh1")
AS SELECT 
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

After the materialized view is created, you can view the property `warehouse` by executing `SHOW MATERIALIZED VIEWS` or `SHOW CREATE MATERIALIZED VIEW`. It will also be recorded in **fe.audit.log**.

### Set warehouse for Compaction

By default, Compaction tasks are running on `default_warehouse`. You can change it by setting the FE configuration item `lake_compaction_warehouse`.

- Execute the following statement to dynamically change the default Compaction warehouse:

  - ```SQL
    ADMIN SET FRONTEND CONFIG ("lake_compaction_warehouse" = "<warehouse_name>");
    ```

- To permanently change the the default Compaction warehouse, add the following configuration item to the FE configuration file **fe.conf**, and restart FE:

  - ```SQL
    lake_compaction_warehouse = <warehouse_name>
    ```

After changing the configuration, you can view the warehouse information of compaction tasks by executing `SHOW PROC '/compactions'`.

### Set warehouse in JDBC URL

```SQL
jdbc.url=jdbc:mysql://<host>/<db_name>?sessionVariables=warehouse=<warehouse_name>
```

### Set warehouse for Superset

You can set the warehouse in the URL used to connect to StarRocks with SQLAlchemy.

```SQL
starrocks://<username>:<password>@<host>:<port>/<database>?init_command=set warehouse = <warehouse_name>
```

## Manage warehouses

### Show all warehouses

Use the following syntax to show all warehouses in a cluster:

```SQL
SHOW WAREHOUSES [ LIKE '<pattern>' ]
```

**Example**:

Show all warehouses in the current cluster.

```SQL
SHOW WAREHOUSES;
```

The returned information is as follows:

```Plain
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+---------------------+-----------+---------------------+----------------------------------------------+
| Id    | Name              | State     | NodeCount | CurrentClusterCount | MaxClusterCount | StartedClusters | RunningSql | QueuedSql | CreatedOn           | ResumedOn | UpdatedOn           | Comment                                      |
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+---------------------+-----------+---------------------+----------------------------------------------+
| 0     | default_warehouse | AVAILABLE | 2         | 1                   | 1               | 1               | 0          | 0         | 2023-10-11 15:28:46 | NULL      | 2023-10-11 15:28:46 | An internal warehouse init after FE is ready |
| 11078 | wh1               | AVAILABLE | 1         | 1                   | 1               | 1               | 0          | 0         | 2023-10-11 16:31:20 | NULL      | 2023-10-11 16:31:20 | NULL                                         |
+-------+-------------------+-----------+-----------+---------------------+-----------------+-----------------+------------+-----------+---------------------+-----------+---------------------+----------------------------------------------+
```

**Return**:

- `Id`: The ID of the warehouse.
- `Name`: The name of the warehouse.
- `State`: The state of the warehouse. Valid values:
  - `AVAILABLE`: The warehouse is running.
  - `SUSPENDED`: The warehouse is suspended.
- `NodeCount`: The number of CNs in the warehouse.
- `CurrentClusterCount`:
- `MaxClusterCount`:
- `StartedClusters`:
- `RunningSql`: The number of queries that are currently running in the warehouse.
- `QueuedSql`: The number of queries that are waiting for execution.
- `CreatedOn`: The time when the warehouse is created.
- `ResumedOn`: The time when the warehouse is resumed.
- `UpdatedOn`: The time when the warehouse is updated.
- `Comment`: The comment of the warehouse.

### Suspend a warehouse

You can suspend a warehouse to reduce resource consumption when you do not need it temporarily. All requests routed to a suspended warehouse are rejected. The state of a suspended warehouse is `SUSPENDED`.

Use the following syntax to suspend a warehouse:

```SQL
SUSPEND WAREHOUSE <warehouse_name>
```

**Parameter**:

`warehouse_name` (Required): The name of the warehouse you want to suspend.

**Example**:

Suspend `wh1`.

```SQL
SUSPEND WAREHOUSE wh1;
```

### Resume a warehouse

You can resume a suspended warehouse. The state of a resumed warehouse becomes `AVAILABLE`.

Use the following syntax to resume a warehouse:

```SQL
RESUME WAREHOUSE <warehouse_name>
```

**Parameter**:

`warehouse_name` (Required): The name of the warehouse you want to resume.

**Example**:

Resume `wh1`.

```SQL
RESUME WAREHOUSE wh1;
```

### Drop a warehouse

You can drop a warehouse when you no longer need it. When a warehouse is dropped, the CNs within it are also dropped from the cluster.

Use the following syntax to drop a warehouse:

```SQL
DROP WAREHOUSE [ IF EXISTS ] <warehouse_name>
```

**Parameter**:

`warehouse_name` (Required): The name of the warehouse you want to drop.

**Example**:

Drop `wh1`.

```SQL
DROP WAREHOUSE wh1;
```

### Manage CNs in warehouses

#### Show all CNs in a cluster

Use the following syntax to show all CNs in the current cluster:

```SQL
SHOW COMPUTE NODES
```

**Example**:

Show all CNs in the current cluster.

```SQL
SHOW COMPUTE NODES;
```

#### Show CNs in specified warehouse(s)

Use the following syntax to show CNs in the specified warehouse(s):

```SQL
SHOW NODES FROM 
    { WAREHOUSE <warehouse_name> 
    | WAREHOUSES  [ LIKE '<pattern>' ] }
```

**Parameter**:

- `warehouse_name` (Required): The name of the warehouse in which you want to view the CNs.
- `WAREHOUSES`: Shows the CNs in all warehouses or a group of warehouses with the specified pattern.

**Example**:

Show all CNs in all warehouses in the cluster.

```SQL
SHOW NODES FROM WAREHOUSES;
```

#### Drop CNs from a warehouse

Use the following syntax to drop CNs from a warehouse:

```SQL
ALTER SYSTEM DROP COMPUTE NODE "<cn_ip_or_fqdn>:<heartbeat_service_port>" [, ...] 
[FROM WAREHOUSE <warehouse_name>]
```

**Parameter**:

- `cn_ip_or_fqdn` (Required): The IP address or FQDN of the CN to drop from the warehouse.
- `heartbeat_service_port` (Required): The heartbeat service port of the CN to drop from the warehouse.
- `warehouse_name` (Optional): The name of the warehouse you want to drop the CN from. If this parameter is not specified, `default_warehouse` is used.

**Example**:

Drop a compute node from `wh1`.

```SQL
ALTER SYSTEM DROP COMPUTE NODE "xxx.xxx.xx.xxx:9050"
FROM WAREHOUSE wh1;
```

## Privileges

| Privilege        | Level     | Description                                                  |
| :--------------- | :-------- | :----------------------------------------------------------- |
| CREATE WAREHOUSE | SYSTEM    | Creates warehouses.                                          |
| ALTER            | WAREHOUSE | Suspends or resumes a warehouse.                             |
| DROP             | WAREHOUSE | Drops a warehouse.                                           |
| USAGE            | WAREHOUSE | Sets a warehouse.                                            |
| ALL              | WAREHOUSE | Has all the above privileges on a warehouse except CREATE WAREHOUSE. |

> **NOTE**
>
> The SHOW privilege on a warehouse will be granted to a user with any of the ALTER, DROP, USAGE, and ALL privileges on the specified warehouse.

**Syntax**:

```SQL
GRANT
    CREATE WAREHOUSE 
    ON SYSTEM
    TO { ROLE | USER } {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { WAREHOUSE <warehouse_name> [, < warehouse_name >,...] ｜ ALL WAREHOUSES} 
    TO { ROLE | USER } {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

## Usage notes

Some of the system background tasks are performed only within the `default_warehouse`. Therefore, suspending the `default_warehouse` or removing all compute nodes from it will cause these tasks to fail. These background tasks are as follows:

- SUBMIT TASK
- Statistics collection
- Dynamic partition creation and deletion
- Schema Change
- AutoVacuum (Garbage Collection after Compaction)
- Garbage Collection
- Statistics report for SHOW DATA
- ANALYZE TABLE

Please note that downgrading a StarRocks cluster with multiple warehouses to versions earlier than v3.2 will cause the CNs in the user-created warehouses to get removed from the cluster.