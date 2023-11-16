---
displayed_sidebar: "English"
---

# Introduction

You can import semi-structured data (for example, JSON) by using stream load or routine load.

## Use Scenarios

* Stream Load: For JSON data stored in text files, use stream load to import.
* Routine Load: For JSON data in Kafka, use routine load to import.

### Stream Load Import

Sample data:

~~~json
{ "id": 123, "city" : "beijing"},
{ "id": 456, "city" : "shanghai"},
    ...
~~~

Example:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "jsonpaths: [\"$.id\", \"$.city\"]" \
    -T example.json \
    http://FE_HOST:HTTP_PORT/api/DATABASE/TABLE/_stream_load
~~~

The `format: json` parameter allows you to execute the format of the imported data. `jsonpaths` is used to execute the corresponding data import path.

Related parameters:

* jsonpaths: Select the JSON path for each column
* json\_root: Select the column where the JSON starts to be parsed
* strip\_outer\_array: Crop the outermost array field
* strict\_mode: Strictly filter for column type conversion during import

When the JSON data schema and StarRocks data schema are not exactly the same, modify the `Jsonpath`.

Sample data:

~~~json
{"k1": 1, "k2": 2}
~~~

Import example:

~~~bash
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" \
    -H "columns: k2, tmp_k1, k1 = tmp_k1 * 100" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

The ETL operation of multiplying k1 by 100 is performed during the import, and the column is matched with the original data by `Jsonpath`.

The import results are as follows:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|  100 |    2 |
+------+------+
~~~

For missing columns, if the column definition is nullable, then `NULL` will be added, or the default value can be added by `ifnull`.

Sample data:

~~~json
[
    {"k1": 1, "k2": "a"},
    {"k1": 2},
    {"k1": 3, "k2": "c"},
]
~~~

Import Example-1:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "strip_outer_array: true" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

The import results are as follows:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|    1 | a    |
+------+------+
|    2 | NULL |
+------+------+
|    3 | c    |
+------+------+
~~~

Import Example-2:

~~~shell
curl -v --location-trusted -u <username>:<password> \
    -H "format: json" -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.k1\", \"$.k2\"]" \
    -H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2, 'x')" \
    -T example.json \
    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
~~~

The import results are as follows:

~~~plain text
+------+------+
| k1   | k2   |
+------+------+
|    1 | a    |
+------+------+
|    2 | x    |
+------+------+
|    3 | c    |
+------+------+
~~~

### Routine Load Import

Similar to stream load, the message content of Kafka data sources is treated as a complete JSON data.

1. If a message contains multiple rows of data in array format, all rows will be imported and Kafka's offset will only be incremented by 1.
2. If a JSON in Array format represents multiple rows of data, but the parsing of the JSON fails due to a JSON format error, the error row will only be incremented by 1 (given that the parsing fails, StarRocks cannot actually determine how many rows of data it contains, and can only record the error data as one row).

### Use Canal to import StarRocks from MySQL with incremental sync binlogs

[Canal](https://github.com/alibaba/canal) is an open-source MySQL binlog synchronization tool from Alibaba, through which we can synchronize MySQL data to Kafka. The data is generated in JSON format in Kafka. Here is a demonstration of how to use routine load to synchronize data in Kafka for incremental data synchronization with MySQL.

* In MySQL we have a data table with the following table creation statement.

~~~sql
CREATE TABLE `query_record` (
  `query_id` varchar(64) NOT NULL,
  `conn_id` int(11) DEFAULT NULL,
  `fe_host` varchar(32) DEFAULT NULL,
  `user` varchar(32) DEFAULT NULL,
  `start_time` datetime NOT NULL,
  `end_time` datetime DEFAULT NULL,
  `time_used` double DEFAULT NULL,
  `state` varchar(16) NOT NULL,
  `error_message` text,
  `sql` text NOT NULL,
  `database` varchar(128) NOT NULL,
  `profile` longtext,
  `plan` longtext,
  PRIMARY KEY (`query_id`),
  KEY `idx_start_time` (`start_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8
~~~

* Prerequisite: Make sure MySQL has binlog enabled and the format is ROW.

~~~bash
[mysqld]
log-bin=mysql-bin # Enable binlog
binlog-format=ROW # Select ROW mode
server_id=1 # MySQL replication need to be defined, and do not duplicate canal's slaveId
~~~

* Create an account and grant privileges to the secondary MySQL server:

~~~sql
CREATE USER canal IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
~~~

* Then download and install Canal.

~~~bash
wget https://github.com/alibaba/canal/releases/download/canal-1.0.17/canal.deployer-1.0.17.tar.gz

mkdir /tmp/canal
tar zxvf canal.deployer-$version.tar.gz -C /tmp/canal
~~~

* Modify the configuration (MySQL related).

`$ vi conf/example/instance.properties`

~~~bash
## mysql serverId
canal.instance.mysql.slaveId = 1234
#position info, need to change to your own database information
canal.instance.master.address = 127.0.0.1:3306
canal.instance.master.journal.name =
canal.instance.master.position =
canal.instance.master.timestamp =
#canal.instance.standby.address =
#canal.instance.standby.journal.name =
#canal.instance.standby.position =
#canal.instance.standby.timestamp =
#username/password, need to change to your own database information
canal.instance.dbUsername = canal  
canal.instance.dbPassword = canal
canal.instance.defaultDatabaseName =
canal.instance.connectionCharset = UTF-8
#table regex
canal.instance.filter.regex = .\*\\\\..\*
# Select the name of the table to be synchronized and the partition name of the kafka target.
canal.mq.dynamicTopic=databasename.query_record
canal.mq.partitionHash= databasename.query_record:query_id
~~~

* Modify the configuration (Kafka related).

`$ vi /usr/local/canal/conf/canal.properties`

~~~bash
# Available options: tcp(by default), kafka, RocketMQ
canal.serverMode = kafka
# ...
# kafka/rocketmq Cluster Configuration: 192.168.1.117:9092,192.168.1.118:9092,192.168.1.119:9092
canal.mq.servers = 127.0.0.1:6667
canal.mq.retries = 0
# This value can be increased in flagMessage mode, but do not exceed the maximum size of the MQ message.
canal.mq.batchSize = 16384
canal.mq.maxRequestSize = 1048576
# In flatMessage mode, please change this value to a larger value, 50-200 is recommended.
canal.mq.lingerMs = 1
canal.mq.bufferMemory = 33554432
# Canal's batch size with a default value of 50K. Please do not exceed 1M due to Kafka's maximum message size limit (under 900K)
canal.mq.canalBatchSize = 50
# Timeout of `Canal get`, in milliseconds. Empty indicates unlimited timeout.
canal.mq.canalGetTimeout = 100
# Whether the object is in flat json format
canal.mq.flatMessage = false
canal.mq.compressionType = none
canal.mq.acks = all
# Whether Kafka message delivery uses transactions
canal.mq.transaction = false
~~~

* Initiation

`bin/startup.sh`

The corresponding synchronization log is shown in `logs/example/example.log` and in Kafka, with the following format:

~~~json
{
    "data": [{
        "query_id": "3c7ebee321e94773-b4d79cc3f08ca2ac",
        "conn_id": "34434",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.578",
        "end_time": "2020-10-19 20:40:10",
        "time_used": "1.0",
        "state": "FINISHED",
        "error_message": "",
        "sql": "COMMIT",
        "database": "",
        "profile": "",
        "plan": ""
    }, {
        "query_id": "7ff2df7551d64f8e-804004341bfa63ad",
        "conn_id": "34432",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.566",
        "end_time": "2020-10-19 20:40:10",
        "time_used": "0.0",
        "state": "FINISHED",
        "error_message": "",
        "sql": "COMMIT",
        "database": "",
        "profile": "",
        "plan": ""
    }, {
        "query_id": "3a4b35d1c1914748-be385f5067759134",
        "conn_id": "34440",
        "fe_host": "172.26.34.139",
        "user": "zhaoheng",
        "start_time": "2020-10-19 20:40:10.601",
        "end_time": "1970-01-01 08:00:00",
        "time_used": "-1.0",
        "state": "RUNNING",
        "error_message": "",
        "sql": " SELECT SUM(length(lo_custkey)), SUM(length(c_custkey)) FROM lineorder_str INNER JOIN customer_str ON lo_custkey=c_custkey;",
        "database": "ssb",
        "profile": "",
        "plan": ""
    }],
    "database": "center_service_lihailei",
    "es": 1603111211000,
    "id": 122,
    "isDdl": false,
    "mysqlType": {
        "query_id": "varchar(64)",
        "conn_id": "int(11)",
        "fe_host": "varchar(32)",
        "user": "varchar(32)",
        "start_time": "datetime(3)",
        "end_time": "datetime",
        "time_used": "double",
        "state": "varchar(16)",
        "error_message": "text",
        "sql": "text",
        "database": "varchar(128)",
        "profile": "longtext",
        "plan": "longtext"
    },
    "old": null,
    "pkNames": ["query_id"],
    "sql": "",
    "sqlType": {
        "query_id": 12,
        "conn_id": 4,
        "fe_host": 12,
        "user": 12,
        "start_time": 93,
        "end_time": 93,
        "time_used": 8,
        "state": 12,
        "error_message": 2005,
        "sql": 2005,
        "database": 12,
        "profile": 2005,
        "plan": 2005
    },
    "table": "query_record",
    "ts": 1603111212015,
    "type": "INSERT"
}
~~~

Add `json_root` and `strip_outer_array = true` to import data from `data`.

~~~sql
create routine load manual.query_job on query_record   
columns (query_id,conn_id,fe_host,user,start_time,end_time,time_used,state,error_message,`sql`,`database`,profile,plan)  
PROPERTIES (  
    "format"="json",  
    "json_root"="$.data",
    "desired_concurrent_number"="1",  
    "strip_outer_array" ="true",    
    "max_error_number"="1000" 
) 
FROM KAFKA (     
    "kafka_broker_list"= "172.26.92.141:9092",     
    "kafka_topic" = "databasename.query_record" 
);
~~~

This completes the near real-time synchronization of data from MySQL to StarRocks.

View status and error messages of the import job by `show routine load`.
