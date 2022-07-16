# Insert Into Loading

The Insert Into statement is used similarly to the Insert Into statement in databases (e.g. MySQL).
However, in StarRocks, all data writes are ***a separate import job***, so Insert Into is also introduced as an import method.

---

## Usage Scenarios

* Users can use the Insert Into command if they want the results to be returned simultaneously during the import.
* Users can use INSERT INTO VALUES to import test data to verify the functionality of the StarRocks system.
* Users can use INSERT INTO SELECT to ETL data that is already in the StarRocks table and import it into a new StarRocks table.
* Users can create an external table (e.g. a MySQL external table that maps a table in a MySQL system). The data from the external table can be imported into the StarRocks table for storage via the `INSERT INTO SELECT` syntax.

---

### Syntax

~~~sql
INSERT INTO table_name
[ PARTITION (p1, ...) ]
[ WITH LABEL label]
[ (column [, ...]) ]
[ [ hint [, ...] ] ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
~~~

### Parameters

* tablet_name: The destination table to which the data will be imported, for example `db_name.table_name`.
* partitions: Specify the partitions to be imported. Partitions must exist in `table_name`. Multiple partition names are separated by commas. If you specify a target partition, only data that matches the target partition will be imported. If not specified, the default value is all partitions of the table.
* label: Specify a Label for the insert job.The Label is the identifier. Each import job has a Label that is unique within a single database.

> * **Note**: It is recommended to specify a Label instead of having it automatically assigned by the system. If a label is assigned automatically by the system, it may be impossible to know whether Insert Into is successful or not when disconnected happens       during execution due to network errors.

* column_name: The specified target column, which must exist in `table_name`. The target columns of the imported table can exist in any order. If no destination column is specified, then the default value is all columns of the imported table. If a column in the target table is not in the imported column, then this column needs to have a default value, otherwise Insert Into will fail. If the type of the result column does not match the type of the target column, then an implicit type conversion is invoked. If the conversion is not possible, then the Insert Into statement reports a syntax parsing error.
* expression: The expression assigned to a column.
* default: The default value of the corresponding column.
* query: A general query whose results are written to the target. The query statement supports any SQL query syntax supported by StarRocks.
* values: Users can insert one or more pieces of data via the VALUES syntax.

> Note: The VALUES method is only suitable for importing a few pieces of data as a DEMO and is not applicable to any test or production environment at all. The StarRocks system itself is also not suitable for single data import scenarios. It is recommended to use `INSERT INTO SELECT` for bulk import.

### Import Results

Insert Into itself is a SQL command, and its return results will be divided into the following depending on the execution result.

Executed successfully

~~~sql
mysql> insert into tbl1 select * from empty_tbl;
Query OK, 0 rows affected (0.02 sec)

mysql> insert into tbl1 select * from tbl2;
Query OK, 4 rows affected (0.38 sec)
{'label':'insert_8510c568-9eda-4173-9e36-6adc7d35291c', 'status':'visible', 'txnId':'4005'}

mysql> insert into tbl1 with label my_label1 select * from tbl2;
Query OK, 4 rows affected (0.38 sec)
{'label':'my_label1', 'status':'visible', 'txnId':'4005'}

mysql> insert into tbl1 select * from tbl2;
Query OK, 2 rows affected, 2 warnings (0.31 sec)
{'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'visible', 'txnId':'4005'}

mysql> insert into tbl1 select * from tbl2;
Query OK, 2 rows affected, 2 warnings (0.31 sec)
{'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'committed', 'txnId':'4005'}

~~~

`rows affected` indicates the total number of rows being imported. `warnings` indicates the number of rows being filtered.

`label` is a user-specified label or an automatically generated label. `label` is the identifier of the Insert Into import job. Each import job has a Label that is unique within a single database.

`status` indicates whether the imported data is visible. If it is visible, it shows `visible`, otherwise it shows `committed`.
`txnId` is the id of the import transaction corresponding to this insert.

The `err` field displays unintended errors. To see the filtered rows, use the following statement. Use the URL in the return result to query the wrong data.

~~~sql
SHOW LOAD WHERE label="xxx";
~~~

Execution Failure

An execution failure indicates that no data was successfully imported and returns the following.

~~~sql
mysql> insert into tbl1 select * from tbl2 where k1 = "a";

ERROR 1064 (HY000): all partitions have no load data. url: [http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2](http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2)

~~~

Where `ERROR 1064 (HY000): all partitions have no load data` shows the reason for the failure. Use the followed URL to query the wrong data.

---

## Related configurations

### FE configuration

* timeout: The timeout of the import job (in seconds). If the import job is not completed within the timeout time, it will be CANCELLED by the system. At present, Insert Into does not support custom timeout value. The timeout value is unified by 1 hour. If the import job cannot be completed within the specified time, adjust the `insert_load_default_timeout_second` parameter of the FE.

### Session parameters

* enable_insert_strict: Insert Into import itself does not control the tolerable error rate. The tolerable error rate can only be controlled by the `enable_insert_strict` parameter. When this parameter is set to false, it means that at least one piece of data has been imported correctly. If there is failed data, a Label will be returned. When this parameter is set to true, it means that the import will fail even if there is only one data error. This parameter is default to  true and can be set by `SET enable_insert_strict = false`;.
* query_timeout: Insert Into is a SQL command, so the statement is limited by the `query_timeout`session parameter . You can increase the timeout in "seconds" by `SET query_timeout = xxx;`.

## Import Example

### Create Database and Data Table

~~~sql
mysql> CREATE DATABASE IF NOT EXISTS load_test;
mysql> USE load_test;
mysql> CREATE TABLE insert_wiki_edit
(
    event_time DATETIME,
    channel VARCHAR(32) DEFAULT '',
    user VARCHAR(128) DEFAULT '',
    is_anonymous TINYINT DEFAULT '0',
    is_minor TINYINT DEFAULT '0',
    is_new TINYINT DEFAULT '0',
    is_robot TINYINT DEFAULT '0',
    is_unpatrolled TINYINT DEFAULT '0',
    delta INT SUM DEFAULT '0',
    added INT SUM DEFAULT '0',
    deleted INT SUM DEFAULT '0'
)
AGGREGATE KEY(event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled)
PARTITION BY RANGE(event_time)
(
    PARTITION p06 VALUES LESS THAN ('2015-09-12 06:00:00'),
    PARTITION p12 VALUES LESS THAN ('2015-09-12 12:00:00'),
    PARTITION p18 VALUES LESS THAN ('2015-09-12 18:00:00'),
    PARTITION p24 VALUES LESS THAN ('2015-09-13 00:00:00')
)
DISTRIBUTED BY HASH(user) BUCKETS 10
PROPERTIES("replication_num" = "1");

~~~

### Import Data Using `values`

~~~sql
mysql> INSERT INTO insert_wiki_edit VALUES("2015-09-12 00:00:00","#en.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),("2015-09-12 00:00:00","#ca.wikipedia","PereBot",0,1,0,1,0,17,17,0);
Query OK, 2 rows affected (0.29 sec)
{'label':'insert_1f12c916-5ff8-4ba9-8452-6fc37fac2e75', 'status':'VISIBLE', 'txnId':'601'}

~~~

### Import Data Using `select`

~~~sql
mysql> INSERT INTO insert_wiki_edit WITH LABEL insert_load_wikipedia SELECT * FROM routine_wiki_edit; 
Query OK, 18203 rows affected (0.40 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'618'}

~~~

## Note

* When the `INSERT` statement is executed, the default behavior is to filter out data that does not match the target table format, such as a string that is too long. However, for scenarios that require data not to be filtered, you can set `enable_insert_strict` to true to ensure that INSERT will not execute successfully when there is data being filtered out.
* StarRocks' insert has the same logic as import. Similar to import, each insert statement will generate a new version of the data. Frequent small batch import operations will generate too many versions of data, which will affect query performance. Therefore it is not recommended to use insert to import data frequently or as a daily routine import job. If there is a need for streaming import or small batch import, it is better to use Stream Load or Routine Load.
