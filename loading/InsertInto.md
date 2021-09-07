# Insert Into loading

Insert Into 语句的使用方式和 MySQL 等数据库中 Insert Into 语句的使用方式类似。
但在 StarRocks 中，所有的数据写入都是 ***一个独立的导入作业*** ，所以这里将 Insert Into 也作为一种导入方式介绍。

---

## 使用场景

* Insert Into 导入命令会同步返回导入流程的运行结果。
* 仅导入几条测试数据，验证一下 StarRocks 系统的功能。此时适合使用 INSERT INTO VALUS 的语法。
* 用户希望将已经在 StarRocks 表中的数据进行 ETL 转换并导入到一个新的 StarRocks 表中，此时适合使用 INSERT INTO SELECT 语法。
* 用户可以创建一种外部表，如 MySQL 外部表映射一张 MySQL 系统中的表。然后通过 INSERT INTO SELECT 语法将外部表中的数据导入到 StarRocks 表中存储。

---

### 语法

~~~sql
INSERT INTO table_name
[ PARTITION (p1, ...) ]
[ WITH LABEL label]
[ (column [, ...]) ]
[ [ hint [, ...] ] ]
{ VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
~~~

### 参数

* tablet_name: 导入数据的目的表。可以是 db_name.table_name 形式。
* partitions: 指定待导入的分区，必须是 table_name 中存在的分区，多个分区名称用逗号分隔。如果指定目标分区，则只会导入符合目标分区的数据。如果没有指定，则默认值为这张表的所有分区。
* label: 为 insert 作业指定一个 Label，Label 是该 Insert Into 导入作业的标识。每个导入作业，都有一个在单 database 内部唯一的 Label。

> * **注意**：建议指定 Label 而不是由系统自动分配。如果由系统自动分配，但在 Insert Into 语句执行过程中，因网络错误导致连接断开等，则无法得知 Insert Into 是否成功。而如果指定 Label，则可以再次通过 Label 查看任务结果。

* column_name: 指定的目的列，必须是 table_name 中存在的列。导入表的目标列，可以以任意的顺序存在。如果没有指定目标列，那么默认值是这张表的所有列。如果导入表中的某个列不在目标列中，那么这个列需要有默认值，否则 Insert Into 会失败。如果查询语句的结果列类型与目标列的类型不一致，那么会调用隐式类型转化，如果不能进行转化，那么 Insert Into 语句会报语法解析错误。
* expression：需要赋值给某个列的对应表达式。
* default：让对应列使用默认值。
* query：一个普通查询，查询的结果会写入到目标中。查询语句支持任意 StarRocks 支持的 SQL 查询语法。
* values：用户可以通过 VALUES 语法插入一条或者多条数据。

> * **注意**：VALUES 方式仅适用于导入几条数据作为 DEMO 的情况，完全不适用于任何测试和生产环境。StarRocks 系统本身也不适合单条数据导入的场景。建议使用 INSERT INTO SELECT 的方式进行批量导入。

### 导入结果

Insert Into 本身就是一个 SQL 命令，其返回结果会根据执行结果的不同，分为以下几种：

执行成功

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

rows affected 表示总共有多少行数据被导入。warnings 表示被过滤的行数。

label 为用户指定的 label 或自动生成的 label。Label 是该 Insert Into 导入作业的标识。每个导入作业，都有一个在单 database 内部唯一的 Label。

status 表示导入数据是否可见。如果可见，显示 visible，如果不可见，显示 committed。
txnId 为这个 insert 对应的导入事务的 id。

err 字段会显示一些其他非预期错误。当需要查看被过滤的行时，用户可以使用如下语句。返回结果中的 URL 可以用于查询错误的数据。

~~~sql
SHOW LOAD WHERE label="xxx";
~~~

执行失败

执行失败表示没有任何数据被成功导入，并返回如下：

~~~sql
mysql> insert into tbl1 select * from tbl2 where k1 = "a";

ERROR 1064 (HY000): all partitions have no load data. url: [http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2](http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2)

~~~

其中 ERROR 1064 (HY000): all partitions have no load data 显示失败原因。后面的 url 可以用于查询错误的数据。

---

## 相关配置

### FE 配置

* timeout：导入任务的超时时间(以秒为单位)。导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。目前 Insert Into 并不支持自定义导入的 timeout 时间，所有 Insert Into 导入的超时时间是统一的，默认的 timeout 时间为1小时。如果导入任务无法在规定时间内完成，则需要调整FE的参数insert_load_default_timeout_second。

### Session 变量

* enable_insert_strict：Insert Into 导入本身不能控制导入可容忍的错误率。用户只能通过 enable_insert_strict 这个 Session 参数用来控制。当该参数设置为 false 时，表示至少有一条数据被正确导入，则返回成功。如果有失败数据，则还会返回一个 Label。当该参数设置为 true 时，表示如果有一条数据错误，则导入失败。该参数默认为 true。可通过 SET enable_insert_strict = false; 来设置。
* query_timeout：Insert Into 本身也是一个 SQL 命令，因此 Insert Into 语句也受到 Session 变量 query_timeout 的限制。可以通过 SET query_timeout = xxx; 来增加超时时间，单位是「秒」。

## 导入示例

### 创建数据库与数据表

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

### 通过values导入数据

~~~sql
mysql> INSERT INTO insert_wiki_edit VALUES("2015-09-12 00:00:00","#en.wikipedia","GELongstreet",0,0,0,0,0,36,36,0),("2015-09-12 00:00:00","#ca.wikipedia","PereBot",0,1,0,1,0,17,17,0);
Query OK, 2 rows affected (0.29 sec)
{'label':'insert_1f12c916-5ff8-4ba9-8452-6fc37fac2e75', 'status':'VISIBLE', 'txnId':'601'}

~~~

### 通过select导入数据

~~~sql
mysql> INSERT INTO insert_wiki_edit WITH LABEL insert_load_wikipedia SELECT * FROM routine_wiki_edit; 
Query OK, 18203 rows affected (0.40 sec)
{'label':'insert_load_wikipedia', 'status':'VISIBLE', 'txnId':'618'}

~~~

## 注意事项

* 当前执行 INSERT 语句时，对于有不符合目标表格式的数据，默认的行为是过滤，比如字符串超长等。但是对于要求数据不能够被过滤的业务场景，可以通过设置会话变量 enable_insert_strict 为 true 来确保当有数据被过滤掉的时候，INSERT 不会成功执行。
* 因为StarRocks的insert复用导入数据的逻辑，所以每一次insert语句都会产生一个新的数据版本。频繁小批量导入操作会产生过多的数据版本，而过多的小版本会影响查询的性能。所以并不建议频繁的使用insert语法导入数据或作为生产环境的日常例行导入任务。如果有流式导入或者小批量导入任务的需求，可以使用Stream Load或者Routine Load的方式进行导入。
