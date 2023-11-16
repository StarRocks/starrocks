# Broker Load常见问题

## Broker Load，能不能再次执行已经执行成功（State: FINISHED）的任务？

<<<<<<< HEAD
已经执行成功的任务，本身不能再次执行。需要再创建一个，因为为了保证导入任务的不丢不重，每个导入成功的label不可复用。可以show load查看历史的导入记录，然后复制下来修改label后重新导入。
=======
Broker Load 不支持再次执行已经执行成功、处于 FINISHED 状态的导入作业。而且，为了保证数据不丢不重，每个执行成功的导入作业的标签 (Label) 均不可复用。可以使用 [SHOW LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句查看历史的导入记录，找到想要再次执行的导入作业，复制作业信息，并修改作业标签后，重新创建一个导入作业并执行。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

## Broker Load 导入时内容乱码

**问题描述：**

<<<<<<< HEAD
导入报错，查看相应错误url显示内容乱码。
=======
## 3. 通过 Broker Load 导入 ORC 格式的数据时，发生 `ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>` 错误应该如何处理？

待导入数据文件和 Starrocks 表两侧的列名不一致，执行 `SET` 子句的时候系统内部会有一个类型推断，但是在调用 cast 函数执行数据类型转换的时候失败了。解决办法是确保两侧的列名一致，这样就不需要 `SET` 子句，也就不会调用 cast 函数执行数据类型转换，导入就可以成功了。

## 4. 为什么 Broker Load 导入作业没报错，但是却查询不到数据？

Broker Load 是一种异步的导入方式，创建导入作业的语句没报错，不代表导入作业成功了。可以通过 [SHOW LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句来查看导入作业的结果状态和 `errmsg` 信息，然后修改导入作业的参数配置后，再重试导入作业。

## 5. 导入报 "failed to send batch"或"TabletWriter add batch with unknown id" 错误应该如何处理？

该错误由数据写入超时而引起。需要修改[系统变量](../../reference/System_variable.md) `query_timeout` 和 [BE 配置项](../../administration/Configuration.md#配置-be-静态参数) `streaming_load_rpc_max_alive_time_sec` 的配置。

## 6. 导入报 "LOAD-RUN-FAIL; msg:OrcScannerAdapter::init_include_columns. col name = xxx not found" 错误应该如何处理？

如果导入的是 Parquet 或 ORC 格式的数据，检查文件头的列名是否与 StarRocks 表中的列名一致，例如:
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

```SQL
Reason: column count mismatch, expect=6 real=1. src line: [$交通];
zcI~跟团+v];   count mismatch, expect=6 real=2. src line: [租e�rD��食休闲娱乐
```

<<<<<<< HEAD
**解决方案：**
=======
上述示例，表示将 Parquet 或 ORC 文件中以 `tmp_c1` 和 `tmp_c2` 为列名的列，分别映射到 StarRocks 表中的 `name` 和 `id` 列。如果没有使用 `SET` 子句，则以 `column_list` 参数中指定的列作为映射。具体请参见 [BROKER LOAD](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。
>>>>>>> a83aa885d ([Doc] fix links in 2.2 (#35221))

format as指定错误，改成文件对应类型重试导入任务。

## Broker Load 导入时hdfs数据导入日期字段异常+8h

**问题描述：**

建表设置timezone为中国时区，brokerload导入的时候设置timezone为中国时区，服务器是utc时区，字段导入时时间字段+8h了。

**解决方案：**

建表时去掉timezone就可以了。

## 【Broker Load】 orc数据导入失败`ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>`

导入源文件和starrocks两边列名称不一致，set的时候系统内部会有一个类型推断，然后cast的时候失败了，设置成两边字段名一样，不需要set，就不会cast，导入就可以成功了

已经执行成功的任务，本身不能再次执行。需要再创建一个，因为为了保证导入任务的不丢不重，每个导入成功的label不可复用。可以show load查看历史的导入记录，然后复制下来修改label后重新导入。

## Broker Load 导入数据没报错，但是查询不到数据

Broker Load导入是异步的，创建load语句没报错，不代表导入成功了。show load查看相应的状态和errmsg，修改相应内容后重试任务。

导入源文件和starrocks两边列名称不一致，set的时候系统内部会有一个类型推断，然后cast的时候失败了，设置成两边字段名一样，不需要set，就不会cast，导入就可以成功了
