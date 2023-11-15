# Broker Load常见问题

## Broker Load，能不能再次执行已经执行成功（State: FINISHED）的任务？

已经执行成功的任务，本身不能再次执行。需要再创建一个，因为为了保证导入任务的不丢不重，每个导入成功的label不可复用。可以show load查看历史的导入记录，然后复制下来修改label后重新导入。

## Broker Load 导入时内容乱码

**问题描述：**

导入报错，查看相应错误url显示内容乱码。

```SQL
Reason: column count mismatch, expect=6 real=1. src line: [$交通];
zcI~跟团+v];   count mismatch, expect=6 real=2. src line: [租e�rD��食休闲娱乐
```

**解决方案：**

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
