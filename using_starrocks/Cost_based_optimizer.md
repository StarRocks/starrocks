
# CBO 优化器

## 背景介绍

在 1.16.0 版本，StarRocks推出的新优化器，可以针对复杂 Ad-hoc 场景生成更优的执行计划。StarRocks采用cascades技术框架，实现基于成本（Cost-based Optimizer 后面简称CBO）的查询规划框架，新增了更多的统计信息来完善成本估算，也补充了各种全新的查询转换（Transformation）和实现（Implementation）规则，能够在数万级别查询计划空间中快速找到最优计划。

## 使用说明

### 查询启用新优化器

全局粒度开启：

~~~SQL
set global enable_cbo = true;
~~~

Session 粒度开启：

~~~SQL
set enable_cbo = true;

~~~

单个 SQL 粒度开启：

~~~SQL
SELECT /*+ SET_VAR(enable_cbo = true) */ * from table;
~~~

> 在1.19版本已经默认打开了CBO。

### 统计信息采集

StarRocks会定时采集统计信息，包括但不限于：行数，平均大小、基数信息、NULL值数据量、MAX/MIN值等等，数据会存储在_statistics_.table_statistic_v1中，当前支持抽样和全量两种收集方式：

* 抽样收集：

    会均匀的从每一个partition中抽取N行数据进行统计信息计算，抽样行数可以通过参数指定。优点在于收集任务消耗的资源小，速度快，缺点在于收集的统计信息不准确，对优化器的帮助有限，默认一般为抽样收集，抽样的行数默认为200000行，采集周期为1天，数据未更新不会重新收集。

* 全量收集

    使用整个表的所有数据计算统计信息。优点在于收集到的统计信息准确，优化器可以更好的评估执行计划，但是缺点也比较明显，收集任务消耗资源非常大，速度慢。全量收集可以使用手动或者定时的方式进行主动触发:

  * 手动Analyze收集: 通过手动触发Analyze命令收集统计信息

  * 定期Analyze调度: 通过Analyze Job定期收集指定的库/表/列的统计信息，默认的Analyze调度一次的周期为2小时

### ANALYZE 相关命令

#### Show Analyze

~~~SQL
-- 展示所有的Analyze Job信息
SHOW ANALYZE;
~~~

#### Analyze

抽样采集

~~~SQL
ANALYZE TABLE tbl_name(columnA, columnB, columnC...)
PROPERTIES(
    "sample_collect_rows" = "10"
);
~~~

全量收集

~~~SQL
ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...);

~~~

#### Analyze Job

可以通过Analyze Job创建一个指定数据库/表/列的统计任务，每个任务有自己的执行周期以及配置，会常驻执行。
当有多个Job中指定了收集同一个列时，会按照最新(job id最大)的Job中指定的配置执行

抽样收集

~~~SQL
-- 定期抽样采集所有数据库的统计信息
CREATE ANALYZE ALL PROPERTIES(...);

-- 定期抽样采集指定数据库下所有表的统计信息
CREATE ANALYZE DATABASE db_name PROPERTIES(...);

-- 定期抽样采集指定表、列的统计信息
CREATE ANALYZE TABLE tbl_name(columnA, columnB, columnC...) PROPERTIES(...);
~~~

全量收集

~~~SQL
-- 定期全量采集所有数据库的统计信息
CREATE ANALYZE FULL ALL PROPERTIES(...);

-- 定期全量采集指定数据库下所有表的统计信息
CREATE ANALYZE FULL DATABASE db_name PROPERTIES(...);

-- 定期全量采集指定表、列的统计信息
CREATE ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...) PROPERTIES(...);
~~~

删除Job

~~~SQL
-- 删除Analyze job，id可以通过SHOW ANALYZE获取
DROP ANALYZE <id>;
~~~

示例&说明

~~~SQL
-- 每隔100秒定期抽样采集所有数据库的统计信息
CREATE ANALYZE ALL PROPERTIES("update_interval_sec" = "100");

-- 定期全量采集tpch数据库下所有表的统计信息
CREATE ANALYZE FULL DATABASE tpch;

-- 定期抽样采集test表中v1列的统计信息
CREATE ANALYZE TABLE test(v1)
~~~

参数说明：

* update_interval_sec：统计任务收集的间隔时间，单位为秒
* sample_collect_rows：抽样的行数

#### FE 相关配置

fe.conf中的相关配置项

~~~conf
# 统计信息收集功能开关
enable_statistic_collect = true;

# 统计信息功能执行周期，默认为2小时
statistic_collect_interval_sec = 7200;

# 统计信息Job的默认收集间隔时间，默认为1天
statistic_update_interval_sec = 86400;

# 采样统计信息Job的默认采样行数，默认为200000行
statistic_sample_collect_rows = 200000;
~~~

### 新优化器结果验证

StarRocks提供一个新旧优化器**对比**的工具，用于回放fe中的audit.log，可以检查新优化器查询结果是否有误，在使用新优化器前，**建议使用StarRocks提供的对比工具检查一段时间**：

1. 确认已经修改了FE的统计信息收集配置。
2. 下载测试工具，Oracle JDK版本 [new\_planner\_test.zip](http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/new_planner_test.zip)，Open JDK版本 [open\_jdk\_new\_planner\_test.zip](http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/open_jdk_new_planner_test.zip) ，然后解压。
3. 按照README配置StarRocks的端口地址，FE的http_port，以及用户名密码
4. 使用命令`java -jar new_planner_test.jar $fe.audit.log.path`执行测试，测试脚本会执行fe.audit.log 中的查询请求，并进行比对，分析查询结果并记录日志。
5. 执行的结果会记录在result文件夹中，如果在result中包含慢查询，可以将result文件夹打包提交给StarRocks，协助我们修复问题。
