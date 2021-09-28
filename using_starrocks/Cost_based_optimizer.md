
# CBO 优化器

## 背景介绍

在 1.16.0 版本，StarRocks推出的新优化器，可以针对复杂 Ad-hoc 场景生成更优的执行计划。StarRocks采用cascades技术框架，实现基于成本（Cost-based Optimizer 后面简称CBO）的查询规划框架，新增了更多的统计信息来完善成本估算，也补充了各种全新的查询转换（Transformation）和实现（Implementation）规则，能够在数万级别查询计划空间中快速找到最优计划。

## 使用说明

### 启用统计信息自动抽样收集

开启新优化器前，需要先开启统计信息收集。启用统计信息自动抽样收集方式：

修改 FE Config ：

~~~Apache
enable_statistic_collect = true
~~~

然后重启FE。

<br/>

### 查询启用新优化器

> 注意：启用新优化器之前，建议先开启统计信息自动抽样收集 1 ~ 2天。

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

<br/>

### 新优化器结果验证

StarRocks提供一个新旧优化器**对比**的工具，用于回放fe中的audit.log，可以检查新优化器查询结果是否有误，在使用新优化器前，**建议使用StarRocks提供的对比工具检查一段时间**：

1. 确认已经修改了FE的统计信息收集配置。
2. 下载测试工具，Oracle JDK版本 [new\_planner\_test.zip](http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/new_planner_test.zip)， Open JDK版本 [open\_jdk\_new\_planner\_test.zip](http://starrocks-public.oss-cn-zhangjiakou.aliyuncs.com/open_jdk_new_planner_test.zip) ，然后解压。
3. 按照README配置StarRocks的端口地址，FE的http_port，以及用户名密码
4. 使用命令`java -jar new_planner_test.jar $fe.audit.log.path`执行测试，测试脚本会执行fe.audit.log 中的查询请求，并进行比对，分析查询结果并记录日志。
5. 执行的结果会记录在result文件夹中，如果在result中包含慢查询，可以将result文件夹打包提交给StarRocks，协助我们修复问题。
