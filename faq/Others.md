# 其他常见问题

## 建表时，varchar(32)和string占用的存储空间是否相同？查询时的性能是否相同？

都是变长存储的，查询性能一样。

## Oracle导出的txt文件，修改文件的字符集utf-8后还是乱码，如何处理？

可以尝试将文件字符集视为gbk进行字符集转换。
以文件“origin.txt”为例，假设用命令查看其字符集得到其当前字符集为iso-8859-1：

```plain text
file --mime-encoding origin.txt
返回值[假设]：iso-8859-1
```

使用iconv命令转换，将文件字符集转换为utf-8：

```shell
iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
```

若此时发现转换后得到的origin_utf-8.txt文件中还存在乱码，我们就可以将origin.txt的原字符集视为gbk，重新转换：

```shell
iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
```

## MySQL中定义字符串长度跟StarRocks定义的是否一致？

目前StarRocks中varchar(n)，n限制的是字节数，MySQL限制的是字符数，所以对应MySQL那边的表，n可以给3倍或者4倍，一般也不会占更多存储。

## 表的分区字段是否可以使用float、double、decimal浮点数类型来分区？

不可以，只能是date、datetime或int整型。

## 如何查看表中的数据占了多大的存储？

`show data`可以看到，可展示数据量、副本数量以及统计行数。注意数据统计，有一定的时间延迟。

## 如果数据超过了这个quota的量会怎么样？这个值可以做更改吗？

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

改动db的quota，调整这个db的容量上限。

## StarRocks有没有upsert这样的语法，就是更新表中某几个字段，没指定更新的字段，值不变化？

目前没有upsert语法，无法更新表中单独的几个字段，暂时只能通过「更新表模型」或者「delete+insert」来实现全字段的更新。

## [数据恢复]中原子替换表/分区功能使用方法

类似CK的分区卸载、装载，跨表分区移动等功能。
下面以原子替换表 table1 的数据，或 table1 的分区数据，为例。可能比insert overwrite更安全些，可以先检查下数据。

### 原子替换「表」

1. 创建一张新表table2;

    ```SQL
    create table2 like table1;
    ```

2. 使用stream load / broker load /insert into 等方式导入数据到新表 table2 中；

3. 原子替换 table1 与 table2：

    ```SQL
    ALTER TABLE table1 SWAP WITH table2;
    ```

这样就可以进行表的原子替换。

### 原子替换「分区」

同样可以用「导入临时分区」的方式进行替换。

1. 创建临时分区:

    ```SQL
    ALTER TABLE table1
    ADD TEMPORARY PARTITION tp1
    VALUES LESS THAN("2020-02-01");
    ```

2. 向临时分区导入数据;

3. 原子替换「分区」:

    ```SQL
    ALTER TABLE table1
    REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

这样可以导入数据后做一定的验证以后再替换,可以进行临时分区的原子替换。

## fe重启报错:error to open replicated environment，will exit

**问题描述：**

重启集群fe后报该错且fe无法启动。

**解决方案：**

是bdbje的一个bug，社区版和1.17版本（不含此版本）以前重启会小概率触发该bug，可以升级到1.17及更高版本，已修复该问题。

## 创建hive表，查询报错:Broker list path exception

**问题描述：**

```plain text
msg:Broker list path exception
path=hdfs://172.31.3.136:9000/user/hive/warehouse/zltest.db/student_info/*, broker=TNetworkAddress(hostname:172.31.4.233, port:8000)
```

**解决方案：**

namenode的地址和端口跟运维人员确认是否正确，权限有没有开启

## 创建hive表，查询报错:get hive partition meta data failed

**问题描述：**

```plain text
msg:get hive partition meta data failed: java.net.UnknownHostException: emr-header-1.cluster-242
```

**解决方案：**

需要把集群里的host文件传一份到每个BE机器上，并确认网络是通的。

## hive外表orc访问失败：do_open failed. reason = Invalid ORC postscript length

**问题描述：**

查询同一sql前几次查询还行，后面报错了，重新建表后没报错了。

```plain text
MySQL [bdp_dim]> select * from dim_page_func_s limit 1;
ERROR 1064 (HY000): HdfsOrcScanner::do_open failed. reason = Invalid ORC postscript length
```

**解决方案：**

目前的版本fe和hive的信息同步是有时差的2h，期间表数据有更新或者插入，会导致scan的数据和fe的判断不一致，导致出现这个错。新版本增加手动reflush功能，可以刷新表结构信息同步。

## mysql外表连接失败：caching_sha2_password cannot be loaded

**问题描述：**

MySQL8.0版本默认的认证方式是caching_sha2_password
MySQL5.7版本默认则为mysql_native_password
认证方式不同，外表链接出错。

**解决方案：**

两种方案：

* 连接终端

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
```

* 修改my.cnf文件

```plain text
vim my.cnf
[mysqld]
default_authentication_plugin=mysql_native_password
```

## drop table 后磁盘空间没有立即释放

执行drop table时磁盘空间会过一会释放,如果想要快速释放磁盘空间可以使用drop table force,使用force会有短暂的等待时间,如果执行 drop table force,则系统不会检查该表是否存在未完成的事务,表将直接被删除并且不能被恢复,一般不建议执行此操作。

## 怎么查看StarRocks版本

可以通过`select current_version();`或者CLI执行`sh bin/show_fe_version.sh`查看版本

## fe内存大小如何设置

可以参考tablet数量,元数据信息都保存在fe的内存,一千万个tablet内存使用在20G左右,目前支持的meta上限约为该级别。

## StarRocks查询时间是如何计算的

StarRocks是多线程计算,查询时间是查询线程所用的时间,ScanTime是所有线程使用的时间加起来的时间,查询时间可以通过执行计划下的Query下的Total查看。

## export目前是否支持导出数据到本地时设置路径

不支持

## StarRocks的并发是什么量级

StarRocks的并发量级建议根据业务场景,或模拟业务场景实际测试一下。在客户的一些场景下,压到过2、3万的QPS。

## StarRocks的ssb测试为什么第一次执行速度较慢,后面较快

第一次查询读盘跟磁盘性能相关,第一次后操作系统的pagecache生效,再次查询会先扫描pagecache,速度提升

## 集群BE最小配置数量是多少,是否支持单节点部署

BE节点最小配置个数是1个,支持单节点部署,推荐集群部署性能更好,be节点需要支持avx2,推荐配置8核16G及以上机器配置

## superset+StarRocks如何配置数据权限

可以通过创建单独用户后,创建View授权给用户进行数据权限控制

## set is_report_success = true;后profile不显示

只有leader所在fe可以查看，因为report信息只汇报给leader节点。同时，如果通过 StarRocks Manager 查看 profile， 必须确保 FE 配置项 `enable_collect_query_detail_info` 为 `true`。

## 给字段加了注释，表里面怎么看呀，没有显示注释一栏，starrocks支持么？

可以通过 `show create table xxx` 查看。

## 建表的时候列不能指定now()这种函数默认值？

目前暂时不支持函数默认值，需要写成常量。
