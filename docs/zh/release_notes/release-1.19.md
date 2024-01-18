---
displayed_sidebar: "Chinese"
---

# StarRocks version 1.19

## 1.19.0

发布日期：2021年10月25日

### New Feature

* 实现Global Runtime Filter，可以支持对shuffle join实现Runtime filter。
* 默认开启CBO Planner，完善了colocate join / bucket shuffle / 统计信息等功能。[参考文档](../using_starrocks/Cost_based_optimizer.md)
* [实验功能] 发布主键模型（Primary Key）：为更好地支持实时/频繁更新功能，StarRocks新增了一种表的类型: 主键模型。该模型支持Stream Load、Broker Load、Routine Load，同时提供了基于Flink-cdc的MySQL数据的秒级同步工具。[参考文档](../table_design/table_types/primary_key_table.md)
* [实验功能] 新增外表写入功能。支持将数据通过外表方式写入另一个StarRocks集群的表中，以解决读写分离需求，提供更好的资源隔离。[参考文档](../data_source/External_table.md)

### Improvement

#### StarRocks

* 性能优化：
  * count distinct int语句
  * group by int 语句
  * or语句
* 优化磁盘Balance算法，单机增加磁盘后可以自动进行数据均衡。
* 支持部分列导出。 [参考文档](../unloading/Export.md)
* 优化show processlist，显示具体SQL。
* SET_VAR支持多个变量设置。
* 完善更多报错信息，包括table_sink、routine load、创建物化视图等。

#### StarRocks-Datax Connector

* StarRocks-DataX Writer 支持设置interval flush。

### Bug Fixes

* 修复动态分区表在数据恢复作业完成后，新分区无法自动创建的问题。 [# 337](https://github.com/StarRocks/starrocks/issues/337)
* 修复CBO开启后row_number函数报错的问题。
* 修复统计信息收集导致fe卡死的问题。
* 修复set_var针对session生效而不是针对语句生效的问题。
* 修复Hive分区外表`select count(*)` 返回异常的问题。

## 1.19.1

发布日期： 2021年11月2日

### Improvement

* 优化show frontends 的性能 [# 507](https://github.com/StarRocks/starrocks/pull/507) [# 984](https://github.com/StarRocks/starrocks/pull/984)
* 补充慢查询监控 [# 502](https://github.com/StarRocks/starrocks/pull/502) [# 891](https://github.com/StarRocks/starrocks/pull/891)
* 优化hive 外表元数据获取，并行获取元数据[# 425](https://github.com/StarRocks/starrocks/pull/425) [# 451](https://github.com/StarRocks/starrocks/pull/451)

### Bug Fixes

* 修复Thrift协议兼容性问题，解决hive外表对接Kerberos的问题 [# 184](https://github.com/StarRocks/starrocks/pull/184) [# 947](https://github.com/StarRocks/starrocks/pull/947) [# 995](https://github.com/StarRocks/starrocks/pull/995) [# 999](https://github.com/StarRocks/starrocks/pull/999)
* 修复view创建的若干bug [# 972](https://github.com/StarRocks/starrocks/pull/972) [# 987](https://github.com/StarRocks/starrocks/pull/987)[# 1001](https://github.com/StarRocks/starrocks/pull/1001)
* 修复FE无法灰度升级的问题 [# 485](https://github.com/StarRocks/starrocks/pull/485) [# 890](https://github.com/StarRocks/starrocks/pull/890)

## 1.19.2

发布日期： 2021年11月20日

### Improvement

* bucket shuffle join 支持right join和 full outer join [# 1209](https://github.com/StarRocks/starrocks/pull/1209)  [# 31234](https://github.com/StarRocks/starrocks/pull/1234)

### Bug Fixes

* 修复 repeat node 无法进行谓词下推的问题[# 1410](https://github.com/StarRocks/starrocks/pull/1410) [# 1417](https://github.com/StarRocks/starrocks/pull/1417)
* 修复routine load在集群切主场景下可能导入丢失数据的问题 [# 1074](https://github.com/StarRocks/starrocks/pull/1074) [# 1272](https://github.com/StarRocks/starrocks/pull/1272)
* 修复创建视图无法支持union的问题 [# 1083](https://github.com/StarRocks/starrocks/pull/1083)
* 修复一些Hive外表稳定性问题[# 1408](https://github.com/StarRocks/starrocks/pull/1408)
* 修复一个group by视图的问题[# 1231](https://github.com/StarRocks/starrocks/pull/1231)

## 1.19.3

发布日期： 2021年11月30日

### Improvement

* 升级jprotobuf版本提升安全性 [# 1506](https://github.com/StarRocks/starrocks/issues/1506)

### Bug Fixes

* 修复部分group by结果正确性问题
* 修复grouping sets部分问题[# 1395](https://github.com/StarRocks/starrocks/issues/1395) [# 1119](https://github.com/StarRocks/starrocks/pull/1119)
* 修复date_format的部分参数问题
* 修复一个聚合streamming的边界条件问题[# 1584](https://github.com/StarRocks/starrocks/pull/1584)
* 详细内容参考[链接](https://github.com/StarRocks/starrocks/compare/1.19.2...1.19.3)

## 1.19.4

发布日期： 2021年12月09日

### Improvement

* 支持 cast(varchar as bitmap) [# 1941](https://github.com/StarRocks/starrocks/pull/1941)
* 更新hive外表访问策略 [# 1394](https://github.com/StarRocks/starrocks/pull/1394) [# 1807](https://github.com/StarRocks/starrocks/pull/1807)

### Bug Fixes

* 修复带谓词Cross Join查询结果错误bug [# 1918](https://github.com/StarRocks/starrocks/pull/1918)
* 修复decimal类型，time类型转换bug [# 1709](https://github.com/StarRocks/starrocks/pull/1709) [# 1738](https://github.com/StarRocks/starrocks/pull/1738)
* 修复colocate join/replicate join选错bug [# 1727](https://github.com/StarRocks/starrocks/pull/1727)
* 修复若干plan cost计算问题

## 1.19.5

发布日期： 2021年12月20日

### Improvement

* 优化shuffle join的一个规划 [# 2184](https://github.com/StarRocks/starrocks/pull/2184)
* 优化多个大文件导入 [# 2067](https://github.com/StarRocks/starrocks/pull/2067)

### Bug Fixes

* 升级Log4j2 到2.17.0， 修复安全漏洞[# 2284](https://github.com/StarRocks/starrocks/pull/2284)[# 2290](https://github.com/StarRocks/starrocks/pull/2290)
* 修复Hive外表的空分区的问题 [# 707](https://github.com/StarRocks/starrocks/pull/707)[# 2082](https://github.com/StarRocks/starrocks/pull/2082)

## 1.19.7

发布日期： 2022年3月18日

### Bug Fixes

* 修复 dateformat 在不同版本输出结果不一致的问题。[#4165](https://github.com/StarRocks/starrocks/pull/4165)
* 修复在导入数据时因错误删除 Parquet 文件而导致 BE 节点崩溃的问题。[#3521](https://github.com/StarRocks/starrocks/pull/3521)
