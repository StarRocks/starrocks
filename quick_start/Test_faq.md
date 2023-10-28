
# 测试常见问题

## 部署

### 如何选择硬件和优化配置

#### 硬件选择

* BE推荐16核64GB以上，FE推荐8核16GB以上。
* 磁盘可以使用HDD或者SSD。
* CPU必须支持AVX2指令集，`cat /proc/cpuinfo |grep avx2` 确认有输出即可，如果没有支持，建议更换机器，StarRocks的向量化技术需要CPU指令集支持才能发挥更好的效果。
* 网络需要万兆网卡和万兆交换机。

#### 参数配置

* 参数配置参考 [配置参数](../administration/Configuration.md)

## 建模

### 如何合理地分区分桶

#### 如何分区

* 通过合理的分区可以有效的裁剪scan的数据量。我们一般从数据的管理角度来选择分区键，选用**时间**或者区域作为分区键。
* 使用动态分区可以定期自动创建分区，比如每天创建出新的分区。

#### 如何分桶

* 选择**高基数的列**来作为分桶键（如果有唯一ID就用这个列来作为 分桶键 即可），这样保证数据在各个bucket中尽可能均衡，如果碰到数据倾斜严重的，数据可以使用多列作为分桶键（但一般不要太多）。
* 分桶的数量影响查询的并行度，最佳实践是计算一下数据存储量，将每个tablet设置成 100MB ~ 1GB 之间。
* 在机器比较少的情况下，如果想充分利用机器资源可以考虑使用 ` BE数量 * cpu core / 2 `来设置bucket数量。例如有100GB的CSV文件(未压缩)，导入StarRocks，有4台BE，每台64C，只有一个分区，那么可以采用 bucket数量 `4 * 64 /2  = 128`，这样每个tablet的数据也在781MB，同时也能充分利用CPU资源。

### 排序键设计

* 排序键要根据查询的特点来设计。
* 将**经常作为过滤条件和group by的列作为排序键**可以加速查询。
* 如果是有**大量点查**，建议把查询点查的ID放到第一列。例如 查询主要类型是 `select sum(revenue) from lineorder where user_id='aaa100'`;  并且有很高的并发，强烈推荐把user\_id 作为排序键的第一列。
* 如果查询的主要是**聚合和scan比较多**，建议把低基数的列放在前面。例如 查询的主要类型是 `select region, nation, count(*)  from lineorder_flat group by region, nation`，把region作为第一列、nation作为第二列会更合适。低基数的列放在前面可以有助于数据局部性。

### 合理选择数据类型

* 用尽量精确的类型。比如能够使用整形就不要用字符串类型，能够使用int就不要使用bigint，精确的数据类型能够更好的发挥数据库的性能。

## 查询

### 如何合理地设置并行度

您可以通过设置 Pipeline 执行引擎变量（推荐），或者设置一个 Fragment 实例的并行数量，来设置查询并行度，从而提高CPU资源利用率和查询效率。设置方式，请参见[查询并行度相关参数](../administration/Query_management.md#查询相关的session变量)。

### 如何查看Profile分析查询瓶颈

* 通过explain sql命令可以查看查询计划。
* 通过 set is\_report\_success = true 可以打开profile的上报。
* 社区版用户在 http:FE\_IP:FE\_HTTP\_PORT/query 可以看到当前的查询和Profile信息
* 企业版用户在StarRocksManager的查询页面可以看到图形化的Profille展示，点击查询链接可以在“执行时间“页面看到树状展示，可以在“执行详情“页面看到完整的Profile详细信息。如果达不到预期可以发送执行详情页面的文本到社区或者技术支持的群里寻求帮助
* Plan和Profile参考[查询分析](../administration/Query_planning.md) 和[性能优化](../administration/Profiling.md)章节
