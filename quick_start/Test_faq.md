
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
* 在机器比较少的情况下，如果想充分利用机器资源可以考虑使用 ` BE数量 * cpu core / 2 `来设置bucket数量。例如有100GB数据的一张表，有4台BE，每台64C，只有一个分区，那么可以采用 bucket数量 `4 * 64 /2  = 144`，这样每个tablet的数据也在694MB，同时也能充分利用CPU资源。

### 排序键设计

* 排序键要根据查询的特点来设计。
* 将**经常作为过滤条件和group by的列作为排序键**可以加速查询。
* 如果是有**大量点查**，建议把查询点查的ID放到第一列。例如 查询主要类型是 `select sum(revenue) from lineorder where user_id='aaa100'`;  并且有很高的并发，强烈推荐把user\_id 作为排序键的第一列。
* 如果查询的主要是**聚合和scan比较多**，建议把低基数的列放在前面。例如 查询的主要类型是 `select region, nation, count(*)  from lineorder_flat group by region, nation`，把region作为第一列、nation作为第二列会更合适。低基数的列放在前面可以有助于数据局部性。

### 合理选择数据类型

* 用尽量精确的类型。比如能够使用整形就不要用字符串类型，能够使用int就不要使用bigint，精确的数据类型能够更好的发挥数据库的性能。

## 查询

### 如何合理地设置并行度

* 通过session变量   parallel\_fragment\_exec\_instance\_num  可以设置查询的并行度（单个 BE 内部的并行度），如果我们觉得查询性能不够好但是cpu资源充足可以通过 set parallel\_fragment\_exec\_instance\_num = 16; 来调整这里并行度可以 **设置成CPU核数量的一半** 。
* 通过set **global** parallel\_fragment\_exec\_instance\_num = 16; 可以让session变量全局有效。
* parallel\_fragment\_exec\_instance\_num 受到每个BE的tablet数量的影响，比如 一张表的bucket数量是 32, 有3个分区，分布在4个BE上，那么每个BE的tablet数量是 32 * 3  / 4 = 24, 那么单机的并行度无法超过24，即使你set parallel\_fragment\_exec\_instance\_num = 32 ，但是实际执行的时候并行度还是会变成24。
* 对于需要进行高QPS查询的场景，因为机器整体资源是充分利用的，所以建议设置parallel\_fragment\_exec\_instance\_num为1，这样可以减少不同查询之间的资源竞争，反而整体可以提升QPS。

### 如何查看Profile分析查询瓶颈

* 通过explain sql命令可以查看查询计划。
* 通过 set is\_report\_success = true 可以打开profile的上报。
* 标准版用户在 http:FE\_IP:FE\_HTTP\_PORT/queries 可以看到当前的查询和Profile信息
* 企业版用户在StarRocksManager的查询页面可以看到图形化的Profille展示，点击查询链接可以在“执行时间“页面看到树状展示，可以在“执行详情“页面看到完整的Profile详细信息。如果达不到预期可以发送执行详情页面的文本到社区或者技术支持的群里寻求帮助
* Plan和Profile参考[查询分析](../administration/Query_planning.md) 和[性能优化](../administration/Profiling.md)章节
