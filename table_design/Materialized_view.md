# 物化视图

## 名词解释

1. Duplicate 数据模型：StarRocks中的用于存放明细数据的数据模型，建表可指定，数据不会被聚合。
2. Base 表：StarRocks 中通过 CREATE TABLE 命令创建出来的表。
3. Materialized Views 表：简称 MVs，物化视图。

## 使用场景

在实际的业务场景中，通常存在两种场景并存的分析需求：对固定维度的聚合分析 和 对原始明细数据任意维度的分析。

例如，在销售场景中，每条订单数据包含这几个维度信息（item\_id, sold\_time, customer\_id, price）。在这种场景下，有两种分析需求并存：

1. 业务方需要获取某个商品在某天的销售额是多少，那么仅需要在维度（item\_id, sold\_time）维度上对 price 进行聚合即可。
2. 分析某个人在某天对某个商品的购买明细数据。

在现有的 StarRocks 数据模型中，如果仅建立一个聚合模型的表，比如（item\_id, sold\_time, customer\_id, sum(price)）。由于聚合损失了数据的部分信息，无法满足用户对明细数据的分析需求。如果仅建立一个 Duplicate 模型，虽可以满足任意维度的分析需求，但由于不支持 Rollup， 分析性能不佳，无法快速完成分析。如果同时建立一个聚合模型和一个 Duplicate 模型，虽可以满足性能和任意维度分析，但两表之间本身无关联，需要业务方自行选择分析表。不灵活也不易用。

## 如何使用

使用聚合函数（如sum和count）的查询，在已经包含聚合数据的表中可以更高效地执行。这种改进的效率对于查询大量数据尤其适用。表中的数据被物化在存储节点中，并且在增量更新中能和 Base 表保持一致。用户创建 MVs 表后，查询优化器支持选择一个最高效的 MVs 映射，并直接对 MVs 表进行查询而不是 Base 表。由于 MVs 表数据通常比 Base 表数据小很多，因此命中 MVs 表的查询速度会快很多。

### **创建物化视图：**

~~~sql
CREATE MATERIALIZED VIEW materialized_view_name
AS SELECT id, SUM(clicks) AS sum_clicks
FROM  database_name.base_table
GROUP BY id ORDER BY id’
~~~

 物化视图的创建当前为异步操作。创建物化视图的语法会立即返回结果，但物化视图的生成操作可能仍在运行。用户可以使用DESC "base\_table\_name" ALL命令查看当前BASE表的物化视图。可以使用 SHOW ALTER TABLE MATERIALIZED VIEW FROM "database\_name"命令查看当前以及历史物化视图的处理状态。

* **限制：**

  * base表中的分区列，必须存在于创建物化视图的group by聚合列中
  * 目前只支持对单表进行构建物化视图，不支持多表JOIN
  * 聚合类型表（Aggregation)，不支持对key列执行聚合算子操作，仅支持对value列进行聚合，且聚合算子类型不能改变。
  * 物化视图中至少包含一个KEY列
  * 不支持表达式计算
  * 不支持指定物化视图查询

### **删除物化视图**

`DROP  MATERIALIZED VIEW [IF  EXISTS] [db_name].<mv_name>`

### **查看物化视图：**

* 查看该database下的所有物化视图

`SHOW MATERIALIZED  VIEW [IN|FROM db_name]`

* 查看指定物化视图的表结构

`DESC table_name all`
( `DESC/DESCRIBUE mv_name` 不再支持）

* 查看物化视图处理进度

`SHOW ALTER MATERIALIZED VIEW FROM db_name`  

* 取消正在创建的物化视图

`CANCEL ALTER MATERIALIZED VIEW FROM db_name.table_name`

* 如何确定查询命中了哪个物化视图

当用户查询时，并不感知物化视图的存在，不必显式的指定物化视图的名称。查询优化器可以根据查询条件自动判断是否可以路由到相应的物化视图上。查询计划是否被重写，可在explain sql 中查看。可以在Mysql 客户端执行：

~~~SQL
Explain SQL:

0:OlapScanNode
TABLE: lineorder4
PREAGGREGATION: ON
partitions=1/1
RollUp表: lineorder4
tabletRatio=1/1
tabletList=15184
cardinality=119994608
avgRowSize=26.375498
numNodes=1
tuple ids: 0
~~~

其中的RollUp表字段表示到底命中了哪个物化视图。其中的PREAGGREGATION 字段如果是On，就表明查询时不需要在StarRocks存储引擎中现场聚合，查询会更快，如果PREAGGREGATION 字段是Off，后面会显示原因， 比如 PREAGGREGATION: OFF. Reason:  The aggregate operator does not match，表示因为查询的聚合函数和物化视图中定义的聚合函数不一致，所以在StarRocks存储引擎中无法使用物化视图，需要现场聚合。

### **导入数据**

对 Base 表的增量导入都会作用到所有关联的 MVs 表中。在 Base 表及所有的 MVs 表均完成后，导入才算完成，数据才能被看到。StarRocks保证Base 表和 MVs 表之间的数据是一致的。查询 Base 表和查询 MVs 表不会存在数据差异。

## 注意事项

### **物化视图函数支持**

具体化视图必须是对单个表的聚合。目前仅支持以下[聚合函数](https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_functions)：

* COUNT
* MAX
* MIN
* SUM
* PERCENTILE\_APPROX
* HLL\_UNION

* 对明细数据进行 HLL 聚合并且在查询时，使用 HLL 函数分析数据。主要适用于快速进行非精确去重计算。对明细数据使用HLL\_UNION聚合，需要先调用hll\_hash函数，对原数据进行转换

~~~SQL
create materialized view dt_uv as 
    select dt, page_id, HLL_UNION(hll_hash(user_id)) 
    from user_view
    group by dt, page_id;
select ndv(user_id) from user_view; 查询可命中该物化视图
~~~

* 目前不支持对 DECIMAL/BITMAP/HLL/PERCENTILE 类型的列使用HLL\_UNION聚合算子

* BITMAP\_UNION

* 对明细数据进行BITMAP聚合并且在查询时，使用BITMAP函数分析数据，主要适用于快速计算count(distinct)的精确去重。对明细数据使用BITMAP\_UNION聚合，需要先调用to\_bitmap函数，对原数据进行转换。

~~~SQL
create materialized view dt_uv  as
    select dt, page_id, bitmap_union(to_bitmap(user_id))
    from user_view
    group by dt, page_id;
select count(distinct user_id) from user_view; 查询可命中该物化视图
~~~

* 目前仅支持TINYINT/SMALLINT/INT/BITINT类型，且存储内容需为正整数（包括0）。

### **物化视图的智能路由**

StarRocks中，查询时不需要显式指定MV表名称，StarRocks会根据查询SQL智能路由到最佳的MV表。在查询时，MV表的选择规则如下：

1. 选择包含所有查询列的MV表
2. 按照过滤和排序的Column筛选最符合的MV表
3. 按照Join的Column筛选最符合的MV表
4. 行数最小的MV表
5. 列数最小的MV表

### **其他限制**

1. RollUp表的模型必须和Base表保持一致（聚合表的RollUp表是聚合模型，明细表的RollUp表是明细模型）。
2. Delete 操作时，如果 Where 条件中的某个 Key 列在某个 RollUp表中不存在，则不允许进行 Delete。这种情况下，可以先删除物化视图，再进行Delete操作，最后再重新增加物化视图。
3. 如果 物化视图中包含 REPLACE 聚合类型的列，则该物化视图必须包含所有 Key 列。
