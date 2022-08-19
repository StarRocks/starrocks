# 背景介绍

用户在使用StarRocks进行精确去重分析时，通常会有两种方式：

* 基于明细去重：即传统的count distinct 方式，好处是可以保留明细数据。提高了分析的灵活性。缺点则是需要消耗极大的计算和存储资源，对大规模数据集和查询延迟敏感的去重场景支持不够友好。
* 基于预计算去重：这种方式也是StarRocks推荐的方式。在某些场景中，用户可能不关心明细数据，仅仅希望知道去重后的结果。这种场景可采用预计算的方式进行去重分析，本质上是利用空间换时间，也是MOLAP聚合模型的核心思路。就是将计算提前到数据导入的过程中，减少存储成本和查询时的现场计算成本。并且可以使用RollUp表降维的方式，进一步减少现场计算的数据集大小。

## 传统Count distinct计算

StarRocks 是基于MPP 架构实现的，在使用count distinct做精准去重时，可以保留明细数据，灵活性较高。但是，由于在查询执行的过程中需要进行多次数据shuffle（不同节点间传输数据，计算去重），会导致性能随着数据量增大而直线下降。

如以下场景。存在表（dt, page, user_id)，需要通过明细数据计算UV。

|  dt   |   page  | user_id |
| :---: | :---: | :---:|
|   20191206  |   waimai  | 101 |
|   20191206  |   waimai  | 102 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   waimai  | 101 |

按page进行分组统计uv

|  page   |   uv  |
| :---: | :---: |
|   waimai  |   2  |
|   xiaoxiang  |  1   |

```sql
select page, count(distinct user_id) as uv from table group by page;
```

对于上图计算 UV 的 SQL，StarRocks 在计算时，会按照下图进行计算，先根据 page 列和 user_id 列 group by，最后再 count。

![alter](../assets/6.1.2-2.png)

> 注：图中是 6 行数据在 2 个 BE 节点上计算的示意图

显然，上面的计算方式，由于数据需要进行多次shuffle，当数据量越来越大时，所需的计算资源就会越来越多，查询也会越来越慢。使用Bitmap技术，就是为了解决传统count distinct在大量数据场景下的性能问题。

## 使用bitmap去重

假如给定一个数组A， 其取值范围为[0, n)(注: 不包括n)， 对该数组去重， 可采用(n+7)/8的字节长度的bitmap， 初始化为全0；逐个处理数组A的元素， 以A中元素取值作为bitmap的下标， 将该下标的bit置1； 最后统计bitmap中1的个数即为数组A的count distinct结果。

## 使用bitmap去重的优势

1. 空间优势:  用bitmap的一个bit位表示对应下标是否存在， 具有极大的空间优势;  比如对int32去重， 使用普通bitmap所需的存储空间只占传统去重的1/32。  StarRocks中的Bitmap采用Roaring Bitmap的优化实现， 对于稀疏的bitmap， 存储空间会进一步显著降低。
2. 时间优势:  bitmap的去重涉及的计算包括对给定下标的bit置位， 统计bitmap的置位个数， 分别为O(1)操作和O(n)操作， 并且后者可使用clz， ctz等指令高效计算。 此外， bitmap去重在MPP执行引擎中还可以并行加速处理， 每个计算节点各自计算本地子bitmap，  使用bitor操作将这些子bitmap合并成最终的bitmap， bitor操作比基于sort和基于hash的去重效率要高， 无条件依赖和数据依赖， 可向量化执行。

Roaring Bitmap实现，细节可以参考：[具体论文和实现](https://github.com/RoaringBitmap/RoaringBitmap)

## 如何使用Bitmap

1. 首先， 用户需要注意bitmap index和bitmap去重二者都是用bitmap技术， 但引入动机和解决的问题完全不同， 前者用于低基数的枚举型列的等值条件过滤， 后者用于计算一组数据行的指标列的不重复元素的个数。
2. 目前Bitmap列只能存在于用聚合表， 明细表和更新不支持BITMAP列。
3. 创建表时指定指标列的数据类型为BITMAP，  聚合函数为BITMAP_UNION。
4. 当在Bitmap类型列上使用count distinct时，StarRocks会自动转化为BITMAP_UNION_COUNT计算。

具体操作函数参见 [Bitmap函数](../sql-reference/sql-functions/bitmap-functions/bitmap_and.md)。

### 示例

以统计某一个页面的UV为例：

首先，创建一张含有BITMAP列的表，其中visit_users列为聚合列，列类型为BITMAP，聚合函数为BITMAP_UNION

```sql
CREATE TABLE `page_uv` (
  `page_id` INT NOT NULL COMMENT '页面id',
  `visit_date` datetime NOT NULL COMMENT '访问时间',
  `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT '访问用户id'
) ENGINE=OLAP
AGGREGATE KEY(`page_id`, `visit_date`)
DISTRIBUTED BY HASH(`page_id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "storage_format" = "DEFAULT"
);
```

向表中导入数据，采用insert into语句导入

```sql
insert into page_uv values
(1, '2020-06-23 01:30:30', to_bitmap(13)),
(1, '2020-06-23 01:30:30', to_bitmap(23)),
(1, '2020-06-23 01:30:30', to_bitmap(33)),
(1, '2020-06-23 02:30:30', to_bitmap(13)),
(2, '2020-06-23 01:30:30', to_bitmap(23));
```

在以上数据导入后，在 page_id = 1， visit_date = '2020-06-23 01:30:30'的数据行，visit_user字段包含着3个bitmap元素（13，23，33）；在page_id = 1， visit_date = '2020-06-23 02:30:30'的数据行，visit_user字段包含着1个bitmap元素（13）；在page_id = 2， visit_date = '2020-06-23 01:30:30'的数据行，visit_user字段包含着1个bitmap元素（23）。

采用本地文件导入

```bash
cat <<<'DONE' | \
    curl --location-trusted -u root: -H "label:label_1600960288796" \
        -H "column_separator:," \
        -H "columns:page_id,visit_date,visit_users, visit_users=to_bitmap(visit_users)" -T - \
        http://StarRocks_be0:8040/api/db0/page_uv/_stream_load
1,2020-06-23 01:30:30,130
1,2020-06-23 01:30:30,230
1,2020-06-23 01:30:30,120
1,2020-06-23 02:30:30,133
2,2020-06-23 01:30:30,234
DONE
```

统计每个页面的UV

```sql
select page_id, count(distinct visit_users) from page_uv group by page_id;
```

查询结果

```shell
mysql> select page_id, count(distinct visit_users) from page_uv group by page_id;

+-----------+------------------------------+
|  page_id  | count(DISTINCT `visit_user`) |
+-----------+------------------------------+
|         1 |                            3 |
+-----------+------------------------------+
|         2 |                            1 |
+-----------+------------------------------+
2 row in set (0.00 sec)
```

## Bitmap全局字典

目前，基于 Bitmap 类型的去重机制有一定限制，即 Bitmap 需要使用整数型类型作为输入。如用户期望将其他数据类型作为 Bitmap 的输入，则需要构建全局字典，将其他类型数据（如字符串类型）通过全局字典映射成为整数类型。构建全局字典有以下几种方案：

### 基于Hive表的全局字典

这种方案中全局字典本身是一张 Hive 表，Hive 表有两个列，一个是原始值，一个是编码的 Int 值。全局字典的生成步骤：

1. 将事实表的字典列去重生成临时表
2. 临时表和全局字典进行left join，悬空的词典项为新value。
3. 对新value进行编码并插入全局字典。
4. 事实表和更新后的全局字典进行left join ， 将词典项替换为ID。

采用这种构建全局字典的方式，可以通过 Spark 或者 MR 实现全局字典的更新，和对事实表中 Value 列的替换。相比基于 Trie 树的全局字典，这种方式可以分布式化，还可以实现全局字典复用。

但这种方式构建全局字典有几个点需要注意：原始事实表会被读取多次，而且还有两次 Join，计算全局字典会使用大量额外资源。

### 基于Trie树构建全局字典

用户还可以使用Trie树自行构建全局字典。Trie 树又叫前缀树或字典树。Trie树中节点的后代存在共同的前缀，可以利用字符串的公共前缀来减少查询时间，可以最大限度地减少字符串比较，所以很适合用来实现字典编码。但Trie树的实现不容易分布式化，在数据量比较大的时候会产生性能瓶颈。

通过构建全局字典，将其他类型的数据转换成为整型数据，就可以利用Bitmap对非整型数据列进行精确去重分析了。
