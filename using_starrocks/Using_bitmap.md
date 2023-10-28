# 使用 Bitmap 实现精确去重

本小节介绍如何通过 Bitmap 在 StarRocks 中精确去重。

Bitmap 去重是指，当给定一个数组 A，其取值范围为 [0, n)，可采用 (n+7)/8 的字节长度的 bitmap 对该数组去重， 初始化为全 0；逐个处理数组 A 的元素，以 A 中元素取值作为 bitmap 的下标，将该下标的 bit 置 1；最后统计 bitmap 中 1 的个数即为数组 A 的 count distinct 结果。

与传统使用 COUNT DISTINCT 方式相比，Bitmap 的优势主要体现在：

1. 空间优势：通过用 Bitmap 的一个 Bit 位表示对应下标是否存在，能节省大量存储空间。例如对 INT 去重，使用普通 Bitmap 所需的存储空间只占传统去重的 1/32。StarRocks 采用 Roaring Bitmap 的优化实现，对于稀疏的 Bitmap，所占用的存储空间会进一步降低。
2. 时间优势：Bitmap 的去重涉及的计算包括对给定下标的 Bit 置位，统计 Bitmap 的置位个数，分别为 O(1) 操作和 O(n) 操作，并且后者可使用 CLZ，CTZ 等指令高效计算。 此外，Bitmap 去重在 MPP 执行引擎中还可以并行加速处理，每个计算节点各自计算本地子 Bitmap，使用 BITOR 操作将这些子 Bitmap 合并成最终的 Bitmap。BITOR 操作比基于 sort 和基于 hash 的去重效率更高，且无条件依赖和数据依赖，可向量化执行。

Roaring Bitmap 实现，细节可以参考：[具体论文和实现](https://github.com/RoaringBitmap/RoaringBitmap)

## 使用 Bitmap 去重

1. Bitmap index 和 Bitmap 去重二者虽然都使用 Bitmap 技术，但引入动机和解决的问题完全不同。前者用于低基数的枚举型列的等值条件过滤，后者则用于计算一组数据行的指标列的不重复元素的个数。
2. 从2.3版本开始，所有数据模型的指标列均支持 BITMAP，但是所有模型的排序键还不支持 BITMAP。
3. 创建表时指定指标列的数据类型为 ·BITMAP， 聚合函数为 BITMAP_UNION。
4. 当在 Bitmap 类型列上使用 count distinct 时，StarRocks 会自动转化为 BITMAP_UNION_COUNT 计算。

具体操作函数参见 [Bitmap函数](../sql-reference/sql-functions/bitmap-functions/bitmap_and.md)。

### 示例

以统计某一个页面的 UV 为例：

创建一张含有 BITMAP 列的表，其中 **visit_users** 列为聚合列，列类型为 **BITMAP**，聚合函数为 **BITMAP_UNION**。

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

向表中导入数据，采用 **insert into** 语句导入。

  ```sql
  insert into page_uv values
  (1, '2020-06-23 01:30:30', to_bitmap(13)),
  (1, '2020-06-23 01:30:30', to_bitmap(23)),
  (1, '2020-06-23 01:30:30', to_bitmap(33)),
  (1, '2020-06-23 02:30:30', to_bitmap(13)),
  (2, '2020-06-23 01:30:30', to_bitmap(23));
  ```

在以上数据导入后，在 page_id = 1， visit_date = '2020-06-23 01:30:30' 的数据行，visit_user 字段包含着 3 个 bitmap 元素（13，23，33）；在 page_id = 1， visit_date = '2020-06-23 02:30:30' 的数据行，visit_user 字段包含着 1 个 bitmap 元素（13）；在 page_id = 2， visit_date = '2020-06-23 01:30:30' 的数据行，visit_user 字段包含着 1 个 bitmap 元素（23）。

采用本地文件导入

```shell
echo -e '1,2020-06-23 01:30:30,130\n1,2020-06-23 01:30:30,230\n1,2020-06-23 01:30:30,120\n1,2020-06-23 02:30:30,133\n2,2020-06-23 01:30:30,234' > tmp.csv | 
curl --location-trusted -u <username>:<password> -H "label:label_1600960288798" \
    -H "column_separator:," \
    -H "columns:page_id,visit_date,visit_users, visit_users=to_bitmap(visit_users)" -T tmp.csv \
    http://StarRocks_be0:8040/api/db0/page_uv/_stream_load
```

统计每个页面的 UV

```sql
select page_id, count(distinct visit_users) from page_uv group by page_id;
```

查询结果如下所示：

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

## Bitmap 全局字典

目前，基于 Bitmap 类型的去重机制有一定限制，即 Bitmap 需要使用整数型类型作为输入。如用户期望将其他数据类型作为 Bitmap 的输入，则需要构建全局字典，将其他类型数据（如字符串类型）通过全局字典映射成为整数类型。构建全局字典有以下几种方案：

### 基于 Hive 表的全局字典

该方案需创建一张 Hive 表作为全局字典。Hive 表有两个列，一个是原始值，一个是编码的 Int 值。以下为全局字典的生成步骤：

1. 将事实表的字典列去重并生成临时表。
2. 对临时表和全局字典进行 left join，以悬空的词典项作为新 value。
3. 对新 value 进行编码并插入全局字典。
4. 对事实表和更新后的全局字典进行 left join，将词典项替换为 ID。

采用这种构建全局字典的方式，可以通过 Spark 或者 MR 实现全局字典的更新，和对事实表中 Value 列的替换。相比基于 Trie 树的全局字典，这种方式可以分布式化，还可以实现全局字典复用。

但需要注意的是，使用这种方式构建全局字典时，事实表会被读取多次，并且过程中有两次 Join 操作，会导致计算全局字典使用大量额外资源。

### 基于 Trie 树构建全局字典

Trie 树又叫前缀树或字典树。Trie 树中节点的后代存在共同的前缀，系统可以利用字符串的公共前缀来减少查询时间，从而最大限度地减少字符串比较。因此，基于 Trie 树构建全局字典的方式适合用于实现字典编码。但基于 Trie 树的全局字典实现难以分布式化，在数据量比较大的时候会产生性能瓶颈。
通过构建全局字典，将其他类型的数据转换成为整型数据，就可以利用 Bitmap 对非整型数据列进行精确去重分析了。

## 传统Count_distinct计算

StarRocks 是基于 MPP 架构实现的，在使用 count distinct 做精准去重时，可以保留明细数据，灵活性较高。但是，由于在查询执行的过程中需要进行多次数据 shuffle（不同节点间传输数据，计算去重），会导致性能随着数据量增大而直线下降。

如以下场景所示，存在表（dt, page, user_id)，需要通过明细数据计算 UV。

|  dt   |   page  | user_id |
| :---: | :---: | :---:|
|   20191206  |   waimai  | 101 |
|   20191206  |   waimai  | 102 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   xiaoxiang  | 101 |
|   20191206  |   waimai  | 101 |

按 page 进行分组统计 UV。

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

显然，在上面的计算方式中，由于数据需要进行多次 shuffle，当数据量越来越大时，所需的计算资源就会越来越多，查询也会越来越慢。而使用 Bitmap 技术去重，就是为了解决传统 count distinct 在大量数据场景下的性能问题。
