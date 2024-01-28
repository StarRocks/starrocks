---
displayed_sidebar: "Chinese"
---

# 使用 Bitmap 实现精确去重

本文介绍如何通过 Bitmap 实现精确去重（Exact Count Distinct）。

Bitmap 去重能够准确计算一个数据集中不重复元素的数量，相比传统的 Count Distinct，可以节省存储空间、加速计算。例如，给定一个数组 A，其取值范围为 [0, n)，可采用 (n+7)/8 的字节长度的 bitmap 对该数组去重。即将所有 bit 初始化为 0，然后以数组 A 中元素的取值作为 bit 的下标，并将 bit 置为 1，那么 bitmap 中 1 的个数即为数组 A 中不同元素 (Count Distinct) 的数量。

## 传统 Count distinct

StarRocks 是基于 MPP 架构实现的，在使用 count distinct 做精确去重时，可以保留明细数据，灵活性较高。但是，由于在查询执行的过程中需要进行多次数据 shuffle（不同节点间传输数据，计算去重），会导致性能随着数据量增大而直线下降。

比如，要通过以下明细数据计算表（dt, page, user_id）每个页面的 UV。

|  dt   |   page  | user_id |
| :---: | :---: | :---:|
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 102 |
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 101 |
|   20191206  |   game  | 101 |
|   20191206  |   shopping  | 101 |

StarRocks 在计算时，会按照下图进行计算，先根据 page 列和 user_id 列 group by，最后再 count。

![alter](../assets/6.1.2-2.png)

> 注：图中是 6 行数据在 2 个 BE 节点上计算的示意图。

在上面的计算方式中，由于数据需要进行多次 shuffle，当数据量越来越大时，所需的计算资源就会越来越多，查询也会越来越慢。而使用 Bitmap 去重，就是为了解决传统 count distinct 在大量数据场景下的性能问题。

```sql
select page, count(distinct user_id) as uv from table group by page;

|  page      |   uv  |
| :---:      | :---: |
|  game      |  1    |
|  shopping  |   2   |
```

## Bitmap 去重的优势

与传统 [count distinct](#传统-count-distinct) 方式相比，Bitmap 的优势主要体现在以下两点 ：

- 节省存储空间：通过用 Bitmap 的一个 Bit 位表示对应下标是否存在，能节省大量存储空间。例如，对 INT32 类型的数据去重，如使用普通的 bitmap，其所需的存储空间只占 COUNT(DISTINCT expr) 的 1/32。StarRocks 采用一种设计的十分精巧的 bitmap，叫做 Roaring Bitmap，相较 Bitmap 会进一步减少内存占用。
- 加速计算：Bitmap 去重使用的是位运算，所以计算速度相较 COUNT(DISTINCT expr) 更快，而且 bitmap 去重在 StarRocks MPP 执行引擎中还可以并行加速处理，提高计算速度。

关于 Roaring Bitmap 的实现，细节可以参考：[具体论文和实现](https://github.com/RoaringBitmap/RoaringBitmap)。

## 使用说明

- Bitmap index 和 Bitmap 去重二者虽然都使用 Bitmap 技术，但引入原因和解决的问题完全不同。前者用于低基数的枚举型列的等值条件过滤，后者则用于计算一组数据行的指标列的不重复元素的个数。
- 从 StarRocks 2.3 版本开始，所有类型的表均支持设置指标列为 BITMAP 类型，但是所有类型的表不支持设置[排序键](../table_design/Sort_key.md)为 BITMAP 类型。
- 建表时，指定指标列类型为 BITMAP，使用 [BITMAP_UNION](../sql-reference/sql-functions/bitmap-functions/bitmap_union.md) 函数进行聚合。
- StarRocks 的 bitmap 去重是基于 Roaring Bitmap 实现的，roaring bitmap 只能对 TINYINT，SMALLINT，INT 和 BIGINT 类型的数据去重。如想要使用 Roaring Bitmap 对其他类型的数据去重，则需要构建全局字典。

## Bitmap 去重使用示例

以统计某一个页面的独立访客人数（UV）为例：

1. 创建一张聚合表 `page_uv`。其中 `visit_users` 列表示访问用户的 ID，为聚合列，列类型为 BITMAP，使用聚合函数 BITMAP_UNION 来聚合数据。

    ```sql
    CREATE TABLE `page_uv` (
      `page_id` INT NOT NULL COMMENT '页面id',
      `visit_date` datetime NOT NULL COMMENT '访问时间',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT '访问用户id'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`)
    PROPERTIES (
      "replication_num" = "3",
      "storage_format" = "DEFAULT"
    );
    ```

2. 向表中导入数据。

    采用 INSERT INTO 语句导入：

    ```sql
    INSERT INTO page_uv VALUES
    (1, '2020-06-23 01:30:30', to_bitmap(13)),
    (1, '2020-06-23 01:30:30', to_bitmap(23)),
    (1, '2020-06-23 01:30:30', to_bitmap(33)),
    (1, '2020-06-23 02:30:30', to_bitmap(13)),
    (2, '2020-06-23 01:30:30', to_bitmap(23));
    ```

    数据导入后:

    - 在 page_id = 1， visit_date = '2020-06-23 01:30:30' 数据行，`visit_users` 字段包含 3 个 bitmap 元素（13，23，33）；
    - 在 page_id = 1， visit_date = '2020-06-23 02:30:30' 的数据行，`visit_users` 字段包含 1 个 bitmap 元素（13）；
    - 在 page_id = 2， visit_date = '2020-06-23 01:30:30' 的数据行，`visit_users` 字段包含 1 个 bitmap 元素（23）。

    采用本地文件导入：

    ```shell
    echo -e '1,2020-06-23 01:30:30,130\n1,2020-06-23 01:30:30,230\n1,2020-06-23 01:30:30,120\n1,2020-06-23 02:30:30,133\n2,2020-06-23 01:30:30,234' > tmp.csv | 
    curl --location-trusted -u <username>:<password> -H "label:label_1600960288798" \
        -H "column_separator:," \
        -H "columns:page_id,visit_date,visit_users, visit_users=to_bitmap(visit_users)" -T tmp.csv \
        http://StarRocks_be0:8040/api/db0/page_uv/_stream_load
    ```

3. 统计每个页面的 UV。

    ```sql
    select page_id, count(distinct visit_users) from page_uv group by page_id;

    +-----------+------------------------------+
    |  page_id  | count(DISTINCT `visit_users`) |
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

采用这种构建全局字典的方式，可以通过 Spark 或者 MapReduce 实现全局字典的更新，和对事实表中 Value 列的替换。相比基于 Trie 树的全局字典，这种方式可以分布式化，还可以实现全局字典复用。

但需要注意的是，使用这种方式构建全局字典时，事实表会被读取多次，并且过程中有两次 Join 操作，会导致计算全局字典使用大量额外资源。

### 基于 Trie 树构建全局字典

Trie 树又叫前缀树或字典树。Trie 树中节点的后代存在共同的前缀，系统可以利用字符串的公共前缀来减少查询时间，从而最大限度地减少字符串比较。因此，基于 Trie 树构建全局字典的方式适合用于实现字典编码。但基于 Trie 树的全局字典实现难以分布式化，在数据量比较大的时候会产生性能瓶颈。
