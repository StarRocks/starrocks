---
displayed_sidebar: docs
---

# 使用 AUTO INCREMENT 列构建全局字典以加速精确去重计算和 Join

## 应用场景

- **场景一**：您需要对海量订单数据（零售订单、快递订单等）计算精确去重。但是去重计数的列为 STRING 类型，此时直接计数，性能会不够理想。例如订单表 `orders` 中表示订单编号的`order_uuid` 列为 STRING 类型，大小通常为 32 ~ 36 字节，由 `UUID()` 或其他类似函数生成。直接基于 STRING 列 `order_uuid` 精确去重计数 `SELECT count(DISTINCT order_uuid) FROM orders WHERE create_date >= CURDATE();`，查询性能可能无法满足您的需要。
  如果能使用 INTEGER 列做精确去重计数，性能则会显著提升。
- **场景二**：您需要[借助bitmap函数进一步加速多维分析中对订单计算精确去重](../using_starrocks/Using_bitmap.md)。然而，`bitmap_count()` 函数要求输入值为 INTEGER 类型，如果业务场景中去重计数的列为 STRING 类型，则需要使用 `bitmap_hash()` 函数，但是这样可能导致最终返回的是近似且值小一点的去重计数。并且，相对于连续分配的 INTEGER 值，`bitmap_hash()` 产生的 INTEGER 值更分散，会导致查询性能下降、存储数据量变大。
- **场景三**：您需要查询从下单到支付的时间相对较短的订单数量，而下单时间和支付时间可能存储在两张表里，由不同的业务团队维护。则您可能需要基于订单编号关联两张表，然后对订单计算精确去重。例如如下语句：

    ```SQL
    SELECT count(distinct order_uuid)
    FROM orders_t1 as t1 JOIN orders_t2 as t2
        ON t1.order_uuid = t2.order_uuid
    WHERE t2.payment_time - t1.create_time <= 3600
        AND create_date >= CURDATE();
    ```

   但是订单编号 `order_uuid` 列是 STRING 类型，直接基于 STRING 列进行 Join，性能也不如基于 INTEGER 列。

## 优化思路

针对上述应用场景，优化思路是将订单数据导入目标表并构建 STRING 和 INTEGER 值之间的映射关系，后续查询分析基于 INTEGER 列进行。该思路可以拆分为如下阶段执行：

1. 阶段一： 创建全局字典并构建 STRING 值和 INTEGER 值之间的映射关系。字典中 key 列为 STRING 类型，value 列为 INTEGER 类型且为自增列。每次导入数据时候，系统都会自动为每个 STRING 值生成一个表内全局唯一的 ID，如此就建立了 STRING 值和 INTEGER 值之间的映射关系。
2. 阶段二：将订单数据和全局字典的映射关系导入至目标表。
3. 阶段三：后续查询分析时基于目标表的 INTEGER 列来计算精确去重或 Join，可以显著提高性能。
4. 阶段四：为了进一步优化性能，您还可以在 INTEGER 列上使用 bitmap 函数来进一步加速计算精确去重。

## 解决方案

在 v3.2.5 之前，阶段二可以通过[两种方案](https://docs.starrocks.io/zh/docs/3.1/using_starrocks/query_acceleration_with_auto_increment/#%E8%A7%A3%E5%86%B3%E6%96%B9%E6%A1%88)实现：

- 采用外表或者内表作为中间表的方式，通过**外表或者中间表 JOIN 字典表**的方式，得到字典数据对应的字典 ID 后进行导入.
- 需要使用主键表，先导入数据，然后通过带有 JOIN 操作的 UPDATE 语句来更新字典数据对应的字典 ID。但这个数据导入的过程其实比较不方便，且有不少约束。

自 v3.2.5 起，StarRocks 提供了 `dict_mapping()` 函数，您只需要在目标表中把对应的字典 ID 列定义为一个表达式为 `dict_mapping()` 生成列即可。而后的数据导入就和普通的数据导入一样，不再需要借助 JOIN 或 UPDATE 语句来写入字典 ID。在您导入数据的过程中，系统将自动关联字典表并插入对应的字典 ID。这将极大地方便有全局字典表的数据导入过程，不再依赖表类型、同时还可以使用各种导入方式导入数据。

### 业务场景

本解决方案以如下两个 CSV 文件 `batch1.csv`  和 `batch2.csv` 为例。文件包含两列 `id` 和 `order_uuid`。

- `batch1.csv`

    ```csv
    1, a1
    2, a2
    3, a3
    11, a1
    11, a2
    12, a1
    ```

- `batch2.csv`

    ```csv
    1, a2
    2, a2
    3, a2
    11, a2
    12, a101
    12, a102
    13, a102
    ```

### 具体步骤

**阶段一：创建全局字典表，并且导入 CSV 文件中的订单编号列值，从而构建 STRING 和 INTEGER 值之间的映射关系。**

1. 创建一个主键表作为全局字典，定义主键也就是 key 列为 `order_uuid`（STRING 类型），value 列为 `order_id_int`（INTEGER 类型）并且为自增列。

   :::info

   `dict_mapping` 要求全局字典表必须为主键表。

   :::

      ```SQL
      CREATE TABLE dict (
          order_uuid STRING,
          order_id_int BIGINT AUTO_INCREMENT  -- 自动为每个 order_uuid 值分配一个唯一 ID 
      )
      PRIMARY KEY (order_uuid)
      DISTRIBUTED BY HASH (order_uuid)
      PROPERTIES("replicated_storage" = "true");
      ```

2. 本示例使用 Stream Load 将两个 CSV 文件的 `order_uuid` 列分批导入至字典表 `dict` 的 `order_uuid` 列，并且需要注意的是，此处需要使用部分列更新。

      ```Bash
      curl --location-trusted -u root: \
          -H "partial_update: true" \
          -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid" \
          -T batch1.csv \
          -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dict/_stream_load
          
      curl --location-trusted -u root: \
          -H "partial_update: true" \
          -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid" \
          -T batch2.csv \
          -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dict/_stream_load
      ```

> **说明**
>
> 在进入下一阶段前，如果数据源有新增数据，需将所有新增数据导入字典表以保证映射一定存在。

**阶段二**：创建目标表，并且包含具有 `dict_mapping` 属性的字典 ID 列，后续导入订单数据至目标表时，系统将自动关联字典表并插入对应的字典 ID。

1. 创建一张表 `dest_table`，包含 CSV 文件的所有列。 并且您还需要定义一个整数类型的 `order_id_int` 列（通常为 BIGINT），与 STRING 类型的 `order_id_int` 列进行映射，并且具有 dict_mapping 列属性。后续会基于 `order_id_int` 列进行查询分析。

      ```SQL
      -- 目标数据表里，订单编号`order_uuid`对应的字典ID 增加 dict_mapping 列属性
      CREATE TABLE dest_table (
          id BIGINT,
          order_uuid STRING, -- 该列记录 STRING 类型订单编号 
          batch int comment 'used to distinguish different batch loading',
          order_id_int BIGINT AS dict_mapping('dict', order_uuid) -- 订单编号`order_uuid`对应的字典 ID，具有 dict_mapping 列属性。
      )
      DUPLICATE KEY (id, order_uuid)
      DISTRIBUTED BY HASH(id);
      ```

2. 正常导入数据到目标表。这步可以采用 Stream Load 在内的各种导入方式。`order_id_int` 列因为有配置 `dict_mapping` 属性，系统会在导入数据时自动从`dict`获取字典 ID 并填充：

      ```Bash
      curl --location-trusted -u root: \
          -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid, batch=1" \
          -T batch1.csv \
          -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table/_stream_load
          
      curl --location-trusted -u root: \
          -H "format: CSV" -H "column_separator:," -H "columns: id, order_uuid, batch=2" \
          -T batch2.csv \
          -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table/_stream_load
      ```

**阶段三**：实际查询分析时，您可以在 INTEGER 类型的列 `order_id_int` 上进行精确去重或者 Join，相较于基于 STRING 类型的列 `order_uuid`，性能会显著提升。

```SQL
-- 基于 BIGINT 类型的 order_id_int 精确去重
SELECT id, COUNT(DISTINCT order_id_int) FROM dest_table GROUP BY id ORDER BY id;
-- 基于 STRING 类型的 order_uuid 精确去重
SELECT id, COUNT(DISTINCT order_uuid) FROM dest_table GROUP BY id ORDER BY id;
```

您还可以[使用 bitmap 函数加速计算精确去重](#使用-bitmap-函数加速计算精确去重)。

### 使用 bitmap 函数加速计算精确去重

为了进一步加速计算，在构建全局字典后，您可以将字典表 INTEGER 列值直接插入到一个 bitmap 列中。后续对该 bitmap 列使用 bitmap 函数来精确去重计数。

**方式一**：

如果您构建了全局字典并且已经导入具体订单数据到 `dest_table` 表，则可以执行如下步骤：

1. 创建聚合表 `dest_table_bitmap`。该表中包含两列，聚合列为 BITMAP 类型的 `order_id_bitmap`，并且指定聚合函数为 `bitmap_union()`，另一列为 INTEGER 类型的 `id`。该表已**不包含**原始 STRING 列（否则每个 bitmap 中只有一个值，无法起到加速效果）。

      ```SQL
      CREATE TABLE dest_table_bitmap (
          id BIGINT,
          order_id_bitmap BITMAP BITMAP_UNION
      )
      AGGREGATE KEY (id)
      DISTRIBUTED BY HASH(id) BUCKETS 6;
      ```

2. 向聚合表 `dest_table_bitmap` 中插入数据。 `id` 列插入表 `dest_table` 的列 `id` 的数据；`order_id_bitmap` 列插入字典表 `dict` INTEGER 列 `order_id_int` 的数据（经过函数 `to_bitmap` 处理后的值）。

    ```SQL
    INSERT INTO dest_table_bitmap (id, order_id_bitmap)
    SELECT id,  to_bitmap(dict_mapping('dict', order_uuid))
    FROM dest_table
    WHERE dest_table.batch = 1; -- 此处 batch 用于模拟不同批次的处理。
        
    INSERT INTO dest_table_bitmap (id, order_id_bitmap)
    SELECT id, to_bitmap(dict_mapping('dict', order_uuid))
    FROM dest_table
    WHERE dest_table.batch = 2;
    ```

3. 然后基于 bitmap 列使用函数 `BITMAP_UNION_COUNT()` 精确去重计数。

      ```SQL
      SELECT id, BITMAP_UNION_COUNT(order_id_bitmap) FROM dest_table_bitmap
      GROUP BY id ORDER BY id;
      ```

**方式二**：

如果您在构建了全局字典后，不需要保留具体订单数据，只想直接一步到位导入数据到 `dest_table_bitmap` 表中，则可以执行如下步骤：

1. 创建聚合表 `dest_table_bitmap`。该表中包含两列，聚合列为 BITMAP 类型的 `order_id_bitmap`，并且指定聚合函数为 `bitmap_union()`，定义另一列为 INTEGER 类型的 `id`。该表已**不包含**原始 STRING 列了（否则每个 bitmap 中只有一个值，无法起到加速效果）。

      ```SQL
      CREATE TABLE dest_table_bitmap (
          id BIGINT,
          order_id_bitmap BITMAP BITMAP_UNION
      )
      AGGREGATE KEY (id)
      DISTRIBUTED BY HASH(id) BUCKETS 6;
      ```

2. 向聚合表中插入数据。`id` 列直接插入 CSV 文件中的`id` 列的数据；`order_id_bitmap` 列插入字典表 `dict` INTEGER 列 `order_id_int` 列的数据（经过函数 `to_bitmap` 处理后的值）。

     ```bash
     curl --location-trusted -u root: \
         -H "format: CSV" -H "column_separator:," \
         -H "columns: id, order_uuid,  order_id_bitmap=to_bitmap(dict_mapping('dict', order_uuid))" \
         -T batch1.csv \
         -XPUT http://<fe_host>:<fe_http_port>/api/example_db/dest_table_bitmap/_stream_load
     
     curl --location-trusted -u root: \
         -H "format: CSV" -H "column_separator:," \
         -H "columns: id, order_uuid, order_id_bitmap=to_bitmap(dict_mapping('dict', order_uuid))" \
         -T batch2.csv \
         -XPUT http:///<fe_host>:<fe_http_port>/api/example_db/dest_table_bitmap/_stream_load
     ```

3. 然后基于 bitmap 列使用函数 `BITMAP_UNION_COUNT()` 精确去重计数。

      ```SQL
      SELECT id, BITMAP_UNION_COUNT(order_id_bitmap) FROM dest_table_bitmap
      GROUP BY id ORDER BY id;
      ```
