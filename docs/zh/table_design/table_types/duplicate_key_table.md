---
displayed_sidebar: "Chinese"
---

# 明细表

明细表是默认创建的表类型。如果在建表时未指定任何 key，默认创建的是明细表。

创建表时，支持定义排序键。如果查询的过滤条件包含排序键，则 StarRocks 能够快速地过滤数据，提高查询效率。明细表适用于日志数据分析等场景，支持追加新数据，不支持修改历史数据。

## 适用场景

- 分析原始数据，例如原始日志、原始操作记录等。

- 查询方式灵活，不需要局限于预聚合的分析方式。

- 导入日志数据或者时序数据，主要特点是旧数据不会更新，只会追加新的数据。

## 创建表

例如，需要分析某时间范围的某一类事件的数据，则可以将事件时间（`event_time`）和事件类型（`event_type`）作为排序键。

在该业务场景下，建表语句如下：

```SQL
CREATE TABLE IF NOT EXISTS detail (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT ""
)
DUPLICATE KEY(event_time, event_type)
DISTRIBUTED BY HASH(user_id)
PROPERTIES (
"replication_num" = "3"
);
```

> **注意**
>
> - 建表时必须使用 `DISTRIBUTED BY HASH` 子句指定分桶键，否则建表失败。分桶键的更多说明，请参见[分桶](../Data_distribution.md#分桶)。
> - 自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../Data_distribution.md#确定分桶数量)。

## 使用说明

- 排序键的相关说明：
  - 在建表语句中，排序键必须定义在其他列之前。
  - 排序键可以通过 `DUPLICATE KEY` 显式定义。本示例中排序键为 `event_time` 和 `event_type`。
    > 如果未指定，则默认选择表的前三列作为排序键。

  - 明细表中的排序键可以为部分或全部维度列。

- 建表时，支持为指标列创建 BITMAP、Bloom Filter 等索引。

## 下一步

建表完成后，您可以创建多种导入作业，导入数据至表中。具体导入方式，请参见[导入概览](../../loading/Loading_intro.md)。

> - 导入时，支持追加新数据，不支持修改历史数据。
> - 如果导入两行完全相同的数据，则明细表会将这两行数据视为两行，而不是一行。
