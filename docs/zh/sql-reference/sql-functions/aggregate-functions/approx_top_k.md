---
displayed_sidebar: docs
---

# approx_top_k

## 功能

返回表达式 `expr` 中最常出现的 `k` 个项目以及每个项目出现的近似次数。

该函数从 3.0 版开始支持。

## 语法

```Haskell
APPROX_TOP_K(<expr> [ , <k> [ , <counter_num> ] ] )
```

## 参数说明

- `expr`：STRING、BOOLEAN、DATE、DATETIME 或数值类型的表达式。
- `k`：可选，大于 0 的整数。如果未指定 `k`，则默认为 `5`。最大值：`100000`。
- `counter_num`：可选，大于等于 `k` 的整数。值越大，结果越准确，但是也会增加 CPU 和内存使用。

  - 最大值 `100000`。
  - 如果未指定 `counter_num`，则默认为 `max(min(2 * k, 100), 100000)`。

## 返回值说明

返回 STRUCT 类型的 Array。每个 STRUCT 都由一个 `item` 字段（和原始输入类型相同）和一个 `count` 字段（类型为 BIGINT）组成。Array 以 `count` 的降序进行排序。

该函数计算次数时会有一定误差，误差不超过 `2.0 * numRows / counter_num`。其中 `numRows` 为总行数。`counter_num` 越大，准确度更高，但是也会增加内存占用。如果 `expr` 内的唯一值 (Unique Value) 个数小于 `counter_num`，会得到精确的 count 数量。返回的 `item` 可以包含 `NULL`。

## 示例

以 [scores](../Window_function.md#窗口函数建表示例) 表中的数据为例。

```plaintext
-- 计算各科目的得分分布情况。
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores GROUP BY subject;
+---------+--------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                              |
+---------+--------------------------------------------------------------------------------------------------------------------+
| physics | [{"item":99,"count":2},{"item":null,"count":1},{"item":100,"count":1},{"item":85,"count":1},{"item":60,"count":1}] |
| english | [{"item":null,"count":1},{"item":92,"count":1},{"item":98,"count":1},{"item":100,"count":1},{"item":85,"count":1}] |
| NULL    | [{"item":90,"count":1}]                                                                                            |
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":92,"count":1},{"item":95,"count":1},{"item":70,"count":1}]  |
+---------+--------------------------------------------------------------------------------------------------------------------+

-- 计算数学科目的得分分布情况。
MySQL > SELECT subject, APPROX_TOP_K(score)  AS top_k FROM scores WHERE subject IN  ('math') GROUP BY subject;
+---------+-------------------------------------------------------------------------------------------------------------------+
| subject | top_k                                                                                                             |
+---------+-------------------------------------------------------------------------------------------------------------------+
| math    | [{"item":80,"count":2},{"item":null,"count":1},{"item":95,"count":1},{"item":92,"count":1},{"item":70,"count":1}] |
+---------+-------------------------------------------------------------------------------------------------------------------+
```

## keyword

APPROX_TOP_K
