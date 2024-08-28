---
displayed_sidebar: docs
---

# week

## 功能

计算指定日期属于一年中的第几周。该函数与 MySQL 中的 WEEK 函数语义相同。该函数从 2.3 版本开始支持。

## 语法

```sql
INT WEEK(DATETIME/DATE date, INT mode)
```

## 参数说明

- `date`: 支持的数据类型为 DATETIME 和 DATE。
- `mode`: 可选，支持的数据类型为 INT。用于确定周数计算逻辑，即一周起始于周日还是周一，以及返回值的范围是 0 到 53 还是 1 到 53。该参数取值范围 0~7，默认值为 `0`。如果未指定该参数，默认按照模式 `0` 对应的规则计算。具体的语义如下：

```Plain Text
Mode    First day of week    Range    Week 1 is the first week …
0       Sunday               0-53     with a Sunday in this year
1       Monday               0-53     with 4 or more days this year
2       Sunday               1-53     with a Sunday in this year
3       Monday               1-53     with 4 or more days this year
4       Sunday               0-53     with 4 or more days this year
5       Monday               0-53     with a Monday in this year
6       Sunday               1-53     with 4 or more days this year
7       Monday               1-53     with a Monday in this year
```

## 返回值说明

返回 INT 类型的值。取值范围 0~53，具体的范围由 `mode` 参数决定。 如果 `date` 取值类型不合法或输入为空，则返回 NULL。

## 示例

查询 `2007-01-01` 所在的周，`2007-01-01` 在日历上是周一。

- `mode` 设置为 `0`，返回结果为 0。此时周日作为一周的第一天，`2007-01-01` 不能作为第一周，因此返回 `0`。

```Plain Text
SELECT WEEK('2007-01-01', 0);
+-----------------------+
| week('2007-01-01', 0) |
+-----------------------+
|                     0 |
+-----------------------+
1 row in set (0.02 sec)
```

- `mode` 设置为 `1`，返回结果为 `1`。此时周一作为一周的第一天。

```Plain Text
SELECT WEEK('2007-01-01', 1);
+-----------------------+
| week('2007-01-01', 1) |
+-----------------------+
|                     1 |
+-----------------------+
1 row in set (0.02 sec)
```

- `mode` 设置为 `2`，返回结果 `53`。此时周日作为一周的第一天，但是取值范围是 1~53，所以返回 53，表示这是上一年的最后一周。

```Plain Text
SELECT WEEK('2007-01-01', 2);
+-----------------------+
| week('2007-01-01', 2) |
+-----------------------+
|                    53 |
+-----------------------+
1 row in set (0.01 sec)
```
