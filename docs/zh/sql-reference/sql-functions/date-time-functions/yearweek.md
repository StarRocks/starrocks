---
displayed_sidebar: docs
---

# yearweek

## 功能

返回给定日期对应的年份和周数。该函数和 MySQL yearweek() 函数原理相同。

该函数从 3.3 版本开始支持。

## 语法

```Haskell
INT YEARWEEK(DATETIME|DATE date[, INT mode])
```

## 参数说明

- `date`: 支持的数据类型为 DATETIME 或 DATE。
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

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 0);
+---------------------------+
| yearweek('2007-01-01', 0) |
+---------------------------+
|                    200653 |
+---------------------------+
```

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 1);
+---------------------------+
| yearweek('2007-01-01', 1) |
+---------------------------+
|                    200701 |
+---------------------------+
```

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 2);
+---------------------------+
| yearweek('2007-01-01', 2) |
+---------------------------+
|                    200653 |
+---------------------------+
1 row in set (0.01 sec)
```
