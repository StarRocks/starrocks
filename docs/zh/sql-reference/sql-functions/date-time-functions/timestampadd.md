---
displayed_sidebar: docs
---

# timestampadd

## 功能

将整数表达式间隔添加到日期或日期时间表达式 `datetime_expr` 中。

`interval` 的单位由 `unit` 参数给出，应该是下列值之一:

MILLISECOND（3.2 及以后），SECOND，MINUTE，HOUR，DAY，WEEK，MONTH，YEAR。

## 语法

```Haskell
DATETIME TIMESTAMPADD(unit, interval, DATETIME datetime_expr)
```

## 参数说明

- `datetime_expr`: 日期或时间日期表达式。
- `interval`：要添加的时间间隔的数量，INT 类型。
- `unit`：时间间隔的单位。支持的单位包括 MILLISECOND (3.2 及以后)，SECOND，MINUTE，HOUR，DAY，WEEK，MONTH，YEAR。

## 返回值说明

返回值的类型与 `datetime_expr` 相同。

## 示例

```plain text

MySQL > SELECT TIMESTAMPADD(MINUTE,1,'2019-01-02');
+------------------------------------------------+
| timestampadd(MINUTE, 1, '2019-01-02 00:00:00') |
+------------------------------------------------+
| 2019-01-02 00:01:00                            |
+------------------------------------------------+

MySQL > SELECT TIMESTAMPADD(WEEK,1,'2019-01-02');
+----------------------------------------------+
| timestampadd(WEEK, 1, '2019-01-02 00:00:00') |
+----------------------------------------------+
| 2019-01-09 00:00:00                          |
+----------------------------------------------+

MySQL > SELECT TIMESTAMPADD(MILLISECOND,1,'2019-01-02');
+--------------------------------------------+
| timestampadd(MILLISECOND, 1, '2019-01-02') |
+--------------------------------------------+
| 2019-01-02 00:00:00.001000                 |
+--------------------------------------------+
```
