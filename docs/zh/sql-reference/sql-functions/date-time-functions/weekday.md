---
displayed_sidebar: docs
---

# weekday

返回指定日期的工作日索引值，即星期一为 0，星期日为 6。

## 语法

```Haskell
INT WEEKDAY(DATETIME date)
```

## 参数说明

`date`：参数为 DATE 或 DATETIME 类型，或者为可以 CAST 成 DATE 或 DATETIME 类型的数字。

## 示例

```SQL
MySQL > select weekday('2023-01-01');
+-----------------------+
| weekday('2023-01-01') |
+-----------------------+
|                     6 |
+-----------------------+
```

## 关键字

WEEKDAY