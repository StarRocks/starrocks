---
displayed_sidebar: "Chinese"
---

# dayofyear

## 功能

计算指定日期为对应年中的哪一天。

参数为 DATE 或 DATETIME 类型。

## 语法

```Haskell
INT DAYOFYEAR(DATETIME date)
```

## 示例

```Plain Text
select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```
