---
displayed_sidebar: docs
---

# dayofmonth

## 功能

获得日期中的天信息，返回值范围 1~31。

参数为 DATE 或者 DATETIME 类型。

## 语法

```Haskell
INT DAYOFMONTH(DATETIME date)
```

## 示例

```Plain Text
select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```
