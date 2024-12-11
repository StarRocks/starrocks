---
displayed_sidebar: docs
---

# dayofmonth

<<<<<<< HEAD
## 功能
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
