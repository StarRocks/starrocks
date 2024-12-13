---
displayed_sidebar: docs
---

# to_date

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

返回 DATETIME 类型值中的日期部分。

## 语法

```Haskell
DATE TO_DATE(DATETIME datetime)
```

## 示例

```Plain Text
select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```
