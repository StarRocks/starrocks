---
displayed_sidebar: docs
---

# from_days

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

通过计算当前时间距离 `0000-01-01` 的天数计算出是哪一天。

## 语法

```Haskell
DATE FROM_DAYS(INT N)
```

## 示例

```Plain Text
select from_days(730669);
+-------------------+
| from_days(730669) |
+-------------------+
| 2000-07-03        |
+-------------------+
```
