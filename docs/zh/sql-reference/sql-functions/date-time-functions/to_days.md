---
displayed_sidebar: docs
---

# to_days

<<<<<<< HEAD
## 功能
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

返回指定日期距离 `0000-01-01` 的天数。

参数必须为 DATE 或 DATETIME 类型。

## 语法

```Haskell
INT TO_DAYS(DATETIME date)
```

## 示例

```Plain Text
select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```
