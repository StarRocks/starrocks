---
displayed_sidebar: docs
---

# monthname

<<<<<<< HEAD
## 功能
=======

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

返回指定日期对应的月份。参数为 DATE 或 DATETIME 类型。

如果日期不存在，返回 NULL。

## 语法

```Haskell
VARCHAR MONTHNAME(DATETIME|DATE date)
```

## 示例

```Plain Text
select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+

select monthname('2008-02-03');
+-------------------------+
| monthname('2008-02-03') |
+-------------------------+
| February                |
+-------------------------+
```
