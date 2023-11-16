---
displayed_sidebar: "Chinese"
---

# dayname

## 功能

返回指定日期对应的星期名称。

参数为 DATE 或者 DATETIME 类型。

## 语法

```Haskell
VARCHAR DAYNAME(DATETIME|DATE date)
```

## 示例

```Plain Text
select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```
