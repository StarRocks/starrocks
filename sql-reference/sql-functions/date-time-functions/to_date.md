# to_date

## description

### Syntax

```Haskell
DATE TO_DATE(DATETIME datetime)
```

返回 DATETIME 类型中的日期部分。

## example

```Plain Text
MySQL > select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```
