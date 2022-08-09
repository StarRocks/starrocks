# monthname

## 功能

返回指定日期对应的月份。参数为Date或者Datetime类型。

## 语法

```Haskell
VARCHAR MONTHNAME(DATE)
```

## 示例

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```
