# dayname

## description

### Syntax

```Haskell
VARCHAR DAYNAME(DATE)
```

返回日期对应的日期名字

参数为Date或者Datetime类型

## example

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```
