# year

## description

### Syntax

```Haskell
INT YEAR(DATETIME date)
```

返回date类型的year部分，范围从1000-9999

参数为Date或者Datetime类型

## example

```Plain Text
MySQL > select year('1987-01-01');
+-----------------------------+
| year('1987-01-01 00:00:00') |
+-----------------------------+
|                        1987 |
+-----------------------------+
```
