# ascii

## description

### Syntax

```Haskell
INT ascii(VARCHAR str)
```

返回字符串第一个字符对应的 ascii 码

## example

```Plain Text
MySQL > select ascii('1');
+------------+
| ascii('1') |
+------------+
|         49 |
+------------+

MySQL > select ascii('234');
+--------------+
| ascii('234') |
+--------------+
|           50 |
+--------------+
```

## keyword

ASCII
