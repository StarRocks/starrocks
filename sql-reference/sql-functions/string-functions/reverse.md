# reverse

## description

### Syntax

```Haskell
VARCHAR reverse(VARCHAR str)
```

将字符串反转，返回的字符串的顺序和源字符串的顺序相反。

## example

```Plain Text
MySQL > SELECT REVERSE('hello');
+------------------+
| REVERSE('hello') |
+------------------+
| olleh            |
+------------------+
1 row in set (0.00 sec)

MySQL > SELECT REVERSE('你好');
+------------------+
| REVERSE('你好')  |
+------------------+
| 好你             |
+------------------+
1 row in set (0.00 sec)
```

## keyword

REVERSE
