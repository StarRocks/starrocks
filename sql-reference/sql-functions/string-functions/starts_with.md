# starts_with

## description

### Syntax

```Haskell
BOOLEAN starts_with(VARCHAR str, VARCHAR prefix)
```

如果字符串以指定前缀开头，返回 1; 否则返回 0. 任意参数为NULL，返回NULL

## example

```Plain Text
mysql> select starts_with("hello world","hello");
+-------------------------------------+
|starts_with('hello world', 'hello')  |
+-------------------------------------+
| 1                                   |
+-------------------------------------+

mysql> select starts_with("hello world","world");
+-------------------------------------+
|starts_with('hello world', 'world')  |
+-------------------------------------+
| 0                                   |
+-------------------------------------+
```

## keyword

START_WITH
