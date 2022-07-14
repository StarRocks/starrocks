# starts_with

## description

### Syntax

```Haskell
BOOLEAN starts_with(VARCHAR str, VARCHAR prefix)
```

This function returns 1 when a string starts with a specified prefix. Otherwise, it returns 0. When the argument is NULL, the result is NULL.

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
