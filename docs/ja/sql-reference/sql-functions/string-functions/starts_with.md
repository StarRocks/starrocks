---
displayed_sidebar: docs
---

# starts_with

この関数は、文字列が指定されたプレフィックスで始まる場合に 1 を返します。それ以外の場合は 0 を返します。引数が NULL の場合、結果は NULL です。

## Syntax

```Haskell
BOOLEAN starts_with(VARCHAR str, VARCHAR prefix)
```

## Examples

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