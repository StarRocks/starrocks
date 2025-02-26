---
displayed_sidebar: docs
---

# null_or_empty

この関数は、文字列が空または NULL の場合に true を返します。それ以外の場合は false を返します。

## Syntax

```Haskell
BOOLEAN NULL_OR_EMPTY (VARCHAR str)
```

## 例

```Plain Text
MySQL > select null_or_empty(null);
+---------------------+
| null_or_empty(NULL) |
+---------------------+
|                   1 |
+---------------------+

MySQL > select null_or_empty("");
+-------------------+
| null_or_empty('') |
+-------------------+
|                 1 |
+-------------------+

MySQL > select null_or_empty("a");
+--------------------+
| null_or_empty('a') |
+--------------------+
|                  0 |
+--------------------+
```

## キーワード

NULL_OR_EMPTY