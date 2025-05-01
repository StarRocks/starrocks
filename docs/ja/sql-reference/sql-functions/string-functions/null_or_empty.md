---
displayed_sidebar: docs
---

# null_or_empty

## 説明

この関数は、文字列が空またはNULLのときにtrueを返します。それ以外の場合はfalseを返します。

## 構文

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