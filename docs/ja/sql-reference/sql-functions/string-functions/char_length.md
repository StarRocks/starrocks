---
displayed_sidebar: docs
---

# char_length

この関数は文字列の長さを返します。マルチバイト文字の場合、文字数を返します。現在、utf8 コーディングのみをサポートしています。注意: この関数は character_length とも呼ばれます。

## Syntax

```Haskell
INT char_length(VARCHAR str)
```

## Examples

```Plain Text
MySQL > select char_length("abc");
+--------------------+
| char_length('abc') |
+--------------------+
|                  3 |
+--------------------+
```

## keyword

CHAR_LENGTH, CHARACTER_LENGTH