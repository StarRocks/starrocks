---
displayed_sidebar: docs
---

# ends_with

文字列が指定されたサフィックスで終わる場合、`true` を返します。それ以外の場合は、`false` を返します。引数が NULL の場合、結果は NULL です。

## Syntax

```Haskell
BOOLEAN ENDS_WITH (VARCHAR str, VARCHAR suffix)
```

## Examples

```Plain Text
MySQL > select ends_with("Hello starrocks", "starrocks");
+-----------------------------------+
| ends_with('Hello starrocks', 'starrocks') |
+-----------------------------------+
|                                 1 |
+-----------------------------------+

MySQL > select ends_with("Hello starrocks", "Hello");
+-----------------------------------+
| ends_with('Hello starrocks', 'Hello') |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

## keyword

ENDS_WITH