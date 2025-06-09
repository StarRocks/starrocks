---
displayed_sidebar: docs
---

# lower

## Description

引数内のすべての文字列を小文字に変換します。

## Syntax

```Haskell
INT lower(VARCHAR str)
```

## Examples

```Plain Text
MySQL > SELECT lower("AbC123");
+-----------------+
| lower('AbC123') |
+-----------------+
| abc123          |
+-----------------+
```

## keyword

LOWER