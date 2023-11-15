# lcase

## description

### Syntax

```Haskell
VARCHAR lcase(VARCHAR str)
```

This function converts a string to lower-case. It is analogous to the function lower.

## example

```Plain Text
mysql> SELECT lcase("AbC123");
+-----------------+
|lcase('AbC123')  |
+-----------------+
|abc123           |
+-----------------+
```

## keyword

LCASE
