# strright

## description

### Syntax

```Haskell
VARCHAR strright(VARCHAR str,INT len)
```

This function extracts a number of characters from a string with specified length (starting from right). The unit for length: utf8 character.

## example

```Plain Text
MySQL > select strright("Hello starrocks",5);
+--------------------------+
|strright('Hello starrocks', 5)|
+--------------------------+
| starrocks                    |
+--------------------------+
```

## keyword

STRRIGHT
