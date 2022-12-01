# strright

## Description

This function extracts a number of characters from a string with specified length (starting from right). The unit for length: utf-8 character.

## Syntax

```Haskell
VARCHAR strright(VARCHAR str,INT len)
```

## Examples

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
