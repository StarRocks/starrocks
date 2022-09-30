# version

## Description

Returns the current version of the MySQL database.

## Syntax

```Haskell
VARCHAR version();
```

## Parameters

None

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```
