# current_version

## Description

Returns the current version of StarRocks. Two syntaxes are provided for compatibility with different clients.

## Syntax

```Haskell
current_version();

@@version_comment;
```

## Parameters

None

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select current_version();
+-------------------+
| current_version() |
+-------------------+
| 2.1.2 0782ad7     |
+-------------------+
1 row in set (0.00 sec)

mysql> select @@version_comment;
+-------------------------+
| @@version_comment       |
+-------------------------+
| StarRocks version 2.1.2 |
+-------------------------+
1 row in set (0.01 sec)
```
