# md5sum

Calculates the 128-bit checksum of multiple strings. The checksum is represented by a 32-character hexadecimal string.

If only one string is passed in, the result is the same as that of [md5](md5.md).

Compared to md5(), md5sum() accepts more parameters and therefore, is more efficient in calculating the checksum of multiple files.

## Syntax

```Haskell
md5sum(expr,...);
```

## Parameters

`expr`: the string to calculate. It must be of the VARCHAR type.

## Return value

Returns a checksum of the VARCHAR type.

## Examples

```Plain Text
mysql> select md5sum("starrocks");
+----------------------------------+
| md5sum('starrocks')              |
+----------------------------------+
| f75523a916caf65f1ad487a9f8017f75 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum("starrocks","star");
+----------------------------------+
| md5sum('starrocks', 'star')      |
+----------------------------------+
| 7af4bfe35b8df2786ad133c57cb2aed8 |
+----------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum(null);
+----------------------------------+
| md5sum(NULL)                     |
+----------------------------------+
| d41d8cd98f00b204e9800998ecf8427e |
+----------------------------------+
1 row in set (0.01 sec)
```
