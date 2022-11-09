# md5sum_numeric

Calculates the 128-bit checksum of multiple strings. The checksum is represented by a decimal string of digits.

## Syntax

```Haskell
LARGEINT md5sum_numeric(VARCHAR expr,...);
```

## Parameters

`expr`: the string to calculate. It must be of the VARCHAR type.

## Return value

Returns a checksum of the LARGEINT type. If the input is empty, an error is returned.

## Examples

```Plain Text
mysql> select md5sum_numeric("starrocks");
+-----------------------------------------+
| md5sum_numeric('starrocks')             |
+-----------------------------------------+
| 328761383472799310362963866384446095221 |
+-----------------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum_numeric("starrocks","star");
+-----------------------------------------+
| md5sum_numeric('starrocks', 'star')     |
+-----------------------------------------+
| 163436627872604162110984707546327199448 |
+-----------------------------------------+
1 row in set (0.01 sec)

mysql> select md5sum_numeric(null);
+-----------------------------------------+
| md5sum_numeric(NULL)                    |
+-----------------------------------------+
| 281949768489412648962353822266799178366 |
+-----------------------------------------+
1 row in set (0.01 sec)
```
