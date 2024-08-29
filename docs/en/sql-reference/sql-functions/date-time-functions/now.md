---
displayed_sidebar: docs
---

# now, current_timestamp, localtime, localtimestamp

## Description

Returns the current date and time.

From 3.1.6 onwards, this function can accept a precision input (`p`), which represents the number of digits to retain after seconds. A maximum of 6 digits can be retained (accurate to the microsecond). The digits after `p` are padded with 0. If `p` is not specified, a DATETIME value accurate to the second is returned.

The following example returns date and time values when different precision is specified.

```plaintext
mysql > select now(),now(1),now(2),now(3),now(4),now(5),now(6)\G
*************************** 1. row ***************************
 now(): 2023-12-08 13:46:45
now(1): 2023-12-08 13:46:45.100000
now(2): 2023-12-08 13:46:45.110000
now(3): 2023-12-08 13:46:45.115000
now(4): 2023-12-08 13:46:45.115800
now(5): 2023-12-08 13:46:45.115840
now(6): 2023-12-08 13:46:45.115843
```

This function may return different results for different time zones. For more information, see [Configure a time zone](../../../administration/management/timezone.md).

## Syntax

```Haskell
DATETIME NOW()
DATETIME NOW(INT p)
```

## Parameters

`p`: optional, the specified precision, that is, the number of digits to retain after seconds. It must be an INT value within the range of [1,6]. `select now(0)` is equivalent to `select now()`.

## Return value

- If `p` is not specified, this function returns a DATETIME value accurate to the second.
- If `p` is specified, this function returns a date and time value of the specified precision.

## Examples

```Plain Text
MySQL > select now();
+---------------------+
| now()               |
+---------------------+
| 2019-05-27 15:58:25 |
+---------------------+

MySQL > select now(),now(1),now(2),now(3),now(4),now(5),now(6)\G
*************************** 1. row ***************************
 now(): 2023-12-08 13:46:45
now(1): 2023-12-08 13:46:45.100000
now(2): 2023-12-08 13:46:45.110000
now(3): 2023-12-08 13:46:45.115000
now(4): 2023-12-08 13:46:45.115800
now(5): 2023-12-08 13:46:45.115840
now(6): 2023-12-08 13:46:45.115843
```

## keyword

NOW, now
