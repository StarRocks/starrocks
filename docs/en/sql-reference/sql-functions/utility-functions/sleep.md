---
displayed_sidebar: "English"
---

# sleep

## Description

Delays the execution of an operation for a specified period of time (in seconds) and returns a BOOLEAN value to indicate whether the sleep is completed without interruption. `1` is returned if the sleep is completed without interruption. Otherwise, `0` is returned.

## Syntax

```Haskell
BOOLEAN sleep(INT x);
```

## Parameters

`x`: the duration for which you want to delay the execution of an operation. It must be of the INT type. Unit: seconds. If the input is is NULL, NULL is returned immediately without sleeping.

## Return value

Returns a value of the BOOLEAN type.

## Examples

```Plain Text
select sleep(3);
+----------+
| sleep(3) |
+----------+
|        1 |
+----------+
1 row in set (3.00 sec)

select sleep(NULL);
+-------------+
| sleep(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.00 sec)
```

## Keywords

SLEEP, sleep
