---
displayed_sidebar: docs
---

# timestamp

## Description

Returns the DATETIME value of a date or datetime expression.

## Syntax

```Haskell
DATETIME timestamp(DATETIME|DATE expr);
```

## Parameters

`expr`: the time expression you want to convert. It must be of the DATETIME or DATE type.

## Return value

Returns a DATETIME value. If the input time is empty or does not exist, such as `2021-02-29`, NULL is returned.

## Examples

```Plain Text
select timestamp("2019-05-27");
+-------------------------+
| timestamp('2019-05-27') |
+-------------------------+
| 2019-05-27 00:00:00     |
+-------------------------+
1 row in set (0.00 sec)
```
