---
displayed_sidebar: docs
---

# quarter

## Description

Returns the quarter part of a date, in the range of 1 to 4.

## Syntax

```Haskell
INT quarter(DATETIME|DATE date);
```

## Parameters

`date`: It must be of the DATETIME or DATE type.

## Return value

Returns an INT value.

NULL is returned in any of the following scenarios:

- The date is not a valid DATETIME or DATE value.

- The input is empty.

- The date does not exist, for example, 2022-02-29.

## Examples

Example 1: Return the quarter part of a DATETIME value.

```Plain
SELECT QUARTER("2022-10-09 15:59:33");
+--------------------------------+
| quarter('2022-10-09 15:59:33') |
+--------------------------------+
|                              4 |
+--------------------------------+
```

Example 2: Return the quarter part of a DATE value.

```Plain
SELECT QUARTER("2022-10-09");
+-----------------------+
| quarter('2022-10-09') |
+-----------------------+
|                     4 |
+-----------------------+
```

Example 3: Return the quarter part that corresponds to the current time or date.

```Plain
SELECT QUARTER(NOW());
+----------------+
| quarter(now()) |
+----------------+
|              4 |
+----------------+

SELECT QUARTER(CURDATE());
+--------------------+
| quarter(curdate()) |
+--------------------+
|                  4 |
+--------------------+
```
