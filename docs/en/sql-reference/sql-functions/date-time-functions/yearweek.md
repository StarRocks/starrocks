---
displayed_sidebar: "English"
---

# yearweek

## Description

Returns the week number for a given date. This function works in the same way as the yearweek function in MySQL.

## Syntax

```Haskell
INT YEARWEEK(DATETIME|DATE date, INT mode)
```

## Parameters

- `Date`: The supported data types are DATETIME and DATE.

- `Mode`: optional. The supported data type is INT. This parameter is used to specify the logic for calculating the yearweek number, that is, whether the week starts on Sunday or Monday, and whether the return value is in the range of 0~53 or 1~53. Value range: 0~7. Default value: `0`. If this parameter is not specified, mode `0` is used by default. The following table describes how this parameter works.

| Mode | First day of week | Range | Week 1 is the first week …    |
| :--- | :---------------- | :---- | :---------------------------- |
| 0    | Sunday            | 0-53  | with a Sunday in this year    |
| 1    | Monday            | 0-53  | with 4 or more days this year |
| 2    | Sunday            | 1-53  | with a Sunday in this year    |
| 3    | Monday            | 1-53  | with 4 or more days this year |
| 4    | Sunday            | 0-53  | with 4 or more days this year |
| 5    | Monday            | 0-53  | with a Monday in this year    |
| 6    | Sunday            | 1-53  | with 4 or more days this year |
| 7    | Monday            | 1-53  | with a Monday in this year    |

## Return value

Returns a value of the INT type. Value range: 0~53. The specific range is determined based on the `mode` parameter. `NULL` is returned if the value of `date` is invalid or the input value is empty.

## Examples

Returns year and week for a date.The value of the mode argument defaults to 0

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 0);
+---------------------------+
| yearweek('2007-01-01', 0) |
+---------------------------+
|                    200653 |
+---------------------------+
```

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 1);
+---------------------------+
| yearweek('2007-01-01', 1) |
+---------------------------+
|                    200701 |
+---------------------------+
```

```Plaintext
mysql> SELECT YEARWEEK('2007-01-01', 2);
+---------------------------+
| yearweek('2007-01-01', 2) |
+---------------------------+
|                    200653 |
+---------------------------+
1 row in set (0.01 sec)
```
