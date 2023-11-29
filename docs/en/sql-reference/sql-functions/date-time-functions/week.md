---
displayed_sidebar: "English"
---

# week

## Description

Returns the week number for a given date. This function works in the same way as the WEEK function in MySQL.

This function is supported from v2.3.

## Syntax

```Haskell
INT WEEK(DATETIME|DATE date, INT mode)
```

## Parameters

- `Date`: The supported data types are DATETIME and DATE.

- `Mode`: optional. The supported data type is INT. This parameter is used to specify the logic for calculating the week number, that is, whether the week starts on Sunday or Monday, and whether the return value is in the range of 0~53 or 1~53. Value range: 0~7. Default value: `0`. If this parameter is not specified, mode `0` is used by default. The following table describes how this parameter works.

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

Calculate the week number for `2007-01-01`. `2007-01-01` is Monday on the calendar.

- `Mode` is set to `0` and `0` is returned. The week starts on Sunday. `2007-01-01` is Monday and cannot be week 1. Therefore, `0` is returned.

```Plaintext
mysql> SELECT WEEK('2007-01-01', 0);
+-----------------------+
| week('2007-01-01', 0) |
+-----------------------+
|                     0 |
+-----------------------+
1 row in set (0.02 sec)
```

- `Mode` is set to `1` and `1` is returned. The week starts on Monday and `2007-01-01` is Monday.

```Plaintext
mysql> SELECT WEEK('2007-01-01', 1);
+-----------------------+
| week('2007-01-01', 1) |
+-----------------------+
|                     1 |
+-----------------------+
1 row in set (0.02 sec)
```

- `Mode` is set to `2` and `53` is returned. The week starts on Sunday. However, `2007-01-01` is Monday and the value range is 1~53. Therefore, `53` is returned, indicating that this is the last week of the previous year.

```Plaintext
mysql> SELECT WEEK('2007-01-01', 2);
+-----------------------+
| week('2007-01-01', 2) |
+-----------------------+
|                    53 |
+-----------------------+
1 row in set (0.01 sec)
```
