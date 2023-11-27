---
displayed_sidebar: "English"
---

# date_trunc

## Description

Truncates a time value based on the specified date part, such as year, day, hour, or minute.

StarRocks also provides the year, quarter, month, week, day, and hour functions for you to extract the specified date part.

## Syntax

```Haskell
DATETIME date_trunc(VARCHAR fmt, DATETIME|DATE datetime)
```

## Parameters

- `datetime`: the time to truncate, which can be of the DATETIME or DATE type. The date and time must exist. Otherwise, NULL will be returned. For example, `2021-02-29 11:12:13` does not exist as a date and NULL will be returned.

- `fmt`: the date part, that is, to which precision `datetime` will be truncated. The value must be a VARCHAR constant.

  `fmt` must be set to a value listed in the following table. If the value is incorrect, an error will be returned.

  If `datetime` is a DATE value, `fmt` can only be `year`，`quarter`，`month`，`week`，or `day`. If you set `fmt` to other date units, for example `hour`, an error is reported. See Example 5.

| Value   | Description                                                  |
| ------- | ------------------------------------------------------------ |
| second  | Truncates to the second.                                     |
| minute  | Truncates to the minute. The second part will be zero out.   |
| hour    | Truncates to the hour. The minute and second parts will be zero out. |
| day     | Truncates to the day. The time part will be zero out.        |
| week    | Truncates to the first date of the week that `datetime` falls in. The time part will be zero out. |
| month   | Truncates to the first date of the month that `datetime` falls in. The time part will be zero out. |
| quarter | Truncates to the first date of the quarter that `datetime` falls in. The time part will be zero out. |
| year    | Truncates to the first date of the year that `datetime` falls in. The time part will be zero out. |

## Return value

Returns a value of the DATETIME type.

If `datetime` is of the DATE type and `fmt` is set to `hour`, `minute`, or `second`, the time part of the returned value defaults to `00:00:00`.

## Examples

Example 1: Truncate the input time to the minute.

```Plain
select date_trunc("minute", "2020-11-04 11:12:13");
+---------------------------------------------+
| date_trunc('minute', '2020-11-04 11:12:13') |
+---------------------------------------------+
| 2020-11-04 11:12:00                         |
+---------------------------------------------+
```

Example 2: Truncate the input time to the hour.

```Plain
select date_trunc("hour", "2020-11-04 11:12:13");
+-------------------------------------------+
| date_trunc('hour', '2020-11-04 11:12:13') |
+-------------------------------------------+
| 2020-11-04 11:00:00                       |
+-------------------------------------------+
```

Example 3: Truncate the input time to the first day of a week.

```Plain
select date_trunc("week", "2020-11-04 11:12:13");
+-------------------------------------------+
| date_trunc('week', '2020-11-04 11:12:13') |
+-------------------------------------------+
| 2020-11-02 00:00:00                       |
+-------------------------------------------+
```

Example 4: Truncate the input time to the first day of a year.

```Plain
select date_trunc("year", "2020-11-04 11:12:13");
+-------------------------------------------+
| date_trunc('year', '2020-11-04 11:12:13') |
+-------------------------------------------+
| 2020-01-01 00:00:00                       |
+-------------------------------------------+
```

Example 5: Truncate a DATE value to the hour. An error is returned.

```Plain
select date_trunc("hour", cast("2020-11-04" as date));

ERROR 1064 (HY000): Getting analyzing error from line 1, column 26 to line 1, column 51. Detail message: date_trunc function can't support argument other than year|quarter|month|week|day.
```
