# date_trunc

## Background

When using the time function, users often need to group time by hour / week and want to display it directly in time format and in the first column. They need a function similar to oracle trunc to effectively truncate the datetime so as to avoid writing sql as ineffective as below:

```sql
select ADDDATE(DATE_FORMAT(DATE_ADD(from_unixtime(`timestamp`), INTERVAL 8 HOUR),
                           '%Y-%m-%d %H:%i:00'),
               INTERVAL 0 SECOND),
    count()
from xxx group 1 ;
```

## Motive

To truncate time more efficiently and to provide vectorized date_trunc functions.

We already have time functions that directly truncate year/month/day.

Similarly, we truncate the high bits of datetime using data_trunc

```Plain Text
date_trunc("minute", datetime):
2020-11-04 11:12:19 => 2020-11-04 11:12:00
```

## Function Signature

1. Oracle uses the function format of TRUNC(date,[fmt]).
2. PostgreSQL/redshift uses the function format of date_trunc(text,time).
3. StarRocks uses the function format of date_trunc([fmt], datetime).

## Function Implementation

Take two easy steps to create date_trunc:

1. Break down the datetime into small parts (Year, Month, Day/Hour, Minute, Second) and extract the parts you need
2. Based on the parts extracted, create a new datetime

Special calculation is needed for week/quarter.

Also, you need to study the week in snowflake.

```SQL
--set the first day of the week in snowflake

--Set Monday as the first day of the week for these two according to the default. 
alter session set week_start = 0
alter session set week_start = 1

--Set Wednesday as the first day of the week
alter session set week_start = 3
```

## description

### Syntax

```Haskell
DATETIME date_trunc(VARCHAR fmt, DATETIME datetime)
```

Truncate datetime into fmt format.

- Fmt supports string literals. But they must be several fixed values: second, minute, hour, day, month, year, week, quarter). Wrong values inputted will be returned as error information by FE analysis.

- The string literals inputted in datetime will be identified. This process happens only once.

  | strings in fmt format | Meaning                                    | example                                    |
  | --------------------- | ------------------------------------------ | ------------------------------------------ |
  | second                | round down to second                       | 2020-10-25 11:15:32 => 2020-10-25 11:15:32 |
  | minute                | round down to minute                       | 2020-11-04 11:12:19 => 2020-11-04 11:12:00 |
  | hour                  | round down to hour                         | 2020-11-04 11:12:13 => 2020-11-04 11:00:00 |
  | day                   | round down to day                          | 2020-11-04 11:12:05 => 2020-11-04 00:00:00 |
  | month                 | round down to the first day of the month   | 2020-11-04 11:12:51 => 2020-11-01 00:00:00 |
  | year                  | round down to the first day of the year    | 2020-11-04 11:12:00 => 2020-01-01 00:00:00 |
  | week                  | round down to the first day of the week    | 2020-11-04 11:12:00 => 2020-11-02 00:00:00 |
  | quarter               | round down to the first day of the quarter | 2020-06-23 11:12:00 => 2020-04-01 00:00:00 |

## example

```Plain Text
MySQL > select date_trunc("hour", "2020-11-04 11:12:13")

2020-11-04 11:00:00
```

## keyword

DATE_TRUNC,DATE
