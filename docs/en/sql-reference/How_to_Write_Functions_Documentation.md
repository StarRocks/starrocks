---
displayed_sidebar: docs
unlisted: true
---

# convert_tz

> *- When you write new function docs, please provide complete example data so users can test it, including the CREATE TABLE, INSERT/LOAD data examples.*
> *- This article uses `CONVERT_TZ` as an example to illustrate the requirements for writing function documentation.*

## Description

Converts a datetime value from one time zone to another.

This function is supported from v2.0.

> *What does this function do, its usage scenario, some details, difference with a similar function, the supported version.*

## Syntax

```Haskell
DATETIME CONVERT_TZ(DATETIME dt, VARCHAR from_tz, VARCHAR to_tz)
```

> *The syntax of this function. Enclose the syntax in code blocks.*

## Parameters

- `dt`: the datetime value to convert. DATETIME is supported.

- `from_tz`: the source time zone. VARCHAR is supported. The time zone can be represented in two  formats: one is Time Zone Database (for example, Asia/Shanghai), the other is UTC offset (for example, +08:00).

- `to_tz`: the destination time zone. VARCHAR is supported. Its format is the same as `from_tz`.

> - The format is `parameter`:`description`+`data type` (list all the data types supported).
> - The description must include information, such as parameter description, value format, value range, whether this parameter is required, formats and units when different data types are used.
> - The data types must be capitalized, for example, DATETIME.

## Return value

Returns a value of the DATETIME data type.

> *Additional notes can be provided if necessary:*
>
> - *Types of the return value if the input value supports multiple data types.*
> - *Type of the error returned (for example, NULL) if the data type of the input value is not supported.*

## Usage notes

For the Time Zone Database, please refer to [tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) (in Wikipedia).

> *You can also add some notes or precautions for using this function. (optional)*

## Examples

Example 1: Convert a date and time in Shanghai to Los_Angeles.

```plaintext
MySQL > select convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles');
        -> 2019-07-31 22:21:03                                                       |
```

Example 2: Convert a date and time in UTC+08:00 to Los_Angeles.

```plaintext
MySQL > select convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles');
+--------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles') |
+--------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                |
+--------------------------------------------------------------------+
```

> - *Provide common examples for using this function and describe the purpose of each example to help users quickly understand the example.*
> - *Please include complete data examples so users can test, including CREATE TABLE, INSERT/LOAD, and query.*
> - *If you need to describe more than one scenario in an example, add a comment for each scenario to help users understand.*
> - *If the example is complex, explain the return result.*
