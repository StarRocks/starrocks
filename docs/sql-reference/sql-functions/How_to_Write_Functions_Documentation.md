# convert_tz

> - *This article uses* `*CONVERT_TZ*` *as an example to illustrate the requirements for writing functions documentation.*
> - If the documentation cites the following syntax and examples, please use code blocks.

## Description

Convert a date and time from one time zone to another.

> *What does this function do.*

## Syntax

```sql
CONVERT_TZ(dt, from_tz, to_tz)
```

> *The syntax of this function. Enclose the syntax in code blocks.*

## Parameters

- `dt` : the data and time to convert. DATETIME is supported.

- `from_tz` : the source time zone. VARCHAR is supported. The time zone can be represented in two  formats: one is Time Zone Database (for example, Asia/Shanghai), the other is UTC offset (for example, +08:00).

- `to_tz`:  the destination time zone. VARCHAR is supported. Its format is the same as `from_tz`.

> - *The format is* `*parameter*`*:* `*description*` *+* `*data type*` *(please list all the data types supported).*
> - *The description must include information, such as parameter description, value format, value range, whether this parameter is required, formats and units when different data types are used.*
> - *The data types must be capitalized, for example, DATETIME.*

## Return value

Returns a value of the DATETIME data type.

> - *The data type must be capitalized.*
> - *Additional notes can be provided if necessary, for example,*
> - *Types of the return value if the input value supports multiple data types.*
> - *Type of the error returned (for example, NULL) if the data type of the input value is not supported.*

## Usage notes

For the Time Zone Database, please refer to [tz database time zones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) (in Wikipedia).

> *You can also add some notes or precautions for using this function. (optional)*

## Examples

Example 1: Convert a date and time in Shanghai to Los_Angeles.

```Plain_Text
MySQL > select convert_tz('2019-08-01 13:21:03', 'Asia/Shanghai', 'America/Los_Angeles');
        -> 2019-07-31 22:21:03                                                       |
```

Example 2: Convert a date and time in UTC+08:00 to Los_Angeles.

```Plain_Text
MySQL > select convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles');
+--------------------------------------------------------------------+
| convert_tz('2019-08-01 13:21:03', '+08:00', 'America/Los_Angeles') |
+--------------------------------------------------------------------+
| 2019-07-31 22:21:03                                                |
+--------------------------------------------------------------------+
```

> - *Enclose the code of different use scenarios in different code blocks.*
> - *Describe the* *scenario* *of each example.*
> - *If you need to describe more than one scenario in an example, add a comment for each scenario to help* *users quickly distinguish between them.*
> - *If the returned result is simple, the format in example* *1* *is recommended. Otherwise, the format in example 2 is recommended.*
