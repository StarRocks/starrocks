# from_unixtime

## Description

Converts a UNIX timestamp into the required time format. The default format is `yyyy-MM-dd HH:mm:ss`. It also supports the formats in [date_format](./date_format.md).

## Syntax

```sql
DATETIME FROM_UNIXTIME(INT unix_timestamp[, VARCHAR string_format])`
```

## Parameters

- `unix_timestamp`: the UNIX timestamp you want to convert.
- `string_format`: the required time format.

Currently, `string_format` supports the following formats:

```plain text
%Y：Year  e.g.：2014，1900
%m：Month   e.g.：12，09
%d：Day  e.g.：11，01
%H：Hour  e.g.：23，01，12
%i：Minute  e.g.：05，11
%s：Second  e.g.：59，01
```

Other formats are invalid and NULL will be returned.

If the specified timstamp is less than 0 or greater than 253402271999, NULL will be returned. The range for timestamp is `1970-01-01 00:00:00` to `9999-12-31 23:59:59`.

## Examples

```plain text
MySQL > select from_unixtime(1196440219);
+---------------------------+
| from_unixtime(1196440219) |
+---------------------------+
| 2007-12-01 00:30:19       |
+---------------------------+

MySQL > select from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------+
| from_unixtime(1196440219, 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+

MySQL > select from_unixtime(1196440219, '%Y-%m-%d');
+-----------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d')   |
+-----------------------------------------+
| 2007-12-01                              |
+-----------------------------------------+

MySQL > select from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s');
+--------------------------------------------------+
| from_unixtime(1196440219, '%Y-%m-%d %H:%i:%s')   |
+--------------------------------------------------+
| 2007-12-01 00:30:19                              |
+--------------------------------------------------+
```

## keyword

FROM_UNIXTIME,FROM,UNIXTIME
