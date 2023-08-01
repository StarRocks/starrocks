# parse_datetime

## Description

Converts a string into a DATETIME value according to the specified format.

The format is [Joda DateTime](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), which is like 'yyyy-MM-dd HH:mm:ss'.

## Syntax

```Haskell
DATETIME PARSE_DATETIME(VARCHAR str, VARCHAR format)
```

## Parameters

- `str`: the time expression you want to convert. It must be of the VARCHAR type.
- `format`: the Joda DateTime format

## Return value

- Returns a `DATETIME`` value if parse succeed.  
- Returns `NULL` if parse failed

## Examples

Example 1: Convert the input into a DATETIME value.

```Plain Text
MySQL > select parse_datetime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------------------+
| parse_datetime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+
```


Example 2: Convert the input into a DATETIME value with text-style month

```Plain Text
MySQL > select parse_datetime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss');
+------------------------------------------------------------------+
| parse_datetime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss') |
+------------------------------------------------------------------+
| 2023-12-21 12:34:56                                              |
+------------------------------------------------------------------+
```


Example 3: Convert the input into a DATETIME value with milliseconds precision

```Plain Text
MySQL root@127.1:(none)> select parse_datetime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS');
+--------------------------------------------------------------------------+
| parse_datetime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS') |
+--------------------------------------------------------------------------+
| 2023-12-21 12:34:56.123000                                               |
+--------------------------------------------------------------------------+
```


## keyword

PARSE_DATETIME, DATETIME
