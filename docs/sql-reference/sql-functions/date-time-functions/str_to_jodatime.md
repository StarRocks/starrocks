---
displayed_sidebar: "English"
---

# str_to_jodatime

## Description

Converts a joda-formatted string into a DATETIME value according to the specified format.

The format is [Joda DateTime](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html), which is like 'yyyy-MM-dd HH:mm:ss'.

## Syntax

```Haskell
DATETIME str_to_jodatime(VARCHAR str, VARCHAR format)
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
MySQL > select str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------------------+
| str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+
```


Example 2: Convert the input into a DATETIME value with text-style month

```Plain Text
MySQL > select str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss');
+------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss') |
+------------------------------------------------------------------+
| 2023-12-21 12:34:56                                              |
+------------------------------------------------------------------+
```


Example 3: Convert the input into a DATETIME value with milliseconds precision

```Plain Text
MySQL root@127.1:(none)> select str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS');
+--------------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS') |
+--------------------------------------------------------------------------+
| 2023-12-21 12:34:56.123000                                               |
+--------------------------------------------------------------------------+
```


## keyword

str_to_jodatime, DATETIME
