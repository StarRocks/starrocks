---
displayed_sidebar: docs
---

# str_to_jodatime

## Description

Converts a Joda-formatted string into a DATETIME value in the specified Joda DateTime format like `yyyy-MM-dd HH:mm:ss`.

## Syntax

```Haskell
DATETIME str_to_jodatime(VARCHAR str, VARCHAR format)
```

## Parameters

- `str`: the time expression you want to convert. It must be of the VARCHAR type.
- `format`: the Joda DateTime format of the DATETIME value to be returned. For information about the formats available, see [Joda DateTime](https://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html).

## Return value

- If parsing the input string succeeds, a DATETIME value is returned.
- If parsing the input string fails, `NULL` is returned.

## Examples

Example 1: Convert the string `2014-12-21 12:34:56` into a DATETIME value in `yyyy-MM-dd HH:mm:ss` format.

```SQL
MySQL > select str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss');
+--------------------------------------------------------------+
| str_to_jodatime('2014-12-21 12:34:56', 'yyyy-MM-dd HH:mm:ss') |
+--------------------------------------------------------------+
| 2014-12-21 12:34:56                                          |
+--------------------------------------------------------------+
```

Example 2: Convert the string `21/December/23 12:34:56` with a text-style month into a DATETIME value in `dd/MMMM/yy HH:mm:ss` format.

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss');
+------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56', 'dd/MMMM/yy HH:mm:ss') |
+------------------------------------------------------------------+
| 2023-12-21 12:34:56                                              |
+------------------------------------------------------------------+
```

Example 3: Convert the string `21/December/23 12:34:56.123` accurate to milliseconds into a DATETIME value in `dd/MMMM/yy HH:mm:ss.SSS` format.

```SQL
MySQL > select str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS');
+--------------------------------------------------------------------------+
| str_to_jodatime('21/December/23 12:34:56.123', 'dd/MMMM/yy HH:mm:ss.SSS') |
+--------------------------------------------------------------------------+
| 2023-12-21 12:34:56.123000                                               |
+--------------------------------------------------------------------------+
```

## Keywords

STR_TO_JODATIME, DATETIME
