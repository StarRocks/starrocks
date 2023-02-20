# date_format

## Description

Converts a date into a string according to the specified format. Currently it supports strings with a maximum of 128 bytes. If the length of the returned value exceeds 128, NULL is returned.

## Syntax

```Haskell
VARCHAR DATE_FORMAT(DATETIME date, VARCHAR format)
```

## Parameters

- The `date` parameter must be a valid date or date expression.

- `format` specifies the output format of the date or time.

Here are the formats available:

```Plain Text
%a | Abbreviated weekday name (Sun to Sat)
%b | Abbreviated month name (Jan to Dec)
%c | Numeric month name (0-12)
%D | Day of the month as a numeric value, followed by suffix in English
%d | Day of the month as a numeric value (00-31)
%e | Day of the month as a numeric value (0-31)
%f | Microseconds
%H | Hour (00-23)
%h | Hour (01-12)
%I | Hour (01-12)
%i | Minutes (00-59)
%j | Day of the year (001-366)
%k | Hour (0-23)
%l | Hour (1-12)
%M | Month name in full
%m | Month name as a numeric value (00-12)
%p | AM or PM
%r | Time in 12 hour（hh:mm:ss AM or PM）
%S | Seconds (00-59)
%s | Seconds (00-59)
%T | Time in 24 hour format (hh:mm:ss)
%U | Week (00-53) where Sunday is the first day of the week
%u | Week (00-53) where Monday is the first day of the week
%V | Week (01-53)  where Sunday is the first day of the week. Used with %X. 
%v | Week (01-53) where Monday is the first day of the week. Used with %x. 
%W | Weekday name in full
%w | Day of the week where Sunday=0 and Saturday=6
%X |  Year for the week where Sunday is the first day of the week. 4-digital value. Used with  %V.
%x | Year for the week where Monday is the first day of the week. 4-digital value. Used with  %v.
%Y | Year. 4-digital value. 
%y | Year. 2-digital value. 
%% |  Represent %. 
```

## Examples

```Plain Text
MySQL > select date_format('2009-10-04 22:23:00', '%W %M %Y');
+------------------------------------------------+
| date_format('2009-10-04 22:23:00', '%W %M %Y') |
+------------------------------------------------+
| Sunday October 2009                            |
+------------------------------------------------+

MySQL > select date_format('2007-10-04 22:23:00', '%H:%i:%s');
+------------------------------------------------+
| date_format('2007-10-04 22:23:00', '%H:%i:%s') |
+------------------------------------------------+
| 22:23:00                                       |
+------------------------------------------------+

MySQL > select date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
+------------------------------------------------------------+
| date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') |
+------------------------------------------------------------+
| 4th 00 Thu 04 10 Oct 277                                   |
+------------------------------------------------------------+

MySQL > select date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
+------------------------------------------------------------+
| date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') |
+------------------------------------------------------------+
| 22 22 10 10:23:00 PM 22:23:00 00 6                         |
+------------------------------------------------------------+

MySQL > select date_format('1999-01-01 00:00:00', '%X %V');
+---------------------------------------------+
| date_format('1999-01-01 00:00:00', '%X %V') |
+---------------------------------------------+
| 1998 52                                     |
+---------------------------------------------+

MySQL > select date_format('2006-06-01', '%d');
+------------------------------------------+
| date_format('2006-06-01 00:00:00', '%d') |
+------------------------------------------+
| 01                                       |
+------------------------------------------+

MySQL > select date_format('2006-06-01', '%%%d');
+--------------------------------------------+
| date_format('2006-06-01 00:00:00', '%%%d') |
+--------------------------------------------+
| %01                                        |
+--------------------------------------------+
```

## keyword

DATE_FORMAT,DATE,FORMAT
