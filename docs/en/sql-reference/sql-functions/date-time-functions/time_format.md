---
displayed_sidebar: docs
---

# time_format



The TIME_FORMAT() function formats the time in the specified format.

* The `date` parameter is a legal date.
* `format` Specifies the output format of the date/time.

The formats that can be used are:

```Plain Text
%f	Microseconds (000000 to 999999)
%H	Hour (00 to 23)
%h	Hour (00 to 12)
%i	Minutes (00 to 59)
%p	AM or PM
%S	Seconds (00 to 59)
%s	Seconds (00 to 59)
```

## Syntax

```Haskell
VARCHAR TIME_FORMAT(TIME time, VARCHAR format)
```

## Examples

```Plain Text
mysql> SELECT TIME_FORMAT("19:30:10", "%h %i %s %p");
+----------------------------------------+
| time_format('19:30:10', '%h %i %s %p') |
+----------------------------------------+
| 12 00 00 AM                            |
+----------------------------------------+
1 row in set (0.01 sec)

```
