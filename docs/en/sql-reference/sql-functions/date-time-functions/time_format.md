---
displayed_sidebar: docs
---

# time_format



Formats TIME-type values in the specified format.

## Syntax

```Haskell
VARCHAR TIME_FORMAT(TIME time, VARCHAR format)
```

## Parameters

- `time` (Required): The time value (of the TIME type) to be formatted.
- `format` (Required): The format to use. Valid values:

```Plain Text
%f	Microseconds (000000 to 999999)
%H	Hour (00 to 23)
%h	Hour (00 to 12)
%i	Minutes (00 to 59)
%p	AM or PM
%S	Seconds (00 to 59)
%s	Seconds (00 to 59)
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
