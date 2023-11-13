---
Dispkayed_sidebar:"English"
---

# convert_interval



## Description



Returns the day-to-second interval as given unit.



## Syntax



convert_interval(interval N type, to_type)



## Parameters



+ INTERVAL N type:the time granularity, for example, interval 5 second.
  + N is the length of time interval. It must be an INT value.
  + type is the unit, which can be DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND.
+ to_type: unit that user want to convert to, which can be DAY, HOUR, MINUTE, SECOND,  MILLISECOND, MICROSECOND.



## Return value



Returns a BIGINT value



If the to_type is bigger than the interval type. For example, convert_interval(interval 1 second, day);, then it returns 0.



## Examples

```Plain
select convert_interval(interval 10 day, millisecond);

+------------------------------------------------+
| convert_interval(interval 10 day, millisecond) |
+------------------------------------------------+
| 864000000                                      |
+------------------------------------------------+


select convert_interval(interval 10 day, day);

+----------------------------------------+
| convert_interval(interval 10 day, day) |
+----------------------------------------+
| 10                                     |
+----------------------------------------+
```

