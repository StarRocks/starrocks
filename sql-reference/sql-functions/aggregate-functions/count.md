
# COUNT

## description

### Syntax

`COUNT([DISTINCT] expr)`

It is used to return the number of rows that satisfy the requirements.

## example

```plain text

MySQL > select count(*)
from log_statis
group by datetime;
+----------+
| count(*) |
+----------+
| 28515903 |
+----------+


MySQL > select count(datetime)
from log_statis
group by datetime;
+-------------------+
| count(`datetime`) |
+-------------------+
|         28521682  |
+-------------------+

MySQL > select count(distinct datetime)
from log_statis
group by datetime;
+----------------------------+
| count(DISTINCT `datetime`) |
+----------------------------+
|                    71045   |
+----------------------------+
```

## keyword

COUNT
