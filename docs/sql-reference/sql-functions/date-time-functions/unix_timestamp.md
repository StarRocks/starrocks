# unix_timestamp

## description

### Syntax

```Haskell
INT UNIX_TIMESTAMP()
INT UNIX_TIMESTAMP(DATETIME date)
INT UNIX_TIMESTAMP(DATETIME date, STRING fmt)
```

unix_timestamp converts Date or Datetime type to unix timestamp.

Without a parameter, it will convert the current time to unix timestamp.

Parameters must be Date or Datetime type.

For time before 1970-01-01 00:00:00 or after 2038-01-19 03:14:07, it returns 0.

Please refer to "date_format" for information about Format.

This function may return different results across different time zones.

## example

```Plain Text
MySQL > select unix_timestamp();
+------------------+
| unix_timestamp() |
+------------------+
|       1558589570 |
+------------------+

MySQL > select unix_timestamp('2007-11-30 10:30:19');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30:19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30-19', '%Y-%m-%d %H:%i-%s');
+---------------------------------------+
| unix_timestamp('2007-11-30 10:30-19') |
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('2007-11-30 10:30%3A19', '%Y-%m-%d %H:%i%%3A%s');
+---------------------------------------+
|unix_timestamp('2007-11-30 10:30%3A19')|
+---------------------------------------+
|                            1196389819 |
+---------------------------------------+

MySQL > select unix_timestamp('1969-01-01 00:00:00');
+---------------------------------------+
| unix_timestamp('1969-01-01 00:00:00') |
+---------------------------------------+
|                                     0 |
+---------------------------------------+
```

## keyword

UNIX_TIMESTAMP,UNIX,TIMESTAMP
