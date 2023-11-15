# utc_timestamp

## description

### Syntax

```Haskell
DATETIME UTC_TIMESTAMP()
```

This function returns the current UTC date and time as a value in 'YYYY-MM-DD HH:MM:SS' or 'YYYYMMDDHHMMSS' format depending on the usage of the function i.e. in a string or numeric context.

## example

```Plain Text
MySQL > select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

## keyword

UTC_TIMESTAMP,UTC,TIMESTAMP
