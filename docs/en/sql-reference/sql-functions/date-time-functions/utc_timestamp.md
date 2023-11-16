---
displayed_sidebar: "English"
---

# utc_timestamp

## Description

Returns the current UTC date and time as a value in 'YYYY-MM-DD HH:MM:SS' or 'YYYYMMDDHHMMSS' format depending on the usage of the function, for example, in a string or numeric context.

## Syntax

```Haskell
DATETIME UTC_TIMESTAMP()
```

## Examples

```Plain Text
MySQL > select utc_timestamp(),utc_timestamp() + 1;
+---------------------+---------------------+
| utc_timestamp()     | utc_timestamp() + 1 |
+---------------------+---------------------+
| 2019-07-10 12:31:18 |      20190710123119 |
+---------------------+---------------------+
```

`utc_timestamp() + N` means adding `N` seconds to the current time.

## keyword

UTC_TIMESTAMP,UTC,TIMESTAMP
