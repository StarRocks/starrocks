# to_date

## description

### Syntax

```Haskell
DATE TO_DATE(DATETIME datetime)
```

to_date returns the date value from DATETIME type.

## example

```Plain Text
MySQL > select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```

## keyword

TO_DATE
