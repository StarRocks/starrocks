# day

## description

### Syntax

```Haskell
INT DAY(DATETIME date)
```

Obtain the day information in the date and return values range from 1 to 31.

The parameter is Date or Datetime type.

## example

```Plain Text
MySQL > select day('1987-01-31');
+----------------------------+
| day('1987-01-31 00:00:00') |
+----------------------------+
|                         31 |
+----------------------------+
```

## keyword

DAY
