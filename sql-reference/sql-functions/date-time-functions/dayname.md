# dayname

## description

### Syntax

```Haskell
VARCHAR DAYNAME(DATE)
```

Return date name corresponding to the date.

The parameter is Date or Datetime type.

## example

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```

## keyword

DAYNAME
