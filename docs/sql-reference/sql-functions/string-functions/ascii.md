# ascii

## description

### Syntax

```Haskell
INT ascii(VARCHAR str)
```

This function returns the ascii value of the leftmost character of a given string.

## example

```Plain Text
MySQL > select ascii('1');
+------------+
| ascii('1') |
+------------+
|         49 |
+------------+

MySQL > select ascii('234');
+--------------+
| ascii('234') |
+--------------+
|           50 |
+--------------+
```

## keyword

ASCII
