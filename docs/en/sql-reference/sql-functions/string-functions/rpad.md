# rpad

## description

### Syntax

```Haskell
VARCHAR rpad(VARCHAR str, INT len, VARCHAR pad)
```

This function returns strings with a length of len (starting counting from the first syllable) in str. If len is longer than str, the return value is lengthened to len characters by adding pad characters in front of str.  If str is longer than len, the return value is shortened to len characters. Len means the length of characters, not bytes.

## example

```Plain Text
MySQL > SELECT rpad("hi", 5, "xy");
+---------------------+
| rpad('hi', 5, 'xy') |
+---------------------+
| hixyx               |
+---------------------+

MySQL > SELECT rpad("hi", 1, "xy");
+---------------------+
| rpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```

## keyword

RPAD
