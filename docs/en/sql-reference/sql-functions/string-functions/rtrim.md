# rtrim

## Description

Removes the trailing spaces from the end of the `str` argument.

## Syntax

```Haskell
rtrim(str);
```

## Parameters

`str`: the string to trim. The supported data type is VARCHAR.

## Return value

Returns a value of the VARCHAR type.

## Examples

Remove the three spaces following `d`.

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```
