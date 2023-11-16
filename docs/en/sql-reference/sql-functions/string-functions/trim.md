# trim

## Description

Remove the space that appears continuously from the beginning and ending of the parameter str

## Syntax

```Haskell
trim(expr);
```

## Parameters

`expr`: the string to trim. The supported data type is VARCHAR.

## Return value

Returns a value of the VARCHAR type.

## Examples

Remove the five spaces.

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```
