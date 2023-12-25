---
displayed_sidebar: "English"
---

# format

## Description
FORMAT(X,D)

Formats the number X to a format like '#,###,###.##', rounded to D decimal places, and returns the result as a string. If D is 0, the result has no decimal point or fractional part. If X or D is NULL, the function returns NULL. If D is negative, then it is rewritten to 0.

## Syntax

```Haskell
VARCHAR format(Number, Int)
```

## Parameters

`Number`: the number to format

`Int`: the integer used to specify the number of decimal places

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
MySQL > select FORMAT(12332.123456,4);
+------------------------+
| FORMAT(12332.123456,4) |
+------------------------+
| 12,332.1235            |
+------------------------+

MySQL > select FORMAT(12332.2,2);
+------------------------+
| FORMAT(12332.2,2)      |
+------------------------+
| 12,332.20              |
+------------------------+

MySQL > select FORMAT(12332.123456, -4);
+--------------------------+
| FORMAT(12332.123456, -4) |
+--------------------------+
| 12,332                   |
+--------------------------+
```

## keyword

FORMAT
