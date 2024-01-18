---
displayed_sidebar: "English"
---

# money_format

## Description

This function returns a string formatted as a currency string. The integer part is separated by comma every three bits and the decimal part is reserved for two bits.

## Syntax

```Haskell
VARCHAR money_format(Number)
```

## Examples

```Plain Text
MySQL > select money_format(17014116);
+------------------------+
| money_format(17014116) |
+------------------------+
| 17,014,116.00          |
+------------------------+

MySQL > select money_format(1123.456);
+------------------------+
| money_format(1123.456) |
+------------------------+
| 1,123.46               |
+------------------------+

MySQL > select money_format(1123.4);
+----------------------+
| money_format(1123.4) |
+----------------------+
| 1,123.40             |
+----------------------+
```

## keyword

MONEY_FORMAT,MONEY,FORMAT
