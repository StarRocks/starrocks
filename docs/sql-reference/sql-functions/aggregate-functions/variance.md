# VARIANCE,VAR_POP,VARIANCE_POP

## Description

Returns the variance of an expression.

## Syntax

```Haskell
VARIANCE(expr)
```

## Return value

Returns a numerical value.

If the type of `expr` is DECIMAL, this function returns a DECIMAL value, or else returns a DOUBLE value.

## Examples

```plain
MySQL [tpcds_1g_orc]> select var_pop(i_current_price), i_rec_start_date from item group by i_rec_start_date;
+--------------------------+------------------+
| var_pop(i_current_price) | i_rec_start_date |
+--------------------------+------------------+
|       314.96177792808226 | 1997-10-27       |
|       463.73633459357285 | NULL             |
|       302.02102643609123 | 1999-10-28       |
|        337.9318386924913 | 2000-10-27       |
|       333.80931439318346 | 2001-10-27       |
+--------------------------+------------------+

MySQL [tpcds_1g_orc]> select variance(i_current_price), i_rec_start_date from item group by i_rec_start_date;
+---------------------------+------------------+
| variance(i_current_price) | i_rec_start_date |
+---------------------------+------------------+
|        314.96177792808226 | 1997-10-27       |
|         463.7363345935729 | NULL             |
|        302.02102643609123 | 1999-10-28       |
|         337.9318386924912 | 2000-10-27       |
|        333.80931439318346 | 2001-10-27       |
+---------------------------+------------------+
```

## keyword

VARIANCE,VAR_POP,VARIANCE_POP
