---
displayed_sidebar: "English"
---

# cot

## Description

Returns the cotangent of the angle in radians `x`. If the angle in radians `x` is `0` or `NULL`, `NULL` is returned.

## **Syntax**

```SQL
COT(x);
```

## **Parameters**

`x`: the angle in radians. DOUBLE is supported.

## **Return value**

Returns a value of the DOUBLE data type.

## **Examples**

```Plaintext
mysql> select cot(3.1415926/2);
+------------------------+
| cot(3.1415926 / 2)     |
+------------------------+
| 2.6794896585028646e-08 |
+------------------------+
1 row in set (0.25 sec)

mysql> select cot(0);
+--------+
| cot(0) |
+--------+
|   NULL |
+--------+
1 row in set (0.03 sec)
```
