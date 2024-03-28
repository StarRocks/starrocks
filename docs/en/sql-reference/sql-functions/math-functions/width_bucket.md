# width_bucket

## Description

Returns the bucket number to which value would be assigned in an equiwidth histogram with num_bucket buckets, in the range min_value to max_value.

## Syntax

```Shell
WIDTH_BUCKET( <expr> , <min_value> , <max_value> , <num_buckets> )
```

## Parameter

- `expr` : The expression for which the histogram is created. This expression must evaluate to a numeric value or   to a value that can be implicitly converted to a numeric value.

- `min_value` and `max_value` : The low and high end points of the acceptable range for the expression. The end points must also evaluate to numeric values and not be equal.

- `num_buckets` : The desired number of buckets; must be a positive integer value. A value from the expression is assigned to each bucket, and the function then returns the corresponding bucket number.

## Notice

   - when the expression is less than lower bound of the bucket, return 0
   - when the expression is greater than upper bound of the bucket, return bucket_num+1
   - return null if any of those arguments is null


## Return value

Returns a value of the bucket number (a positive integer value).

## Examples

```Plain
mysql> select width_bucket(5.35, 0.024, 10.06, 5);
+-------------------------------------+
| width_bucket(5.35, 0.024, 10.06, 5) |
+-------------------------------------+
|                                   3 |
+-------------------------------------+
1 row in set (0.00 sec)
```
