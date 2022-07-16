# array_sum

## description

### Syntax

```Haskell
array_sum(array(type))
```

Sum all data in an ARRAY and return the result.

## example

```plain text
mysql> select array_sum([11, 11, 12]);
+-----------------------+
| array_sum([11,11,12]) |
+-----------------------+
| 34                    |
+-----------------------+

mysql> select array_sum([11.33, 11.11, 12.324]);
+---------------------------------+
| array_sum([11.33,11.11,12.324]) |
+---------------------------------+
| 34.764                          |
+---------------------------------+
```

## keyword

ARRAY_SUM,ARRAY
