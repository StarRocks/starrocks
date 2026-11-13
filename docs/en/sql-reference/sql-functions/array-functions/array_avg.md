---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the average of all elements in an ARRAY, ignoring NULL values."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# array_avg



Calculates the average value of all data in an ARRAY and return this result.

## Syntax

```plaintext
array_avg(array(type))
```

`array(type)` supports the following types of elements: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMALV2.

## Examples

```plain text
mysql> select array_avg([11, 11, 12]);
+-----------------------+
| array_avg([11,11,12]) |
+-----------------------+
| 11.333333333333334    |
+-----------------------+

mysql> select array_avg([11.33, 11.11, 12.324]);
+---------------------------------+
| array_avg([11.33,11.11,12.324]) |
+---------------------------------+
| 11.588                          |
+---------------------------------+
```

## keyword

ARRAY_AVG,ARRAY
