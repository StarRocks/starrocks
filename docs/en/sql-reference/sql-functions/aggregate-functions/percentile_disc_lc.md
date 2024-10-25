---
displayed_sidebar: docs
---

# percentile_disc_lc



Returns a percentile value based on a discrete distribution of the input column `expr`. Same behavior as percentile_disc. However, the implementation algorithm is different. percentile_disc needs to obtain all input data, and the memory consumed by merge sorting to obtain percentile values ​​is the memory of all input data. On the other hand, percentile_disc_lc builds a hash table of key->count, so when the input cardinality is low, there is no obvious memory increase even if the input data size is large.

This function is supported from v3.4 onwards.

## Syntax

```SQL
PERCENTILE_DISC_LC (expr, percentile) 
```

## Parameters

- `expr`: the column for which you want to calculate the percentile value. The column can be of any data type that is sortable.
- `percentile`: the percentile of the value you want to find. It must be a constant floating-point number between 0 and 1. For example, if you want to find the median value, set this parameter to `0.5`. If you want to find the value at the 70th percentile, specify 0.7.

## Return value

The data type of the return value is the same as `expr`.

## Usage notes

NULL values are ignored in the calculation.

## Examples

Create table `exam` and insert data into this table.

```sql
CREATE TABLE exam (
    subject STRING,
    score INT
) 
DISTRIBUTED BY HASH(`subject`);

INSERT INTO exam VALUES
('chemistry',80),
('chemistry',100),
('chemistry',null),
('math',60),
('math',70),
('math',85),
('physics',75),
('physics',80),
('physics',85),
('physics',99);
```

```Plain
select * from exam order by subject;
+-----------+-------+
| subject   | score |
+-----------+-------+
| chemistry |    80 |
| chemistry |   100 |
| chemistry |  NULL |
| math      |    60 |
| math      |    70 |
| math      |    85 |
| physics   |    75 |
| physics   |    80 |
| physics   |    85 |
| physics   |    99 |
+-----------+-------+
```

Calculate the median of each subject.

```SQL
select subject, percentile_disc_lc (score, 0.5)
from exam group by subject;
```

Output

```Plain
+-----------+--------------------------------+
| subject   | percentile_disc_lc(score, 0.5) |
+-----------+--------------------------------+
| physics   |                             85 |
| chemistry |                            100 |
| math      |                             70 |
+-----------+--------------------------------+
```
