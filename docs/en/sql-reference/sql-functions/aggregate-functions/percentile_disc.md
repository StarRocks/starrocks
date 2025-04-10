---
displayed_sidebar: docs
---

# percentile_disc

## Description

Returns a percentile value based on a discrete distribution of the input column `expr`. If the exact percentile value cannot be found, this function returns the larger value between the two closest values.

This function is supported from v2.5 onwards.

## Syntax

```SQL
PERCENTILE_DISC (expr, percentile) 
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
select subject, percentile_disc(score, 0.5)
from exam group by subject;
```

Output

```Plain
+-----------+-----------------------------+
| subject   | percentile_disc(score, 0.5) |
+-----------+-----------------------------+
| chemistry |                         100 |
| math      |                          70 |
| physics   |                          85 |
+-----------+-----------------------------+
```
