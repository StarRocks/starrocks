---
displayed_sidebar: "English"
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
    exam_result INT
) 
DISTRIBUTED BY HASH(`subject`);

insert into exam values
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
select * from exam order by Subject;
+-----------+-------+
| Subject   | Score |
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
SELECT Subject, PERCENTILE_DISC (Score, 0.5)
FROM exam group by Subject;
```

Output

```Plain
+-----------+-----------------------------+
| Subject   | percentile_disc(Score, 0.5) |
+-----------+-----------------------------+
| chemistry |                         100 |
| math      |                          70 |
| physics   |                          85 |
+-----------+-----------------------------+
```
