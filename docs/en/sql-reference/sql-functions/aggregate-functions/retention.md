# retention

## Description

Checks whether a table contains data that meets specified conditions and returns an array of BOOLEAN values.

## Syntax

```SQL
retention(input)
```

## Parameters

`input`: an array that consists of event expressions.

## Return value

Returns an array of BOOLEAN values. The array that is returned is of the same length as the array that is specified by the `input` parameter.

- The value of the first element in the array specified by the `output` parameter is `input[1]`.

- If both `input[1]` and `output[`*`n`*`]` are `true`, the value of the *n*th element in the array specified by the `output` parameter is `true`.

## Examples

Example 1:

```Plain
mysql> SELECT retention([lo_orderdate = '1997-08-01' AND lo_orderpriority = '2-HIGH', 
    lo_orderdate = '1997-08-02' AND lo_orderpriority = '1-URGENT', lo_orderdate = '1997-08-03' AND 
    lo_orderpriority = '5-LOW']) AS r FROM lineorder_flat GROUP BY lo_linenumber;
+---------+
| r       |
+---------+
| [1,1,1] |
| [1,1,1] |
| [1,1,1] |
| [1,1,1] |
| [1,1,1] |
| [1,1,1] |
| [1,1,1] |
+---------+
```

Example 2:

Calculate the proportion of elements that meet the following conditions:

- `lo_orderdate = '1997-08-02' AND lo_orderpriority = '1-URGENT'`

- `lo_orderdate = '1997-08-03' AND lo_orderpriority = '5-LOW'`

```Plain
mysql> SELECT sum(r[1]), sum(r[2]) / sum(r[1]), sum(r[3]) / sum(r[1]) FROM 
    (SELECT retention([lo_orderdate = '1997-08-01' AND lo_orderpriority = '2-HIGH', 
        lo_orderdate = '1997-08-02' AND lo_orderpriority = '1-URGENT', lo_orderdate = '1997-08-03' AND 
        lo_orderpriority = '5-LOW']) AS r FROM lineorder_flat GROUP BY lo_suppkey) t;
+-------------+---------------------------+---------------------------+
| sum(`r`[1]) | sum(`r`[2]) / sum(`r`[1]) | sum(`r`[3]) / sum(`r`[1]) |
+-------------+---------------------------+---------------------------+
|       43951 |        0.2228163181725103 |        0.2214056562990603 |
+-------------+---------------------------+---------------------------+
```

Example 3:

Suppose that you have a table named `lineorder_flat` and the table consists of the following data:

```Plain
+--------------+------------------+------------+
| lo_orderdate | lo_orderpriority | lo_custkey |
+--------------+------------------+------------+
| 1022-11-20   | 4-NOT SPECI      |     309050 |
| 1222-10-31   | 2-HIGH           |     492238 |
| 1380-09-30   | 5-LOW            |     123099 |
| 1380-09-30   | 5-LOW            |     460237 |
| 1380-09-30   | 5-LOW            |     426502 |
| 1022-11-20   | 4-NOT SPECI      |     197081 |
| 1380-09-30   | 5-LOW            |     918918 |
| 1022-11-20   | 4-NOT SPECI      |     327825 |
| 1380-09-30   | 5-LOW            |     252542 |
| 1380-09-30   | 5-LOW            |     194171 |
+--------------+------------------+------------+
10 rows in set (0.02 sec)
```

Execute the following statement to call the `retention` function:

```Plain
mysql> SELECT lo_custkey,
    retention([lo_orderdate='1022-11-20' AND lo_orderpriority='4-NOT SPECI',
        lo_orderdate='1022-11-21' AND lo_orderpriority='4-LONG']) AS retention
    FROM lineorder_flat
    GROUP BY lo_custkey;
+------------+-----------+
| lo_custkey | retention |
+------------+-----------+
|     327825 | [1,0]     |
|     309050 | [1,0]     |
|     252542 | [0,0]     |
|     123099 | [0,0]     |
|     460237 | [0,0]     |
|     194171 | [0,0]     |
|     197081 | [1,0]     |
|     918918 | [0,0]     |
|     492238 | [0,0]     |
|     426502 | [0,0]     |
+------------+-----------+
10 rows in set (0.01 sec)
```

None of the second bits of the output results is `1`, because no data meets the preceding conditions.

Insert the following data record into the table:

```Plain
（lo_orderdate='1022-11-21'， lo_orderpriority='4-LONG'， lo_custkey=460237）
```

Execute the following statement to call the `retention` function:

```Plain
mysql> SELECT lo_custkey,
    retention([lo_orderdate='1022-11-20' AND lo_orderpriority='4-NOT SPECI',
        lo_orderdate='1022-11-21' AND lo_orderpriority='4-LONG']) AS retention
    FROM lineorder_flat
    GROUP BY lo_custkey;
+------------+-----------+
| lo_custkey | retention |
+------------+-----------+
|     327825 | [1,0]     |
|     309050 | [1,0]     |
|     252542 | [0,0]     |
|     123099 | [0,0]     |
|     460237 | [0,0]     |
|     194171 | [0,0]     |
|     197081 | [1,0]     |
|     918918 | [0,0]     |
|     492238 | [0,0]     |
|     426502 | [0,0]     |
+------------+-----------+
10 rows in set (0.01 sec)
```

Still, none of the second bits of the output results is `1`, because no data meets the preceding conditions.

Insert the following data record into the table:

```Plain
（lo_orderdate='1022-11-21'， lo_orderpriority='4-LONG'， lo_custkey=327825）
```

Execute the following statement to call the `retention` function:

```Plain
mysql> SELECT lo_custkey,
    retention([lo_orderdate='1022-11-20' AND lo_orderpriority='4-NOT SPECI',
        lo_orderdate='1022-11-21' AND lo_orderpriority='4-LONG']) AS retention
    FROM lineorder_flat
    GROUP BY lo_custkey;
+------------+-----------+
| lo_custkey | retention |
+------------+-----------+
|     327825 | [1,1]     |
|     309050 | [1,0]     |
|     252542 | [0,0]     |
|     123099 | [0,0]     |
|     460237 | [0,0]     |
|     194171 | [0,0]     |
|     197081 | [1,0]     |
|     918918 | [0,0]     |
|     492238 | [0,0]     |
|     426502 | [0,0]     |
+------------+-----------+
10 rows in set (0.01 sec)
```

The second bit of the output result `327825` becomes `1`.
