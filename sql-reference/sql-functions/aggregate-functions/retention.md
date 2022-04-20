# retention

## 功能

通过应用一系列条件表达式，计算得到关于留存分布的结果(Array)。

## 语法

```Haskell
output retention(input)
```

## 参数说明

* input：一系列表示事件的表达式构成的Array，类型为Array\<BOOLEAN\>。

## 返回值说明

类型为Array\<BOOLEAN\>，长度与input一致，其中：

* output中第1个元素的值是input[1]。
* output中第n(n > 1)个元素的值是true，如果input[1]和output[n]都为true。

## 示例

示例一:

```plain text
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

示例二:

计算满足条件`lo_orderdate = '1997-08-02' AND lo_orderpriority = '1-URGENT'`和<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;满足条件`lo_orderdate = '1997-08-03' AND lo_orderpriority = '5-LOW'`所占的比例

```plain text
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

示例三:

假设现在有表`lineorder_flat`，其中数据为

```plain text
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

我们使用`retention`函数，

```plain text
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

可以看到没有第2位是1的结果，原因在于没有满足条件的数据。
我们插入一条数据包含

```plain text
（lo_orderdate='1022-11-21'， lo_orderpriority='4-LONG'， lo_custkey=460237）
```

再次执行

```plain text
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

结果依然没变，因为`custkey(460237)`对应的第1位条件不满足，
我们插入另一条数据包含

```plain text
（lo_orderdate='1022-11-21'， lo_orderpriority='4-LONG'， lo_custkey=327825）
```

再次执行

```plain text
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

可以看到 `327825` 对应结果的第二位变为了1。

## 关键字

RETENTION
