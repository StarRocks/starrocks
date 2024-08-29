---
displayed_sidebar: docs
---

# window_funnel

## Description

Searches for an event chain in a sliding window and calculates the maximum number of consecutive events in the event chain. This function is commonly used for analyzing conversion rate. It is supported from v2.3.

This function works according to the following rules:

- It starts the count from the first event in the event chain. If the first event is found, the event counter is set to 1 and the sliding window starts. If the first event is not found, 0 is returned.

- In the sliding window, the counter is incremented if the events in the event chain occur in sequence. If the sliding window is exceeded, the event counter is no longer incremented.

- If multiple event chains match the specified condition, the longest event chain is returned.

## Syntax

```Plain
BIGINT window_funnel(BIGINT window, DATE|DATETIME time, INT mode, array[cond1, cond2, ..., condN])
```

## Parameters

- `window`: The length of the sliding window. The supported data type is BIGINT. The unit depends on the `time` parameter. If the data type of `time` is DATE, the unit is days. If the data type of `time` is DATETIME, the unit is seconds.

- `time`: The column containing timestamps. DATE and DATETIME types are supported.

- `mode`: The mode in which the event chain is filtered. The supported data type is INT. Value range: 0, 1, 2.
  - `0` is the default value, which indicates common funnel calculation.
  - `1` indicates the `DEDUPLICATION` mode, that is, the filtered event chain cannot have repeated events. Suppose the `array` parameter is `[event_type = 'A', event_type = 'B', event_type = 'C', event_type = 'D']` and the original event chain is "A-B-C-B-D". Event B is repeated and the filtered event chain is "A-B-C".
  - `2` indicates the `FIXED` mode, that is, the filtered event chain cannot have events that disrupt the specified sequence. Suppose the previous `array` parameter is used and the original event chain is "A-B-D-C". Event D interrupts the sequence and the filtered event chain is "A-B".
  - `4` indicates the `INCREASE` mode, which means the filtered events must have strictly increasing timestamps. Duplicate timestamp disrupts the event chain. This mode is supported since version 2.5.

- `array`: The defined event chain. It must be an array.

## Return value

Returns a value of the BIGINT type.

## Examples

**Example 1**: Calculate the maximum number of consecutive events based on `uid`. The sliding window is 1800s and the filtering mode is `0`.

This example uses table `action`, in which data is sorted by `uid`.

```Plaintext
mysql> select * from action;
+------+------------+---------------------+
| uid  | event_type | time                |
+------+------------+---------------------+
| 1    | Browse     | 2020-01-02 11:00:00 |
| 1    | Click      | 2020-01-02 11:10:00 |
| 1    | Order      | 2020-01-02 11:20:00 |
| 1    | Pay        | 2020-01-02 11:30:00 |
| 1    | Browse     | 2020-01-02 11:00:00 |
| 2    | Order      | 2020-01-02 11:00:00 |
| 2    | Pay        | 2020-01-02 11:10:00 |
| 3    | Browse     | 2020-01-02 11:20:00 |
| 3    | Click      | 2020-01-02 12:00:00 |
| 4    | Browse     | 2020-01-02 11:50:00 |
| 4    | Click      | 2020-01-02 12:00:00 |
| 5    | Browse     | 2020-01-02 11:50:00 |
| 5    | Click      | 2020-01-02 12:00:00 |
| 5    | Order      | 2020-01-02 11:10:00 |
| 6    | Browse     | 2020-01-02 11:50:00 |
| 6    | Click      | 2020-01-02 12:00:00 |
| 6    | Order      | 2020-01-02 12:10:00 |
+------+------------+---------------------+
17 rows in set (0.01 sec)
```

Execute the following statement:

```Plaintext
select uid,
       window_funnel(1800,time,0,[event_type='Browse', event_type='Click', 
        event_type='Order', event_type='Pay']) AS level
from action
group by uid
order by uid; 
+------+-------+
| uid  | level |
+------+-------+
| 1    |     4 |
| 2    |     0 |
| 3    |     1 |
| 4    |     2 |
| 5    |     2 |
| 6    |     3 |
+------+-------+
```

Description of the results:

- The matching event chain for `uid = 1` is "Browse-Click-Order-Pay", and `4` is returned. The time of the last "Browse" event (2020-01-02 11:00:00) does not meet the condition and is not counted.

- The event chain of `uid = 2` does not start from the first event "Browse", and `0` is returned.

- The matching event chain for `uid = 3` is "Browse", and `1` is returned. The "Click" event exceeds the 1800s time window and is not counted.

- The matching event chain for `uid = 4` is "Browse-Click", and `2` is returned.

- The matching event chain for `uid = 5` is "Browse-Click", and `2` is returned. The "Order" event (2020-01-02 11:10:00) does not belong to the event chain and is not counted.

- The matching event chain for `uid = 6` is "Browse-Click-Order", and `3` is returned.

**Example 2**: Calculate the maximum number of consecutive events based on `uid`. The sliding window is 1800s, and filtering modes `0` and `1` are used.

This example uses table `action1`, in which data is sorted by `time`.

```Plaintext
mysql> select * from action1 order by time;
+------+------------+---------------------+ 
| uid  | event_type | time                |     
+------+------------+---------------------+
| 1    | Browse     | 2020-01-02 11:00:00 |
| 2    | Browse     | 2020-01-02 11:00:01 |
| 1    | Click      | 2020-01-02 11:10:00 |
| 1    | Order      | 2020-01-02 11:29:00 |
| 1    | Click      | 2020-01-02 11:29:50 |
| 1    | Pay        | 2020-01-02 11:30:00 |
| 1    | Click      | 2020-01-02 11:40:00 |
+------+------------+---------------------+
7 rows in set (0.03 sec)
```

Execute the following statement:

```Plaintext
select uid,
       window_funnel(1800,time,0,[event_type='Browse', 
        event_type='Click', event_type='Order', event_type='Pay']) AS level
from action1
group by uid
order by uid;
+------+-------+
| uid  | level |
+------+-------+
| 1    |     4 |
| 2    |     1 |
+------+-------+
2 rows in set (0.02 sec)
```

For `uid = 1`, the "Click" event (2020-01-02 11:29:50) is a repeated event but it is still counted because mode `0` is used. Therefore, `4` is returned.

Change `mode` to `1` and execute the statement again.

```Plaintext
+------+-------+
| uid  | level |
+------+-------+
| 1    |     3 |
| 2    |     1 |
+------+-------+
2 rows in set (0.05 sec)
```

The longest event chain filtered after deduplication is "Browse-Click-Order", and `3` is returned.

**Example 3**: Calculate the maximum number of consecutive events based on `uid`. The sliding window is 1900s, and filter modes `0` and `2` are used.

This example uses table `action2`, in which data is sorted by `time`.

```Plaintext
mysql> select * from action2 order by time;
+------+------------+---------------------+
| uid  | event_type | time                |
+------+------------+---------------------+
| 1    | Browse     | 2020-01-02 11:00:00 |
| 2    | Browse     | 2020-01-02 11:00:01 |
| 1    | Click      | 2020-01-02 11:10:00 |
| 1    | Pay        | 2020-01-02 11:30:00 |
| 1    | Order      | 2020-01-02 11:31:00 |
+------+------------+---------------------+
5 rows in set (0.01 sec)
```

Execute the following statement:

```Plaintext
select uid,
       window_funnel(1900,time,0,[event_type='Browse', event_type='Click', 
        event_type='Order', event_type='Pay']) AS level
from action2
group by uid
order by uid;
+------+-------+
| uid  | level |
+------+-------+
| 1    |     3 |
| 2    |     1 |
+------+-------+
2 rows in set (0.02 sec)
```

`3` is returned for `uid = 1` because mode `0` is used and the "Pay" event (2020-01-02 11:30:00) does not disrupt the event chain.

Change `mode` to `2` and execute the statement again.

```Plaintext
select uid,
       window_funnel(1900,time,2,[event_type='Browse', event_type='Click', 
        event_type='Order', event_type='Pay']) AS level
from action2
group by uid
order by uid;
+------+-------+
| uid  | level |
+------+-------+
| 1    |     2 |
| 2    |     1 |
+------+-------+
2 rows in set (0.06 sec)
```

`2` is returned because the "Pay" event disrupts the event chain and the event counter stops. The filtered event chain is "Browse-Click".

**Example 4**: Calculate the maximum number of consecutive events based on `uid`. The sliding window is 1900s, and filter modes `0` and `4` are used.

This example uses table `action3`, in which data is sorted by `time`.

```Plaintext
select * from action3 order by time;
+------+------------+---------------------+
| uid  | event_type | time                |
+------+------------+---------------------+
| 1    | Browse     | 2020-01-02 11:00:00 |
| 1    | Click      | 2020-01-02 11:00:01 |
| 2    | Browse     | 2020-01-02 11:00:03 |
| 1    | Order      | 2020-01-02 11:00:31 |
| 2    | Click      | 2020-01-02 11:00:03 |
| 2    | Order      | 2020-01-02 11:01:03 |
+------+------------+---------------------+
3 rows in set (0.02 sec)
```

Execute the following statement:

```Plaintext
select uid,
       window_funnel(1900,time,0,[event_type='Browse', event_type='Click',
        event_type='Order']) AS level
from action3
group by uid
order by uid;
+------+-------+
| uid  | level |
+------+-------+
|    1 |     3 |
|    2 |     3 |
+------+-------+
```

`3` is returned for `uid = 1` and `uid = 2`.

Change `mode` to `4` and execute the statement again.

```Plaintext
select uid,
       window_funnel(1900,time,4,[event_type='Browse', event_type='Click',
        event_type='Order']) AS level
from action3
group by uid
order by uid;
+------+-------+
| uid  | level |
+------+-------+
|    1 |     3 |
|    2 |     1 |
+------+-------+
1 row in set (0.02 sec)
```

`1` is returned for `uid = 2` because mode `4` (strictly increasing) is used. "Click" happens at the same second as "BROWSE". Therefore, "Click" and "Order" are not counted.

## Keywords

window funnel, funnel, window_funnel
