---
displayed_sidebar: "English"
---

# Query Management

## Number of user connections

`Property` is set for user granularity. To set the maximum number of connections between Client and FE, use the following command.

```sql
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

User properties include the resources assigned to the user. The properties set here are for the user, not `user_identity`. That is, if two users `jack'@'%` and `jack'@'192.%` are created by the `CREATE USER` statement, then the `SET PROPERTY` statement can work on the user `jack`, not `jack'@'%` or `jack'@'192.%`.

Example 1:

```sql
For the user `jack`, change the maximum number of connections to 1000
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';

Check the connection limit for the root user
SHOW PROPERTY FOR 'root'; 
```

## Query-related session variables

The session variables can be set by 'key' = 'value', which can limit the concurrency, memory and other query parameters in the current session. For example:

- parallel_fragment_exec_instance_num

  The parallelism of the query with a default value of 1. It indicates the number of fragment instances on each BE. You can set this to half the number of CPU cores of the BE to improve query performance.

- exec_mem_limit

  Memory limit of query, can be adjusted when a query reports insufficient memory.

- load_mem_limit

  Memory limit for import, can be adjusted when an import job reports insufficient memory.

Example 2:

```sql
set parallel_fragment_exec_instance_num  = 8; 
set exec_mem_limit  = 137438953472;
```

## capacity quota of database storage

The capacity quota of database storage is unlimited by default. And you can change quota value by using `alter database`.

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

The quota units are: B/K/KB/M/MB/G/GB/T/TB/P/PB

Example 3:

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

## Kill queries

To terminate a query on a particular connection with the following  command:

```sql
kill connection_id;
```

The `connection_id` can be seen by `show processlist;` or `select connection_id();`.

```plain text
 show processlist;
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
| Id   | User       | Host                | Cluster         | Db            | Command | Time | State | Info |
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
|    1 | starrocksmgr | 172.26.34.147:56208 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  129 | root       | 172.26.92.139:54818 | default_cluster |               | Query   |    0 |       |      |
|  114 | test       | 172.26.34.147:57974 | default_cluster | ssb_100g      | Query   |    3 |       |      |
|    3 | starrocksmgr | 172.26.34.147:57268 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  100 | root       | 172.26.34.147:58472 | default_cluster | ssb_100       | Sleep   |  637 |       |      |
|  117 | starrocksmgr | 172.26.34.147:33790 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|    6 | starrocksmgr | 172.26.34.147:57632 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  119 | starrocksmgr | 172.26.34.147:33804 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  111 | root       | 172.26.92.139:55472 | default_cluster |               | Sleep   | 2758 |       |      |
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
9 rows in set (0.00 sec)

mysql> select connection_id();
+-----------------+
| CONNECTION_ID() |
+-----------------+
|              98 |
+-----------------+


mysql> kill 114;
Query OK, 0 rows affected (0.02 sec)

```
