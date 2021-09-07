# 查询管理

## 用户连接数

Property 是针对用户粒度的属性设置，客户端到FE的最大连接数可以通过该命令设置

```sql
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

设置用户的属性，包括分配给用户的资源等。这里设置的用户属性，是针对 user 的，而不是 user_identity。即假设通过 CREATE USER 语句创建了两个用户 'jack'@'%' 和 'jack'@'192.%'，则使用 SET PROPERTY 语句，只能针对 jack 这个用户，而不是 'jack'@'%' 或 'jack'@'192.%'

例如

```sql
修改用户 jack 最大连接数为1000
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';

查看root用户的连接数限制
SHOW PROPERTY FOR 'root'; 
```

## 查询相关的session变量

通过 set 'key' = 'value' 可以设置session级别的变量，可以限制当前session中查询的并发度，内存等等参数，例如：

- parallel_fragment_exec_instance_num

  查询的并行度，默认为1.表示每个BE上fragment的实例数量，如果希望提升单个查询的性能，可以设置为BE的CPU核数的一半。
- exec_mem_limit

  查询的内存限制，在查询报内存不足时可以调整。
- load_mem_limit

  导入的内存限制，在导入报内存不足时可以调整。

例如：

```sql
set parallel_fragment_exec_instance_num  = 8; 
set exec_mem_limit  = 137438953472;
```

## 数据库存储容量Quota

默认每个DB的容量限制是1TB，我们可以通过alter database 修改

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

这里quota 单位为：B/K/KB/M/MB/G/GB/T/TB/P/PB
例如

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

更多参考 [ALTER DATABASE](../sql-reference/sql-statements/data-definition/ALTER%20DATABASE.md)

## 杀死查询

我们可以通过kill 命令杀掉某一个连接上的查询，其语法是：

```sql
kill connection_id;
```

connection_id 可以通过show processlist; 或者select connection_id(); 查询到

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
