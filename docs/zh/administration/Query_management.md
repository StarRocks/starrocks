# 查询管理

## 用户连接数

Property 是针对用户粒度的属性设置，客户端到 FE 的最大连接数可以通过该命令设置

```sql
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

设置用户的属性，包括分配给用户的资源等。这里设置的用户属性，是针对 user 的，而不是 user_identity。即假设通过 CREATE USER 语句创建了两个用户 'jack'@'%' 和 'jack'@'192.%'，则使用 SET PROPERTY 语句，只能针对 jack 这个用户，而不是 'jack'@'%' 或 'jack'@'192.%'

例如

```sql
-- 修改用户 jack 最大连接数为1000
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';

-- 查看root用户的连接数限制
SHOW PROPERTY FOR 'root'; 
```

## 查询相关的 session 变量

设置查询相关的 session 级别变量，以调整当前 session 中查询的并发度，内存等，例如：

- 查询并发度相关变量。
  - Pipeline 执行引擎相关变量（推荐）
  > 自 StarRocks 2.2 版本起，正式发布 Pipeline 引擎。如果新部署的 StarRocks 为 2.2 版本， 则已经默认开启 Pipeline 执行引擎。如果升级 StarRocks 至 2.2 版本，则需要手动开启 Pipeline 执行引擎。

    ```sql
    set enable_pipeline_engine = true;
    set pipeline_dop = 0;
    ```

    参数说明

    | 参数                                  | 说明                                                         |
    | ------------------------------------- | ------------------------------------------------------------ |
    | `enable_pipeline_engine`              | 是否启用 Pipeline 执行引擎。取值： **true**：启用（默认）。 **false**：不启用。 |
    | `pipeline_dop`                        | 一个 Pipeline 实例的并行数量。建议设为默认值 **0**，表示自适应调整每个 pipeline 的并行度。 也可以设为大于 **0** 的数值，通常为 BE 节点 CPU 物理核数的一半。 |

  - parallel_fragment_exec_instance_num

    一个 Fragment 实例的并行数量。一个 Fragment 实例占用 BE 节点的一个 CPU ，所以一个查询的并行度为一个 Fragment 实例的并行数量。如果您希望提升一个查询的并行度，则可以设置为 BE 的 CPU 核数的一半。

    > - 实际使用时，一个 Fragment 实例的并行数量存在上限，为一张表在一个 BE 中的 Tablet 数量。比如一张表的 3 个分区，32 个分桶，分布在 4 个 BE 节点上，则一个 BE 的 Tablet 数量为 32 * 3 / 4 = 24，因此该 BE 上一个 Fragment 实例的并行数上限为 24，即使 `set global parallel_fragment_exec_instance_num = 32`，但是实际使用时并行数为 24。
    > - 在高并发场景下，CPU 资源往往已充分利用，因此建议设置 `set parallel_fragment_exec_instance_num = 1`，以减少不同查询间资源竞争，从而提高整体查询效率。

- 内存相关变量包括 exec_mem_limit，表示查询的内存上限。

## 数据库存储容量 Quota

默认每个 DB 的容量无限制，我们可以通过 alter database 修改

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

这里 quota 单位为：B/K/KB/M/MB/G/GB/T/TB/P/PB
例如

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

更多参考 [ALTER DATABASE](../sql-reference/sql-statements/data-definition/ALTER_DATABASE.md)

## 杀死查询

我们可以通过 kill 命令杀掉某一个连接上的查询，其语法是：

```sql
kill connection_id;
```

connection_id 可以通过 show processlist; 或者 select connection_id(); 查询到

```plain text
 show processlist;
+------+--------------+---------------------+-----------------+-------------------+---------+------+-------+------+
| Id   | User         | Host                | Cluster         | Db                | Command | Time | State | Info |
+------+--------------+---------------------+-----------------+-------------------+---------+------+-------+------+
|    1 | starrocksmgr | 172.26.34.147:56208 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  129 | root         | 172.26.92.139:54818 | default_cluster |                   | Query   |    0 |       |      |
|  114 | test         | 172.26.34.147:57974 | default_cluster | ssb_100g          | Query   |    3 |       |      |
|    3 | starrocksmgr | 172.26.34.147:57268 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  100 | root         | 172.26.34.147:58472 | default_cluster | ssb_100           | Sleep   |  637 |       |      |
|  117 | starrocksmgr | 172.26.34.147:33790 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|    6 | starrocksmgr | 172.26.34.147:57632 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  119 | starrocksmgr | 172.26.34.147:33804 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  111 | root         | 172.26.92.139:55472 | default_cluster |                   | Sleep   | 2758 |       |      |
+------+--------------+---------------------+-----------------+-------------------+---------+------+-------+------+
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
