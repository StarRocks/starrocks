---
displayed_sidebar: "Chinese"
---

# 管理查询

本文介绍如何管理查询。

## 管理用户连接数

Property 指针对用户粒度的属性的设置项。设置用户的属性，包括分配给用户的资源等。此处的用户属性，是指针对 user，而非 user_identity 的属性。

您可以通过以下命令管理特定用户的客户端到 FE 最大连接数。

```sql
SET PROPERTY [FOR 'user'] 'key' = 'value'[, ...];
```

以下示例修改用户 jack 的最大连接数为 1000。

```sql
SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
```

您可以通过以下命令查看特定用户的连接数限制。

```sql
SHOW PROPERTY FOR 'user';
```

## 设置查询相关的 Session 变量

您可以设置查询相关的 Session 级别变量，以调整当前 Session 中查询的并发度，内存等。

### 调整查询并发度

如需调整查询并发度，推荐您修改 Pipeline 执行引擎相关变量。

> **说明**
>
> - 自 2.2 版本起，正式发布 Pipeline 引擎。
> - 自 3.0 版本起，支持根据查询并发度自适应调节 `pipeline_dop`。

```sql
SET enable_pipeline_engine = true;
SET pipeline_dop = 0;
```

| 参数                                 | 说明                                                         |
| ----------------------------------- | ------------------------------------------------------------ |
| enable_pipeline_engine              | 是否启用 Pipeline 执行引擎。true：启用（默认），false：不启用。 |
| pipeline_dop                        | 一个 Pipeline 实例的并行数量。建议设为默认值 0，即系统自适应调整每个 pipeline 的并行度。您也可以设置为大于 0 的数值，通常为 BE 节点 CPU 物理核数的一半。 |

您也可通过设置实例的并行数量调整查询并发度。

```sql
SET GLOBAL parallel_fragment_exec_instance_num = INT;
```

`parallel_fragment_exec_instance_num`：一个 Fragment 实例的并行数量。一个 Fragment 实例占用 BE 节点的一个 CPU，所以一个查询的并行度为一个 Fragment 实例的并行数量。如果您希望提升一个查询的并行度，则可以设置该参数为 BE 的 CPU 核数的一半。

> 注意：
>
> - 实际场景中，一个 Fragment 实例的并行数量存在上限，为一张表在一个 BE 中的 Tablet 数量。例如，一张表的 3 个分区，32 个分桶，分布在 4 个 BE 节点上，则一个 BE 节点的 Tablet 数量为 32 * 3 / 4 = 24，因此该 BE 节点上一个 Fragment 实例的并行数上限为 24，此时即使设置该参数为 `32`，实际使用时并行数仍为 24。
> - 在高并发场景下，CPU 资源往往已充分利用，因此建议设置 Fragment 实例的并行数量为 `1`，以减少不同查询间资源竞争，从而提高整体查询效率。

### 调整查询内存上限

您可以通过以下命令调整查询内存上限。

```sql
SET query_mem_limit = INT;
```

`query_mem_limit`：单个查询的内存限制，单位是 Byte。建议设置为 17179869184（16GB）以上。

## 调整数据库存储容量 Quota

默认设置下，每个数据库的存储容量无限制。您可以通过以下命令调整。

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

> 说明：`quota` 单位为 `B`，`K`，`KB`，`M`，`MB`，`G`，`GB`，`T`，`TB`，`P`，或 `PB`。

示例：

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

详细内容，参考 [ALTER DATABASE](../sql-reference/sql-statements/data-definition/ALTER_DATABASE.md)

## 停止查询

您可以通过以下命令停止某一个连接上的查询。

```sql
KILL connection_id;
```

`connection_id`：特定连接的 ID。您可以通过 `SHOW processlist;` 或者 `select connection_id();` 查看。

示例：

```plain text
mysql> show processlist;
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

> 说明
>
> `Info` 列中展示对应的 SQL 语句。如果因为 SQL 语句较长而导致被截断，您可以使用 `SHOW FULL processlist;` 来查看完整的 SQL 语句。
