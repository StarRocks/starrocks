# 系统变量

## 变量设置与查看

本文介绍 StarRocks 系统支持的变量（variables）。可以通过命令 `SHOW VARIABLES` 查看变量。您可以设置（SET）变量在系统全局范围内生效，或者仅在当前会话中生效。

StarRocks 中的变量参考 MySQL 中的变量设置，但**部分变量仅用于兼容 MySQL 客户端协议，并不产生其在 MySQL 数据库中的实际意义**。

### 查看变量

可以通过 `SHOW VARIABLES [LIKE 'xxx']`; 查看所有或指定的变量。如：

```SQL
SHOW VARIABLES;

SHOW VARIABLES LIKE '%time_zone%';
```

### 设置变量

变量一般可以设置为**全局**生效或**仅当前会话**生效。设置为全局生效后，**后续新的会话**连接中会使用新设置的值，而不影响当前会话；设置为仅当前会话生效时，变量仅对当前会话产生作用。

通过 `SET var_name=xxx;` 语句设置的变量仅在当前会话生效。如：

```SQL
SET exec_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

通过 `SET GLOBAL var_name=xxx;` 语句设置的变量全局生效。如：

 ```SQL
SET GLOBAL exec_mem_limit = 137438953472;
```

> 注：只有 admin 用户可以设置变量为全局生效。全局生效的变量不影响当前会话，仅影响后续新的会话。

既支持当前会话生效又支持全局生效的变量包括：

* batch_size
* disable_streaming_preaggregations
* exec_mem_limit
* force_streaming_aggregate
* is_report_success
* hash_join_push_down_right_table
* parallel_fragment_exec_instance_num
* parallel_exchange_instance_num
* prefer_compute_node
* query_timeout
* sql_mode
* time_zone
* use_compute_nodes
* vectorized_engine_enable
* wait_timeout

只支持全局生效的变量包括：

* default_rowset_type

此外，变量设置也支持常量表达式，如：

 ```SQL
SET exec_mem_limit = 10 * 1024 * 1024 * 1024;
```

 ```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### 在查询语句中设置变量

在一些场景中，我们可能需要对某些查询专门设置变量。通过使用SET_VAR提示可以在查询中设置仅在单个语句内生效的会话变量。举例：

```sql
SELECT /*+ SET_VAR(exec_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);
```

> 注：提示必须以"/*+"开头，并且只能跟随在 SELECT 关键字之后。

## 支持的变量

* SQL_AUTO_IS_NULL

  用于兼容 JDBC 连接池 C3P0。 无实际作用。

* auto_increment_increment

  用于兼容 MySQL 客户端。无实际作用。

* autocommit

  用于兼容 MySQL 客户端。无实际作用。

* batch_size

  用于指定在查询执行过程中，各个节点传输的单个数据包的行数。默认一个数据包的行数为 1024 行，即源端节点每产生 1024 行数据后，打包发给目的节点。较大的行数，会在扫描大数据量场景下提升查询的吞吐率，但可能会在小查询场景下增加查询延迟。同时，也会增加查询的内存开销。建议设置范围 1024 至 4096。

* default_rowset_type

  用于设置计算节点存储引擎默认的存储格式。当前支持的存储格式包括：alpha/beta。

* disable_colocate_join

  控制是否启用 Colocate Join 功能。默认为 false，表示启用该功能。true 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Colocate Join。

* disable_streaming_preaggregations

  控制是否开启流式预聚合。默认为 false，即开启。

* div_precision_increment

  用于兼容 MySQL 客户端。无实际作用。

* enable_insert_strict

  用于设置通过 INSERT 语句进行数据导入时，是否开启 strict 模式。默认为 false，即不开启 strict 模式。关于该模式的介绍，可以参阅[数据导入](../loading/Loading_intro.md)章节。

* enable_spilling

  用于设置是否开启大数据量落盘排序。默认为 false，即关闭该功能。当用户未指定 ORDER BY 子句的 LIMIT 条件，同时设置 enable_spilling 为 true 时，才会开启落盘排序。该功能启用后，会使用 BE 数据目录下 starrocks-scratch/ 目录存放临时的落盘数据，并在查询结束后清空临时数据。
  
  该功能主要用于使用有限的内存进行大数据量的排序操作。

  > 注：该功能为实验性质，不保证稳定性，请谨慎开启。

* event_scheduler

  用于兼容 MySQL 客户端。无实际作用。

* exec_mem_limit

  用于设置单个查询计划实例所能使用的内存限制。默认为 2GB，单位为：B/K/KB/M/MB/G/GB/T/TB/P/PB, 默认为B。

  一个查询计划可能有多个实例，一个 BE 节点可能执行一个或多个实例。所以该参数并不能准确限制一个查询在整个集群中的内存使用，也不能准确限制一个查询在单一 BE 节点上的内存使用。具体需要根据生成的查询计划判断。

  通常只有在一些阻塞节点（如排序节点、聚合节点、Join 节点）上才会消耗较多的内存，而其他节点（如扫描节点）中，数据为流式通过，并不会占用较多的内存。

  当出现 Memory Exceed Limit 错误时，可以尝试指数级增加该参数，如 4G、8G、16G 等。

* force_streaming_aggregate

  用于控制聚合节点是否启用流式聚合计算策略。默认为false，表示不启用该策略。

* forward_to_master

  用于设置是否将一些命令转发到 Leader FE 节点执行。默认为 false，即不转发。StarRocks 中存在多个 FE 节点，其中一个为 Master 节点。通常用户可以连接任意 FE 节点进行全功能操作。但部分信息查看指令只有从 Leader FE 节点才能获取详细信息。

  如 `SHOW BACKENDS;` 命令，如果不转发到 Leader FE 节点，则仅能看到节点是否存活等一些基本信息，而转发到 Leader FE 则可以获取包括节点启动时间、最后一次心跳时间等更详细的信息。

  当前受该参数影响的命令如下：

  * SHOW FRONTENDS;

    转发到 Leader 可以查看最后一次心跳信息。

  * SHOW BACKENDS;

    转发到 Leader 可以查看启动时间、最后一次心跳信息、磁盘容量信息。

  * SHOW BROKER;

    转发到 Leader 可以查看启动时间、最后一次心跳信息。

  * SHOW TABLET;
  * ADMIN SHOW REPLICA DISTRIBUTION;
  * ADMIN SHOW REPLICA STATUS;

    转发到 Leader 可以查看 Leader FE 元数据中存储的 tablet 信息。正常情况下，不同 FE 元数据中 tablet 信息应该是一致的。当出现问题时，可以通过这个方法比较当前 FE 和 Leader FE 元数据的差异。

  * SHOW PROC;

    转发到 Leader 可以查看 Leader FE 元数据中存储的相关 PROC 的信息。主要用于元数据比对。

* hash_join_push_down_right_table

  用于控制在Join查询中是否可以使用针对右表的过滤条件来过滤左表的数据，可以减少Join过程中需要处理的左表的数据量。取值为true时表示允许该操作，系统将根据实际情况决定是否能对左表进行过滤；取值为false表示禁用该操作。缺省值为true。

* init_connect

  用于兼容 MySQL 客户端。无实际作用。

* interactive_timeout

  用于兼容 MySQL 客户端。无实际作用。

* is_report_success

  用于设置是否需要查看查询的 profile。默认为 `false`，即不需要查看 profile。

  默认情况下，只有在查询发生错误时，BE 才会发送 profile 给 FE，用于查看错误。正常结束的查询不会发送 profile。发送 profile 会产生一定的网络开销，对高并发查询场景不利。 当用户希望对一个查询的 profile 进行分析时，可以将这个变量设为 true 后，发送查询。查询结束后，可以通过在当前连接的 FE 的 web 页面（地址：fe_host:fe_http_port/query）查看 profile。该页面会显示最近100条开启了 is_report_success 的查询的 profile。

* language

  用于兼容 MySQL 客户端。无实际作用。

* license

  显示 StarRocks 的 license。无其他作用。

* load_mem_limit

  用于指定导入操作的内存限制，单位为 Byte。默认值为 0，即表示不使用该变量，而采用 `exec_mem_limit` 作为导入操作的内存限制。
  
  这个变量仅用于 INSERT 操作。因为 INSERT 操作涉及查询和导入两个部分，如果用户不设置此变量，则查询和导入操作各自的内存限制均为 `exec_mem_limit`。否则，INSERT 的查询部分内存限制为 `exec_mem_limit`，而导入部分限制为 l`oad_mem_limit`。

  其他导入方式，如 Broker Load，STREAM LOAD 的内存限制依然使用 `exec_mem_limit`。

* lower_case_table_names

  用于兼容 MySQL 客户端，无实际作用。StarRocks 中的表名是大小写敏感的。

* max_allowed_packet

  用于兼容 JDBC 连接池 C3P0。 无实际作用。

* max_pushdown_conditions_per_column

  该变量的具体含义请参阅 BE 配置项中 `max_pushdown_conditions_per_column` 的说明。该变量默认置为 -1，表示使用 be.conf 中的配置值。如果设置大于 0，则当前会话中的查询会使用该变量值，而忽略 be.conf 中的配置值。

* max_scan_key_num

  该变量的具体含义请参阅 BE 配置项中 `starrocks_max_scan_key_num` 的说明。该变量默认置为 -1，表示使用 be.conf 中的配置值。如果设置大于 0，则当前会话中的查询会使用该变量值，而忽略 be.conf 中的配置值。

* net_buffer_length

  用于兼容 MySQL 客户端。无实际作用。

* net_read_timeout

  用于兼容 MySQL 客户端。无实际作用。

* net_write_timeout

  用于兼容 MySQL 客户端。无实际作用。

* parallel_exchange_instance_num

  用于设置执行计划中，一个上层节点接收下层节点数据所使用的 exchange node 数量。默认为 -1，即表示 exchange node 数量等于下层节点执行实例的个数（默认行为）。当设置大于0，并且小于下层节点执行实例的个数，则 exchange node 数量等于设置值。

  在一个分布式的查询执行计划中，上层节点通常有一个或多个 exchange node 用于接收来自下层节点在不同 BE 上的执行实例的数据。通常 exchange node 数量等于下层节点执行实例数量。

  在一些聚合查询场景下，如果底层需要扫描的数据量较大，但聚合之后的数据量很小，则可以尝试修改此变量为一个较小的值，可以降低此类查询的资源开销。如在 DUPLICATE KEY 明细模型上进行聚合查询的场景。

* parallel_fragment_exec_instance_num

  针对扫描节点，设置其在每个 BE 节点上执行实例的个数。默认为 1。

   一个查询计划通常会产生一组 scan range，即需要扫描的数据范围。这些数据分布在多个 BE 节点上。一个 BE 节点会有一个或多个 scan range。默认情况下，每个 BE 节点的一组 scan range 只由一个执行实例处理。当机器资源比较充裕时，可以将增加该变量，让更多的执行实例同时处理一组 scan range，从而提升查询效率。

  而 scan 实例的数量决定了上层其他执行节点，如聚合节点，join 节点的数量。因此相当于增加了整个查询计划执行的并发度。修改该参数会对大查询效率提升有帮助，但较大数值会消耗更多的机器资源，如CPU、内存、磁盘I/O。

* performance_schema

  用于兼容 8.0.16及以上版本的MySQL JDBC。无实际作用。

* prefer_compute_node

  将部分执行计划调度到 CN 节点执行。默认为 false。

* query_cache_size

  用于兼容 MySQL 客户端。无实际作用。

* query_cache_type

   用于兼容 JDBC 连接池 C3P0。 无实际作用。

* query_timeout

   用于设置查询超时，单位是「秒」。该变量会作用于当前连接中所有的查询语句，以及 INSERT 语句。默认为300秒，即 5 分钟。

* resource_group

  暂不使用。

* rewrite_count_distinct_to_bitmap_hll

   是否将 Bitmap 和 HLL 类型的 count distinct 查询重写为 bitmap_union_count 和 hll_union_agg 。

* sql_mode

  用于指定 SQL 模式，以适应某些 SQL 方言。

* sql_safe_updates

  用于兼容 MySQL 客户端。无实际作用。

* sql_select_limit

  用于兼容 MySQL 客户端。无实际作用。

* storage_engine

  指定系统使用的存储引擎。StarRocks支持的引擎类型包括：

  * olap：StarRocks系统自有引擎。
  * mysql：使用MySQL外部表。
  * broker：通过Broker程序访问外部表。
  * elasticsearch 或者 es：使用Elasticsearch外部表。
  * hive：使用Hive外部表。

* system_time_zone

  显示当前系统时区。不可更改。

* time_zone

  用于设置当前会话的时区。时区会对某些时间函数的结果产生影响。

* tx_isolation

  用于兼容 MySQL 客户端。无实际作用。

* use_compute_nodes

  用于设置使用 CN 节点的数量上限。该设置只会在 `prefer_compute_node=true` 时才会生效。
    -1 表示使用所有 CN 节点，0 表示不使用 CN 节点。

* use_v2_rollup

  用于控制查询使用segment v2存储格式的Rollup索引获取数据。该变量用于上线segment v2的时进行验证使用。其他情况不建议使用。

* vectorized_engine_enable

  用于控制是否使用向量化引擎执行查询。值为true时表示使用向量化引擎，否则使用非向量化引擎。缺省值为true。

* version

  用于兼容 MySQL 客户端。无实际作用。

* version_comment

  用于显示 StarRocks 的版本。不可更改。

* wait_timeout

  用于设置空闲连接的连接时长。当一个空闲连接在该时长内与 StarRocks 没有任何交互，则 StarRocks 会主动断开这个链接。默认为 8 小时，单位是「秒」。
