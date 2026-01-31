---
displayed_sidebar: docs
---

# Routine Load 导入常见问题

## 1. 如何提高导入性能？

**方式一**：**增加实际任务并行度**，将一个导入作业拆分成尽可能多的导入任务并行执行。

> **注意**
>
> 该方式可能会消耗更多的 CPU 资源，并且导致导入版本过多。

实际任务并行度由如下多个参数组成的公式决定，上限为 BE 节点的数量或者消费分区的数量。

```Plain
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

参数说明：

- `alive_be_number`：存活 BE 数量。

- `partition_number`：消费分区数量。

- `desired_concurrent_number`：Routine Load 导入作业时为单个导入作业设置较高的期望任务并行度，默认值为 3。
  - 如果还未创建导入作业，则需要在执行 [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md) 创建导入作业时，设置该参数。
  - 如果已经创建导入作业，则需要执行 [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md)，修改该参数。

- `max_routine_load_task_concurrent_num`：Routine Load 导入作业的默认最大任务并行度的 ，默认值为 `5`。该参数为 FE 动态参数，更多说明和设置方式，请参见 [配置参数](../../administration/management/FE_configuration.md#导入导出)。

因此当消费分区和 BE 节点数量较多，并且大于其余两个参数时，如果您需要增加实际任务并行度，则可以提高如下参数。

假设消费分区数量为 `7`，存活 BE 数量为 `5`，`max_routine_load_task_concurrent_num` 为默认值 `5`。此时如果需要增加实际任务并发度至上限，则需要将 `desired_concurrent_number` 设置为 `5`（默认值为 `3`），则计算实际任务并行度 `min(5,7,5,5)` 为 `5`。

更多参数说明，请参见 [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#示例)。

**方式二**：**增大单个导入任务消费分区的数据量**。

> **注意**
>
> 该方式会造成数据导入的延迟变大。

单个 Routine Load 导入任务消费消息的上限由导入任务最多消费消息量 `max_routine_load_batch_size`，或者消费最大时长 `routine_load_task_consume_second` 决定。当导入任务消费数据并达到上述要求后，则完成消费。上述两个参数为 FE 配置项，更多说明和设置方式，请参见 [配置参数](../../administration/management/FE_configuration.md#导入导出)。

您可以通过查看 **be/log/be.INFO** 中的日志，分析单个导入任务消费数据量的上限由上述何种参数决定，并且通过提高该参数，增大单个导入任务消费的数据量。

```bash
I0325 20:27:50.410579 15259 data_consumer_group.cpp:131] consumer group done: 41448fb1a0ca59ad-30e34dabfa7e47a0. consume time(ms)=3261, received rows=179190, received bytes=9855450, eos: 1, left_time: -261, left_bytes: 514432550, blocking get time(us): 3065086, blocking put time(us): 24855
```

正常情况下，该日志的 `left_bytes` 字段应该 >= `0`，表示在 `routine_load_task_consume_second` 时间内一次读取的数据量还未超过 `max_routine_load_batch_size`，说明调度的一批导入任务都可以消费完 Kafka 的数据，不存在消费延迟，则此时您可以通过提高 `routine_load_task_consume_second`，增大单个导入任务消费分区的数据量 。

如果 `left_bytes < 0`，则表示未到在 `routine_load_task_consume_second` 规定时间，一次读取的数据量已经达到 `max_routine_load_batch_size`，每次 Kafka 的数据都会把调度的一批导入任务填满，因此极有可能 Kafka 还有剩余数据没有消费完，存在消费积压，则可以提高 `max_routine_load_batch_size`。

## 2. 执行 SHOW ROUTINE LOAD，导入作业为 **PAUSED** 或者 **CANCELLED 状态，如何排查错误并修复？**

- **报错提示**：导入作业变成 **PAUSED** 状态，并且 `ReasonOfStateChanged` 报错 `Broker: Offset out of range`。

  **原因分析**：导入作业的消费位点在 Kafka 分区中不存在。

  **解决方式**：您可以通过执行 SHOW ROUTINE LOAD，在 `Progress` 参数中查看导入作业的最新消费位点，并且在 Kafka 分区中查看是否存在该位点的消息。如果不存在，则可能有如下两个原因：

  - 创建导入作业时指定的消费位点为将来的时间点。
  - Kafka 分区中该位点的消息还未被导入作业消费，就已经被清理。建议您根据导入作业的导入速度设置合理的 Kafka 日志清理策略和参数，比如 `log.retention.hours`, `log.retention.bytes`等。

- **报错提示**：导入作业变成 **PAUSED** 状态。

  **原因分析**：可能为导入任务错误行数超过阈值 `max_error_number`。

  **解决方式**：您可以根据 `ReasonOfStateChanged`, `ErrorLogUrls`报错进行排查。

  - 如果是数据源的数据格式问题，则需要检查数据源数据格式，并进行修复。修复后您可以使用 [RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md)，恢复 **PAUSED** 状态的导入作业。

  - 如果是数据源的数据格式无法被 StarRocks 解析，则需要调整错误行数阈值`max_error_number`。
  您可以先执行 [SHOW ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/SHOW_ROUTINE_LOAD.md)，查看错误行数阈值 `max_error_number`，然后执行 [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/ALTER_ROUTINE_LOAD.md)，适当提高错误行数阈值 `max_error_number`。修改阈值后您可以使用 [RESUME ROUTINE LOAD](../../sql-reference/sql-statements/loading_unloading/routine_load/RESUME_ROUTINE_LOAD.md)，恢复 **PAUSED** 状态的导入作业。

- **报错提示**：如果导入作业变成为 **CANCELLED** 状态。

  **原因分析**：可能为导入任务执行时遇到异常，如表被删除。
  
  **解决方式**：您可以参考 `ReasonOfStateChanged`, `ErrorLogUrls` 报错进行排查和修复。但是修复后，您无法恢复 **CANCELLED** 状态的导入作业。

## 3. 使用 Routine Load 消费 Kafka 写入 StarRocks 是否能保证一致性语义？

Routine Load 能够保证 Exactly-once 语义。

一个导入任务是一个单独的事务，如果该事务在执行过程中出现错误，则事务会中止，FE 不会更新该导入任务相关分区的消费进度。并且 FE 在下一次调度任务执行队列中的导入任务时，从上次保存的分区消费位点发起消费请求，如此可以保证 Exactly once 语义。

## 如果 Routine Load 返回SSL身份验证错误该怎么办？

  **错误信息：**`routines:tls_process_server_certificate:certificate verify failed: broker certificate could not be verified, verify that ssl.ca.location is correctly configured or root CA certificates are installed (install ca-certificates package) (after 273ms in state SSL_HANDSHAKE)`

  **原因分析：**证书中的域名与 Kafka Broker 域名不一致。详见[更多细节](https://github.com/confluentinc/librdkafka/issues/4349)。

  **解决方案：**在 Routine Load Job 中添加属性 `"property.ssl.endpoint.identification.algorithm" = "none"`。

## 为什么 Routine Load 报告 “JSON data is an array.strip_outer_array must be set true”？

您的输入数据是一个JSON数组 `([{},{}])`。将属性 `strip_outer_array` 设置为 `true` 以展开它。

## 为什么创建 Routine Load 作业时收到 “There are more than 100 routine load jobs running”？

增加FE配置 `max_routine_load_job_num` 的值。

## 为什么即使配置了 SASL，Routine Load 作业创建仍然失败并显示 “failed to get partition meta”？

实际原因可能是 SASL 配置不正确。

## 如何处理Routine Load错误 “Create replicas failed …”？

调整以下FE配置：

```SQL
admin set frontend config ("tablet_create_timeout_second"="5");
admin set frontend config ("max_create_table_timeout_second"="600");
```

您也可以在配置文件中设置它们以持久化修改。

## 为什么 Routine Load 在消费 Kafka 时报告 “Bad message format”？

Kafka使用主机名进行通信。在所有托管StarRocks节点的服务器上的 `/etc/hosts` 中为Kafka节点添加主机名解析。

## 什么原因导致 Routine Load 失败并显示 "failed to send task: failed to submit task. error code: TOO MANY TASKS"？

这是因为总的 Routine Load 并发超过了集群能力（等于 `routine_load_thread_pool_size × 活跃 BE 数量`）。

解决方案：

- 减少导入QPS（推荐集群QPS < 10）。根据 `cluster routine_load_task_num / routine_load_task_consume_second` 计算集群QPS。

- 通过调整 `max_routine_load_batch_size` 和 `routine_load_task_timeout_second` 增加每个任务的批次大小（> 1 GB）。

- 确保 `routine_load_thread_pool_size` 小于BE CPU核心数的一半。

一个作业的并发由以下值的最小值决定：

- `kafka_partition_num`
- `desired_concurrent_number`
- `alive_be_num`
- `max_routine_load_task_concurrent_num`

您可以通过减少 `max_routine_load_task_concurrent_num` 来开始调整并发。