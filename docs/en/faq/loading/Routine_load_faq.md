---
displayed_sidebar: "English"
---

# Routine Load

## How can I improve loading performance?

**Method 1: Increase the actual load task parallelism** by splitting a load job into as many parallel load tasks as possible.

> **NOTICE**
>
> This method may consume more CPU resources and cause too many tablet versions.

The actual load task parallelism is determined by the following formula composed of several parameters, with an upper limit of the number of BE nodes alive or the number of partitions to be consumed.

```Plaintext
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

Parameter description:

- `alive_be_number`: the number of BE nodes alive.
- `partition_number`: the number of partitions to be consumed.
- `desired_concurrent_number`: the desired load task parallelism for a Routine Load job. The default value is `3`. You can set a higher value for this parameter to increase the actual load task parallelism.
  - If you have not created a Routine Load job, you need to set this parameter when using [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md) to create a Routine Load job.
  - If you have already created a Routine Load job, you need to use [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/ALTER_ROUTINE_LOAD.md) to modify this parameter.
- `max_routine_load_task_concurrent_num`: the default maximum task parallelism for a Routine Load job. The default value is `5`. This parameter is a an FE dynamic parameter. For more information and the configuration method, see [Parameter configuration](../../administration/FE_configuration.md#loading-and-unloading).

Therefore, when the number of partitions to be consumed and the number of BE nodes alive are greater than the other two parameters, you can increase the values of `desired_concurrent_number` and `max_routine_load_task_concurrent_num` parameters to increase the actual load task parallelism.

For example, the number of partitions to be consumed is `7`, the number of live BE nodes is `5`, and `max_routine_load_task_concurrent_num` is the default value `5`. At this time, if you need to increase the load task parallelism to the upper limit, you need to set `desired_concurrent_number` to `5` (the default value is `3`). Then, the actual task parallelism `min(5,7,5,5)` is computed to be `5`.

For more parameter descriptions, see [CREATE ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md).

**Method 2: Increase the amount of data consumed by a Routine Load task from one or more partitions.**

> **NOTICE**
>
> This method may cause delay in data loading.

The upper limit of the number of messages that a Routine Load task can consume is determined by either the parameter `max_routine_load_batch_size` which means the maximum number of messages that a load task can consume or the parameter `routine_load_task_consume_second` which means the maximum duration of message consumption. Once an load task consumes enough data that meets either requirement, the consumption is complete. These two parameters are FE dynamic parameters. For more information and the configuration method, see [Parameter configuration](../../administration/FE_configuration.md#loading-and-unloading).

You can analyze which parameter determines the upper limit of the amount of data consumed by a load task by viewing the log in **be/log/be.INFO**. By increasing that parameter, you can increase the amount of data consumed by a load task.

```Plaintext
I0325 20:27:50.410579 15259 data_consumer_group.cpp:131] consumer group done: 41448fb1a0ca59ad-30e34dabfa7e47a0. consume time(ms)=3261, received rows=179190, received bytes=9855450, eos: 1, left_time: -261, left_bytes: 514432550, blocking get time(us): 3065086, blocking put time(us): 24855
```

Normally, the field `left_bytes` in the log is greater than or equal to `0`, indicating that the amount of data consumed by a load task has not exceeded `max_routine_load_batch_size` within `routine_load_task_consume_second`. This means that a batch of scheduled load tasks can consume all data from Kafka without delay in consumption. In this scenario, you can set a larger value for `routine_load_task_consume_second` to increase the amount of data consumed by a load task from one or more partitions.

If the field `left_bytes` is less than `0`, it means that the amount of data consumed by a load task has reached `max_routine_load_batch_size` within `routine_load_task_consume_second`. Every time data from Kafka fills the batch of scheduled load tasks. Therefore, it is highly likely that there is remaining data that has not been consumed in Kafka, causing delay in consumption. In this case, you can set a larger value for `max_routine_load_batch_size`.

## What do I do if the result of SHOW ROUTINE LOAD shows that the load job is in the `PAUSED` state?

- Check the field `ReasonOfStateChanged` and it reports the error message `Broker: Offset out of range`.

  **Cause analysis:** The consumer offset of the load job does not exist in the Kafka partition.

  **Solution:** You can execute [SHOW ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md) and check the latest consumer offset of the load job in the parameter `Progress`. Then, you can verify if the corresponding message exists in the Kafka partition. If it does not exist, it may be because

  - The consumer offset specified when the load job is created is an offset in the future.
  - The message at the specified consumer offset in the Kafka partition has been removed before being consumed by the load job. It is recommended to set a reasonable Kafka log cleaning policy and parameters, such as `log.retention.hours and log.retention.bytes`, based on the loading speed.

- Check the field `ReasonOfStateChanged` and it doesn't report the error message `Broker: Offset out of range`.

  **Cause analysis:** The number of error rows in the load task exceeds the threshold `max_error_number`.

  **Solution:** You can troubleshoot and fix the issue by using error messages in the fields `ReasonOfStateChanged` and `ErrorLogUrls`.

  - If it is caused by incorrect data format in the data source, you need to check the data format and fix the issue. After successfully fixing the issue, you can use [RESUME ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/RESUME_ROUTINE_LOAD.md) to resume the paused load job.

  - If it is because that StarRocks cannot parse the data format in the data source, you need to adjust the threshold `max_error_number`. You can first execute [SHOW ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD.md) to view the value of `max_error_number`, and then use [ALTER ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/ALTER_ROUTINE_LOAD.md) to increase the threshold. After modifying the threshold, you can use [RESUME ROUTINE LOAD](../../sql-reference/sql-statements/data-manipulation/RESUME_ROUTINE_LOAD.md) to resume the paused load job.

## What do I do if the result of SHOW ROUTINE LOAD shows that the load job is in the `CANCELLED` state?

  **Cause analysis:** The load job encountered an exception during loading, such as the table deleted.

  **Solution:** When troubleshooting and fixing the issue, you can refer to the error messages in the fields `ReasonOfStateChanged` and `ErrorLogUrls`. However, after fixing the issue, you cannot resume the cancelled load job.

## Can Routine Load guarantee consistency semantics when consuming from Kafka and writing to StarRocks?

   Routine Load guarantees exactly-once semantics.

   Each load task is a individual transaction. If an error occurs during the execution of the transaction, the transaction is aborted, and the FE does not update the consumption progress of the relevant partitions of the load tasks. When the FE schedules the load tasks from the task queue next time, the load tasks send the consumption request from the last saved consumption position of the partitions, thus ensuring exactly-once semantics.
