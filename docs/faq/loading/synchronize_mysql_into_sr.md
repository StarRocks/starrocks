# Synchronize data from MySQL in real time

## What do I do if a Flink job reports an error?

A Flink job reports the error `Could not execute SQL statement. Reason:org.apache.flink.table.api.ValidationException: One or more required options are missing.`

A possible reason is that the required configuration information is missing in multiple sets of rules, such as `[table-rule.1]` and `[table-rule.2]`, in the SMT configuration file **config_prod.conf**.

You can check whether each set of rules, such as `[table-rule.1]` and `[table-rule.2]` is configured with the required database, table, and Flink connector information.

## How can I make Flink automatically restart failed tasks?

Flink automatically restarts failed tasks through the [checkpointing mechanism](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/) and [restart strategy](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/ops/state/task_failure_recovery/).

For example, if you need to enable the checkpointing mechanism and use the default restart strategy, which is the fixed delay restart strategy, you can configure the following information in the configuration file **flink-conf.yaml**:

```Bash
execution.checkpointing.interval: 300000
state.backend: filesystem
state.checkpoints.dir: file:///tmp/flink-checkpoints-directory
```

Parameter description:

> **NOTE**
>
> For more detailed parameter descriptions in Flink documentation, see [Checkpointing](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/).

- `execution.checkpointing.interval`: the base time interval of checkpointing. Unit: millisecond. To enable the checkpointing mechanism, you need to set this parameter to a value greater than `0`.
- `state.backend`: specifies the state backend to determine how the state is represented internally, and how and where it is persisted upon checkpointing. Common values are `filesystem` or `rocksdb`. After the checkpointing mechanism is enabled, the state is persisted upon checkpoints to prevent data loss and ensure data consistency after recovery. For more information on state, see [State Backends](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/).
- `state.checkpoints.dir`: the directory to which checkpoints are written to.

## How can I manually stop a Flink job and later restore it to the state before stopping?

You can manually trigger a [savepoint](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/savepoints/) when stopping a Flink job (a savepoint is a consistent image of the execution state of a streaming Flink job, and is created based on the checkpointing mechanism). Later, you can restore the Flink job from the specified savepoint.

1. Stop the Flink job with a savepoint. The following command automatically triggers a savepoint for the Flink job `jobId` and stops the Flink job. Additionally, you can specify a target file system directory to store the savepoint.

    ```Bash
    bin/flink stop --type [native/canonical] --savepointPath [:targetDirectory] :jobId
    ```

    Parameter description:

    - `jobId`: You can view the Flink job ID from the Flink WebUI or by running `flink list -running` on the command line.
    - `targetDirectory`: You can specify `state.savepoints.dir` as the default directory for storing savepoints in the Flink configuration file **flink-conf.yml**. When a savepoint is triggered, the savepoint is stored in this default directory and you do not need to specify a directory .

    ```Bash
    state.savepoints.dir: [file:// or hdfs://]/home/user/savepoints_dir
    ```

2. Resubmit the Flink job with the preceding savepoint specified.

    ```Bash
    ./flink run -c com.starrocks.connector.flink.tools.ExecuteSQL -s savepoints_dir/savepoints-xxxxxxxx flink-connector-starrocks-xxxx.jar -f flink-create.all.sql 
    ```
