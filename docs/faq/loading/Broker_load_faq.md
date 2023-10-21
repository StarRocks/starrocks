# Broker Load

## In Broker Load, can a task that has already been executed successfully (State: FINISHED) be executed again?

A successfully executed cannot be executed again. To execute the task again, you need to reinitiate it. This is because the imported labels are not reusable, avoiding loss or duplication of imported tasks. You can use show load to see the import history, then copy it, modify the label and re-import it.

## Garbles in Broker Load

Issue description:

<<<<<<< HEAD
The load job reports errors. When you use the provided URL to view the error data, you can find garbles like below:
=======
## 3. When I load ORC-formatted data by using Broker Load, what do I do if the `ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>` error occurs?

The source data file has different column names than the destination StarRocks table. In this situation, you must use the `SET` clause in the load statement to specify the column mapping between the file and the table. When executing the `SET` clause, StarRocks needs to perform a type inference, but it fails in invoking the [cast](../../sql-reference/sql-functions/cast.md) function to transform the source data to the destination data types. To resolve this issue, make sure that the source data file has the same column names as the destination StarRocks table. As such, the `SET` clause is not needed and therefore StarRocks does not need to invoke the cast function to perform data type conversions. Then the Broker Load job can be run successfully.

## 4. The Broker Load job does not report errors, but why am I unable to query the loaded data?

Broker Load is an asynchronous loading method. The load job may still fail even if the load statement does not return errors. After you run a Broker Load job, you can use [SHOW LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) to view the result and `errmsg` of the load job. Then, you can modify the job configuration and retry.

## 5. What do I do if the "failed to send batch" or "TabletWriter add batch with unknown id" error occurs?

The amount of time taken to write the data exceeds the upper limit, causing a timeout error. To resolve this issue, modify the settings of the [session variable](../../reference/System_variable.md) `query_timeout` and the [BE configuration item](../../administration/Configuration.md#configure-be-static-parameters) `streaming_load_rpc_max_alive_time_sec` based on your business requirements.

## 6. What do I do if the "LOAD-RUN-FAIL; msg:OrcScannerAdapter::init_include_columns. col name = xxx not found" error occurs?

If you are loading Parquet- or ORC-formatted data, check whether the column names held in the first row of the source data file are the same as the column names of the destination StarRocks table.
>>>>>>> 1bdc91c00 ([Doc] Markdown 23 (#33341))

```SQL
Reason: column count mismatch, expect=6 real=1. src line: [��I have a pen,I have an apple];
```

**Solution:**

The source data is not in UTF-8 encoding format, but StarRocks supports loading only data in UTF-8 encoding format. Convert the source data into UTF-8 encoding format and try again.

## In Broker Load, hdfs data import exceptional date fields (+8h)

Issue description:

Timezone was set to China time when the table was created and when brokerload is performed. The server is in UTC timezone. So the imported date fields gained 8 hours.

**Solution:**

Remove the timezone when creating the table.

## In Broker Load, orc data import failed. ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to `ARRAY<VARCHAR(30)>`

Column names on both sides are not consistent in the source file and the StarRocks. Type inference within the system was performed on Set and failed on Cast. To resolve this, you can change the field names on both sides to be identical. After that, Set or Cast is not needed, thus making import successful.

A successfully executed cannot be executed again. To execute the task again, you need to reinitiate it. This is because the imported labels are not reusable, avoiding loss or duplication of imported tasks. You can use show load to see the import history, then copy it, modify the label and re-import it.

## No error is reported in Broker Load, but the data still cannot be queried

Broker Load is asynchronous. Successfully creating load statement does not guarantee successful import. You can use show load to view the current status and errmsg. Modify related contents and retry the task.

Column names on both sides are not consistent in the source file and the StarRocks. Type inference within the system was performed on Set and failed on Cast. To resolve this, you can change the field names on both sides to be identical. After that, Set or Cast is not needed, thus making import successful.
