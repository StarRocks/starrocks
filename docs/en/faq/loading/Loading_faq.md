---
displayed_sidebar: "English"
---

# Data loading common questions

## 1. What do I do if the "close index channel failed" or "too many tablet versions" error occurs?

You were running load jobs too frequently, and the data was not compacted in a timely manner. As a result, the number of data versions generated during loading exceeds the maximum number (which defaults to 1000) of data versions that are allowed. Use one of the following methods to resolve this issue:

- Increase the amount of data loaded in each individual job, thereby reducing loading frequency.

- Modify the configuration items in the BE configuration file **be.conf** of each BE as follows, thereby accelerating data compactions:

    ```Plain
    cumulative_compaction_num_threads_per_disk = 4
    base_compaction_num_threads_per_disk = 2
    cumulative_compaction_check_interval_seconds = 2
    ```

  After you modify the settings of the preceding configuration items, you must observe the memory and I/O to ensure that they are normal.

## 2. What do I do if the "Label Already Exists" error occurs?

This error occurs because the load job has the same label as another load job, which has been successfully run or is being run, within the same StarRocks database.

Stream Load jobs are submitted according to HTTP. In general, request retry logic is embedded in HTTP clients of all programmatic languages. When the StarRocks cluster receives a load job request from an HTTP client, it immediately starts to process the request, but it does not return the job result to the HTTP client in a timely manner. As a result, the HTTP client sends the same load job request again. However, the StarRocks cluster is already processing the first request and therefore returns the `Label Already Exists` error for the second request.

Do as follows to check that load jobs submitted by using different loading methods do not have the same label and are not repeatedly submitted:

- View the FE log and check whether the label of the failed load job is recorded twice. If the label is recorded twice, the client has submitted the load job request twice.

  > **NOTE**
  >
  > The StarRocks cluster does not distinguish between the labels of load jobs based on loading methods. Therefore, load jobs submitted by using different loading methods may have the same label.

- Run SHOW LOAD WHERE LABEL = "xxx" to check for load jobs that have the same label and are in the **FINISHED** state.

  > **NOTE**
  >
  > `xxx` is the label that you want to check.

Before you submit a load job, we recommend that you calculate the approximate amount of time required to load the data and then adjust the client-side request timeout period accordingly. This way, you can prevent the client from submitting the load job request multiple times.

## 3. What do I do if the "ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel" error occurs?

Execute [SHOW LOAD](../../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md), and use the error URL in the returned execution result to view the error details.

Common data quality errors are as follows:

- "convert csv string to INT failed."
  
  Strings from a source column failed to be transformed into the data type of the matching destination column. For example, `abc` failed to be transformed into a numeric value.

- "the length of input is too long than schema."
  
  Values from a source column are in lengths that are not supported by the matching destination column. For example, the source column values of CHAR data type exceed the destination column's maximum length specified at table creation, or the source column values of INT data type exceed 4 bytes.

- "actual column number is less than schema column number."
  
  After a source row is parsed based on the specified column separator, the number of columns obtained is smaller than the number of columns in the destination table. A possible reason is that the column separator specified in the load command or statement differs from the column separator that is actually used in that row.

- "actual column number is more than schema column number."
  
  After a source row is parsed based on the specified column separator, the number of columns obtained is greater than the number of columns in the destination table. A possible reason is that the column separator specified in the load command or statement differs from the column separator that is actually used in that row.

- "the frac part length longer than schema scale."
  
  The decimal parts of values from a DECIMAL-type source column exceed the specified length.

- "the int part length longer than schema precision."
  
  The integer parts of values from a DECIMAL-type source column exceed the specified length.

- "there is no corresponding partition for this key."
  
  The value in the partition column for a source row is not within the partition range.

## 4. What do I do if RPC times out?

Check the setting of the `write_buffer_size` configuration item in the BE configuration file **be.conf** of each BE. This configuration item is used to control the maximum size per memory block on the BE. The default maximum size is 100 MB. If the maximum size is exceedingly large, Remote Procedure Call (RPC) may time out. To resolve this issue, adjust the settings of the `write_buffer_size` and `tablet_writer_rpc_timeout_sec` configuration items in the BE configuration file. For more information, see [BE configurations](../../loading/Loading_intro.md#be-configurations).

## 5. What do I do if the "Value count does not match column count" error occurs?

After my load job failed, I used the error URL returned in the job result to retrieve the error details and found the "Value count does not match column count" error, which indicates a mismatch between the number of columns in the source data file and the number of columns in the destination StarRocks table:

```Java
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:00Z,cpu0,80.99
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:10Z,cpu1,75.23
Error: Value count does not match column count. Expect 3, but got 1. Row: 2023-01-01T18:29:20Z,cpu2,59.44
```

The reason for this issue is as follows:

The column separator specified in the load command or statement differs from the column separator that is actually used in the source data file. In the preceding example, the CSV-formatted data file consists of three columns, which are separated with commas (`,`). However, `\t` is specified as the column separator in the load command or statement. As a result, the three columns from the source data file are incorrectly parsed into one column.

Specify commas (`,`) as the column separator in the load command or statement. Then, submit the load job again.
