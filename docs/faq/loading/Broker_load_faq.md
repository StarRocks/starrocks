# Broker Load

## In Broker Load, can a task that has already been executed successfully (State: FINISHED) be executed again?

A successfully executed cannot be executed again. To execute the task again, you need to reinitiate it. This is because the imported labels are not reusable, avoiding loss or duplication of imported tasks. You can use show load to see the import history, then copy it, modify the label and re-import it.

## Garbles in Broker Load

Issue description:

The load job reports errors. When you use the provided URL to view the error data, you can find garbles like below:

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

## In Broker Load, orc data import failed. `ErrorMsg: type:ETL_RUN_FAIL; msg:Cannot cast '<slot 6>' from VARCHAR to ARRAY<VARCHAR(30)>`

Column names on both sides are not consistent in the source file and the StarRocks. Type inference within the system was performed on Set and failed on Cast. To resolve this, you can change the field names on both sides to be identical. After that, Set or Cast is not needed, thus making import successful.

A successfully executed cannot be executed again. To execute the task again, you need to reinitiate it. This is because the imported labels are not reusable, avoiding loss or duplication of imported tasks. You can use show load to see the import history, then copy it, modify the label and re-import it.

## No error is reported in Broker Load, but the data still cannot be queried

Broker Load is asynchronous. Successfully creating load statement does not guarantee successful import. You can use show load to view the current status and errmsg. Modify related contents and retry the task.

Column names on both sides are not consistent in the source file and the StarRocks. Type inference within the system was performed on Set and failed on Cast. To resolve this, you can change the field names on both sides to be identical. After that, Set or Cast is not needed, thus making import successful.
