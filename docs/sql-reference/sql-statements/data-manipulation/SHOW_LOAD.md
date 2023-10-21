# SHOW LOAD

## description

This statement is used to display the execution of the specified import task.

Syntax:

```sql
SHOW LOAD
[FROM db_name]
[
WHERE
[LABEL [ = "your_label" | LIKE "label_matcher"]]
[STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
]
[ORDER BY ...]
[LIMIT limit][OFFSET offset];
```

Note：

```plain text
1. If db_ name is not specified, use the current default db
2. If LABEL LIKE is used, the label of the import task will be matched. Include import task of label_ matcher.
3. If LABEL = is used, the specified label will be matched exactly
4. If STATE is specified, LOAD status is matched
5. You can use ORDER BY to sort any combination of columns
6. If LIMIT is specified, limit matching records will be displayed. Otherwise, everything is displayed
7. If OFFSET is specified, query results will be displayed starting from offset. By default, the offset is 0.
8. If you are using broker load, the connection in the URL column can be viewed using the following command:
```

```sql
SHOW LOAD WARNINGS ON 'url'
```

<<<<<<< HEAD
## example
=======
| **Field**      | **Broker Load**                                              | **Spark Load**                                               | **INSERT**                                                   |
| -------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| JobId          | The unique ID assigned by StarRocks to identify the load job in your StarRocks cluster. | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| Label          | The label of the load job. The label of a load job is unique within a database but can be duplicate across different databases. | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| State          | The state of the load job.<ul><li>`PENDING`: The load job is in the queue waiting to be scheduled.</li><li>`LOADING`: The load job is running.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> | The state of the load job.<ul><li>`PENDING`: Your StarRocks cluster is preparing configurations related to ETL and then submits an ETL job to your Spark cluster.</li><li>`ETL`: Your Spark cluster is executing the ETL job and then writes the data into the corresponding HDFS cluster.</li><li>`LOADING`: The data in the HDFS cluster is being loaded to your StarRocks cluster, which means the load job is running.</li><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> | The state of the load job.<ul><li>`FINISHED`: The load job succeeded.</li><li>`CANCELLED`: The load job failed.</li></ul> |
| Progress       | The stage of the load job. A Broker Load job only has the `LOAD` stage, which ranges from 0% to 100% to describe the progress of the stage. When the load job enters the `LOAD` stage, `LOADING` is returned for the `State` parameter. A Broker Load job does not have the `ETL` stage. The `ETL` parameter is valid only for a Spark Load job.<br />**Note**<ul><li>The formula to calculate the progress of the `LOAD` stage: Number of StarRocks tables that complete data loading/Number of StarRocks tables that you plan to load data into * 100%.</li><li>When all data is loaded into StarRocks, `99%` is returned for the `LOAD` parameter. Then, loaded data starts taking effect in StarRocks. After the data takes effect, `100%` is returned for the `LOAD` parameter.</li><li>The progress of the `LOAD` stage is not linear. Therefore, the value of the `LOAD` parameter may not change over a period of time even if data loading is still ongoing.</li></ul> | The stage of the load job. A Spark Load job has two stages:<ul><li>`ETL`: ranges from 0% to 100% to describe the progress of the `ETL` stage.</li><li>`LOAD`: ranges from 0% to 100% to describe the progress of the `Load` stage. </li></ul>When the load job enters the `ETL` stage, `ETL` is returned for the `State` parameter. When the load job moves to the `LOAD` stage, `LOADING` is returned for the `State` parameter. <br />The **Note** is the same as those for Broker Load. | The stage of the load job. An INSERT job only has the `LOAD` stage, which ranges from 0% to 100% to describe the progress of the stage. When the load job enters the `LOAD` stage, `LOADING` is returned for the `State` parameter. An INSERT job does not have the `ETL` stage. The `ETL` parameter is valid only for a Spark Load job.<br />The **Note** is the same as those for Broker Load. |
| Type           | The method of the load job. The value of this parameter defaults to `BROKER`. | The method of the load job. The value of this parameter defaults to `SPARK`. | The method of the load job. The value of this parameter defaults to `INSERT`. |
| EtlInfo        | The metrics related to ETL.<ul><li>`unselected.rows`: The number of rows that are filtered out by the WHERE clause.</li><li>`dpp.abnorm.ALL`: The number of rows that are filtered out due to data quality issues, which refers to mismatches between source tables and StarRocks tables in, for example, the data type and the number of columns.</li><li>`dpp.norm.ALL`: The number of rows that are loaded into your StarRocks cluster.</li></ul>The sum of the preceding metrics is the total number of rows of raw data. You can use the following formula to calculate whether the percentage of unqualified data exceeds the value of the `max-filter-ratio` parameter:`dpp.abnorm.ALL`/(`unselected.rows` + `dpp.abnorm.ALL` + `dpp.norm.ALL`). | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The metrics related to ETL. An INSERT job does not have the `ETL` stage. Therefore, `NULL` is returned. |
| TaskInfo       | The parameters that are specified when you create the load job.<ul><li>`resource`: This parameter is valid only in a Spark Load job.</li><li>`timeout`: The time period that a load job is allowed to run. Unit: seconds.</li><li>`max-filter-ratio`: The largest percentage of rows that are filtered out due to data quality issues.</li></ul>For more information, see [BROKER LOAD](../data-manipulation/BROKER_LOAD.md). | The parameters that are specified when you create the load job.<ul><li>`resource`: The resource name.</li><li>`timeout`: The time period that a load job is allowed to run. Unit: seconds.</li><li>`max-filter-ratio`: The largest percentage of rows that are filtered out due to data quality issues.</li></ul>For more information, see [SPARK LOAD](../data-manipulation/SPARK_LOAD.md). | The parameters that are specified when you create the load job.<ul><li>`resource`: This parameter is valid only in a Spark Load job.</li><li>`timeout`: The time period that a load job is allowed to run. Unit: seconds.</li><li>`max-filter-ratio`: The largest percentage of rows that are filtered out due to data quality issues.</li></ul>For more information, see [INSERT](../data-manipulation/insert.md). |
| ErrorMsg       | The error message returned when the load job fails. When the state of the loading job is `PENDING`, `LOADING`, or `FINISHED`, `NULL` is returned for the `ErrorMsg` field. When the state of the loading job is `CANCELLED`, the value returned for the `ErrorMsg` field consists of two parts: `type` and `msg`.<ul><li>The `type` part can be any of the following values:<ul><li>`USER_CANCEL`: The load job was manually canceled.</li><li>`ETL_SUBMIT_FAIL`: The load job failed to be submitted.</li><li>`ETL-QUALITY-UNSATISFIED`: The load job failed because the percentage of unqualified data exceeds the value of the `max-filter-ratio` parameter.</li><li>`LOAD-RUN-FAIL`: The load job failed in the `LOAD` stage.</li><li>`TIMEOUT`: The load job failed to finish within the specified timeout period.</li><li>`UNKNOWN`: The load job failed due to an unknown error.</li></ul></li><li>The `msg` part provides the detailed cause of the load failure.</li></ul> | The error message returned when the load job fails. When the state of the loading job is `PENDING`, `LOADING`, or `FINISHED`, `NULL` is returned for the `ErrorMsg` field. When the state of the loading job is `CANCELLED`, the value returned for the `ErrorMsg` field consists of two parts: `type` and `msg`.<ul><li>The `type` part can be any of the following values:<ul><li>`USER_CANCEL`: The load job was manually canceled.</li><li>`ETL_SUBMIT_FAIL`: StarRocks failed to submit an ETL job to Spark.</li><li>`ETL-RUN-FAIL`: Spark failed to execute the ETL job. </li><li>`ETL-QUALITY-UNSATISFIED`: The load job failed because the percentage of unqualified data exceeds the value of the `max-filter-ratio` parameter.</li><li>`LOAD-RUN-FAIL`: The load job failed in the `LOAD` stage.</li><li>`TIMEOUT`: The load job failed to finish within the specified timeout period.</li><li>`UNKNOWN`: The load job failed due to an unknown error.</li></ul></li><li>The `msg` part provides the detailed cause of the load failure.</li></ul> | The error message returned when the load job fails. When the state of the loading job is `FINISHED`, `NULL` is returned for the `ErrorMsg` field. When the state of the loading job is `CANCELLED`, the value returned for the `ErrorMsg` field consists of two parts: `type` and `msg`.<ul><li>The `type` part can be any of the following values:<ul><li>`USER_CANCEL`: The load job was manually canceled.</li><li>`ETL_SUBMIT_FAIL`: The load job failed to be submitted.</li><li>`ETL_RUN_FAIL`: The load job failed to run.</li><li>`ETL_QUALITY_UNSATISFIED`: The load job failed due to quality issues of raw data.</li><li>`LOAD-RUN-FAIL`: The load job failed in the `LOAD` stage.</li><li>`TIMEOUT`: The load job failed to finish within the specified timeout period.</li><li>`UNKNOWN`: The load job failed due to an unknown error.</li><li>`TXN_UNKNOWN`: The load job failed because the state of the transaction of the load job is unknown.</li></ul></li><li>The `msg` part provides the detailed cause of the load failure.</li></ul> |
| CreateTime     | The time at which the load job was created.                  | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| EtlStartTime   | A Broker Load job does not have the `ETL` stage. Therefore, the value of this field is the same as the value of the `LoadStartTime` field. | The time at which the `ETL` stage starts.                    | An INSERT job does not have the `ETL` stage. Therefore, the value of this field is the same as the value of the `LoadStartTime` field. |
| EtlFinishTime  | A Broker Load job does not have the `ETL` stage. Therefore, the value of this field is the same as the value of the `LoadStartTime` field. | The time at which the `ETL` stage finishes.                  | An INSERT job does not have the `ETL` stage. Therefore, the value of this field is the same as the value of the `LoadStartTime` field. |
| LoadStartTime  | The time at which the `LOAD` stage starts.                   | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| LoadFinishTime | The time at which the load job finishes.                     | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| URL            | The URL that is used to access the unqualified data detected in the load job. You can use the `curl` or `wget` command to access the URL and obtain the unqualified data. If no unqualified data is detected, `NULL` is returned. | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
| JobDetails     | Other information related to the load job.<ul><li>`Unfinished backends`: The ID of the BE that does not complete data loading.</li><li>`ScannedRows`: The total number of rows that are loaded into StarRocks and the number of rows that are filtered out.</li><li>`TaskNumber`: A load job can be split into one or more tasks that concurrently run. This field indicates the number of load tasks.</li><li>`All backends`: The ID of the BE that is executing data loading.</li><li>`FileNumber`: The number of source data files.</li><li>`FileSize`: The data volume of source data files. Unit: bytes.</li></ul> | The field has the same meaning in a Spark Load job as it does in a Broker Load job. | The field has the same meaning in a INSERT job as it does in a Broker Load job. |
>>>>>>> 1bdc91c00 ([Doc] Markdown 23 (#33341))

1. Show all import tasks of default db

    ```sql
    SHOW LOAD;
    ```

2. Show the import task of the specified db. The label contains the string "2014_01_02", showing the oldest 10

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
    ```

3. Show the import task of the specified db, specify the label as "load_example_db_20140102" and sort it in descending order by LoadStartTime

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;
    ````

4. Display the import task of the specified db, specify label as "load_example_db_20140102", state as "loading", and sort by LoadStartTime in descending order

    ```sql
    SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;
    ```

5. Display the import tasks of the specified db, sort them in descending order by LoadStartTime, and display 10 query results starting from offset 5

    ```sql
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 5,10;
    SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 10 offset 5;
    ```

6. Small batch import is a command to view the import status.

    ```bash
    curl --location-trusted -u {user}:{passwd} \
        http://{hostname}:{port}/api/{database}/_load_info?label={labelname}
    ```

## keyword

SHOW,LOAD
