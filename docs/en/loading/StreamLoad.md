# Load data from a local file system or a streaming data source using HTTP push

StarRocks provides the loading method HTTP-based Stream Load to help you load data from a local file system or a streaming data source.

Stream Load runs in synchronous loading mode. After you submit a load job, StarRocks synchronously runs the job, and returns the result of the job after the job finishes. You can determine whether the job is successful based on the job result.

Stream Load is suitable for the following business scenarios:

- Load a local data file.
  
  In most cases, we recommend that you use curl to submit a load job, which is run to load the data of a local data file into StarRocks.

- Load streaming data.
  
  In most cases, we recommend that you use programs such as Apache Flink® to submit a load job, within which a series of tasks can be generated to continuously load streaming data in real time into StarRocks.

Additionally, Stream Load supports data transformation at data loading. For more information, see [Transform data at loading](../loading/Etl_in_loading.md).

> Note: After you load data into a StarRocks table by using Stream Load, the data of the materialized views that are created on that table is also updated.

## Supported data file formats

Stream Load supports the following data file formats:

- CSV

- JSON

You can use the `streaming_load_max_mb` parameter to specify the maximum size of each data file you want to load. The default maximum size is 10 GB. We recommend that you retain the default value of this parameter. For more information, see the "[Parameter configurations](../loading/StreamLoad.md#parameter-configurations)" section of this topic.

## Limits

Stream Load does not support loading the data of a CSV file that contains a JSON-formatted column.

## Principles

If you choose the loading method Stream Load, you must submit a load request on your client to an FE according to HTTP. The FE uses an HTTP redirect to forward the load request to a specific BE.

> Note: You can also create a load job on your client to send a load request to a BE of your choice.

The BE that receives the load request runs as the Coordinator BE to split data based on the used schema into portions and assign each portion of the data to the other involved BEs. After the load finishes, the Coordinator BE returns the result of the load job to your client.

> Note: If you send load requests to an FE, the FE uses a polling mechanism to decide which BE will receive the load requests. The polling mechanism helps achieve load balancing within your StarRocks cluster. Therefore, we recommend that you send load requests to an FE and let the FE decide which BE will run as the Coordinator BE to process the load requests.

The following figure shows the workflow of a Stream Load job.

![Workflow of Stream Load](../assets/4.2-1.png)

## Load a local data file

### Create a load job

This section uses curl as an example to describe how to load the data of a CSV or JSON file from your local file system into StarRocks. For detailed syntax and parameter descriptions, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).

#### Load CSV data

##### Data examples

1. In your StarRocks database `test_db`, create a table named `table1` that uses the Primary Key model. The table consists of three columns: `id`, `name`, and `score`, of which `id` is the primary key.

   ```SQL
   MySQL [test_db]> CREATE TABLE `table1`
   (
      `id` int(11) NOT NULL COMMENT "user ID",
       `name` varchar(65533) NULL COMMENT "user name",
       `score` int(11) NOT NULL COMMENT "user score"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

2. In your local file system, create a CSV file named `example1.csv`. The file consists of three columns, which represent the user ID, user name, and user score in sequence.

   ```Plain
   1,Lily,23
   2,Rose,23
   3,Alice,24
   4,Julia,25
   ```

##### Load data

Run the following command to load the data of `example1.csv` into `table1`:

```Bash
curl --location-trusted -u root: -H "label:123" \
    -H "column_separator:," \
    -H "columns: id, name, score" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

`example1.csv` consists of three columns, which are separated by commas (,) and can be mapped in sequence onto the `id`, `name`, and `score` columns of `table1`. Therefore, you need to use the `column_separator` parameter to specify the comma (,) as the column separator. You also need to use the `columns` parameter to temporarily name the three columns of `example1.csv` as `id`, `name`, and `score`, which are mapped in sequence onto the three columns of `table1`.

##### Query data

After the load is complete, query the data of `table1` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table1;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    23 |
|    2 | Rose  |    23 |
|    3 | Alice |    24 |
|    4 | Julia |    25 |
+------+-------+-------+
4 rows in set (0.00 sec)
```

#### Load JSON data

##### Data examples

1. In your StarRocks database `test_db`, create a table named `table2` that uses the Primary Key model. The table consists of two columns: `id` and `city`, of which `id` is the primary key.

   ```SQL
   MySQL [test_db]> CREATE TABLE `table2`
   (
       `id` int(11) NOT NULL COMMENT "city ID",
       `city` varchar(65533) NULL COMMENT "city name"
   )
   ENGINE=OLAP
   PRIMARY KEY(`id`)
   DISTRIBUTED BY HASH(`id`) BUCKETS 10;
   ```

2. In your local file system, create a JSON file named `example2.json`. The file consists of two columns, which represent city ID and city name in sequence.

   ```JSON
   {"name": "Beijing", "code": 2}
   ```

##### Load data

Run the following command to load the data of `example2.json` into `table2`:

```Bash
curl -v --location-trusted -u root: -H "strict_mode: true" \
    -H "format: json" -H "jsonpaths: [\"$.name\", \"$.code\"]" \
    -H "columns: city,tmp_id, id = tmp_id * 100" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

`example2.json` consists of two keys, `name` and `code`, which are mapped onto the `id` and `city` columns of `table2`, as shown in the following figure.

![img](../assets/4.2-2en.png)

The mappings shown in the preceding figure are described as follows:

- StarRocks extracts the `name` and `code` keys of `example2.json` and maps them onto the `name` and `code` fields declared in the `jsonpaths` parameter.

- StarRocks extracts the `name` and `code` fields declared in the `jsonpaths` parameter and **maps them in sequence** onto the `city` and `tmp_id` fields declared in the `columns` parameter.

- StarRocks extracts the `city` and `tmp_id` fields declared in the `columns` parameter and **maps them by name** onto the `city` and `id` columns of `table2`.

> Note: In the preceding example, the value of `code` in `example2.json` is multiplied by 100 before it is loaded into the `id` column of `table2`.

For detailed mappings between `jsonpaths`, `columns`, and the columns of the StarRocks table, see the "Usage notes" section in [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).

##### Query data

After the load is complete, query the data of `table2` to verify that the load is successful:

```SQL
MySQL [test_db]> SELECT * FROM table2;
+------+--------+
| id   | city   |
+------+--------+
| 200  | Beijing|
+------+--------+
4 rows in set (0.01 sec)
```

### View a load job

After a load job is complete, StarRocks returns the result of the job in JSON format. For more information, see the "Return value" section in [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).

Stream Load does not allow you to query the result of a load job by using the SHOW LOAD statement.

### Cancel a load job

Stream Load does not allow you to cancel a load job. If a load job times out or encounters errors, StarRocks automatically cancels the job.

## Load streaming data

Stream Load allows you to load streaming data into StarRocks in real time by using programs. For more information, see the following topics:

- For information about how to run Stream Load jobs by using Flink, see [Load data by using flink-connector-starrocks](../loading/Flink-connector-starrocks.md).

- For information about how to run Stream Load jobs by using Java programs, visit [https://github.com/StarRocks/demo/MiscDemo/stream_load](https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load).

- For information about how to run Stream Load jobs by using Apache Spark™, see [01_sparkStreaming2StarRocks](https://github.com/StarRocks/demo/blob/master/docs/01_sparkStreaming2StarRocks.md).

## Parameter configurations

This section describes some system parameters that you need to configure if you choose the loading method Stream Load. These parameter configurations take effect on all Stream Load jobs.

- `streaming_load_max_mb`: the maximum size of each data file you want to load. The default maximum size is 10 GB. For more information, see [BE configuration items](../administration/Configuration.md#be-configuration-items).
  
  We recommend that you do not load more than 10 GB of data at a time. If the size of a data file exceeds 10 GB, we recommend that you split the data file into small files that each are less than 10 GB in size and then load these files one by one. If you cannot split a data file greater than 10 GB, you can increase the value of this parameter based on the file size.

  After you increase the value of this parameter, the new value can take effect only after you restart the BEs of your StarRocks cluster. Additionally, system performance may deteriorate, and the costs of retries in the event of load failures also increase.

  > Note: When you load the data of a JSON file, make sure that the size of each JSON object in the file does not exceed 4 GB. If any JSON object in the file exceeds 4 GB, StarRocks throws an error "This parser can't support a document that big."

- `stream_load_default_timeout_second`: the timeout period of each load job. The default timeout period is 600 seconds. For more information, see [FE configuration items](../administration/Configuration.md#fe-configuration-items).
  
  If many of the load jobs that you create time out, you can increase the value of this parameter based on the calculation result that you obtain from the following formula:

  **Timeout period of each load job > Amount of data to be loaded/Average loading speed**

  > Note: **Average loading speed** in the preceding formula is the average loading speed of your StarRocks cluster. It varies depending on the server configurations and the number of allowed concurrent queries. You need to deduct the average loading speed based on the loading speeds of historical load jobs.

  For example, if the size of the data file that you want to load is 10 GB and the average loading speed of your StarRocks cluster is 10 MB/s, set the timeout period to more than 1024 seconds.

  Stream Load also provides the `timeout` parameter, which allows you to specify the timeout period of an individual load job. For more information, see [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md).

## Usage notes

If a field is missing for a record in the data file you want to load and the column onto which the field is mapped in your StarRocks table is defined as `NOT NULL`, StarRocks automatically fills a `NULL` value in the mapping column of your StarRocks table during the load of the record. You can also use the `ifnull()` function to specify the default value that you want to fill.

For example, if the field that represents city ID in the preceding `example2.json` file is missing and you want to fill an `x` value in the mapping column of `table2`, you can specify `"columns: city, tmp_id, id = ifnull(tmp_id, 'x')"`.
