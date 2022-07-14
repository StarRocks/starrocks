# Stream load

StarRocks supports importing data directly from local files in CSV file format. The imported data size is up to 10GB.

Stream Load is a synchronous import method, where the user sends an HTTP request to import a local file or data stream into StarRocks. The return value of the request reflects whether the import was successful or not.

## Explanation of terms

* **Coordinator**: a coordinating node. Responsible for receiving data, distributing it to other data nodes, and returning the result to the user after the import is complete.

## Basic Principle

In Stream Load, a user submits the import command via the HTTP protocol. If the command is submitted to an FE node, the FE node will forward the request to a particular BE node via the HTTP redirect command. The user can also submit the import command directly to a specified BE node. This BE node acts as the coordinator node to divide the data by table schema and distribute the data to the relevant BE nodes. Once done, this coordinator node returns the final import results  to the user.

The following diagram shows the main flow of stream load:

![stream load](/assets/4.4.2-1.png)

## Import example

### Creating an import job

Stream Load submits and transfers data via the HTTP protocol. The curl command shows how to submit an import job. Users can also do this through other HTTP clients.

**Syntax:**

~~~bash
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT \
    http://fe_host:http_port/api/{db}/{table}/_stream_load
~~~

The attributes supported in the header are described in the import job parameters description below in the format      `-H "key1:value1"`. If there are multiple job parameters, they need to be indicated by multiple `-H, similar to \-H "key1:value1" -H "key2:value2" ......`

**Example:**

~~~bash
curl --location-trusted -u root -T data.file -H "label:123" \
    http://abc.com:8030/api/test/date/_stream_load
~~~

**Description:**

* HTTP chunked and non-chunked imports are currently supported. For the non-chunked import, `content-length` is required to ensure data integrity.
* It is better for users to set expect header to `100-continue` to avoid unnecessary data transmission.

**Signature parameters:**

* `user/passwd`: Stream load creates the import job via the HTTP protocol, which can be signed by basic access authentication. StarRocks will verify the user identity and import permission based on the signature.

**Import job parameters:**

All parameters related to the import job in stream load are set in the header. The following describes these parameters:

* **label** :The label of the import job. Data with the same label cannot be imported more than once. StarRocks will keep the labels of jobs that are successfully completed within the last 30 minutes.
* **column-separator** : Used to specify the column separator in the import file with a default value of `t`. If it is an invisible character, you need to add `x` as a prefix and use hexadecimal to represent the separator. For example, the delimiter `x01` for Hive files needs to be specified as `-H "column-separator:x01"`
* **row-delimiter**: Used to specify the row separator in the import file with a default value of `n`.
* **columns**: Used to specify the correspondence between the columns in the import file and the columns in the table. If the columns in the source file correspond exactly to the table schema, then there is no need to specify this parameter. If the source file does not correspond to the table schema, then specify this parameter to configure the data conversion rules. There are two forms of columns. One corresponds directly to the fields in the import file, which can be represented directly using the field names. The other needs to be derived by calculation. See the following examples:

  * Example 1: There are 3 columns "c1, c2, c3" in the table, and the 3 columns in the source file correspond to "c3, c2, c1"; then you need to specify `-H "columns: c3, c2, c1"`.
  * Example 2: There are 3 columns in the table "c1, c2, c3", and the first 3 columns in the source file correspond to each other, but there is an extra column. In this condition, you need to specify `-H "columns: c1, c2, c3, temp"`. and just specify any name to be used as a placeholder for the last column.
  * Example 3: There are 3 columns in the table "year, month, day", and there is only one time column in the source file, which is in the format of "2018-06-01 01:02:03"; then you can specify `-H "columns: col, year = year (col), month=month(col), day=day(col)"` to complete the import.

* **where**: Used to import a portion of data. This parameter can be set to filter out unwanted data.

  * Example 4: Import the data in k1 column that is equal to 20180601, then you can specify `-H "where: k1 = 20180601"` when importing.

* **max-filter-ratio**: the maximum tolerable percentage of data that can be filtered (due to data irregularities, etc.).The d     efault value is zero. Data irregularities do not include rows that are filtered out by the `where` condition.
* **partitions**: Used to specify the partitions involved in an      import, which is recommended if the user is able to determine the partitions of the data. Data that does not satisfy these partitions will be filtered out. For example, specify import to partition p1, p2:`-H "partitions: p1, p2"`.
* **timeout**: Used to specify the timeout for importing, in seconds, with a default value of 600. The range is 1 second ~ 259200 seconds.
* **strict-mode**: Used to specify whether to enable strict mode for this import with a default value of on. It can be disabled by: `-H "strict-mode: false"`.
* **timezone**: Used to specify the time zone for an import, with a default value of GMT+8. This parameter affects the results of all time-zone-related functions involved in the import.
* **exec-mem-limit**: Import memory limit, in bytes, with a default value of 2GB.

**Results returned:**

After the import is completed, Stream load will return the relevant details of this import in json format. See the following example:

~~~json
{

"TxnId": 1003,

"Label": "b6f3bc78-0d2c-45d9-9e4c-faa0a0149bee",

"Status": "Success",

"ExistingJobStatus": "FINISHED", // optional

"Message": "OK",

"NumberTotalRows": 1000001,

"NumberLoadedRows": 1000000,

"NumberFilteredRows": 1,

"NumberUnselectedRows": 0,

"LoadBytes": 40888898,

"LoadTimeMs": 2144,

"ErrorURL": "[http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005](http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005)"

}
~~~

* TxnId: The ID of the imported transaction.  
* Status: The last status of an import.
* Success: Indicates that the import was successful and the data is queryable.
* Publish Timeout: Indicates that the import job has been successfully committed, but for some reason is not immediately queryable. The user can treat it as a successful import without having to retry the import.
* Label Already Exists: Indicates that the Label has been occupied by another job, which may be a successful import, or may be an import in processing.
* Other: Indicates the import failed, and the user can specify Label to retry the job.
* Message: Detailed description of the import status. A specific reason for the failure is returned when the import fails.
* NumberTotalRows: The total number of rows read from the data stream.
* NumberLoadedRows: The number of rows of data imported, valid only on Success.
* NumberFilteredRows: The number of rows that were filtered out of this import, i.e. rows that did not pass the data quality.
* NumberUnselectedRows: The number of rows that were filtered out by the `where` condition of the import.
* LoadBytes: The size of the data in the source file for this import.
* LoadTimeMs: The time (ms) used for this import.
* ErrorURL: The details of the filtered data, only the first 1000 rows are shown. If the import job fails, you can directly get the filtered data in the following way and analyze it for troubleshooting.

  * ~~~bash
    wget http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005
    ~~~

### Cancel Import

Users cannot cancel stream load manually. A stream load job are cancelled automatically by system timeout or import error.

### Best Practices

### Application Scenarios

The best use case for stream load is when the original file is in memory or stored on a local disk. Since Stream Load is a synchronous import, users who want to get the import results in a synchronous way can consider this method.

### Data volume

Since Stream Load is initiated and distributed by BE, the recommended amount of imported data is between 1GB and 10GB. For example, if the imported file is 15G, then set      `streaming-load-max-mb` to 16000 in the BE configuration file.

The default timeout for Stream Load is 300 seconds. According to StarRocks' current maximum import speed limit, imported files larger than 3GB will require users to modify the default timeout.

`Import job timeout = Imported data volume / 10M/s (the specific average import speed should be calculated by users according to their cluster)`

For example, to import a 10GB file, the timeout should be set to 1000s.

### Example

**Data situation**: Data is in the client's local disk path `/home/store-sales`, and the amount of imported data is about 15GB. We      want to import data to the table `store-sales` in the database `bj-sales`.

**Cluster situation**: The number of any concurrent stream load is not affected by the cluster size.

* Step1: The import file size exceeds the default maximum import size (i.e. 10GB), so you need to modify BE's configuration file `BE.conf`as follow:

`streaming_load_max_mb = 16000`

* Step2: Calculate if the approximate import time exceeds the default timeout value. The import time ≈ 15000 / 10 = 1500s, exceeds the default timeout time, you need to modify the FE configuration `FE.conf` as follow:

`stream_load_default_timeout_second = 1500`

* Step3: Create an import job

~~~bash
curl --location-trusted -u user:password -T /home/store_sales \
    -H "label:abc" [http://abc.com:8000/api/bj_sales/store_sales/_stream_load](http://abc.com:8000/api/bj_sales/store_sales/_stream_load)
~~~

### Code integration examples

JAVA development stream load, reference: [https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load](https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load)
Spark integration stream load, Reference: [01_sparkStreaming2StarRocks](https://github.com/StarRocks/demo/blob/master/docs/01_sparkStreaming2StarRocks.md)

## FAQs

* Error reported for data quality issue: `ETL-QUALITY-UNSATISFIED; msg:quality not good enough to cancel`

See the section [Loading_intro/FAQs](/loading/Loading_intro.md#FAQs).

* Label Already Exists

See the section [Loading_intro/FAQs](/loading/Loading_intro.md#FAQs). Stream load jobs are submitted via the HTTP protocol, and generally the HTTP Client of each language has its own request retry logic. StarRocks will start to operate stream load after receiving the first request, but it is possible that the client will retry to create the request again because the result is not returned to them in time. At this time, StarRocks is already operating the first request, so the second request will encounter the error:`Label Already Exists`.
One possible way to troubleshoot the above situation is to use `Label` to search the leader FE's logs and see if the same label is present twice. If so, that indicates that the client has submitted the request repeatedly.

It is recommended to calculate the approximate import time based on the data volume of the current request. Meanwhile, increase the request timeout on the client side based on the import timeout to avoid the request being submitted multiple times by the Client.
