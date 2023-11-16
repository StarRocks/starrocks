# STREAM LOAD

## Description

StarRocks provides the loading method HTTP-based Stream Load to help you load data from a local file system or a streaming data source. After you submit a load job, StarRocks synchronously runs the job, and returns the result of the job after the job finishes. You can determine whether the job is successful based on the job result. For information about the application scenarios, limits, principles, and supported data file formats of Stream Load, see [Load data from a local file system or a streaming data source using HTTP PUT](../../../loading/StreamLoad.md).

> **NOTICE**
>
> - After you load data into a StarRocks table by using Stream Load, the data of the materialized views that are created on that table is also updated.
> - You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.

## Syntax

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

This topic uses curl as an example to describe how to load data by using Stream Load. In addition to curl, you can also use other HTTP-compatible tools or languages to perform Stream Load. Load-related parameters are included in HTTP request header fields. When you input these parameters, take note of the following points:

- You can use chunked transfer encoding, as demonstrated in this topic. If you do not choose chunked transfer encoding, you must input a `Content-Length` header field to indicate the length of content to be transferred, thereby ensuring data integrity.

  > **NOTE**
  >
  > If you use curl to perform Stream Load, StarRocks automatically adds a `Content-Length` header field and you do not need manually input it.

- You must add an `Expect` header field and specify its value as `100-continue`, as in `"Expect:100-continue"`. This helps prevent unnecessary data transfers and reduce resource overheads in case your job request is denied.

Note that in StarRocks some literals are used as reserved keywords by the SQL language. Do not directly use these keywords in SQL statements. If you want to use such a keyword in an SQL statement, enclose it in a pair of backticks (`). See [Keywords](../../../sql-reference/sql-statements/keywords.md).

## Parameters

### username and password

Specify the username and password of the account that you use to connect to your StarRocks cluster. This is a required parameter. If you use an account for which no password is set, you need to input only `<username>:`.

### XPUT

Specifies the HTTP request method. This is a required parameter. Stream Load supports only the PUT method.

### url

Specifies the URL of the StarRocks table. Syntax:

```Plain
http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

The following table describes the parameters in the URL.

| Parameter     | Required | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| fe_host       | Yes      | The IP address of the FE node in your StarRocks cluster.<br/>**NOTE**<br/>If you submit the load job to a specific BE node, you must input the IP address of the BE node. |
| fe_http_port  | Yes      | The HTTP port number of the FE node in your StarRocks cluster. The default port number is `8030`.<br/>**NOTE**<br/>If you submit the load job to a specific BE node, you must input the HTTP port number of the BE node. The default port number is `8030`. |
| database_name | Yes      | The name of the database to which the StarRocks table belongs. |
| table_name    | Yes      | The name of the StarRocks table.                             |

> **NOTE**
>
> You can use [SHOW FRONTENDS](../Administration/SHOW_FRONTENDS.md) to view the IP address and HTTP port of the FE node.

### data_desc

Describes the data file that you want to load. The `data_desc` descriptor can include the data file's name, format, column separator, row separator, destination partitions, and column mapping against the StarRocks table. Syntax:

```Bash
-T <file_path>
-H "format: CSV | JSON"
-H "column_separator: <column_separator>"
-H "row_delimiter: <row_delimiter>"
-H "columns: <column1_name>[, <column2_name>, ... ]"
-H "partitions: <partition1_name>[, <partition2_name>, ...]"
-H "temporary_partitions: <temporary_partition1_name>[, <temporary_partition2_name>, ...]"
-H "jsonpaths: [ \"<json_path1>\"[, \"<json_path2>\", ...] ]"
-H "strip_outer_array:  true | false"
-H "json_root: <json_path>"
```

The parameters in the `data_desc` descriptor can be divided into three types: common parameters, CSV parameters, and JSON parameters.

#### Common parameters

| Parameter  | Required | Description                                                  |
| ---------- | -------- | ------------------------------------------------------------ |
| file_path  | Yes      | The save path of the data file. You can optionally include the extension of the file name. |
| format     | No       | The format of the data file. Valid values: `CSV` and `JSON`. Default value: `CSV`. |
| partitions | No       | The partitions into which you want to load the data file. By default, if you do not specify this parameter, StarRocks loads the data file into all partitions of the StarRocks table. |
| temporary_partitions|  No       | The name of the [temporary partition](../../../table_design/Temporary_partition.md) into which you want to load data file. You can specify multiple temporary partitions, which must be separated by commas (,).|
| columns    | No       | The column mapping between the data file and the StarRocks table.<br/>If the fields in the data file can be mapped in sequence onto the columns in the StarRocks table, you do not need to specify this parameter. Instead, you can use this parameter to implement data conversions. For example, if you load a CSV data file and the file consists of two columns that can be mapped in sequence onto the two columns, `id` and `city`, of the StarRocks table, you can specify `"columns: city,tmp_id, id = tmp_id * 100"`. For more information, see the "[Column mapping](#column-mapping)" section in this topic. |

#### CSV parameters

| Parameter        | Required | Description                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| column_separator | No       | The characters that are used in the data file to separate fields. If you do not specify this parameter, this parameter defaults to `\t`, which indicates tab.<br/>Make sure that the column separator you specify by using this parameter is the same as the column separator used in the data file.<br/>**NOTE**<br/>For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (\|), whose length does not exceed 50 bytes as a text delimiter. |
| row_delimiter    | No       | The characters that are used in the data file to separate rows. If you do not specify this parameter, this parameter defaults to `\n`. |
| skip_header      | No       | Specifies whether to skip the first few rows of the data file when the data file is in CSV format. Type: INTEGER. Default value: `0`.<br />In some CSV-formatted data files, the first few rows at the beginning are used to define metadata such as column names and column data types. By setting the `skip_header` parameter, you can enable StarRocks to skip the first few rows of the data file during data loading. For example, if you set this parameter to `1`, StarRocks skips the first row of the data file during data loading.<br />The first few rows at the beginning in the data file must be separated by using the row separator that you specify in the load command. |
| trim_space       | No       | Specifies whether to remove spaces preceding and following column separators from the data file when the data file is in CSV format. Type: BOOLEAN. Default value: `false`.<br />For some databases, spaces are added to column separators when you export data as a CSV-formatted data file. Such spaces are called leading spaces or trailing spaces depending on their locations. By setting the `trim_space` parameter, you can enable StarRocks to remove such unnecessary spaces during data loading.<br />Note that StarRocks does not remove the spaces (including leading spaces and trailing spaces) within a field wrapped in a pair of `enclose`-specified characters. For example, the following field values use pipe (<code class="language-text">&#124;</code>) as the column separator and double quotation marks (`"`) as the `enclose`-specified character:<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />If you set `trim_space` to `true`, StarRocks processes the preceding field values as follows:<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose          | No       | Specifies the character that is used to wrap the field values in the data file according to [RFC4180](https://www.rfc-editor.org/rfc/rfc4180) when the data file is in CSV format. Type: single-byte character. Default value: `NONE`. The most prevalent characters are single quotation mark (`'`) and double quotation mark (`"`).<br />All special characters (including row separators and column separators) wrapped by using the `enclose`-specified character are considered normal symbols. StarRocks can do more than RFC4180 as it allows you to specify any single-byte character as the `enclose`-specified character.<br />If a field value contains an `enclose`-specified character, you can use the same character to escape that `enclose`-specified character. For example, you set `enclose` to `"`, and a field value is `a "quoted" c`. In this case, you can enter the field value as `"a ""quoted"" c"` into the data file. |
| escape           | No       | Specifies the character that is used to escape various special characters, such as row separators, column separators, escape characters, and `enclose`-specified characters, which are then considered by StarRocks to be common characters and are parsed as part of the field values in which they reside. Type: single-byte character. Default value: `NONE`. The most prevalent character is slash (`\`), which must be written as double slashes (`\\`) in SQL statements.<br />**NOTE**<br />The character specified by `escape` is applied to both inside and outside of each pair of `enclose`-specified characters.<br />Two examples are as follows:<ul><li>When you set `enclose` to `"` and `escape` to `\`, StarRocks parses `"say \"Hello world\""` into `say "Hello world"`.</li><li>Assume that the column separator is comma (`,`). When you set `escape` to `\`, StarRocks parses `a, b\, c` into two separate field values: `a` and `b, c`.</li></ul> |

> **NOTE**
>
> - For CSV data, you can use a UTF-8 string, such as a comma (,), tab, or pipe (|), whose length does not exceed 50 bytes as a text delimiter.
> - Null values are denoted by using `\N`. For example, a data file consists of three columns, and a record from that data file holds data in the first and third columns but no data in the second column. In this situation, you need to use `\N` in the second column to denote a null value. This means the record must be compiled as `a,\N,b` instead of `a,,b`. `a,,b` denotes that the second column of the record holds an empty string.
> - The format options, including `skip_header`, `trim_space`, `enclose`, and `escape`, are supported in v3.0 and later.

#### JSON parameters

| Parameter         | Required | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| jsonpaths         | No       | The names of the keys that you want to load from the JSON data file. You need to specify this parameter only when you load JSON data by using the matched mode. The value of this parameter is in JSON format. See [Configure column mapping for JSON data loading](#configure-column-mapping-for-json-data-loading).           |
| strip_outer_array | No       | Specifies whether to strip the outermost array structure. Valid values: `true` and `false`. Default value: `false`.<br/>In real-world business scenarios, the JSON data may have an outermost array structure as indicated by a pair of square brackets `[]`. In this situation, we recommend that you set this parameter to `true`, so StarRocks removes the outermost square brackets `[]` and loads each inner array as a separate data record. If you set this parameter to `false`, StarRocks parses the entire JSON data file into one array and loads the array as a single data record.<br/>For example, the JSON data is `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`. If you set this parameter to `true`,  `{"category" : 1, "author" : 2}` and `{"category" : 3, "author" : 4}` are parsed into separate data records that are loaded into separate StarRocks table rows. |
| json_root         | No       | The root element of the JSON data that you want to load from the JSON data file. You need to specify this parameter only when you load JSON data by using the matched mode. The value of this parameter is a valid JsonPath string. By default, the value of this parameter is empty, indicating that all data of the JSON data file will be loaded. For more information, see the "[Load JSON data using matched mode with root element specified](#load-json-data-using-matched-mode-with-root-element-specified)" section of this topic. |
| ignore_json_size  | No       | Specifies whether to check the size of the JSON body in the HTTP request.<br/>**NOTE**<br/>By default, the size of the JSON body in an HTTP request cannot exceed 100 MB. If the JSON body exceeds 100 MB in size, an error "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." is reported. To prevent this error, you can add `"ignore_json_size:true"` in the HTTP request header to instruct StarRocks not to check the JSON body size. |

When you load JSON data, also note that the size per JSON object cannot exceed 4 GB. If an individual JSON object in the JSON data file exceeds 4 GB in size, an error "This parser can't support a document that big." is reported.

### opt_properties

Specifies some optional parameters, which are applied to the entire load job. Syntax:

```Bash
-H "label: <label_name>"
-H "where: <condition1>[, <condition2>, ...]"
-H "max_filter_ratio: <num>"
-H "timeout: <num>"
-H "strict_mode: true | false"
-H "timezone: <string>"
-H "load_mem_limit: <num>"
-H "merge_condition: <column_name>"
```

The following table describes the optional parameters.

| Parameter        | Required | Description                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| label            | No       | The label of the load job. If you do not specify this parameter, StarRocks automatically generates a label for the load job.<br/>StarRocks does not allow you to use one label to load a data batch multiple times. As such, StarRocks prevents the same data from being repeatedly loaded. For label naming conventions, see [System limits](../../../reference/System_limit.md).<br/>By default, StarRocks retains the labels of load jobs that were successfully completed over the most recent three days. You can use the [FE parameter](../../../administration/Configuration.md) `label_keep_max_second` to change the label retention period. |
| where            | No       | The conditions based on which StarRocks filters the pre-processed data. StarRocks loads only the pre-processed data that meets the filter conditions specified in the WHERE clause. |
| max_filter_ratio | No       | The maximum error tolerance of the load job. The error tolerance is the maximum percentage of data records that can be filtered out due to inadequate data quality in all data records requested by the load job. Valid values: `0` to `1`. Default value: `0`.<br/>We recommend that you retain the default value `0`. This way, if unqualified data records are detected, the load job fails, thereby ensuring data correctness.<br/>If you want to ignore unqualified data records, you can set this parameter to a value greater than `0`. This way, the load job can succeed even if the data file contains unqualified data records.<br/>**NOTE**<br/>Unqualified data records do not include data records that are filtered out by the WHERE clause. |
| log_rejected_record_num | No           | Specifies the maximum number of unqualified data rows that can be logged. This parameter is supported from v3.1 onwards. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.<ul><li>The value `0` specifies that data rows that are filtered out will not be logged.</li><li>The value `-1` specifies that all data rows that are filtered out will be logged.</li><li>A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.</li></ul> |
| timeout          | No       | The timeout period of the load job. Valid values: `1` to `259200`. Unit: second. Default value: `600`.<br/>**NOTE**In addition to the `timeout` parameter, you can also use the [FE parameter](../../../administration/Configuration.md) `stream_load_default_timeout_second` to centrally control the timeout period for all Stream Load jobs in your StarRocks cluster. If you specify the `timeout` parameter, the timeout period specified by the `timeout` parameter prevails. If you do not specify the `timeout` parameter, the timeout period specified by the `stream_load_default_timeout_second` parameter prevails. |
| strict_mode      | No       | Specifies whether to enable the [strict mode](../../../loading/load_concept/strict_mode.md). Valid values: `true` and `false`. Default value: `false`.  The value `true` specifies to enable the strict mode, and the value `false` specifies to disable the strict mode. |
| timezone         | No       | The time zone used by the load job. Default value: `Asia/Shanghai`. The value of this parameter affects the results returned by functions such as strftime, alignment_timestamp, and from_unixtime. The time zone specified by this parameter is a session-level time zone. For more information, see [Configure a time zone](../../../administration/timezone.md). |
| load_mem_limit   | No       | The maximum amount of memory that can be provisioned to the load job. Unit: bytes. By default, the maximum memory size for a load job is 2 GB. The value of this parameter cannot exceed the maximum amount of memory that can be provisioned to each BE. |
| merge_condition  | No       | Specifies the name of the column you want to use as the condition to determine whether updates can take effect. The update from a source record to a destination record takes effect only when the source data record has a greater or equal value than the destination data record in the specified column. StarRocks supports conditional updates since v2.5. For more information, see [Change data through loading](../../../loading/Load_to_Primary_Key_tables.md). <br/>**NOTE**<br/>The column that you specify cannot be a primary key column. Additionally, only tables that use the Primary Key table support conditional updates. |

## Column mapping

### Configure column mapping for CSV data loading

If the columns of the data file can be mapped one on one in sequence to the columns of the StarRocks table, you do not need to configure the column mapping between the data file and the StarRocks table.

If the columns of the data file cannot be mapped one on one in sequence to the columns of the StarRocks table, you need to use the `columns` parameter to configure the column mapping between the data file and the StarRocks table. This includes the following two use cases:

- **Same number of columns but different column sequence.** **Also, the data from the data file does not need to be computed by functions before it is loaded into the matching StarRocks table columns.**

  In the `columns` parameter, you need to specify the names of the StarRocks table columns in the same sequence as how the data file columns are arranged.

  For example, the StarRocks table consists of three columns, which are `col1`, `col2`, and `col3` in sequence, and the data file also consists of three columns, which can be mapped to the StarRocks table columns `col3`, `col2`, and `col1` in sequence. In this case, you need to specify `"columns: col3, col2, col1"`.

- **Different number of columns and different column sequence. Also, the data from the data file needs to be computed by functions before it is loaded into the matching StarRocks table columns.**

  In the `columns` parameter, you need to specify the names of the StarRocks table columns in the same sequence as how the data file columns are arranged and specify the functions you want to use to compute the data. Two examples are as follows:

  - The StarRocks table consists of three columns, which are `col1`, `col2`, and `col3` in sequence. The data file consists of four columns, among which the first three columns can be mapped in sequence to the StarRocks table columns `col1`, `col2`, and `col3` and the fourth column cannot be mapped to any of the StarRocks table columns. In this case, you need to temporarily specify a name for the fourth column of the data file, and the temporary name must be different from any of the StarRocks table column names. For example, you can specify `"columns: col1, col2, col3, temp"`, in which the fourth column of the data file is temporarily named `temp`.
  - The StarRocks table consists of three columns, which are `year`, `month`, and `day` in sequence. The data file consists of only one column that accommodates date and time values in `yyyy-mm-dd hh:mm:ss` format. In this case, you can specify `"columns: col, year = year(col), month=month(col), day=day(col)"`, in which `col` is the temporary name of the data file column and the functions `year = year(col)`, `month=month(col)`, and `day=day(col)` are used to extract data from the data file column `col` and loads the data into the mapping StarRocks table columns. For example, `year = year(col)` is used to extract the `yyyy` data from the data file column `col` and loads the data into the StarRocks table column `year`.

For detailed examples, see [Configure column mapping](#configure-column-mapping).

### Configure column mapping for JSON data loading

If the keys of the JSON document have the same names as the columns of the StarRocks table, you can load the JSON-formatted data by using the simple mode. In simple mode, you do not need to specify the `jsonpaths` parameter. This mode requires that the JSON-formatted data must be an object as indicated by curly brackets `{}`, such as `{"category": 1, "author": 2, "price": "3"}`. In this example, `category`, `author`, and `price` are key names, and these keys can be mapped one on one by name to the columns `category`, `author`, and `price` of the StarRocks table.

If the keys of the JSON document have different names than the columns of the StarRocks table, you can load the JSON-formatted data by using the matched mode. In matched mode, you need to use the `jsonpaths` and `COLUMNS` parameters to specify the column mapping between the JSON document and the StarRocks table:

- In the `jsonpaths` parameter, specify the JSON keys in the sequence as how they are arranged in the JSON document.
- In the `COLUMNS` parameter, specify the mapping between the JSON keys and the StarRocks table columns:
  - The column names specified in the `COLUMNS` parameter are mapped one on one in sequence to the JSON keys.
  - The column names specified in the `COLUMNS` parameter are mapped one on one by name to the StarRocks table columns.

For examples about loading JSON-formatted data by using the matched mode, see [Load JSON data using matched mode](#load-json-data-using-matched-mode).

## Return value

After the load job finishes, StarRocks returns the job result in JSON format. Example:

```JSON
{
    "TxnId": 1003,
    "Label": "label123",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 999999,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 40888898,
    "LoadTimeMs": 2144,
    "BeginTxnTimeMs": 0,
    "StreamLoadPlanTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 11,
    "CommitAndPublishTimeMs": 16,
}
```

The following table describes the parameters in the returned job result.

| Parameter              | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| TxnId                  | The transaction ID of the load job.                          |
| Label                  | The label of the load job.                                   |
| Status                 | The final status of the data loaded.<ul><li>`Success`: The data is successfully loaded and can be queried.</li><li>`Publish Timeout`: The load job is successfully submitted, but the data still cannot be queried. You do not need to retry to load the data.</li><li>`Label Already Exists`: The label of the load job has been used for another load job. The data may have been successfully loaded or is being loaded.</li><li>`Fail`: The data failed to be loaded. You can retry the load job.</li></ul> |
| Message                | The status of the load job. If the load job fails, the detailed failure cause is returned. |
| NumberTotalRows        | The total number of data records that are read.              |
| NumberLoadedRows       | The total number of data records that are successfully loaded. This parameter is valid only when the value returned for `Status` is `Success`. |
| NumberFilteredRows     | The number of data records that are filtered out due to inadequate data quality. |
| NumberUnselectedRows   | The number of data records that are filtered out by the WHERE clause. |
| LoadBytes              | The amount of data that is loaded. Unit: bytes.              |
| LoadTimeMs             | The amount of time that is taken by the load job. Unit: ms.  |
| BeginTxnTimeMs         | The amount of time that is taken to run a transaction for the load job. |
| StreamLoadPlanTimeMs   | The amount of time that is taken to generate a execution plan for the load job. |
| ReadDataTimeMs         | The amount of time that is taken to read data for the load job. |
| WriteDataTimeMs        | The amount of time that is taken to write data for the load job. |
| CommitAndPublishTimeMs | The amount of time that is taken to commit and publish data for the load job. |

If the load job fails, StarRocks also returns `ErrorURL`. Example:

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL` provides a URL from which you can obtain details about unqualified data records that have been filtered out. You can specify the maximum number of unqualified data rows that can be logged by using the optional parameter `log_rejected_record_num`, which is set when you submit a load job.

You can run `curl "url"` to directly view details about the filtered-out, unqualified data records. You can also run `wget "url"` to export the details about these data records:

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

The exported data record details are saved to a local file with a name similar to `_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be`. You can use the `cat` command to view the file.

Then, you can adjust the configuration of the load job, and submit the load job again.

## Examples

### Load CSV data

This section CSV data as an example to describe how you can employ various parameter settings and combinations to meet your diverse loading requirements.

#### Set timeout period

Your StarRocks database `test_db` contains a table named `table1`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example1.csv` also consists of three columns, which can be mapped in sequence onto `col1`, `col2`, and `col3` of `table1`.

If you want to load all data from `example1.csv` into `table1` within up to 100 seconds, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

#### Set error tolerance

Your StarRocks database `test_db` contains a table named `table2`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example2.csv` also consists of three columns, which can be mapped in sequence onto `col1`, `col2`, and `col3` of `table2`.

If you want to load all data from `example2.csv` into `table2` with a maximum error tolerance of `0.2`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

#### Configure column mapping

Your StarRocks database `test_db` contains a table named `table3`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example3.csv` also consists of three columns, which can be mapped in sequence onto `col2`, `col1`, and `col3` of `table3`.

If you want to load all data from `example3.csv` into `table3`, run the following command:

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
```

> **NOTE**
>
> In the preceding example, the columns of `example3.csv` cannot be mapped onto the columns of `table3` in the same sequence as how these columns are arranged in `table3`. Therefore, you need to use the `columns` parameter to configure the column mapping between `example3.csv` and `table3`.

#### Set filter conditions

Your StarRocks database `test_db` contains a table named `table4`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example4.csv` also consists of three columns, which can be mapped in sequence onto `col1`, `col2`, and `col3` of `table4`.

If you want to load only the data records whose values in the first column of `example4.csv` are equal to `20180601` into `table4`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3]"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
```

> **NOTE**
>
> In the preceding example, `example4.csv` and `table4` have the same number of columns that can be mapped in sequence, but you need to use the WHERE clause to specify column-based filter conditions. Therefore, you need to use the `columns` parameter to define temporary names for the columns of `example4.csv`.

#### Set destination partitions

Your StarRocks database `test_db` contains a table named `table5`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example5.csv` also consists of three columns, which can be mapped in sequence onto `col1`, `col2`, and `col3` of `table5`.

If you want to load all data from `example5.csv` into partitions `p1` and `p2` of `table5`, run the following command:

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
```

#### Set strict mode and time zone

Your StarRocks database `test_db` contains a table named `table6`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example6.csv` also consists of three columns, which can be mapped in sequence onto `col1`, `col2`, and `col3` of `table6`.

If you want to load all data from `example6.csv` into `table6` by using the strict mode and the time zone `Africa/Abidjan`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table6/_stream_load
```

#### Load data into tables containing HLL-type columns

Your StarRocks database `test_db` contains a table named `table7`. The table consists of two HLL-type columns, which are `col1` and `col2` in sequence.

Your data file `example7.csv` also consists of two columns, among which the first column can be mapped onto `col1` of `table7` and the second column cannot be mapped onto any column of `table7`. The values in the first column of `example7.csv` can be converted into HLL-type data by using functions before they are loaded into `col1` of `table7`.

If you want to load data from `example7.csv` into `table7`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```

> **NOTE**
>
> In the preceding example, the two columns of `example7.csv` are named `temp1` and `temp2` in sequence by using the `columns` parameter. Then, functions are used to convert data as follows:
>
> - The `hll_hash` function is used to convert the values in `temp1` of `example7.csv` into HLL-type data and map `temp1` of `example7.csv` onto `col1` of `table7`.
>
> - The `hll_empty` function is used to fill the specified default value into `col2` of `table7`.

For usage of the functions `hll_hash` and `hll_empty`, see [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) and [hll_empty](../../sql-functions/aggregate-functions/hll_empty.md).

#### Load data into tables containing BITMAP-type columns

Your StarRocks database `test_db` contains a table named `table8`. The table consists of two BITMAP-type columns, which are `col1` and `col2`, in sequence.

Your data file `example8.csv` also consists of two columns, among which the first column can be mapped onto `col1` of `table8` and the second column cannot be mapped onto any column of `table8`. The values in the first column of `example8.csv` can be converted by using functions before they are loaded into `col1` of `table8`.

If you want to load data from `example8.csv` into `table8`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table8/_stream_load
```

> **NOTE**
>
> In the preceding example, the two columns of `example8.csv` are named `temp1` and `temp2` in sequence by using the `columns` parameter. Then, functions are used to convert data as follows:
>
> - The `to_bitmap` function is used to convert the values in `temp1` of `example8.csv` into BITMAP-type data and map `temp1` of `example8.csv` onto `col1` of `table8`.
>
> - The `bitmap_empty` function is used to fill the specified default value into `col2` of `table8`.

For usage of the functions `to_bitmap` and `bitmap_empty`, see [to_bitmap](../../../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) and [bitmap_empty](../../../sql-reference/sql-functions/bitmap-functions/bitmap_empty.md).

#### Setting `skip_header`, `trim_space`, `enclose`, and `escape`

Your StarRocks database `test_db` contains a table named `table9`. The table consists of three columns, which are `col1`, `col2`, and `col3` in sequence.

Your data file `example9.csv` also consists of three columns, which are mapped in sequence onto `col2`, `col1`, and `col3` of `table13`.

If you want to load all data from `example9.csv` into `table9`, with the intention of skipping the first five rows of `example9.csv`, removing the spaces preceding and following column separators, and setting `enclose` to `\` and `escape` to `\`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl9/_stream_load
```

### Load JSON data

This section describes the parameter settings that you need to take note of when you load JSON data.

Your StarRocks database `test_db` contains a table named `tbl1`, whose schema is as follows:

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### Load JSON data using simple mode

Suppose that your data file `example1.json` consists of the following data:

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

To load all data from `example1.json` into `tbl1`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **NOTE**
>
> In the preceding example, the parameters `columns` and `jsonpaths` are not specified. Therefore, the keys in `example1.json` are mapped by name onto the columns of `tbl1`.

To increase throughput, Stream Load supports loading multiple data records all at once. Example:

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### Load JSON data using matched mode

StarRocks performs the following steps to match and process JSON data:

1. (Optional) Strips the outermost array structure as instructed by the `strip_outer_array` parameter setting.

   > **NOTE**
   >
   > This step is performed only when the outermost layer of the JSON data is an array structure as indicated by a pair of square brackets `[]`. You need to set `strip_outer_array` to `true`.

2. (Optional) Matches the root element of the JSON data as instructed by the `json_root` parameter setting.

   > **NOTE**
   >
   > This step is performed only when the JSON data has a root element. You need to specify the root element by using the `json_root` parameter.

3. Extracts the specified JSON data as instructed by the `jsonpaths` parameter setting.

##### Load JSON data using matched without root element specified

Suppose that your data file `example2.json` consists of the following data:

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

To load only `category`, `author`, and `price` from `example2.json`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> -H "label:label7" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **NOTE**
>
> In the preceding example, the outermost layer of the JSON data is an array structure as indicated by a pair of square brackets `[]`. The array structure consists of multiple JSON objects that each represent a data record. Therefore, you need to set `strip_outer_array` to `true` to strip the outermost array structure. The key **title** that you do not want to load is ignored during loading.

##### Load JSON data using matched mode with root element specified

Suppose your data file `example3.json` consists of the following data:

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

To load only `category`, `author`, and `price` from `example3.json`, run the following command:

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "json_root: $.RECORDS" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" -H "label:label8" \
    -T example3.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **NOTE**
>
> In the preceding example, the outermost layer of the JSON data is an array structure as indicated by a pair of square brackets `[]`. The array structure consists of multiple JSON objects that each represent a data record. Therefore, you need to set `strip_outer_array` to `true` to strip the outermost array structure. The keys `title` and `timestamp` that you do not want to load are ignored during loading. Additionally, the `json_root` parameter is used to specify the root element, which is an array, of the JSON data.
