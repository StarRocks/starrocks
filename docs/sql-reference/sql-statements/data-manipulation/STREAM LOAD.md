# STREAM LOAD

## description

```plain text
NAME:
stream-load: load data to table in streaming

SYNOPSIS
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT \
    http://fe_host:http_port/api/{db}/{table}/_stream_load

DESCRIPTION
This statement is used to import data into the specified table. The difference from normal Load is that this import method is synchronous import.
This import method can still ensure the atomicity of a batch of import tasks. Either all data is imported successfully or all data fails.
This operation will update the data of rollup table related to this base table at the same time.
This is a synchronous operation. After the entire data import is completed, the import results will be returned to the user.
Currently, HTTP chunked and non chunked uploads are supported. For non chunked uploads, Content_Length must be used to indicate the length of the uploaded content, so as to ensure the integrity of the data.
In addition, you'd better set the Expected Header field content 100_ continue to avoid unnecessary data transmission in some error scenarios.
```

```plain text
OPTIONS
Users can pass in import parameters through the Header part of HTTP

label: label imported at one time. Data of the same label cannot be imported multiple times. Users can avoid the problem of repeated import of a copy of data by specifying a Label.
Currently, StarRocks keeps the most recently successful label within 30 minutes.

column_separator：used to specify the column separator in the import file. The default is \t. If it is an invisible character, you need to prefix it with \x and use hexadecimal to represent the separator.
For example, the separator of hive file \x01 should be specified as - H "column_separator: \x01"

columns：used to specify the correspondence between the columns in the import file and the columns in the table. If the column in the source file exactly corresponds to the content in the table, you do not need to specify the content of this field.
If the source file does not correspond to the table schema, this field needs some data conversion. There are two forms of column. One is to directly correspond to the fields in the import file, which are directly represented by the field name;
One is derived column, and the syntax is column - name = expression. Give a few examples to help understand.
Example 1: there are three columns "c1, c2, c3" in the table, and the three columns in the source file correspond to "c3, c2, c1" at one time; Then you need to specify - H "columns: c3, c2, c1"
Example 2: there are three columns "c1, c2, c3" in the table. The first three columns in the source file correspond in turn, but there is more than one column. Then you need to specify - H "columns: c1, c2, c3, XXX";
The last column can be filled with any name
Example 3: there are three columns "year, month and day" in the table, and there is only one time column in the source file in the format of "2018-06-01 01:02:03";
Then you can specify - H "columns: column, year = year (column), month = month (column), day = day (Col)" to complete the import

where: used to extract some data. If users need to filter out unwanted data, they can set this option.
Example 1: if you only import data with columns greater than K1 and equal to 20180601, you can specify - H "where: k1 = 20180601" during import

max_filter_ratio：the maximum allowable data proportion that can be filtered (due to data irregularity, etc.). The default is zero tolerance. Data irregularity does not include rows filtered through the where condition.

partitions: used to specify the partition designed for this import. If the user can determine the partition corresponding to the data, it is recommended to specify this item. Data that does not meet these partitions will be filtered out.
For example, specify to import to p1, p2 partitions, - H "partitions: p1, p2"

timeout: Specifies the timeout of import. The unit is seconds. The default is 600 seconds. The settable range is 1 second ~ 259200 seconds.

strict_mode: the user specifies whether strict mode is enabled for this import. It is off by default. The enabling method is - H "strict_mode: true".

timezone: Specifies the time zone used for this import. The default is Dongba zone. This parameter will affect the results of all functions related to time zone involved in import.

exec_mem_limit: import memory limit. The default is 2GB. The unit is bytes.

format: Specifies the import data format. The default is csv. json format is supported.
```

```PALIN TEXT
jsonpaths: there are two ways to import json: simple mode and precise mode.
Simple mode: it is a simple mode without setting the jsonpaths parameter. In this mode, json data is required to be an object type, for example:
{"k1": 1, "k2": 2, "k3": "hello"}, where k1, k2 and k3 are column names.

Matching pattern: the json data is relatively complex, and the corresponding value needs to be matched through the jsonpaths parameter.

strip_ outer_ array: Boolean type. true means that json data starts with an array object and flattens the array object. The default value is false. For example:
[
{"k1" : 1, "v1" : 2},
{"k1" : 3, "v1" : 4}
]
When strip_ outer_ array is true, and two rows of data will be generated when it is finally imported into starrocks.

json_ root: json_ root is a legal jsonpath string, which is used to specify the root node of json document. The default value is' '.

RETURN VALUES
After the import is completed, the relevant contents of the import will be returned in Json format. The following fields are currently included:
Status: imports the last status.
Success: indicates that the import is successful and the data is visible;
Publish Timeout: indicates that the import job has been successfully committed, but it cannot be seen immediately for some reason. The user can be regarded as having succeeded without having to retry the import
Label Already Exists: indicates that the Label has been occupied by other jobs. It may be imported successfully or being imported.
Users need to use the get label state command to determine subsequent operations
Other: the import failed. The user can specify Label to retry the job
Message: detailed description of import status. The specific failure reason will be returned in case of failure.
NumberTotalRows: the total number of rows read from the data stream
NumberLoadedRows: the number of data rows imported this time, which is valid only when Success
NumberFilteredRows: the number of rows filtered in this import, that is, the number of rows with unqualified data quality
Numberunselectedrows: the number of rows filtered through the where condition in this import
LoadBytes: the amount of source file data imported this time
LoadTimeMs: time taken for this import
ErrorURL: the specific content of the filtered data. Only the first 1000 items are reserved
```

ERRORS
Errors you can view the import error details through the following statement:

```SQL
SHOW LOAD WARNINGS ON 'url'
```

Where url is the url given by ErrorURL.

## example

1. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', and use label for deduplication. Specify a timeout of 100 seconds

    ```bash
    curl --location-trusted -u root -H "label:123" -H "timeout:100" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

2. Import the data in the local file 'testData' into the table testTbl' in the database 'testDb', use label for de duplication, and only import the data with k1 equal to 20180601

    ```bash
    curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

3. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', allowing an error rate of 20% (the user is in the defalut_cluster)

    ```bash
    curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

4. Import the data in the local file 'testData' into the table 'testTbl' in the database 'testDb', allow an error rate of 20%, and specify the column name of the file (the user is in the defalut_cluster)

    ```bash
    curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" \
        -H "columns: k2, k1, v1" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

5. Import the data in the local file 'testData' into the p1 and p2 partitions in the table 'testTbl' in the database 'testDb', allowing an error rate of 20%.

    ```bash
    curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" \
        -H "partitions: p1, p2" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

6. Import using streaming (the user is from the default_cluster)

    ```sql
    seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - \
     http://host:port/api/testDb/testTbl/_stream_load
    ```

7. Import a table containing HLL columns, which can be columns in the table or columns in the data. It can be used to generate HLL columns, or use hll_ empty to supplement columns that are not in the data

    ```bash
    curl --location-trusted -u root \
        -H "columns: k1, k2, v1=hll_hash(k1), v2=hll_empty()" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

8. Import data for strict mode filtering, and set the time zone to Africa / Abidjan

    ```bash
    curl --location-trusted -u root -H "strict_mode: true" \
        -H "timezone: Africa/Abidjan" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

9. Import a table containing BITMAP columns, which can be columns in the table or columns in the data. It can be used to generate BITMAP columns or use bitmap_ empty to fill

    ```bash
    curl --location-trusted -u root \
        -H "columns: k1, k2, v1=to_bitmap(k1), v2=bitmap_empty()" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

10. Simple mode, importing json data

    ```plain text
    Table structure:
    `category` varchar(512) NULL COMMENT "",
    `author` varchar(512) NULL COMMENT "",
    `title` varchar(512) NULL COMMENT "",
    `price` double NULL COMMENT ""
    json data format:
    {"category":"C++","author":"avc","title":"C++ primer","price":895}
    Import command:
    curl --location-trusted -u root  -H "label:123" -H "format: json" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    In order to improve throughput, it supports one-time import of data. json data format:
    [
    {"category":"C++","author":"avc","title":"C++ primer","price":89.5},
    {"category":"Java","author":"avc","title":"Effective Java","price":95},
    {"category":"Linux","author":"avc","title":"Linux kernel","price":195}
    ]
    ```

11. Matching patterns, importing json data

     ```plain text
     json data format:
     [
     {"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},
     {"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},
     {"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
     ]
     Import precisely by specifying jsonpath. For example, only category, author and price attributes are imported
     curl --location-trusted -u root \
         -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -T testData \
         http://host:port/api/testDb/testTbl/_stream_load
     ```

     ```plain text
     Note：
     1) If the json data starts with an array and each object in the array is a record, you need to set strip_ outer_ array to true to flatten the array.
     2) If the json data starts with an array and each object in the array is a record, when setting the jsonpath, our ROOT node is actually an object in the array.
     ```

12. User specified json root node

    ```plain text
    json data format:
    {
    "RECORDS":[
    {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
    {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
    {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
    ]
    }
    Import precisely by specifying jsonpath. For example, only category, author and price attributes are imported
    curl --location-trusted -u root \
        -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData \
        http://host:port/api/testDb/testTbl/_stream_load
    ```

## keyword

STREAM,LOAD
