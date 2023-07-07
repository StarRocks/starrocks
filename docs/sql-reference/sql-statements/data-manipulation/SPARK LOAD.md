# SPARK LOAD

## Description

Spark load preprocesses the imported data through external spark resources, improves the import performance of a large amount of StarRocks data, and saves the computing resources of StarRocks cluster. It is mainly used in the scenario of initial migration and large amount of data import into StarRocks.

Spark load is an asynchronous import method. Users need to create Spark type import tasks through MySQL protocol and view the import results through`SHOW LOAD`.

> **NOTICE**
>
> - You can load data into StarRocks tables only as a user who has the INSERT privilege on those StarRocks tables. If you do not have the INSERT privilege, follow the instructions provided in [GRANT](../account-management/GRANT.md) to grant the INSERT privilege to the user that you use to connect to your StarRocks cluster.
> - When Spark Load is used to load data into a StarRocks table, the bucketing column of the StarRocks table cannot be of DATE, DATETIME, or DECIMAL type.

Syntax

```sql
LOAD LABEL load_label
(
data_desc1[, data_desc2, ...]
)
WITH RESOURCE resource_name
[resource_properties]
[opt_properties]
```

1.load_label

Label of the currently imported batch. Unique within a database.

Syntax:

```sql
[database_name.]your_label
```

2.data_desc

Used to describe a batch of imported data.

Syntax:

```sql
DATA INFILE
(
"file_path1"[, file_path2, ...]
)
[NEGATIVE]
INTO TABLE `table_name`
[PARTITION (p1, p2)]
[COLUMNS TERMINATED BY "column_separator"]
[FORMAT AS "file_type"]
[(column_list)]
[COLUMNS FROM PATH AS (col2, ...)]
[SET (k1 = func(k2))]
[WHERE predicate]

DATA FROM TABLE hive_external_tbl
[NEGATIVE]
INTO TABLE tbl_name
[PARTITION (p1, p2)]
[SET (k1=f1(xx), k2=f2(xx))]
[WHERE predicate]
```

Note

```plain text
file_path:

The file path can be specified to one file, or the * wildcard can be used to specify all files in a directory. Wildcards must match files, not directories.

hive_external_tbl:

hive external table name.
It is required that the columns in the imported starrocks table must exist in the hive external table.
Each load task only supports loading from one Hive external table.
Cannot be used with file_ path mode at the same time.

PARTITION:

If this parameter is specified, only the specified partition will be imported, and the data outside the imported partition will be filtered out.
If not specified, all partitions of table will be imported by default.

NEGATIVE:

If this parameter is specified, it is equivalent to loading a batch of "negative" data. Used to offset the same batch of previously imported data.
This parameter is only applicable when the value column exists and the aggregation type of the value column is SUM only.

column_separator:

Specifies the column separator in the import file. Default is \ t
If it is an invisible character, you need to prefix it with \ \ x and use hexadecimal to represent the separator.
For example, the separator of hive file \ x01 is specified as "\ \ x01"

file_type:

Used to specify the type of imported file. Currently, only csv is supported.

column_list:

Used to specify the correspondence between the columns in the import file and the columns in the table.
When you need to skip a column in the import file, specify the column as a column name that does not exist in the table.

Syntax:
(col_name1, col_name2, ...)

SET:

If specify this parameter, you can convert a column of the source file according to the function, and then import the converted results into table. Syntax is column_name = expression.
Only Spark SQL build_in functions are supported. Please refer to https://spark.apache.org/docs/2.4.6/api/sql/index.html.
Give a few examples to help understand.
Example 1: there are three columns "c1, c2, c3" in the table, and the first two columns in the source file correspond to (c1, c2), and the sum of the last two columns corresponds to C3; then columns (c1, c2, tmp_c3, tmp_c4) set (c3 = tmp_c3 + tmp_c4) needs to be specified;
Example 2: there are three columns "year, month and day" in the table, and there is only one time column in the source file in the format of "2018-06-01 01:02:03".
Then you can specify columns (tmp_time) set (year = year (tmp_time), month = month (tmp_time), day = day (tmp_time)) to complete the import.

WHERE:

Filter the transformed data, and only the data that meets the where condition can be imported. Only the column names in the table can be referenced in the WHERE statement
```

3.resource_name

The name of the spark resource used can be viewed through `SHOW RESOURCES` command.

4.resource_properties

When you have a temporary need, such as modifying the Spark and HDFS configurations, you can set the parameters here, which takes effect only in this specific spark loading job and not to affect the existing configurations in the StarRocks cluster.

5.opt_properties

Used to specify some special parameters.

Syntax:

```sql
[PROPERTIES ("key"="value", ...)]
```

You can specify the following parameters:
timeout:         specifies the timeout of the import operation. The default timeout is 4 hours. In seconds.
max_filter_ratio:the maximum allowable data proportion that can be filtered (for reasons such as non-standard data). The default is zero tolerance.
strict mode:     whether to strictly restrict the data. The default is false.
timezone:         specifies the time zone of some functions affected by the time zone, such as strftime / alignment_timestamp/from_unixtime, etc. Please refer to the [time zone] document for details. If not specified, the "Asia / Shanghai" time zone is used.

6.Import data format example

int (TINYINT/SMALLINT/INT/BIGINT/LARGEINT): 1, 1000, 1234
float (FLOAT/DOUBLE/DECIMAL): 1.1, 0.23, .356
date (DATE/DATETIME) :2017-10-03, 2017-06-13 12:34:03.
(Note: for other date formats, you can use strftime or time_format function to convert in the Import command) string class (CHAR/VARCHAR): "I am a student", "a"

NULL value: \ N

## Examples

1. Import a batch of data from HDFS, and specify the timeout time and filtering ratio. Use the name my_ spark resources for spark.

    ```sql
    LOAD LABEL example_db.label1
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    )
    WITH RESOURCE 'my_spark'
    PROPERTIES
    (
        "timeout" = "3600",
        "max_filter_ratio" = "0.1"
    );
    ```

    Where hdfs_host is the host of namenode, hdfs_port is fs.defaultfs port (default 9000)

2. Import a batch of "negative" data from HDFS, specify the separator as comma, use the wildcard * to specify all files in the directory, and specify the temporary parameters of spark resources.

    ```sql
    LOAD LABEL example_db.label3
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/*")
    NEGATIVE
    INTO TABLE `my_table`
    COLUMNS TERMINATED BY ","
    )
    WITH RESOURCE 'my_spark'
    (
        "spark.executor.memory" = "3g",
        "broker.username" = "hdfs_user",
        "broker.password" = "hdfs_passwd"
    );
    ```

3. Import a batch of data from HDFS, specify the partition, and do some conversion on the columns of the imported file, as follows:

    ```plain text
    The table structure is:
    k1 varchar(20)
    k2 int
    
    Assume that the data file has only one line of data:
    
    Adele,1,1
    
    Each column in the data file corresponds to each column specified in the import statement:
    k1,tmp_k2,tmp_k3
    
    The conversion is as follows:
    
    1. k1: no conversion
    2. k2:is the sum of tmp_ k2 and tmp_k3
    
    LOAD LABEL example_db.label6
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
    (k1, tmp_k2, tmp_k3)
    SET (
    k2 = tmp_k2 + tmp_k3
    )
    )
    WITH RESOURCE 'my_spark';
    ```

4. Extract the partition field in the file path

    If necessary, the partitioned fields in the file path will be resolved according to the field types defined in the table, similar to the function of Partition Discovery in Spark

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/*/*")
    INTO TABLE `my_table`
    (k1, k2, k3)
    COLUMNS FROM PATH AS (city, utc_date)
    SET (uniq_id = md5sum(k1, city))
    )
    WITH RESOURCE 'my_spark';
    ```

    hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing  The directory includes the following files:

    [hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv, hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv, ...]

    The city and utc_date field in the file path are extracted

5. Filter the data to be imported. Only columns with k1 value greater than 10 can be imported.

    ```sql
    LOAD LABEL example_db.label10
    (
    DATA INFILE("hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file")
    INTO TABLE `my_table`
    WHERE k1 > 10
    )
    WITH RESOURCE 'my_spark';
    ```

6. Import from the hive external table and convert the uuid column in the source table to the bitmap type through the global dictionary.

    ```sql
    LOAD LABEL db1.label1
    (
    DATA FROM TABLE hive_t1
    INTO TABLE tbl1
    SET
    (
    uuid=bitmap_dict(uuid)
    )
    )
    WITH RESOURCE 'my_spark';
    ```
