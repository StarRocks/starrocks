---
displayed_sidebar: "English"
---

# Stream Load

## 1. Does Stream Load support identifying column names held in the first few rows of a CSV-formatted file, or skipping the first few rows during data reading?

Stream Load does not support identifying column names held in the first few rows of a CSV-formatted file. Stream Load considers the first few rows to be normal data like the other rows.

In v2.5 and earlier, Stream Load does not support skipping the first few rows of a CSV file during data reading. If the first few rows of the CSV file you want to load hold column names, take one of the following actions:

- Modify the settings of the tool that you use to export the data. Then, re-export the data as a CSV file that does not hold column names in the first few rows.
- Use commands such as `sed -i '1d' filename` to delete the first few rows of the CSV file.
- In the load command or statement, use `-H "where: <column_name> != '<column_name>'"` to filter out the first few rows of the CSV file. `<column_name>` is any of the column names held in the first few rows. Note that StarRocks first transforms and then filters the source data. Therefore, if the column names in the first few rows fail to be transformed into their matching destination data types, `NULL` values are returned for them. This means the destination StarRocks table cannot contain columns that are set to `NOT NULL`.
- In the load command or statement, add `-H "max_filter_ratio:0.01"` to set a maximum error tolerance that is 1% or lower but can tolerate a few error rows, thereby allowing StarRocks to ignore the data transformation failures in the first few rows. In this case, the Stream Load job can still succeed even if `ErrorURL` is returned to indicate error rows. Do not set `max_filter_ratio` to a large value. If you set `max_filter_ratio` to a large value, some important data quality issues may be missed.

From v3.0 onwards, Stream Load supports the `skip_header` parameter, which specifies whether to skip the first few rows of a CSV file. For more information,see [CSV parameters](../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#csv-parameters).

## 2. The data to be loaded into the partition column is not of standard DATE or INT type. For example, the data is in a format like 202106.00. How do I transform the data if I load it by using Stream Load?

StarRocks supports transforming data at loading. For more information, see [Transform data at loading](../../loading/Etl_in_loading.md).

Suppose that you want to load a CSV-formatted file named `TEST` and the file consists of four columns, `NO`, `DATE`, `VERSION`, and `PRICE`, among which the data from the `DATE` column is in a non-standard format such as 202106.00. If you want to use `DATE` as the partition column in StarRocks, you need to first create a StarRocks table, for example, one that consists of the following four columns: `NO`, `VERSION`, `PRICE`, and `DATE`. Then, you need to specify the data type of the `DATE` column of the StarRocks table as DATE, DATETIME, or INT. Finally, when you create a Stream Load job, you need to specify the following setting in the load command or statement to transform data from the source `DATE` column's data type to the destination column's data type:

```Plain
-H "columns: NO,DATE_1, VERSION, PRICE, DATE=LEFT(DATE_1,6)"
```

In the preceding example, `DATE_1` can be considered to be a temporarily named column mapping the destination `DATE` column, and the final results loaded into the destination `DATE` column are computed by the `left()` function. Note that you must first list the temporary names of the source columns and then use functions to transform data. The functions supported are scalar functions, including non-aggregate functions and window functions.

## 3. What do I do if my Stream Load job reports the "body exceed max size: 10737418240, limit: 10737418240" error?

The size of the source data file exceeds 10 GB, which is the maximum file size supported by Stream Load. Take one of the following actions:

- Use `seq -w 0 n` to split the source data file into smaller files.
- Use `curl -XPOST http://be_host:http_port/api/update_config?streaming_load_max_mb=<file_size>` to adjust the value of the [BE configuration item](../../administration/BE_configuration.md#configure-be-dynamic-parameters) `streaming_load_max_mb` to increase the maximum file size.
