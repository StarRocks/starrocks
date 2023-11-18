---
displayed_sidebar: "English"
---

# BITMAP

BITMAP is often used to accelerate count distinct. It is similar but more accurate in count distinct than HyperLogLog (HLL). BITMAP consumes more memory and disk resources. It supports only aggregation of INT data. If you want to apply bitmap to string data, you must map the data using the low-cardinality dictionary.

This topic provides a simple example on how to create a BITMAP column and use bitmap functions to aggregate data for that column. For detailed function definitions or more Bitmap functions, see "Bitmap functions".

## Create a table

- Create an Aggregate table, in which the data type of the `user_id` column is BITMAP and the bitmap_union() function is used to aggregate data.

    ```SQL
    CREATE TABLE `pv_bitmap` (
    `dt` int(11) NULL COMMENT "",
    `page` varchar(10) NULL COMMENT "",
    `user_id` bitmap BITMAP_UNION NULL COMMENT ""
    ) ENGINE=OLAP
    AGGREGATE KEY(`dt`, `page`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`dt`) BUCKETS 2;
    ```

- Create a Primary Key table, in which the data type of the `userid` column is BITMAP.

    ```SQL
    CREATE TABLE primary_bitmap (
    `tagname` varchar(65533) NOT NULL COMMENT "Tag name",
    `tagvalue` varchar(65533) NOT NULL COMMENT "Tag value",
    `userid` bitmap NOT NULL COMMENT "User ID")
    ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`) BUCKETS 1;
    ```

Before you insert data into BITMAP columns, you must first use the to_bitmap() function to convert data.

For details about how to use BITMAP, for example, load BITMAP data into a table, see [bitmap](../../sql-functions/aggregate-functions/bitmap.md).
