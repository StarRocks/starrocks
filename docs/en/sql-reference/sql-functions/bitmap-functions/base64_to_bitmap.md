---
displayed_sidebar: "English"
---

# base64_to_bitmap

## Description

Before you import bitmap data into StarRocks, you need to serialize the data and encode the data as a Base64 string. When you import the Base64 string into StarRocks, you need to convert the string into bitmap data.
This function is used to convert Base64 strings into bitmap data.

This function is supported from v2.3.

## Syntax

```Haskell
BITMAP base64_to_bitmap(VARCHAR bitmap)
```

## Parameters

`bitmap`: The supported data type is VARCHAR. Before you load Bitmap data into StarRocks, you can use Java or C++ to [create a BitmapValue object](https://github.com/StarRocks/starrocks/blob/main/fe/plugin-common/src/test/java/com/starrocks/types/BitmapValueTest.java), add an element, serialize the data, and encode the data as a Base64 string. Then, pass the Base64 string as an input parameter into this function.

## Return value

Returns a value of the BITMAP type.

## Examples

Create a database named `bitmapdb` and a table named `bitmap`. Use Stream Load to import JSON data into `bitmap_table`. During this process, use base64_to_bitmap to convert the Base64 string in the JSON file into bitmap data.

1. Create a database and a table in StarRocks. In this example, a Primary Key table is created.

    ```SQL
    CREATE database bitmapdb;
    USE bitmapdb;
    CREATE TABLE `bitmap_table` (
    `tagname` varchar(65533) NOT NULL COMMENT "Tag name",
    `tagvalue` varchar(65533) NOT NULL COMMENT "Tag value",
    `userid` bitmap NOT NULL COMMENT "User ID"
    ) ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`)
    PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
    );
    ```

2. Use [Stream Load](../../../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) to import JSON data into `bitmap_table`.

    Suppose there is a JSON file named **simpledata**. This file has the following content and `userid` is a Base64-encoded string.

    ```JSON
    {
        "tagname": "Product", "tagvalue": "Insurance", "userid":"AjowAAABAAAAAAACABAAAAABAAIAAwA="
    }
    ```

    Use base64_to_bitmap to convert  `userid` into a bitmap value.

    ```Plain
    curl --location-trusted -u <username>:<password>\
        -H "columns: c1,c2,c3,tagname=c1,tagvalue=c2,userid=base64_to_bitmap(c3)"\
        -H "label:bitmap123"\
        -H "format: json"\
        -H "jsonpaths: [\"$.tagname\",\"$.tagvalue\",\"$.userid\"]"\
        -T simpleData http://host:port/api/bitmapdb/bitmap_table/_stream_load
    ```

3. Query data from `bitmap_table`.

    ```Plaintext
    mysql> select tagname,tagvalue,bitmap_to_string(userid) from bitmap_table;
    +--------------+----------+----------------------------+
    | tagname      | tagvalue | bitmap_to_string(`userid`) |
    +--------------+----------+----------------------------+
    | Product      | Insurance      | 1,2,3                |
    +--------------+----------+----------------------------+
    1 rows in set (0.01 sec)
    ```
