# base64_to_bitmap

## 功能

导入外部bitmap数据到StarRocks表时，需要先对bitmap进行序列化和Base64编码，生成Base64字符串。导入字符串到StarRocks表里时再进行Base64到bitmap的转化。
该函数用于将Base64编码的字符串转化为bitmap。
该函数从2.3版本开始支持。

## 语法

```Plain Text
BITMAP base64_to_bitmap(STRING bitmap)
```

## 参数说明

`bitmap`：支持的数据类型为STRING。导入外部bitmap数据时，可使用Java或者C++接口先创建BitmapValue对象，然后添加元素、序列化、Base64编码，将得到的Base64字符串作为该函数的入参。

## 返回值说明

返回BITMAP类型的数据。

## 示例

创建库表`bitmapdb.bitmap_table`，使用Stream Load将JSON格式数据导入到`bitmap_table`中，过程中使用base64_to_bitmap函数进行数据转换。

1. 在StarRocks中创建库表。

    ```SQL
    CREATE database bitmapdb;
    USE bitmapdb;
    CREATE TABLE `bitmap_table` (
    `tagname` varchar(65533) NOT NULL COMMENT "标签名称",
    `tagvalue` varchar(65533) NOT NULL COMMENT "标签值",
    `userid` bitmap NOT NULL COMMENT "访问用户ID"
    ) ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`) BUCKETS 1
    PROPERTIES (
    "replication_num" = "3",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
    );
    ```

2. 使用Stream Load将JSON格式数据导入到`bitmap_table`中。

    假设有JSON格式文件**simpledata**, 内容如下，`userid`为Base64编码后的字符串:

    ```JSON
    {
        "tagname": "持有产品", "tagvalue": "保险", "userid":"AjowAAABAAAAAAACABAAAAABAAIAAwA="
    }
    ```

    - 导入JSON文件中的数据到`bitmap_table`，使用base64_to_bitmap函数将`userid`转化为bitmap。

    ```Plain Text
    curl --location-trusted -u root: -H "columns: c1,c2,c3,tagname=c1,tagvalue=c2,userid=base64_to_bitmap(c3)" -H "label:bitmap123" -H "format: json" -H "jsonpaths: [\"$.tagname\",\"$.tagvalue\",\"$.userid\"]" -T simpleData http://host:port/api/bitmapdb/bitmap_table/_stream_load
    ```

3. 查询`bitmap_table`表中数据。

    ```Plain Text
    mysql> select tagname,tagvalue,bitmap_to_string(userid) from bitmap_table;
    +--------------+----------+----------------------------+
    | tagname      | tagvalue | bitmap_to_string(`userid`) |
    +--------------+----------+----------------------------+
    | 持有产品      | 保险      | 1,2,3                      |
    +--------------+----------+----------------------------+
    1 rows in set (0.01 sec)
    ```
