---
displayed_sidebar: docs
---

# base64_to_bitmap

## 功能

导入外部 bitmap 数据到 StarRocks 时，需要先对 bitmap 数据进行序列化和 Base64 编码，生成 Base64 字符串。导入字符串到 StarRocks 时再进行 Base64 到 bitmap 的转化。
该函数用于将 Base64 编码的字符串转化为 bitmap。

该函数从 2.3 版本开始支持。

## 语法

```Haskell
BITMAP base64_to_bitmap(VARCHAR bitmap)
```

## 参数说明

`bitmap`：支持的数据类型为 VARCHAR。导入外部 bitmap 数据时，可使用 Java 或者 C++ 接口先[创建 BitmapValue 对象](https://github.com/StarRocks/starrocks/blob/main/fe/plugin-common/src/test/java/com/starrocks/types/BitmapValueTest.java)，然后添加元素、序列化、Base64 编码，将得到的 Base64 字符串作为该函数的入参。

## 返回值说明

返回 BITMAP 类型的数据。

## 示例

创建库表 `bitmapdb.bitmap_table`，使用 Stream Load 将 JSON 格式数据导入到 `bitmap_table` 中，过程中使用base64_to_bitmap 函数进行数据转换。

1. 在 StarRocks 中创建库和表，以创建主键表为例。

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
    DISTRIBUTED BY HASH(`tagname`)
    PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
    );
    ```

2. 使用 [Stream Load](../../sql-statements/loading_unloading/STREAM_LOAD.md) 将 JSON 格式数据导入到 `bitmap_table` 中。

    假设有 JSON 格式文件**simpledata**, 内容如下，`userid`为 Base64 编码后的字符串:

    ```JSON
    {
        "tagname": "持有产品",
        "tagvalue": "保险",
        "userid":"AjowAAABAAAAAAACABAAAAABAAIAAwA="
    }
    ```

    - 导入 JSON 文件中的数据到 `bitmap_table`，使用 base64_to_bitmap 函数将 `userid` 转化为bitmap。

    ```Plain Text
    curl --location-trusted -u <username>:<password>\
        -H "columns: c1,c2,c3,tagname=c1,tagvalue=c2,userid=base64_to_bitmap(c3)"\
        -H "label:bitmap123"\
        -H "format: json" -H "jsonpaths: [\"$.tagname\",\"$.tagvalue\",\"$.userid\"]"\
        -T simpleData http://<host:port>/api/bitmapdb/bitmap_table/_stream_load
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
