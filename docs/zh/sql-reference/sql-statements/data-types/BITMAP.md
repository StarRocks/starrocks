---
displayed_sidebar: "Chinese"
---

# BITMAP

## 描述

BITMAP 与 HLL (HyperLogLog) 类似，常用来加速 count distinct 的去重计数使用。

您可以通过 bitmap 函数进行集合的各种操作，相比 HLL 可以获得更精确的结果。但是 BITMAP 需要消耗更多的内存和磁盘资源，另外 BITMAP 只能支持整数类型的聚合，如果是字符串等类型需要采用字典进行映射。

## 示例

1. 聚合模型建表时指定字段类型为 BITMAP。

    ```sql
    CREATE TABLE pv_bitmap (
        dt INT(11) NULL COMMENT "",
        page VARCHAR(10) NULL COMMENT "",
        user_id bitmap BITMAP_UNION NULL COMMENT ""
    ) ENGINE=OLAP
    AGGREGATE KEY(dt, page)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(dt) BUCKETS 2;
    ```

2. 主键模型建表时指定字段类型为 BITMAP。

    ```sql
    CREATE TABLE primary_bitmap (
      `tagname` varchar(65533) NOT NULL COMMENT "Tag name",
      `tagvalue` varchar(65533) NOT NULL COMMENT "Tag value",
      `userid` bitmap NOT NULL COMMENT "User ID"
    ) ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`) BUCKETS 1;
    ```

向 BITMAP 列中插入数据需要先使用 to_bitmap() 函数进行转换。

BITMAP 类型的详细使用方法，如向表中插入 BITMAP 值，请参考 [bitmap](../../sql-functions/aggregate-functions/bitmap.md)。

BITMAP 类型的字段支持多种 BITMAP 函数，如 bitmap_and()，bitmap_andnot() 等。具体请参考 [bitmap-functions](../../sql-functions/bitmap-functions/bitmap_and.md)。
