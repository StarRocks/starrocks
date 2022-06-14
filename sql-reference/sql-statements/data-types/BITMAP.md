# BITMAP

## 描述

BITMAP 与 HLL 类似 **只能作为聚合表** 的 value 类型使用，常见用来加速 count distinct 的去重计数使用。

通过位图可以进行精确计数，可以通过 bitmap 函数可以进行集合的各种操作，相对 HLL 他可以获得精确的结果。

但是需要消耗更多的内存和磁盘资源，另外 Bitmap 只能支持整数类型的聚合，如果是字符串等类型需要采用字典进行映射。

## 示例

创建表时指定字段类型为 BITMAP

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

向 bitmap 列中插入数据需要使用 to_bitmap()函数进行转换。

BITMAP 类型的详细使用方法，如向表中插入 bitmap value 请参考 [bitmap](/sql-reference/sql-functions/aggregate-functions/bitmap.md)。

BITMAP 类型的字段支持多种 BITMAP 函数，如 bitmap_and，bitmap_andnot 等。具体的函请参考 [bitmap-functions](/sql-reference/sql-functions/bitmap-functions/bitmap_and.md)。
