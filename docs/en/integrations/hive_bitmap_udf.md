---
displayed_sidebar: "English"
---

# Hive Bitmap UDF

Hive Bitmap UDF provides udf that can be used directly in hive, which can be used to generate bitmap and perform bitmap operations.

The Bitmap format defined by udf is consistent with the format in StarRocks and can be directly used for import and export.

User scenario:

* The amount of original data is relatively large. Directly importing it into StarRocks for calculation will put a lot of pressure on the StarRocks cluster. Therefore, it is hoped that the Bitmap will be generated after calculation in Hive and then imported into StarRocks.
* Export the Bitmap generated in StarRocks to hive to facilitate use by other systems.

Export format:

Version 3.1+ supports import and export in three formats: string, base64, and binary.
Versions 2.5 and 3.0 only support string and base64 formats for import and export.

# UDF function

com.starrocks.hive.udf.UDAFBitmapAgg: Combine multiple rows of non-null values in a column into one row of bitmap values, which is equivalent to StarRocks' built-in aggregation function bitmap_agg.

com.starrocks.hive.udf.UDAFBitmapUnion: Calculate the union of a set of bitmaps, which is equivalent to StarRocks' built-in aggregation function bitmap_union.

com.starrocks.hive.udf.UDFBase64ToBitmap: Converts a base64-encoded string into a bitmap, which is equivalent to StarRocks' built-in function base64_to_bitmap.

com.starrocks.hive.udf.UDFBitmapAnd: Calculate the intersection of two bitmaps, which is equivalent to StarRocks' built-in function bitmap_add.

com.starrocks.hive.udf.UDFBitmapCount: Counts the number of values the bitmap, which is equivalent to StarRocks' built-in function bitmap_count.

com.starrocks.hive.udf.UDFBitmapFromString: Converts a comma-separated string to a bitmap, equivalent to StarRocks' built-in function bitmap_from_string.

com.starrocks.hive.udf.UDFBitmapOr: Calculate the union of two bitmaps, equivalent to StarRocks' built-in function bitmap_or.

com.starrocks.hive.udf.UDFBitmapToBase64: Converts bitmap to base64 string, equivalent to StarRocks' built-in function bitmap_to_base64.

com.starrocks.hive.udf.UDFBitmapToString: Converts a bitmap to a comma-separated string, equivalent to StarRocks' built-in function bitmap_to_string.

com.starrocks.hive.udf.UDFBitmapXor: Calculates the set of unique elements in two Bitmaps, which is equivalent to StarRocks' built-in function bitmap_xor.

## Instructions

1. Compile and generate HiveUDF

```
./build.sh --hive-udf
```

A jar package will be generated in the fe/hive-udf/ directory: hive-udf-1.0.0.jar

2. Upload the jar package to HDFS

```
hadoop  fs -put -f ./hive-udf-1.0.0.jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar
```

3. Load jar package into Hive

```
hive> add jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar;
```

4. Load UDF function

```
hive> create temporary function bitmap_agg as 'com.starrocks.hive.udf.UDAFBitmapAgg';
hive> create temporary function bitmap_union as 'com.starrocks.hive.udf.UDAFBitmapUnion';
hive> create temporary function base64_to_bitmap as 'com.starrocks.hive.udf.UDFBase64ToBitmap';
hive> create temporary function bitmap_and as 'com.starrocks.hive.udf.UDFBitmapAnd';
hive> create temporary function bitmap_count as 'com.starrocks.hive.udf.UDFBitmapCount';
hive> create temporary function bitmap_from_string as 'com.starrocks.hive.udf.UDFBitmapFromString';
hive> create temporary function bitmap_or as 'com.starrocks.hive.udf.UDFBitmapOr';
hive> create temporary function bitmap_to_base64 as 'com.starrocks.hive.udf.UDFBitmapToBase64';
hive> create temporary function bitmap_to_string as 'com.starrocks.hive.udf.UDFBitmapToString';
hive> create temporary function bitmap_xor as 'com.starrocks.hive.udf.UDFBitmapXor';
```

## Usage example

1. Example 1: Generate Bitmap in Hive and import it into StarRocks in binary format.

Create hive source data table:

```
hive> create table t_src(c1 bigint, c2 bigint) stored as parquet;

hive> insert into t_src values (1,1), (1,2), (1,3), (2,4), (2,5);

hive> select * from t_src;
1       1
1       2
1       3
2       4
2       5
```
 
create Hive bitmap table:

```
hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
```

Hive generates bitmap through UDFBitmapAgg and writes it into the bitmap table.

```
hive> insert into t_bitmap select c1, bitmap_agg(c2) from t_src group by c1;
```

create StarRocks bitmap table

```
mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1)  distributed by hash(c1);
```

Import into StarRocks in different ways

Import via `files` function

```
mysql> insert into t1 select c1, bitmap_from_binary(c2) from files (
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/*",
    "format"="parquet",
    "compression" = "uncompressed"
    );
```

Import via hive catalog

```
mysql> insert into t1 select c1, bitmap_from_binary(c2) from hive_catalog_hms.xxx_db.t_bitmap;
```

View results

```
mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
+------+----------------------+                                                                                                                                                                                                                                                   
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3                |
|    2 | 4,5                  |
+------+----------------------+
```

2. Example 2: Export bitmap in StarRocks to hive.

Create a bitmap table in StarRocks and write data

```
mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1) buckets 3 distributed by hash(c1);

mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
+------+----------------------+                                                                                                                                                                                                                                                   
| c1   | bitmap_to_string(c2) |
+------+----------------------+
|    1 | 1,2,3                |
|    2 | 4,5                  |
+------+----------------------+
```

Create bitmap table in hive.

```
hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
```

Export in different ways.

Export via insert into files (binary format).

```
mysql> insert into files (
    "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/",
    "format"="parquet",
    "compression" = "uncompressed"
) select c1, bitmap_to_binary(c2) as c2 from t1;
```

Export via hive catalog (binary format).

```
mysql> insert into hive_catalog_hms.<hdfs_db>.t_bitmap select c1, bitmap_to_binary(c2) from t1;
```

View results in hive.

```
hive> select c1, bitmap_to_string(c2) from t_bitmap;
1       1,2,3
2       4,5
```
