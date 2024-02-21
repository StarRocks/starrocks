---
displayed_sidebar: "English"
---

# Hive Bitmap UDF

Hive Bitmap UDF provides UDFs that can be directly used in Hive. They can be used to generate Bitmap data and perform Bitmap-related calculations.

The Bitmap format defined by Hive Bitmap UDF is consistent with the format in StarRocks and can be directly used for loading Bitmap data into StarRocks and unloading Bitmap data from StarRocks to Hive.

Applicable scenarios:

- The amount of raw data is large and directly loading these data into StarRocks for computing will cause tremendous pressure on StarRocks clusters. The desired solution is generating Bitmap data in Hive and then loading Bitmap into StarRocks.
- Export the Bitmap data generated in StarRocks to Hive for other systems to use.

Supported source and target data types:

- v3.1 and later support loading and unloading data of these types: String, Base64, and Binary.
- v2.5 and v3.0 only support loading and unloading of String and Base64 data.

## Hive Bitmap UDFs that can be generated

- com.starrocks.hive.udf.UDAFBitmapAgg

  Combines multiple rows of non-null values in a column into one row of Bitmap values, which is equivalent to StarRocks' built-in aggregate function [bitmap_agg](../sql-reference/sql-functions/bitmap-functions/bitmap_agg.md).

- com.starrocks.hive.udf.UDAFBitmapUnion

  Calculates the union of a set of bitmaps, which is equivalent to StarRocks' built-in aggregate function [bitmap_union](../sql-reference/sql-functions/bitmap-functions/bitmap_union.md).

- com.starrocks.hive.udf.UDFBase64ToBitmap

  Converts a base64-encoded string into a bitmap, which is equivalent to StarRocks' built-in function [base64_to_bitmap](../sql-reference/sql-functions/bitmap-functions/base64_to_bitmap.md).

- com.starrocks.hive.udf.UDFBitmapAnd

  Calculates the intersection of two bitmaps, which is equivalent to StarRocks' built-in function [bitmap_and](../sql-reference/sql-functions/bitmap-functions/bitmap_and.md).

- com.starrocks.hive.udf.UDFBitmapCount

  Counts the number of values in the bitmap, which is equivalent to StarRocks' built-in function [bitmap_count](../sql-reference/sql-functions/bitmap-functions/bitmap_count.md).

- com.starrocks.hive.udf.UDFBitmapFromString

  Converts a comma-separated string to a bitmap, equivalent to StarRocks' built-in function [bitmap_from_string](../sql-reference/sql-functions/bitmap-functions/bitmap_from_string.md).

- com.starrocks.hive.udf.UDFBitmapOr

  Calculates the union of two bitmaps, equivalent to StarRocks' built-in function [bitmap_or](../sql-reference/sql-functions/bitmap-functions/bitmap_or.md).

- com.starrocks.hive.udf.UDFBitmapToBase64

  Converts Bitmap to Base64 string, equivalent to StarRocks' built-in function [bitmap_to_base64](../sql-reference/sql-functions/bitmap-functions/bitmap_to_base64.md).

- com.starrocks.hive.udf.UDFBitmapToString

  Converts a bitmap to a comma-separated string, equivalent to StarRocks' built-in function [bitmap_to_string](../sql-reference/sql-functions/bitmap-functions/bitmap_to_string.md).

- com.starrocks.hive.udf.UDFBitmapXor

  Calculates the set of unique elements in two Bitmaps, which is equivalent to StarRocks' built-in function [bitmap_xor](../sql-reference/sql-functions/bitmap-functions/bitmap_xor.md).

## How to use

1. Compile and generate Hive UDF on the FE.

   ```bash
   ./build.sh --hive-udf
   ```

   A JAR package `hive-udf-1.0.0.jar` will be generated in the `fe/hive-udf/` directory.

2. Upload the JAR package to HDFS.

   ```bash
   hadoop  fs -put -f ./hive-udf-1.0.0.jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar
   ```

3. Load the JAR package to Hive.

   ```bash
   hive> add jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar;
   ```

4. Load UDF functions.

   ```sql
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

## Usage examples

### Generate Bitmap in Hive and load it into StarRocks in Binary format

1. Create a Hive source table.

   ```sql
   hive> create table t_src(c1 bigint, c2 bigint) stored as parquet;

   hive> insert into t_src values (1,1), (1,2), (1,3), (2,4), (2,5);

   hive> select * from t_src;
   1       1
   1       2
   1       3
   2       4
   2       5
   ```

2. Create a Hive bitmap table.

   ```sql
   hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
   ```

   Hive generates bitmap through UDFBitmapAgg and writes it into the bitmap table.

   ```sql
   hive> insert into t_bitmap select c1, bitmap_agg(c2) from t_src group by c1;
   ```

3. Create a StarRocks Bitmap table.

   ```sql
   mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1)  distributed by hash(c1);
   ```

4. Load Bitmap data into StarRocks in different ways.

   - Load data via the [files](../sql-reference/sql-functions/table-functions/files.md) function.

   ```sql
   mysql> insert into t1 select c1, bitmap_from_binary(c2) from files (
       "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/*",
       "format"="parquet",
       "compression" = "uncompressed"
       );
   ```

   - Load data via [Hive Catalog](../data_source/catalog/hive_catalog.md).

   ```sql
   mysql> insert into t1 select c1, bitmap_from_binary(c2) from hive_catalog_hms.xxx_db.t_bitmap;
   ```

5. View the results.

   ```sql
   mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
   +------+----------------------+                                                                                                                                                                                                                                                   
   | c1   | bitmap_to_string(c2) |
   +------+----------------------+
   |    1 | 1,2,3                |
   |    2 | 4,5                  |
   +------+----------------------+
   ```

### Export Bitmap from StarRocks to Hive

1. Create a Bitmap table in StarRocks and write data into this table.

   ```sql
   mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1) buckets 3 distributed by hash(c1);

   mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
   +------+----------------------+                                                                                                                                                                                                                                                   
   | c1   | bitmap_to_string(c2) |
   +------+----------------------+
   |    1 | 1,2,3                |
   |    2 | 4,5                  |
   +------+----------------------+
   ```

2. Create a Bitmap table in Hive.

   ```sql
   hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
   ```

3. Export data in different ways.

   - Export data via INSERT INTO FILES (Binary format).

   ```sql
   mysql> insert into files (
       "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/",
       "format"="parquet",
       "compression" = "uncompressed"
   ) select c1, bitmap_to_binary(c2) as c2 from t1;
   ```

   - Export data via [Hive Catalog](../data_source/catalog/hive_catalog.md) (Binary format).

   ```sql
   mysql> insert into hive_catalog_hms.<hdfs_db>.t_bitmap select c1, bitmap_to_binary(c2) from t1;
   ```

4. View results in Hive.

   ```plain
   hive> select c1, bitmap_to_string(c2) from t_bitmap;
   1       1,2,3
   2       4,5
   ```
