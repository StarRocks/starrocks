---
displayed_sidebar: "Chinese"
---

# Hive Bitmap UDF

Hive Bitmap UDF 提供了可以直接在 Hive 里使用的 UDF，可用于生成 Bitmap 和执行 Bitmap 运算。

UDF 定义的 Bitmap 格式与 StarRocks 里格式一致，可直接用于导入导出。

适用场景:

- 原始数据量比较大，直接导入 StarRocks 计算，对于 StarRocks 集群的压力比较大，所以希望在 Hive 里计算完生成 Bitmap 后，再导入 StarRocks。
- 将 StarRocks 里生成的 Bitmap，导出到 Hive，方便其它系统使用。

导出格式:

- 3.1 及以上版本支持 String, Base64，Binary 三种格式导入导出。
- 2.5 和 3.0 版本只支持 String，Base64 两种格式导入导出。

## UDF 函数

- com.starrocks.hive.udf.UDAFBitmapAgg: 将一列中的多行非 NULL 数值合并成一行 BITMAP 值，等同于 StarRocks 的内置聚合函数 bitmap_agg。
- com.starrocks.hive.udf.UDAFBitmapUnion: 输入一组 Bitmap 值，求这一组 Bitmap 值的并集，等同于 StarRocks 的内置聚合函数 bitmap_union。
- com.starrocks.hive.udf.UDFBase64ToBitmap: 将 Base64 编码的字符串转化为 Bitmap，等同于 StarRocks 的内置函数 base64_to_bitmap。
- com.starrocks.hive.udf.UDFBitmapAnd: 计算两个 Bitmap 的交集，等同于 StarRocks 的内置函数 bitmap_add。
- com.starrocks.hive.udf.UDFBitmapCount: 统计 Bitmap 中值的个数，等同于 StarRocks 的内置函数 bitmap_count。
- com.starrocks.hive.udf.UDFBitmapFromString: 将一个逗号分隔的字符串转化为一个 Bitmap，等同于 StarRocks 的内置函数 bitmap_from_string。
- com.starrocks.hive.udf.UDFBitmapOr: 计算两个 Bitmap 的并集，等同于 StarRocks 的内置函数 bitmap_or。
- com.starrocks.hive.udf.UDFBitmapToBase64: 将 Bitmap 转换为 Base64 字符串，等同于 StarRocks 的内置函数 bitmap_to_base64。
- com.starrocks.hive.udf.UDFBitmapToString: 将 Bitmap 转换为逗号分隔的字符串，等同于 StarRocks 的内置函数 bitmap_to_string。
- com.starrocks.hive.udf.UDFBitmapXor: 计算两个 Bitmap 中不重复元素所构成的集合，等同于 StarRocks 的内置函数 bitmap_xor。

## 使用方法

1. 编译生成 HiveUDF。

```bash
./build.sh --hive-udf
```

会在 `fe/hive-udf/` 目录下生成一个 jar 包 `hive-udf-1.0.0.jar`。

2. 将 jar 包上传到 HDFS。

```bash
hadoop  fs -put -f ./hive-udf-1.0.0.jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar
```

3. Hive 里加载 jar 包。

```bash
hive> add jar hdfs://<hdfs_ip>:<hdfs_port>/hive1-udf-1.0.0.jar;
```

4. 加载 UDF 函数。

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

## 使用示例

### 示例一: Hive 里生成 Bitmap 并通过 Binary 格式导入到 StarRocks。

1. 创建 Hive 源数据表。

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

2. 创建 Hive Bitmap 表。

    ```sql
    hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
    ```

    Hive 通过 UDFBitmapAgg 生成 Bitmap，并写入 Bitmap 表。

    ```sql
    hive> insert into t_bitmap select c1, bitmap_agg(c2) from t_src group by c1;
    ```

3. 创建 StarRocks Bitmap 表。

    ```sql
    mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1)  distributed by hash(c1);
    ```

4. 通过不同方式导入到 StarRocks。

   - 通过 Files 函数导入。

    ```sql
    mysql> insert into t1 select c1, bitmap_from_binary(c2) from files (
        "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/*",
        "format"="parquet",
        "compression" = "uncompressed"
        );
    ```

   - 通过 Hive Catalog 导入。

    ```sql
    mysql> insert into t1 select c1, bitmap_from_binary(c2) from hive_catalog_hms.xxx_db.t_bitmap;
    ```

5. 查看结果。

    ```plain
    mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
    +------+----------------------+                                                                                                                                                                                                                                                   
    | c1   | bitmap_to_string(c2) |
    +------+----------------------+
    |    1 | 1,2,3                |
    |    2 | 4,5                  |
    +------+----------------------+
    ```

### 示例二: 将 StarRocks 里的 Bitmap 导出到 Hive。

1. 在 StarRocks 里创建 Bitmap 表，并写入数据。

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

2. 在 Hive 里创建 Bitmap 表。

    ```sql
    hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
    ```

3. 通过不同方式导出。

   - 通过 INSERT INTO FILES 导出（binary 格式）。
    
    ```sql
    mysql> insert into files (
        "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/",
        "format"="parquet",
        "compression" = "uncompressed"
    ) select c1, bitmap_to_binary(c2) as c2 from t1;
    ```

   - 通过 Hive Catalog 方式导出（binary 格式）。
    
    ```sql
    mysql> insert into hive_catalog_hms.<hdfs_db>.t_bitmap select c1, bitmap_to_binary(c2) from t1;
    ```

4. 在 Hive 里查看结果。

    ```sql
    hive> select c1, bitmap_to_string(c2) from t_bitmap;
    1       1,2,3
    2       4,5
    ```
