---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Hive Bitmap UDF

Hive Bitmap UDF は、Hive で直接使用できる UDF を提供します。これらは、Bitmap データを生成し、Bitmap に関連する計算を行うために使用できます。

Hive Bitmap UDF で定義された Bitmap フォーマットは、StarRocks のフォーマットと一致しており、Bitmap データを StarRocks にロードしたり、StarRocks から Hive にアンロードしたりする際に直接使用できます。

適用シナリオ:

- 生データの量が多く、これらのデータを直接 StarRocks にロードして計算することは、StarRocks クラスターに大きな負担をかけます。望ましい解決策は、Hive で Bitmap データを生成し、それを StarRocks にロードすることです。
- StarRocks で生成された Bitmap データを他のシステムで使用するために Hive にエクスポートします。

サポートされるソースおよびターゲットデータタイプ:

- v3.1 以降では、これらのタイプのデータのロードとアンロードをサポートします: String, Base64, Binary。
- v2.5 および v3.0 では、String と Base64 データのロードとアンロードのみをサポートします。

## 生成可能な Hive Bitmap UDF

- com.starrocks.hive.udf.UDAFBitmapAgg

  列内の複数行の非 null 値を 1 行の Bitmap 値に結合します。これは、StarRocks の組み込み集計関数 [bitmap_agg](bitmap-functions/bitmap_agg.md) と同等です。

- com.starrocks.hive.udf.UDAFBitmapUnion

  一連のビットマップの和集合を計算します。これは、StarRocks の組み込み集計関数 [bitmap_union](bitmap-functions/bitmap_union.md) と同等です。

- com.starrocks.hive.udf.UDFBase64ToBitmap

  base64 エンコードされた文字列をビットマップに変換します。これは、StarRocks の組み込み関数 [base64_to_bitmap](bitmap-functions/base64_to_bitmap.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapAnd

  2 つのビットマップの交差を計算します。これは、StarRocks の組み込み関数 [bitmap_and](bitmap-functions/bitmap_and.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapCount

  ビットマップ内の値の数をカウントします。これは、StarRocks の組み込み関数 [bitmap_count](bitmap-functions/bitmap_count.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapFromString

  カンマ区切りの文字列をビットマップに変換します。これは、StarRocks の組み込み関数 [bitmap_from_string](bitmap-functions/bitmap_from_string.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapOr

  2 つのビットマップの和集合を計算します。これは、StarRocks の組み込み関数 [bitmap_or](bitmap-functions/bitmap_or.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapToBase64

  Bitmap を Base64 文字列に変換します。これは、StarRocks の組み込み関数 [bitmap_to_base64](bitmap-functions/bitmap_to_base64.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapToString

  ビットマップをカンマ区切りの文字列に変換します。これは、StarRocks の組み込み関数 [bitmap_to_string](bitmap-functions/bitmap_to_string.md) と同等です。

- com.starrocks.hive.udf.UDFBitmapXor

  2 つのビットマップのユニークな要素の集合を計算します。これは、StarRocks の組み込み関数 [bitmap_xor](bitmap-functions/bitmap_xor.md) と同等です。

## 使用方法

1. FE で Hive UDF をコンパイルして生成します。

   ```bash
   ./build.sh --hive-udf
   ```

   `fe/hive-udf/` ディレクトリに JAR パッケージ `hive-udf-1.0.0.jar` が生成されます。

2. JAR パッケージを HDFS にアップロードします。

   ```bash
   hadoop  fs -put -f ./hive-udf-1.0.0.jar hdfs://<hdfs_ip>:<hdfs_port>/hive-udf-1.0.0.jar
   ```

3. JAR パッケージを Hive にロードします。

   ```bash
   hive> add jar hdfs://<hdfs_ip>:<hdfs_port>/hive-udf-1.0.0.jar;
   ```

4. UDF 関数をロードします。

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

## 使用例

### Hive で Bitmap を生成し、Binary フォーマットで StarRocks にロードする

1. Hive ソーステーブルを作成します。

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

2. Hive ビットマップテーブルを作成します。

   ```sql
   hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
   ```

   Hive は UDFBitmapAgg を通じてビットマップを生成し、それをビットマップテーブルに書き込みます。

   ```sql
   hive> insert into t_bitmap select c1, bitmap_agg(c2) from t_src group by c1;
   ```

3. StarRocks ビットマップテーブルを作成します。

   ```sql
   mysql> create table t1(c1 int, c2 bitmap bitmap_union) aggregate key(c1)  distributed by hash(c1);
   ```

4. Bitmap データを StarRocks にさまざまな方法でロードします。

   - [files](table-functions/files.md) 関数を使用してデータをロードします。

   ```sql
   mysql> insert into t1 select c1, bitmap_from_binary(c2) from files (
       "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/*",
       "format"="parquet",
       "compression" = "uncompressed"
       );
   ```

   - [Hive Catalog](../../data_source/catalog/hive_catalog.md) を使用してデータをロードします。

   ```sql
   mysql> insert into t1 select c1, bitmap_from_binary(c2) from hive_catalog_hms.xxx_db.t_bitmap;
   ```

5. 結果を確認します。

   ```sql
   mysql> select c1, bitmap_to_string(c2) from t1;                                                                                                                                                                                                                                   
   +------+----------------------+                                                                                                                                                                                                                                                   
   | c1   | bitmap_to_string(c2) |
   +------+----------------------+
   |    1 | 1,2,3                |
   |    2 | 4,5                  |
   +------+----------------------+
   ```

### StarRocks から Hive への Bitmap のエクスポート

1. StarRocks に Bitmap テーブルを作成し、このテーブルにデータを書き込みます。

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

2. Hive に Bitmap テーブルを作成します。

   ```sql
   hive> create table t_bitmap(c1 bigint, c2 binary) stored as parquet;
   ```

3. さまざまな方法でデータをエクスポートします。

   - INSERT INTO FILES (Binary フォーマット) を使用してデータをエクスポートします。

   ```sql
   mysql> insert into files (
       "path" = "hdfs://<hdfs_ip>:<hdfs_port>/<hdfs_db>/t_bitmap/",
       "format"="parquet",
       "compression" = "uncompressed"
   ) select c1, bitmap_to_binary(c2) as c2 from t1;
   ```

   - [Hive Catalog](../../data_source/catalog/hive_catalog.md) (Binary フォーマット) を使用してデータをエクスポートします。

   ```sql
   mysql> insert into hive_catalog_hms.<hdfs_db>.t_bitmap select c1, bitmap_to_binary(c2) from t1;
   ```

4. Hive で結果を確認します。

   ```plain
   hive> select c1, bitmap_to_string(c2) from t_bitmap;
   1       1,2,3
   2       4,5
   ```