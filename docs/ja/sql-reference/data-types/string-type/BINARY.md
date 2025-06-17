---
displayed_sidebar: docs
---

# BINARY/VARBINARY

## 説明

BINARY(M)

VARBINARY(M)

バージョン 3.0 以降、StarRocks は BINARY/VARBINARY データ型をサポートしており、バイナリデータを格納するために使用されます。サポートされる最大長は VARCHAR と同じで、[1, 1048576] です。単位: バイト。`M` が指定されていない場合、デフォルトで 1048576 が使用されます。バイナリデータ型はバイト文字列を含み、文字データ型は文字列を含みます。

BINARY は VARBINARY のエイリアスです。使用方法は VARBINARY と同じです。

## 制限と使用上の注意

- VARBINARY カラムは、Duplicate Key、Primary Key、ユニークキーテーブルでサポートされています。集計テーブルではサポートされていません。

- VARBINARY カラムは、Duplicate Key、Primary Key、ユニークキーテーブルのパーティションキー、バケッティングキー、またはディメンションカラムとして使用できません。また、ORDER BY、GROUP BY、JOIN 句でも使用できません。

- BINARY(M)/VARBINARY(M) は、長さが揃っていない場合に右詰めされません。

## 例

### VARBINARY 型のカラムを作成する

テーブルを作成する際に、キーワード `VARBINARY` を使用してカラム `j` を VARBINARY カラムとして指定します。

```SQL
CREATE TABLE `test_binary` (
    `id` INT(11) NOT NULL COMMENT "",
    `j`  VARBINARY NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
);

mysql> DESC test_binary;
+-------+-----------+------+-------+---------+-------+
| Field | Type      | Null | Key   | Default | Extra |
+-------+-----------+------+-------+---------+-------+
| id    | int       | NO   | true  | NULL    |       |
| j     | varbinary | YES  | false | NULL    |       |
+-------+-----------+------+-------+---------+-------+
2 rows in set (0.01 sec)

```

### データをロードして BINARY 型として保存する

StarRocks は、データをロードして BINARY 型として保存する以下の方法をサポートしています。

- 方法 1: `x''` をプレフィックスとして持つ定数カラム（例えばカラム `j`）にデータを書き込むために INSERT INTO を使用します。

    ```SQL
    INSERT INTO test_binary (id, j) VALUES (1, x'abab');
    INSERT INTO test_binary (id, j) VALUES (2, x'baba');
    INSERT INTO test_binary (id, j) VALUES (3, x'010102');
    INSERT INTO test_binary (id, j) VALUES (4, x'0000'); 
    ```

- 方法 2: [to_binary](../../sql-functions/binary-functions/to_binary.md) 関数を使用して VARCHAR データをバイナリデータに変換します。

    ```SQL
    INSERT INTO test_binary select 5, to_binary('abab', 'hex');
    INSERT INTO test_binary select 6, to_binary('abab', 'base64');
    INSERT INTO test_binary select 7, to_binary('abab', 'utf8');
    ```

- 方法 3: Broker Load を使用して Parquet または ORC ファイルをロードし、ファイルを BINARY データとして保存します。詳細は [Broker Load](../../sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

  - Parquet ファイルの場合、`parquet::Type::type::BYTE_ARRAY` を `TYPE_VARBINARY` に直接変換します。
  - ORC ファイルの場合、`orc::BINARY` を `TYPE_VARBINARY` に直接変換します。

- 方法 4: Stream Load を使用して CSV ファイルをロードし、ファイルを `BINARY` データとして保存します。詳細は [Load CSV data](../../../loading/StreamLoad.md#load-csv-data) を参照してください。
  - CSV ファイルはバイナリデータに対して 16 進数形式を使用します。入力されるバイナリ値が有効な 16 進数値であることを確認してください。
  - `BINARY` 型は CSV ファイルでのみサポートされています。JSON ファイルでは `BINARY` 型はサポートされていません。

  例えば、`t1` は VARBINARY カラム `b` を持つテーブルです。

    ```sql
    CREATE TABLE `t1` (
    `k` int(11) NOT NULL COMMENT "",
    `v` int(11) NOT NULL COMMENT "",
    `b` varbinary
    ) ENGINE = OLAP
    DUPLICATE KEY(`k`)
    PARTITION BY RANGE(`v`) (
    PARTITION p1 VALUES [("-2147483648"), ("0")),
    PARTITION p2 VALUES [("0"), ("10")),
    PARTITION p3 VALUES [("10"), ("20")))
    DISTRIBUTED BY HASH(`k`)
    PROPERTIES ("replication_num" = "1");

    -- csv file
    -- cat temp_data
    0,0,ab

    -- Stream Load を使用して CSV ファイルをロードします。
    curl --location-trusted -u <username>:<password> -T temp_data -XPUT -H column_separator:, -H label:xx http://172.17.0.1:8131/api/test_mv/t1/_stream_load

    -- ロードされたデータをクエリします。
    mysql> select * from t1;
    +------+------+------------+
    | k    | v    | xx         |
    +------+------+------------+
    |    0 |    0 | 0xAB       |
    +------+------+------------+
    1 rows in set (0.11 sec)
    ```

### BINARY データをクエリして処理する

StarRocks は BINARY データのクエリと処理をサポートしており、BINARY 関数と演算子の使用をサポートしています。この例ではテーブル `test_binary` を使用します。

注意: MySQL クライアントから StarRocks にアクセスする際に `--binary-as-hex` オプションを追加すると、バイナリデータは 16 進数表記で表示されます。

```Plain Text
mysql> select * from test_binary;
+------+------------+
| id   | j          |
+------+------------+
|    1 | 0xABAB     |
|    2 | 0xBABA     |
|    3 | 0x010102   |
|    4 | 0x0000     |
|    5 | 0xABAB     |
|    6 | 0xABAB     |
|    7 | 0x61626162 |
+------+------------+
7 rows in set (0.08 sec)
```

例 1: [hex](../../sql-functions/string-functions/hex.md) 関数を使用してバイナリデータを表示します。

```plain
mysql> select id, hex(j) from test_binary;
+------+----------+
| id   | hex(j)   |
+------+----------+
|    1 | ABAB     |
|    2 | BABA     |
|    3 | 010102   |
|    4 | 0000     |
|    5 | ABAB     |
|    6 | ABAB     |
|    7 | 61626162 |
+------+----------+
7 rows in set (0.02 sec)
```

例 2: [to_base64](../../sql-functions/crytographic-functions/to_base64.md) 関数を使用してバイナリデータを表示します。

```plain
mysql> select id, to_base64(j) from test_binary;
+------+--------------+
| id   | to_base64(j) |
+------+--------------+
|    1 | q6s=         |
|    2 | uro=         |
|    3 | AQEC         |
|    4 | AAA=         |
|    5 | q6s=         |
|    6 | q6s=         |
|    7 | YWJhYg==     |
+------+--------------+
7 rows in set (0.01 sec)
```

例 3: [from_binary](../../sql-functions/binary-functions/from_binary.md) 関数を使用してバイナリデータを表示します。

```plain
mysql> select id, from_binary(j, 'hex') from test_binary;
+------+-----------------------+
| id   | from_binary(j, 'hex') |
+------+-----------------------+
|    1 | ABAB                  |
|    2 | BABA                  |
|    3 | 010102                |
|    4 | 0000                  |
|    5 | ABAB                  |
|    6 | ABAB                  |
|    7 | 61626162              |
+------+-----------------------+
7 rows in set (0.01 sec)
```