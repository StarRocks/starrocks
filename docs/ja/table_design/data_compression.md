---
displayed_sidebar: docs
sidebar_position: 50
---

# データ圧縮

StarRocks はテーブルおよびインデックスストレージのデータ圧縮をサポートしています。データ圧縮はストレージスペースを節約するだけでなく、I/O 集中型タスクのパフォーマンスも向上させます。これは、StarRocks が各リクエストに対してディスクから読み込むページ数を減らせるためです。ただし、データの圧縮および解凍には追加の CPU リソースが必要です。

## データ圧縮アルゴリズムの選択

StarRocks は 4 つのデータ圧縮アルゴリズムをサポートしています: LZ4、Zstandard (または zstd)、zlib、および Snappy。これらのデータ圧縮アルゴリズムは、圧縮率および圧縮/解凍パフォーマンスが異なります。一般的に、これらのアルゴリズムの圧縮率は次のようにランク付けされます: zlib > Zstandard > LZ4 > Snappy。中でも、zlib は比較的高い圧縮率を示しています。高圧縮データの結果として、zlib 圧縮アルゴリズムを使用したテーブルのロードおよびクエリパフォーマンスも影響を受けます。特に LZ4 および Zstandard は、圧縮率と解凍パフォーマンスのバランスが良好です。ストレージスペースの削減やパフォーマンス向上のビジネスニーズに合わせて、これらの圧縮アルゴリズムを選択できます。特にストレージスペースの削減に特別な要求がない場合は、LZ4 または Zstandard をお勧めします。

> **注意**
>
> データ型の違いが圧縮率に影響を与えることがあります。

## テーブルにデータ圧縮アルゴリズムを指定する

テーブルにデータ圧縮アルゴリズムを指定できるのはテーブル作成時のみで、その後変更することはできません。

以下の例は、Zstandard アルゴリズムを使用して `data_compression` テーブルを作成するものです。詳細な手順については、 [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

```SQL
CREATE TABLE `data_compression` (
  `id`      INT(11)     NOT NULL     COMMENT "",
  `name`    CHAR(200)   NULL         COMMENT ""
)
ENGINE=OLAP 
UNIQUE KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"compression" = "ZSTD"
);
```

> **注意**
>
> データ圧縮アルゴリズムが指定されていない場合、StarRocks はデフォルトで LZ4 を使用します。

テーブルの圧縮アルゴリズムは [SHOW CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) を使用して確認できます。