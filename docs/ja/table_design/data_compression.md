---
displayed_sidebar: docs
sidebar_position: 50
---

# データ圧縮

StarRocks はテーブルとインデックスのストレージに対してデータ圧縮をサポートしています。データ圧縮はストレージスペースを節約するだけでなく、I/O 集中型のタスクのパフォーマンスを向上させます。これは、StarRocks が各リクエストに対してディスクから読み込むページ数を減らせるためです。ただし、データの圧縮と解凍には追加の CPU リソースが必要です。

## データ圧縮アルゴリズムの選択

StarRocks は、LZ4、Zstandard (または zstd)、zlib、Snappy の4つのデータ圧縮アルゴリズムをサポートしています。これらのデータ圧縮アルゴリズムは、圧縮率と圧縮/解凍のパフォーマンスが異なります。一般的に、これらのアルゴリズムの圧縮率は次のようにランク付けされます: zlib > Zstandard > LZ4 > Snappy。中でも、zlib は比較的高い圧縮率を示しています。データが高圧縮される結果、zlib 圧縮アルゴリズムを使用したテーブルのロードおよびクエリパフォーマンスにも影響があります。特に LZ4 と Zstandard は、圧縮率と解凍パフォーマンスのバランスが良好です。ストレージスペースの削減やパフォーマンス向上のビジネスニーズに応じて、これらの圧縮アルゴリズムを選択できます。特にストレージスペースの削減に特別な要求がない場合は、LZ4 または Zstandard をお勧めします。

> **注意**
>
> データ型の違いが圧縮率に影響を与えることがあります。

## テーブルにデータ圧縮アルゴリズムを指定する

テーブルにデータ圧縮アルゴリズムを指定できるのは、テーブル作成時のみであり、その後変更することはできません。

以下の例では、Zstandard アルゴリズムを使用して `data_compression` テーブルを作成します。詳細な手順については、 [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。

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