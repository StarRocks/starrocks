---
displayed_sidebar: docs
---

# base64_to_bitmap

StarRocks にビットマップデータをインポートする前に、データをシリアライズし、Base64 文字列としてエンコードする必要があります。Base64 文字列を StarRocks にインポートする際には、その文字列をビットマップデータに変換する必要があります。この関数は、Base64 文字列をビットマップデータに変換するために使用されます。

この関数は v2.3 からサポートされています。

## Syntax

```Haskell
BITMAP base64_to_bitmap(VARCHAR bitmap)
```

## Parameters

`bitmap`: サポートされているデータ型は VARCHAR です。StarRocks にビットマップデータをロードする前に、Java または C++ を使用して [BitmapValue オブジェクトを作成](https://github.com/StarRocks/starrocks/blob/main/fe/plugin-common/src/test/java/com/starrocks/types/BitmapValueTest.java)し、要素を追加し、データをシリアライズして Base64 文字列としてエンコードします。その後、この関数の入力パラメータとして Base64 文字列を渡します。

## Return value

BITMAP 型の値を返します。

## Examples

`bitmapdb` という名前のデータベースと `bitmap` という名前のテーブルを作成します。Stream Load を使用して JSON データを `bitmap_table` にインポートします。このプロセス中に、base64_to_bitmap を使用して JSON ファイル内の Base64 文字列をビットマップデータに変換します。

1. StarRocks でデータベースとテーブルを作成します。この例では、主キーテーブルを作成します。

    ```SQL
    CREATE database bitmapdb;
    USE bitmapdb;
    CREATE TABLE `bitmap_table` (
    `tagname` varchar(65533) NOT NULL COMMENT "Tag name",
    `tagvalue` varchar(65533) NOT NULL COMMENT "Tag value",
    `userid` bitmap NOT NULL COMMENT "User ID"
    ) ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`)
    PROPERTIES (
    "replication_num" = "3",
    "storage_format" = "DEFAULT"
    );
    ```

2. [Stream Load](../../sql-statements/loading_unloading/STREAM_LOAD.md) を使用して JSON データを `bitmap_table` にインポートします。

    **simpledata** という名前の JSON ファイルがあると仮定します。このファイルには以下の内容が含まれており、`userid` は Base64 エンコードされた文字列です。

    ```JSON
    {
        "tagname": "Product", "tagvalue": "Insurance", "userid":"AjowAAABAAAAAAACABAAAAABAAIAAwA="
    }
    ```

    base64_to_bitmap を使用して `userid` をビットマップ値に変換します。

    ```Plain
    curl --location-trusted -u <username>:<password>\
        -H "columns: c1,c2,c3,tagname=c1,tagvalue=c2,userid=base64_to_bitmap(c3)"\
        -H "label:bitmap123"\
        -H "format: json"\
        -H "jsonpaths: [\"$.tagname\",\"$.tagvalue\",\"$.userid\"]"\
        -T simpleData http://host:port/api/bitmapdb/bitmap_table/_stream_load
    ```

3. `bitmap_table` からデータをクエリします。

    ```Plaintext
    mysql> select tagname,tagvalue,bitmap_to_string(userid) from bitmap_table;
    +--------------+----------+----------------------------+
    | tagname      | tagvalue | bitmap_to_string(`userid`) |
    +--------------+----------+----------------------------+
    | Product      | Insurance      | 1,2,3                |
    +--------------+----------+----------------------------+
    1 rows in set (0.01 sec)
    ```