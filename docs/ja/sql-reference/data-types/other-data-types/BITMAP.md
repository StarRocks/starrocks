---
displayed_sidebar: docs
---

# BITMAP

BITMAP は、count distinct を高速化するためによく使用されます。HyperLogLog (HLL) に似ていますが、count distinct においてより正確です。BITMAP は、より多くのメモリとディスクリソースを消費します。INT データの集計のみをサポートします。文字列データに bitmap を適用したい場合は、低カーディナリティの辞書を使用してデータをマッピングする必要があります。

このトピックでは、BITMAP カラムを作成し、そのカラムのデータを集計するために bitmap 関数を使用する簡単な例を提供します。関数の詳細な定義やその他の Bitmap 関数については、「Bitmap functions」を参照してください。

## テーブルを作成する

- `user_id` カラムのデータ型が BITMAP であり、bitmap_union() 関数を使用してデータを集計する集計テーブルを作成します。

    ```SQL
    CREATE TABLE `pv_bitmap` (
    `dt` int(11) NULL COMMENT "",
    `page` varchar(10) NULL COMMENT "",
    `user_id` bitmap BITMAP_UNION NULL COMMENT ""
    ) ENGINE=OLAP
    AGGREGATE KEY(`dt`, `page`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`dt`);
    ```

- `userid` カラムのデータ型が BITMAP である主キーテーブルを作成します。

    ```SQL
    CREATE TABLE primary_bitmap (
    `tagname` varchar(65533) NOT NULL COMMENT "Tag name",
    `tagvalue` varchar(65533) NOT NULL COMMENT "Tag value",
    `userid` bitmap NOT NULL COMMENT "User ID")
    ENGINE=OLAP
    PRIMARY KEY(`tagname`, `tagvalue`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`tagname`);
    ```

BITMAP カラムにデータを挿入する前に、まず to_bitmap() 関数を使用してデータを変換する必要があります。

BITMAP の使用方法、たとえばテーブルに BITMAP データをロードする方法の詳細については、[bitmap](../../sql-functions/aggregate-functions/bitmap.md) を参照してください。