---
displayed_sidebar: docs
---

# bitmap_to_binary

Bitmap 値を Binary 文字列に変換します。

bitmap_to_binary は主に Bitmap データをエクスポートするために使用されます。圧縮効果は [bitmap_to_base64](./bitmap_to_base64.md) よりも優れています。

データを Parquet などのバイナリファイルに直接エクスポートする場合、この関数を推奨します。

この関数は v3.0 からサポートされています。

## 構文

```Haskell
VARBINARY bitmap_to_binary(BITMAP bitmap)
```

## パラメータ

`bitmap`: 変換する Bitmap。必須です。入力値が無効な場合、エラーが返されます。

## 戻り値

VARBINARY 型の値を返します。

## 例

例 1: 他の bitmap 関数と一緒にこの関数を使用します。

```Plain
mysql> select hex(bitmap_to_binary(bitmap_from_string("0, 1, 2, 3")));
+---------------------------------------------------------+
| hex(bitmap_to_binary(bitmap_from_string('0, 1, 2, 3'))) |
+---------------------------------------------------------+
| 023A3000000100000000000300100000000000010002000300      |
+---------------------------------------------------------+
1 row in set (0.01 sec)

mysql> select hex(bitmap_to_binary(to_bitmap(1)));
+-------------------------------------+
| hex(bitmap_to_binary(to_bitmap(1))) |
+-------------------------------------+
| 0101000000                          |
+-------------------------------------+
1 row in set (0.01 sec)

mysql> select hex(bitmap_to_binary(bitmap_empty()));
+---------------------------------------+
| hex(bitmap_to_binary(bitmap_empty())) |
+---------------------------------------+
| 00                                    |
+---------------------------------------+
1 row in set (0.01 sec)
```

例 2: BITMAP カラム内の各値をバイナリ文字列に変換します。

1. `AGGREGATE KEY` が (`page_id`, `visit_date`) の集計テーブル `page_uv` を作成します。このテーブルには、値が集計される BITMAP カラム `visit_users` が含まれています。

    ```SQL
        CREATE TABLE `page_uv`
        (`page_id` INT NOT NULL,
        `visit_date` datetime NOT NULL,
        `visit_users` BITMAP BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`page_id`, `visit_date`)
        DISTRIBUTED BY HASH(`page_id`)
        PROPERTIES (
        "replication_num" = "3",
        "storage_format" = "DEFAULT"
        );
    ```

2. このテーブルにデータを挿入します。

    ```SQL
      insert into page_uv values
      (1, '2020-06-23 01:30:30', to_bitmap(13)),
      (1, '2020-06-23 01:30:30', to_bitmap(23)),
      (1, '2020-06-23 01:30:30', to_bitmap(33)),
      (1, '2020-06-23 02:30:30', to_bitmap(13)),
      (2, '2020-06-23 01:30:30', to_bitmap(23));
      
      select * from page_uv order by page_id;
      +---------+---------------------+-------------+
      | page_id | visit_date          | visit_users |
      +---------+---------------------+-------------+
      |       1 | 2020-06-23 01:30:30 | NULL        |
      |       1 | 2020-06-23 02:30:30 | NULL        |
      |       2 | 2020-06-23 01:30:30 | NULL        |
      +---------+---------------------+-------------+
    ```

3. `visit_users` カラム内の各値をバイナリエンコードされた文字列に変換します。

    ```Plain
       mysql> select page_id, hex(bitmap_to_binary(visit_users)) from page_uv;
       +---------+------------------------------------------------------------+
       | page_id | hex(bitmap_to_binary(visit_users))                         |
       +---------+------------------------------------------------------------+
       |       1 | 0A030000000D0000000000000017000000000000002100000000000000 |
       |       1 | 010D000000                                                 |
       |       2 | 0117000000                                                 |
       +---------+------------------------------------------------------------+
       3 rows in set (0.01 sec)
    ```