---
displayed_sidebar: docs
---

# bitmap_to_base64

ビットマップを Base64 エンコードされた文字列に変換します。この関数は v2.5 からサポートされています。

## 構文

```Haskell
VARCHAR bitmap_to_base64(BITMAP bitmap)
```

## パラメータ

`bitmap`: 変換するビットマップ。このパラメータは必須です。入力値が無効な場合、エラーが返されます。

## 戻り値

VARCHAR 型の値を返します。

## 例

例 1: 他のビットマップ関数と一緒にこの関数を使用します。

```Plain
select bitmap_to_base64(bitmap_from_string("0, 1, 2, 3"));
+----------------------------------------------------+
| bitmap_to_base64(bitmap_from_string('0, 1, 2, 3')) |
+----------------------------------------------------+
| AjowAAABAAAAAAADABAAAAAAAAEAAgADAA==               |
+----------------------------------------------------+
1 row in set (0.00 sec)


select bitmap_to_base64(to_bitmap(1));
+--------------------------------+
| bitmap_to_base64(to_bitmap(1)) |
+--------------------------------+
| AQEAAAA=                       |
+--------------------------------+
1 row in set (0.00 sec)


select bitmap_to_base64(bitmap_empty());
+----------------------------------+
| bitmap_to_base64(bitmap_empty()) |
+----------------------------------+
| AA==                             |
+----------------------------------+
1 row in set (0.00 sec)
```

例 2: BITMAP 列の各値を Base64 エンコードされた文字列に変換します。

1. `AGGREGATE KEY` が (`page_id`, `visit_date`) の集計テーブル `page_uv` を作成します。このテーブルには、値が集計される BITMAP 列 `visit_users` が含まれています。

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

3. `visit_users` 列の各値を Base64 エンコードされた文字列に変換します。

    ```Plain
      select page_id, bitmap_to_base64(visit_users) from page_uv;
      +---------+------------------------------------------+
      | page_id | bitmap_to_base64(visit_users)            |
      +---------+------------------------------------------+
      |       1 | CgMAAAANAAAAAAAAABcAAAAAAAAAIQAAAAAAAAA= |
      |       1 | AQ0AAAA=                                 |
      |       2 | ARcAAAA=                                 |
      +---------+------------------------------------------+
    ```