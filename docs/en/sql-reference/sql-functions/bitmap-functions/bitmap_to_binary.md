---
displayed_sidebar: "English"
---

# bitmap_to_binary

## Description

Converts Bitmap values to a Binary string.

bitmap_to_binary is mainly used for exporting Bitmap data. The compression effect is better than [bitmap_to_base64](./bitmap_to_base64.md).

If you plan to export data directly to a binary file such as Parquet, this function is recommended.

This function is supported from v3.0.

## Syntax

```Haskell
VARBINARY bitmap_to_binary(BITMAP bitmap)
```

## Parameters

`bitmap`: the Bitmap to convert, required. If the input value is invalid, an error is returned.

## Return value

Returns a value of the VARBINARY type.

## Examples

Example 1: Use this function with other bitmap functions.

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

Example 2: Convert each value in a BITMAP column into a binary string.

1. Create an Aggregate table `page_uv` where the `AGGREGATE KEY` is (`page_id`, `visit_date`). This table contains a BITMAP column `visit_users` whose values are to be aggregated.

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

2. Insert data into this table.

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

3. Convert each value in the `visit_users` column into a binary-encoded string.

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
    
