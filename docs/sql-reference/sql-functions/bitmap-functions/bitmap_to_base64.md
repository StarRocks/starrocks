# bitmap_to_base64

## Description

Converts a bitmap to a Base64-encoded string. This function is supported from v2.5.

## Syntax

```Haskell
VARCHAR bitmap_to_base64(BITMAP bitmap)
```

## Parameters

`bitmap`: the bitmap to convert. This parameter is required. If the input value is invalid, an error is returned.

## Return value

Returns a value of the VARCHAR type.

## Examples

Example 1: Use this function with other bitmap functions.

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

Example 2: Convert each value in a BITMAP column into Base64-encoded strings.

1. Create an aggregate table `page_uv` where the aggregate key is (`page_id`, `visit_date`). This table contains a BITMAP column `visit_users` whose values are to be aggregated.

    ```SQL
        CREATE TABLE `page_uv`
        (`page_id` INT NOT NULL,
        `visit_date` datetime NOT NULL,
        `visit_users` BITMAP BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`page_id`, `visit_date`)
        DISTRIBUTED BY HASH(`page_id`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1","storage_format" = "DEFAULT"
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

3. Convert each value in the `visit_users` column into a Base64-encoded string.

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
