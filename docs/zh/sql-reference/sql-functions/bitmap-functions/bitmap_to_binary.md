---
displayed_sidebar: docs
---

# bitmap_to_binary

## 功能

将 Bitmap 根据 StarRocks 定义的规则，转换为二进制字符串。

该函数主要用于将 Bitmap 数据导出，压缩效果要好于 [bitmap_to_base64](./bitmap_to_base64.md)，如果要导出 Bitmap 数据到 Parquet 等二进制文件，建议用 bitmap_to_binary 函数。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
VARBINARY bitmap_to_binary(BITMAP bitmap)
```

## 参数说明

`bitmap`: 待转换的 bitmap, 必填。

## 返回值说明

返回 VARBINARY 类型的值。

## 示例

示例一: 该函数与其它 Bitmap 函数搭配使用。

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

示例二: 将数据表中的 Bitmap 列转换为 Varbinary 字符串。

1. 创建一张含有 BITMAP 列的聚合表，其中 `visit_users` 列为聚合列，列类型为 BITMAP，使用聚合函数 bitmap_union()。

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

2. 向表中导入数据。

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

3. 将 `visit_users` 列的每行 Bitmap 值转为 Varbinary 字符串。hex 函数只是为了将不可见字符串显示到客户端，用于导出时可以不使用。

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
