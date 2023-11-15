# bitmap_to_base64

## 功能

将 bitmap 转换为 Base64 字符串。该函数从 2.5 版本开始支持。

## 语法

```Haskell
VARCHAR bitmap_to_base64(BITMAP bitmap)
```

## 参数说明

`bitmap`: 待转换的 bitmap，必填。如果输入值格式非法，返回报错。

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

示例一：该函数与其他 bitmap 函数搭配使用。

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

示例二：将数据表中的 BITMAP 列转换为 Base64 字符串。

1. 创建一张含有 BITMAP 列的聚合表，其中 `visit_users` 列为聚合列，列类型为 BITMAP，使用聚合函数 bitmap_union()。

    ```SQL
    CREATE TABLE `page_uv`
        (`page_id` INT NOT NULL COMMENT '页面id',
        `visit_date` datetime NOT NULL COMMENT '访问时间',
        `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT '访问用户id'
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

3. 将 `visit_users` 列的每行 `bitmap` 值转为 Base64 字符串。

    ```Plain
        select page_id, bitmap_to_base64(visit_users)
        from page_uv;
        +---------+------------------------------------------+
        | page_id | bitmap_to_base64(visit_users)            |
        +---------+------------------------------------------+
        |       1 | CgMAAAANAAAAAAAAABcAAAAAAAAAIQAAAAAAAAA= |
        |       1 | AQ0AAAA=                                 |
        |       2 | ARcAAAA=                                 |
        +---------+------------------------------------------+
    ```
