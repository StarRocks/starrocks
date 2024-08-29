---
displayed_sidebar: docs
---

# bitmap_from_binary

## 功能

将特定格式的 Varbinary 类型的字符串转换为 Bitmap。可用于导入 Bitmap 数据到 StarRocks。

该函数从 3.0 版本开始支持。

## 语法

```Haskell
BITMAP bitmap_from_binary(VARBINARY str)
```

## 参数说明

`str`：支持的数据类型为 VARBINARY。

## 返回值说明

返回 BITMAP 类型的数据。

## 示例

示例一: 该函数与其它 Bitmap 函数搭配使用。

   ```Plain
   mysql> select bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string("0,1,2,3"))));
   +---------------------------------------------------------------------------------------+
   | bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string('0,1,2,3')))) |
   +---------------------------------------------------------------------------------------+
   | 0,1,2,3                                                                               |
   +---------------------------------------------------------------------------------------+
   1 row in set (0.01 sec)
   ```
