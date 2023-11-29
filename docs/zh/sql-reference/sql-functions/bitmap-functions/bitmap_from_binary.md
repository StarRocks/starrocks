---
displayed_sidebar: "Chinese"
---

# bitmap_from_binary

## 功能

这个函数用于将特定格式的 varbinary 类型的字符串转换为 bitmap。

这个函数可以用于导入 bitmap 到 StarRocks。

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

示例1: 该函数与其它 bitmap 函数搭配使用。

   ```Plain
   mysql> select bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string("0,1,2,3"))));
   +---------------------------------------------------------------------------------------+
   | bitmap_to_string(bitmap_from_binary(bitmap_to_binary(bitmap_from_string('0,1,2,3')))) |
   +---------------------------------------------------------------------------------------+
   | 0,1,2,3                                                                               |
   +---------------------------------------------------------------------------------------+
   1 row in set (0.01 sec)
   ```
