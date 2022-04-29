# bitmap_from_string

## 功能

将一个字符串转化为一个 BITAMP, 字符串是由逗号分隔的一组 UINT32 数字组成。比如 "0, 1, 2" 字符串会转化为一个 Bitmap, 其中的第 0, 1, 2 位被设置, 当输入字段不合法时返回 NULL

## 语法

```Haskell
BITMAP_FROM_STRING(input)
```

## 参数说明

`input`: 输入的字符串, 支持的数据类型为 VARCHAR

## 返回值说明

返回值的数据类型为 BITMAP

## 示例

```Plain Text
MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

MySQL > select bitmap_to_string(bitmap_from_string("0, 1, 2"));
+-------------------------------------------------+
| bitmap_to_string(bitmap_from_string('0, 1, 2')) |
+-------------------------------------------------+
| 0,1,2                                           |
+-------------------------------------------------+

MySQL > select bitmap_from_string("-1, 0, 1, 2");
+-----------------------------------+
| bitmap_from_string('-1, 0, 1, 2') |
+-----------------------------------+
| NULL                              |
+-----------------------------------+
```

## 关键词

BITMAP_FROM_STRING, BITMAP
