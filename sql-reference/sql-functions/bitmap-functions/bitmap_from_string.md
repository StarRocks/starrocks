# bitmap_from_string

## description

### Syntax

```Haskell
BITMAP BITMAP_FROM_STRING(VARCHAR input)
```

将一个字符串转化为一个BITAMP，字符串是由逗号分隔的一组UINT32数字组成.
比如"0, 1, 2"字符串会转化为一个Bitmap，其中的第0, 1, 2位被设置.
当输入字段不合法时，返回NULL

## example

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

## keyword

BITMAP_FROM_STRING,BITMAP
