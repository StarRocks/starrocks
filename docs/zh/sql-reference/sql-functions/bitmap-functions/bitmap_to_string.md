# bitmap_to_string

## description

### Syntax

```Haskell
VARCHAR BITMAP_TO_STRING(BITMAP input)
```

将一个bitmap转化成一个逗号分隔的字符串，字符串中包含所有设置的BIT位。输入是null的话会返回null。

## example

```Plain Text
MySQL > select bitmap_to_string(null);
+------------------------+
| bitmap_to_string(NULL) |
+------------------------+
| NULL                   |
+------------------------+

MySQL > select bitmap_to_string(bitmap_empty());
+----------------------------------+
| bitmap_to_string(bitmap_empty()) |
+----------------------------------+
|                                  |
+----------------------------------+

MySQL > select bitmap_to_string(to_bitmap(1));
+--------------------------------+
| bitmap_to_string(to_bitmap(1)) |
+--------------------------------+
| 1                              |
+--------------------------------+

MySQL > select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));
+---------------------------------------------------------+
| bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) |
+---------------------------------------------------------+
| 1,2                                                     |
+---------------------------------------------------------+

```

## keyword

BITMAP_TO_STRING,BITMAP
