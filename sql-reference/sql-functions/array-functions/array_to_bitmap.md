# array_to_bitmap

## 功能

将array类型转化为bitmap类型。

## 语法

```Plain Text
array_to_bitmap(array)
```

## 参数说明

`array`: array内的元素支持的数据类型包括INT，TINYINT，SMALLINT。

## 返回值说明

返回BITMAP类型的值。

## 注意事项

- 如果输入的array为非法数据类型，如STRING、DECIMAL等，则返回报错。

- 如果输入空array，则返回空bitmap。

- 如果输入NULL，则返回NULL。

## 示例

示例1：输入array，转化为bitmap。此处因为bitmap类型无法显示，故嵌套`bitmap_to_array`以方便说明。

```Plain Text
MySQL > select bitmap_to_array(array_to_bitmap([1,2,3]));
+-------------------------------------------+
| bitmap_to_array(array_to_bitmap([1,2,3])) |
+-------------------------------------------+
| [1,2,3]                                   |
+-------------------------------------------+
```

示例2：输入空array。

```Plain Text
MySQL > select bitmap_to_array(array_to_bitmap([]));
+--------------------------------------+
| bitmap_to_array(array_to_bitmap([])) |
+--------------------------------------+
| []                                   |
+--------------------------------------+
```

示例3：输入NULL。

```Plain Text
MySQL > select array_to_bitmap(NULL);
+-----------------------+
| array_to_bitmap(NULL) |
+-----------------------+
| NULL                  |
+-----------------------+
```
