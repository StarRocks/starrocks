---
displayed_sidebar: docs
---

# sub_bitmap

## 功能

根据 `offset` 指定的起始位置，从 BITMAP 类型的 `src` 中截取 `len` 个元素，返回的元素为 `src` 的子集。该函数主要用于分页查询相关场景。该函数从 2.5 版本开始支持。

该函数与 [bitmap_subset_limit](./bitmap_subset_limit.md) 功能相似，不同之处在于 bitmap_subset_limit 指定的是起始值，sub_bitmap 指定的是 offset。

## 语法

```Haskell
BITMAP sub_bitmap(BITMAP src, BIGINT offset, BIGINT len)
```

## 参数说明

- `src`: 要截断的目标 bitmap。
- `offset`: 用于指定起始位置，支持的数据类型为 BIGINT。Offset 的使用注意事项：

  - Offset 从 0 开始。
  - 负偏移量指的是从字符串结尾开始从右向左计数，见示例三和四。
  - `offset` 指定的起始位置不能超出 BITMAP 值的实际长度，否则返回 NULL，见示例六。

- `len`: 要截取的元素个数，必须为正整数，否则返回 NULL。如果符合条件的元素个数小于 `len` 取值，则返回满足条件的所有元素，见示例二、三、七。

## 返回值说明

返回值的数据类型为 BITMAP。如果输入参数非法，则返回 NULL。

## 示例

在以下示例中，sub_bitmap() 函数的输入值为 [bitmap_from_string](./bitmap_from_string.md) 函数计算后的结果。比如示例中的 `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` 实际输出的 BITMAP 值为 1, 3, 5, 7, 9。sub_bitmap() 会基于这个值进行计算。

示例一：从偏移量 0 开始，截取 BITMAP 值中 2 个元素。

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 2)) value;
+-------+
| value |
+-------+
| 1,3   |
+-------+
```

示例二：从偏移量 0 开始，截取 BITMAP 值中 100 个元素。由于 BITMAP 值中没有 100 个元素，所以输出符合条件的所有元素。

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

示例三：从偏移量 -3 开始，截取 BITMAP 值中 100 个元素。由于 BITMAP 值中没有 100 个元素，所以输出符合条件的所有元素。

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -3, 100)) value;
+-------+
| value |
+-------+
| 5,7,9 |
+-------+
```

示例四：从偏移量 -3 开始，截取 BITMAP 值中 2 个元素。

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -3, 2)) value;
+-------+
| value |
+-------+
| 5,7   |
+-------+
```

示例五：`-10` 为 `len` 的非法输入，返回 NULL。

```Plaintext
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, -10)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

示例六：`offset` 指定的起始位置 5 超出 BITMAP 值 `1,3,5,7,9` 的长度，返回 NULL。

```Plain
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 5, 1)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

示例七：实际符合条件的元素只有 2 个，小于 `len` 的值 5，返回所有满足条件的元素。

```Plain
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), -2, 5)) value;
+-------+
| value |
+-------+
| 7,9   |
+-------+
```
