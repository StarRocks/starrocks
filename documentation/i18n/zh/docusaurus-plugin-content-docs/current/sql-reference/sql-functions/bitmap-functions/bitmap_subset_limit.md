# bitmap_subset_limit

## 功能

根据指定的起始值，从 BITMAP 中截取指定个数的元素。返回的元素是 Bitmap 的子集。

该函数从 3.1 版本开始支持，主要用于分页查询场景。

该函数与 [sub_bitmap](./sub_bitmap.md) 功能相似，不同之处在于 sub_bitmap 指定的是 offset，bitmap_subset_limit 指定的是起始值。

## 语法

```Haskell
BITMAP bitmap_subset_limit(BITMAP src, BIGINT start_range, BIGINT limit)
```

## 参数说明

- `src`: 要截取的目标 bitmap。
- `start_range`: 用于指定起始值，必须是 BIGINT 类型。如果指定的起始值超过了 Bitmap 的最大长度并且 `limit` 是正数，则返回 NULL。参见示例四。
- `limit`: 从 `start_range` 开始，要截取的元素个数，必须是 BIGINT 类型。如果取值为负，表示从 `start_range` 开始由右向左计数。如果符合条件的元素个数小于 `len` 取值，则返回所有满足条件的元素。

## 返回值说明

返回输入 BITMAP 的子集。如果任何一个输入参数无效，则返回 NULL。

## 使用说明

- 返回的子集包含 `start_range`。
- 如果 `limit` 为负，表示从 `start_range` 开始由右向左计数，返回 `limit` 个元素。参见示例三。

## 示例

在以下示例中，bitmap_subset_in_range() 函数的输入值为 [bitmap_from_string](./bitmap_from_string.md) 函数计算后的结果。比如示例中的 `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` 实际输出的 BITMAP 值为 `1, 3, 5, 7, 9`。bitmap_subset_in_range() 会基于这个值进行计算。

示例一：从起始值 1 开始，从 Bitmap 中截取 4 个元素。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+---------+
|  value  |
+---------+
| 1,3,5,7 |
+---------+
```

示例二：从起始值 1 开始，从 Bitmap 中截取 100 个元素。100 超过了 Bitmap 的最大长度，返回所有匹配的元素。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

示例三：从起始值 5 开始，由右向左计数，从 Bitmap 中截取 2 个元素。

```Plaintext
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 5, -2)) value;
+-----------+
| value     |
+-----------+
| 3,5       |
+-----------+
```

示例四：指定的起始值 10 超过了 Bitmap 的最大长度而且 `limit` 是正数，返回 NULL。

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

## 相关参考

[sub_bitmap](./sub_bitmap.md), [bitmap_subset_in_range](./bitmap_subset_in_range.md)
