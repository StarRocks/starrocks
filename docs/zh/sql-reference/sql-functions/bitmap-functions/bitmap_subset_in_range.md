---
displayed_sidebar: docs
---

# bitmap_subset_in_range

## 功能

从 Bitmap 中返回取值在指定范围内的元素。返回的元素是 Bitmap 的子集。

该函数从 3.1 版本开始支持，主要用于分页查询场景。

## 语法

```Haskell
BITMAP bitmap_subset_in_range(BITMAP src, BIGINT start_range, BIGINT end_range)
```

## 参数说明

- `src`: 要截取的目标 bitmap。
- `start_range`: 用于指定范围的起始值，必须是 BIGINT 类型。如果指定的起始值超过了 Bitmap 的最大长度，则返回 NULL，参见示例四。如果 `start_range` 存在于 Bitmap 中，返回值会包括 `start_range`。
- `end_range`: 用于指定范围的结束值，必须是 BIGINT 类型. 如果 `end_range` 小于或等于 `start range`，返回 NULL，参见示例三。注意返回值不包括 `end_range`。

## 返回值说明

返回输入 BITMAP 的子集。如果任何一个输入参数无效，则返回 NULL。

## 使用说明

返回的子集包括 `start_range` 但是不包括 `end_range`，参见示例五。

## 示例

在以下示例中，bitmap_subset_in_range() 函数的输入值为 [bitmap_from_string](./bitmap_from_string.md) 函数计算后的结果。比如示例中的 `bitmap_from_string('1,1,3,1,5,3,5,7,7,9')` 实际输出的 BITMAP 值为 `1, 3, 5, 7, 9`。bitmap_subset_in_range() 会基于这个值进行计算。

示例一：从 BITMAP 中返回取值在 [1,4) 之间的元素。

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 1, 4)) value;
+-------+
| value |
+-------+
| 1,3   |
+-------+
```

示例二：从 Bitmap 中返回取值在 [1,100) 之间的元素。100 超过了 Bitmap 的最大长度，所以返回所有匹配的元素。

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 0, 100)) value;
+-----------+
| value     |
+-----------+
| 1,3,5,7,9 |
+-----------+
```

示例三：结束值 3 小于起始值 4，返回 NULL。

```Plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 4, 3)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

示例四：起始值 10 超过了 Bitmap 的最大长度，返回 NULL。

```Plain
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,3,5,7,7,9'), 10, 15)) value;
+-------+
| value |
+-------+
| NULL  |
+-------+
```

示例五：从 Bitmap 中返回取值在 [1,3) 之间的元素。返回的子集包括起始值 1，不包括结束值 3。

```plaintext
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,1,3,1,5,4,5,6,7,9'), 1, 3)) value;
+-------+
| value |
+-------+
| 1     |
+-------+
```

## 相关文档

[bitmap_subset_limit](./bitmap_subset_limit.md)
