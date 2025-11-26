---
displayed_sidebar: docs
---

# index_size

返回列索引的字节大小，用于存储分析和优化。此函数需要与 `[_META_]` 提示一起使用来检查段文件元数据。

## 语法

```SQL
-- 不要省略提示中的方括号 []。
SELECT index_size(column_name [, index_type]) FROM table_name [_META_];
```

## 参数

- `column_name`：要获取索引大小的列名。
- `index_type`（可选）：要查询的索引类型。有效值包括：
  - `'BITMAP'`：位图索引大小
  - `'BLOOM'`：布隆过滤器索引大小
  - `'ZONEMAP'`：区域映射索引大小
  - `'ALL'`（默认）：所有索引的总大小

## 返回值

返回索引大小（以字节为单位）作为 BIGINT 值。如果列不存在指定的索引，则返回 0。

## 使用说明

- 此函数必须与 `[_META_]` 提示一起使用才能访问元数据信息。
- 该函数使用 META_SCAN 操作符扫描底层段文件的元数据。
- 对于复杂数据类型（JSON、ARRAY、MAP、STRUCT），该函数递归计算所有子列的索引大小。
- 如果未指定 `index_type`，默认使用 `'ALL'`，返回所有索引的总大小。
- 对于没有指定索引类型的列，函数返回 0。

## 示例

```sql
-- 获取列所有索引的总大小
SELECT index_size(city) FROM locations [_META_];

-- 获取特定索引类型的大小
SELECT 
    index_size(city, 'BITMAP') as bitmap_index_size,
    index_size(city, 'BLOOM') as bloom_filter_size,
    index_size(city, 'ZONEMAP') as zonemap_size,
    index_size(city, 'ALL') as total_index_size
FROM locations [_META_];

-- 比较多个列的索引大小
SELECT 
    index_size(name) as name_index_size,
    index_size(description) as desc_index_size,
    index_size(category, 'BITMAP') as category_bitmap_size
FROM products [_META_];
```
