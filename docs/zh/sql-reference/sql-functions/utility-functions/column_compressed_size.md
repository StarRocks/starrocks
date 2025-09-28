---
displayed_sidebar: docs
---

# column_compressed_size

返回列的压缩大小（以字节为单位）。此函数与 `[_META_]` 提示一起使用，用于检查段文件的元数据并分析列存储效率。

## 语法

```SQL
column_compressed_size(column_name)
```

## 参数

- `column_name`: 要获取压缩大小的列名。

## 返回值

返回列的压缩大小（以字节为单位）作为 BIGINT 值。

## 使用说明

- 此函数必须与 `[_META_]` 提示一起使用才能访问元数据信息。
- 该函数使用 META_SCAN 操作符扫描底层段文件的元数据。
- 返回所有段中列数据的总压缩大小。
- 对于复杂数据类型（JSON、ARRAY、MAP、STRUCT），该函数递归计算所有子列的压缩大小。
- 压缩大小通过序数索引范围的数据页大小求和来计算。

## 示例

示例 1：获取简单列的压缩大小。

```sql
SELECT column_compressed_size(id) FROM users [_META_];
```

示例 2：获取复杂数据类型的压缩大小。

```sql
-- 获取 JSON 列的压缩大小
SELECT column_compressed_size(json_data) FROM events [_META_];

-- 获取 ARRAY 列的压缩大小
SELECT column_compressed_size(tags) FROM products [_META_];

-- 获取 MAP 列的压缩大小
SELECT column_compressed_size(attributes) FROM items [_META_];

-- 获取 STRUCT 列的压缩大小
SELECT column_compressed_size(address) FROM customers [_META_];
```

示例 3：比较不同列的压缩大小。

```sql
SELECT 
    column_compressed_size(name) as name_compressed_size,
    column_compressed_size(description) as desc_compressed_size,
    column_compressed_size(metadata) as meta_compressed_size
FROM products [_META_];
```

示例 4：计算压缩比。

```sql
SELECT 
    column_name,
    column_size(column_name) as decompressed_size,
    column_compressed_size(column_name) as compressed_size,
    ROUND((1 - column_compressed_size(column_name) / column_size(column_name)) * 100, 2) as compression_ratio_percent
FROM (
    SELECT 'name' as column_name FROM products [_META_]
    UNION ALL
    SELECT 'description' as column_name FROM products [_META_]
    UNION ALL  
    SELECT 'content' as column_name FROM products [_META_]
) t;
```

示例 5：与聚合函数一起使用。

```sql
SELECT 
    SUM(column_compressed_size(id)) as total_id_compressed_size,
    AVG(column_compressed_size(content)) as avg_content_compressed_size
FROM articles [_META_];
```

## 相关函数

- [column_size](./column_size.md): 返回列的未压缩大小。
- [META_SCAN 操作符](../../../using_starrocks/Cost_based_optimizer.md): 有关元数据扫描的更多信息。