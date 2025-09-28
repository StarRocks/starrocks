---
displayed_sidebar: docs
---

# column_size

返回列的未压缩大小（以字节为单位）。此函数与 `[_META_]` 提示一起使用，用于检查段文件的元数据并分析列存储特征。

## 语法

```SQL
column_size(column_name)
```

## 参数

- `column_name`: 要获取未压缩大小的列名。

## 返回值

返回列的未压缩大小（以字节为单位）作为 BIGINT 值。

## 使用说明

- 此函数必须与 `[_META_]` 提示一起使用才能访问元数据信息。
- 该函数使用 META_SCAN 操作符扫描底层段文件的元数据。
- 返回所有段中列数据的总内存占用。
- 对于复杂数据类型（JSON、ARRAY、MAP、STRUCT），该函数递归计算所有子列的大小。

## 示例

示例 1：获取简单列的未压缩大小。

```sql
SELECT column_size(id) FROM users [_META_];
```

示例 2：获取复杂数据类型的未压缩大小。

```sql
-- 获取 JSON 列的大小
SELECT column_size(json_data) FROM events [_META_];

-- 获取 ARRAY 列的大小
SELECT column_size(tags) FROM products [_META_];

-- 获取 MAP 列的大小
SELECT column_size(attributes) FROM items [_META_];

-- 获取 STRUCT 列的大小
SELECT column_size(address) FROM customers [_META_];
```

示例 3：比较不同列的列大小。

```sql
SELECT 
    column_size(name) as name_size,
    column_size(description) as desc_size,
    column_size(metadata) as meta_size
FROM products [_META_];
```

示例 4：与聚合函数一起使用。

```sql
SELECT 
    SUM(column_size(id)) as total_id_size,
    AVG(column_size(content)) as avg_content_size
FROM articles [_META_];
```

## 相关函数

- [column_compressed_size](./column_compressed_size.md): 返回列的压缩大小。
- [META_SCAN 操作符](../../../using_starrocks/Cost_based_optimizer.md): 有关元数据扫描的更多信息。