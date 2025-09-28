---
displayed_sidebar: docs
---

# column_size & column_compressed_size

这些函数返回表列的大小信息，用于存储分析和优化。两个函数都与 `[_META_]` 提示一起使用来检查段文件元数据。

## column_size

返回列的未压缩大小（以字节为单位）。

### 语法

```SQL
column_size(column_name)
```

### 参数

- `column_name`: 要获取未压缩大小的列名。

### 返回值

返回列的未压缩大小（以字节为单位）作为 BIGINT 值。

## column_compressed_size

返回列的压缩大小（以字节为单位）。

### 语法

```SQL
column_compressed_size(column_name)
```

### 参数

- `column_name`: 要获取压缩大小的列名。

### 返回值

返回列的压缩大小（以字节为单位）作为 BIGINT 值。

## 使用说明

- 两个函数都必须与 `[_META_]` 提示一起使用才能访问元数据信息。
- 这些函数使用 META_SCAN 操作符扫描底层段文件的元数据。
- 对于复杂数据类型（JSON、ARRAY、MAP、STRUCT），这些函数递归计算所有子列的大小。
- `column_size` 返回所有段中列数据的总内存占用。
- `column_compressed_size` 返回通过序数索引范围的数据页大小求和计算的总压缩大小。

## 示例

```sql
-- 获取列的未压缩和压缩大小
SELECT 
    column_size(name) as name_decompressed_size,
    column_compressed_size(name) as name_compressed_size,
    column_size(description) as desc_decompressed_size,
    column_compressed_size(description) as desc_compressed_size
FROM products [_META_];
```

## 相关函数

- [META_SCAN 操作符](../../../using_starrocks/Cost_based_optimizer.md): 有关元数据扫描的更多信息。