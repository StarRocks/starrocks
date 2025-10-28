---
displayed_sidebar: docs
---

# encode_sort_key

从多个异构输入列创建保持顺序的复合二进制键。此函数对于创建在按字典序比较时保持原始排序顺序的高效排序键至关重要。

## 语法

```SQL
encode_sort_key(column1, column2, ..., columnN)
```

## 参数

- `column1, column2, ..., columnN`: 一个或多个任何支持的数据类型的列。该函数接受可变数量的参数。

## 返回值

返回表示复合排序键的 VARBINARY 类型的值。

## 支持的数据类型

支持以下数据类型：

| 数据类型 | 描述 |
|----------|------|
| `TINYINT` | 8位有符号整数 |
| `SMALLINT` | 16位有符号整数 |
| `INT` | 32位有符号整数 |
| `BIGINT` | 64位有符号整数 |
| `LARGEINT` | 128位有符号整数 |
| `FLOAT` | 32位浮点数 |
| `DOUBLE` | 64位浮点数 |
| `VARCHAR` | 可变长度字符串 |
| `CHAR` | 固定长度字符串 |
| `DATE` | 日期值 |
| `DATETIME` | 日期时间值 |
| `TIMESTAMP` | 时间戳值 |

不支持以下复杂类型，将返回错误：

- `JSON`
- `ARRAY`
- `MAP`
- `STRUCT`
- `HLL`
- `BITMAP`
- `PERCENTILE`

## 编码策略

该函数对不同数据类型使用不同的编码策略，以确保结果二进制键的字典序比较保持原始排序顺序：

- **整数类型**: 大端字节序，有符号类型的符号位翻转
- **浮点类型**: 自定义编码以确保正确的排序顺序
- **字符串类型**: 特殊编码，非最后字段使用0x00字节转义和0x00 0x00终止符
- **日期/时间类型**: 内部整数表示编码为整数值

### NULL 处理

- 每列每行都获得一个 NULL 标记（NULL 为 0x00，非 NULL 为 0x01）
- 即使是非空列也添加 NULL 标记以确保编码一致性
- 使用分隔字节（0x00）分隔列（最后一列后除外）

## 示例

### 生成排序键列

```sql
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score, created_date)
    )
) ORDER BY (sort_key);
```

### JSON 字段提取

```sql
CREATE TABLE json_data (
    id INT,
    json_content JSON,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(
            get_json_int(json_content, '$.priority'),
            get_json_string(json_content, '$.category'),
            get_json_double(json_content, '$.score')
        )
    )
) ORDER BY (sort_key);
```

## 限制

### 数据类型限制

复杂类型如 JSON、ARRAY、MAP 和 STRUCT 无法直接编码。使用 JSON 提取函数获取原始值：

```sql
-- 替代: encode_sort_key(json_col)
-- 使用: encode_sort_key(get_json_int(json_col, '$.field1'), get_json_string(json_col, '$.field2'))
```

### 性能考虑

- 每次调用 `encode_sort_key` 都需要编码所有输入列
- 二进制键可能比原始数据大得多
- 使用生成列避免重复编码

### 键大小限制

- 保持列数合理（通常< 10）
- 尽可能使用较短的字符串列
- 对于很长的字符串考虑使用哈希函数