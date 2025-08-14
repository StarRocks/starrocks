---
displayed_sidebar: docs
---

# format_bytes

将字节数转换为带有适当单位（B、KB、MB、GB、TB、PB、EB）的易读字符串。

此函数用于以用户友好的格式显示文件大小、表大小、内存使用情况和其他与存储相关的指标。它使用基于1024的计算（二进制前缀），但为了简单和习惯，显示单位为KB、MB、GB。

此函数从v3.4开始支持。

## 语法

```Haskell
VARCHAR format_bytes(BIGINT bytes)
```

## 参数

- `bytes`：要格式化的字节数。支持BIGINT。必须是非负整数值。

## 返回值

返回一个VARCHAR值，表示带有适当单位的格式化字节大小。

- 对于小于1024的值：返回带有“B”（字节）的确切数字
- 对于较大的值：返回带有2位小数和适当单位（KB、MB、GB、TB、PB、EB）的格式化字符串
- 如果输入为负或NULL，则返回NULL

## 使用说明

- 该函数内部使用基于1024的（二进制）计算（1 KB = 1024字节，1 MB = 1024²字节，等等）
- 单位显示为KB、MB、GB、TB、PB、EB以迎合用户习惯，尽管它们代表二进制前缀（KiB、MiB、GiB等）
- 大于或等于1 KB的值以精确到2位小数显示
- 字节值（小于1024）以整数显示，不带小数位
- 负值返回NULL
- 支持的最大值为8 EB（艾字节），覆盖BIGINT的完整范围

## 示例

示例1：格式化各种字节大小。

```sql
SELECT format_bytes(0);
-- 0 B

SELECT format_bytes(123);
-- 123 B

SELECT format_bytes(1024);
-- 1.00 KB

SELECT format_bytes(4096);
-- 4.00 KB

SELECT format_bytes(123456789);
-- 117.74 MB

SELECT format_bytes(10737418240);
-- 10.00 GB
```

示例2：处理边缘情况和空值。

```sql
SELECT format_bytes(-1);
-- NULL

SELECT format_bytes(NULL);
-- NULL

SELECT format_bytes(0);
-- 0 B
```

示例3：用于表大小监控的实际应用。

```sql
-- 创建一个包含大小信息的示例表
CREATE TABLE storage_info (
    table_name VARCHAR(64),
    size_bytes BIGINT
);

INSERT INTO storage_info VALUES
('user_profiles', 1073741824),
('transaction_logs', 5368709120),
('product_catalog', 524288000),
('analytics_data', 2199023255552);

-- 以易读格式显示表大小
SELECT 
    table_name,
    size_bytes,
    format_bytes(size_bytes) as formatted_size
FROM storage_info
ORDER BY size_bytes DESC;

-- 预期输出:
-- +------------------+---------------+----------------+
-- | table_name       | size_bytes    | formatted_size |
-- +------------------+---------------+----------------+
-- | analytics_data   | 2199023255552 | 2.00 TB        |
-- | transaction_logs | 5368709120    | 5.00 GB        |
-- | user_profiles    | 1073741824    | 1.00 GB        |
-- | product_catalog  | 524288000     | 500.00 MB      |
-- +------------------+---------------+----------------+
```

示例4：精度和舍入行为。

```sql
SELECT format_bytes(1536);       -- 1.50 KB (精确到1.5 KB)
SELECT format_bytes(1025);       -- 1.00 KB (舍入到2位小数)
SELECT format_bytes(1048576 + 52429);  -- 1.05 MB (舍入到2位小数)
```

示例5：支持单位的完整范围。

```sql
SELECT format_bytes(512);                    -- 512 B
SELECT format_bytes(2048);                   -- 2.00 KB  
SELECT format_bytes(1572864);                -- 1.50 MB
SELECT format_bytes(2147483648);             -- 2.00 GB
SELECT format_bytes(1099511627776);          -- 1.00 TB
SELECT format_bytes(1125899906842624);       -- 1.00 PB
SELECT format_bytes(1152921504606846976);    -- 1.00 EB
```

## Keyword

FORMAT_BYTES
