# encode_sort_key Function Documentation

## Overview / 概述 / 概要

The `encode_sort_key` function creates an order-preserving composite binary key from multiple heterogeneous input columns. This function is essential for creating efficient sort keys that maintain the original sort order when compared lexicographically.

`encode_sort_key` 函数从多个异构输入列创建保持顺序的复合二进制键。此函数对于创建在按字典序比较时保持原始排序顺序的高效排序键至关重要。

`encode_sort_key` 関数は、複数の異種入力列から順序を保持する複合バイナリキーを作成します。この関数は、辞書順で比較した際に元のソート順序を維持する効率的なソートキーを作成するために不可欠です。

## Function Signature / 函数签名 / 関数シグネチャ

```sql
encode_sort_key(column1, column2, ..., columnN) -> VARBINARY
```

**Parameters / 参数 / パラメータ:**
- `column1, column2, ..., columnN`: One or more columns of any supported data type
- `column1, column2, ..., columnN`: 一个或多个任何支持的数据类型的列
- `column1, column2, ..., columnN`: サポートされている任意のデータ型の1つ以上の列

**Return Type / 返回类型 / 戻り値の型:**
- `VARBINARY`: A binary string representing the composite sort key
- `VARBINARY`: 表示复合排序键的二进制字符串
- `VARBINARY`: 複合ソートキーを表すバイナリ文字列

## Supported Data Types / 支持的数据类型 / サポートされるデータ型

### Supported Types / 支持的类型 / サポートされる型

| Data Type | English | Chinese | Japanese |
|-----------|---------|---------|----------|
| `TINYINT` | 8-bit signed integer | 8位有符号整数 | 8ビット符号付き整数 |
| `SMALLINT` | 16-bit signed integer | 16位有符号整数 | 16ビット符号付き整数 |
| `INT` | 32-bit signed integer | 32位有符号整数 | 32ビット符号付き整数 |
| `BIGINT` | 64-bit signed integer | 64位有符号整数 | 64ビット符号付き整数 |
| `LARGEINT` | 128-bit signed integer | 128位有符号整数 | 128ビット符号付き整数 |
| `FLOAT` | 32-bit floating point | 32位浮点数 | 32ビット浮動小数点数 |
| `DOUBLE` | 64-bit floating point | 64位浮点数 | 64ビット浮動小数点数 |
| `VARCHAR` | Variable-length string | 可变长度字符串 | 可変長文字列 |
| `CHAR` | Fixed-length string | 固定长度字符串 | 固定長文字列 |
| `DATE` | Date value | 日期值 | 日付値 |
| `DATETIME` | Date and time value | 日期时间值 | 日時値 |
| `TIMESTAMP` | Timestamp value | 时间戳值 | タイムスタンプ値 |

### Unsupported Types / 不支持的类型 / サポートされない型

| Data Type | English | Chinese | Japanese |
|-----------|---------|---------|----------|
| `JSON` | JSON object | JSON对象 | JSONオブジェクト |
| `ARRAY` | Array type | 数组类型 | 配列型 |
| `MAP` | Map type | 映射类型 | マップ型 |
| `STRUCT` | Structure type | 结构类型 | 構造体型 |
| `HLL` | HyperLogLog | HyperLogLog | HyperLogLog |
| `BITMAP` | Bitmap | 位图 | ビットマップ |
| `PERCENTILE` | Percentile | 百分位数 | パーセンタイル |

## Encoding Strategy / 编码策略 / エンコーディング戦略

### Order-Preserving Encoding / 保序编码 / 順序保持エンコーディング

The function uses different encoding strategies for different data types to ensure that lexicographic comparison of the resulting binary keys preserves the original sort order:

该函数对不同数据类型使用不同的编码策略，以确保结果二进制键的字典序比较保持原始排序顺序：

この関数は、結果のバイナリキーの辞書順比較が元のソート順序を保持するように、異なるデータ型に対して異なるエンコーディング戦略を使用します：

#### Integer Types / 整数类型 / 整数型
- **Encoding**: Big-endian byte order with sign bit flipped for signed types
- **编码**: 大端字节序，有符号类型的符号位翻转
- **エンコーディング**: ビッグエンディアンバイト順序、符号付き型の符号ビットを反転

#### Floating-Point Types / 浮点类型 / 浮動小数点型
- **Encoding**: Custom encoding to ensure correct sort order
- **编码**: 自定义编码以确保正确的排序顺序
- **エンコーディング**: 正しいソート順序を保証するカスタムエンコーディング

#### String Types / 字符串类型 / 文字列型
- **Encoding**: Special encoding with 0x00 byte escaping and 0x00 0x00 terminator for non-last fields
- **编码**: 特殊编码，非最后字段使用0x00字节转义和0x00 0x00终止符
- **エンコーディング**: 非最後フィールドで0x00バイトエスケープと0x00 0x00終端子を使用する特殊エンコーディング

#### Date/Time Types / 日期时间类型 / 日時型
- **Encoding**: Internal integer representation encoded as integral value
- **编码**: 内部整数表示编码为整数值
- **エンコーディング**: 内部整数表現を整数値としてエンコード

### NULL Handling / NULL处理 / NULL処理

- **NULL Marker**: Each column gets a NULL marker (0x00 for NULL, 0x01 for NOT NULL) for every row
- **NULL标记**: 每列每行都获得一个NULL标记（NULL为0x00，非NULL为0x01）
- **NULLマーカー**: 各列の各行にNULLマーカー（NULLは0x00、非NULLは0x01）が付与される

- **Consistency**: NULL markers are added even for non-nullable columns to ensure consistent encoding
- **一致性**: 即使是非空列也添加NULL标记以确保编码一致性
- **一貫性**: 非NULL列でも一貫したエンコーディングを保証するためにNULLマーカーが追加される

### Column Separation / 列分隔 / 列分離

- **Separator**: 0x00 byte is used to separate columns (except after the last column)
- **分隔符**: 使用0x00字节分隔列（最后一列后除外）
- **区切り文字**: 列を区切るために0x00バイトが使用される（最後の列の後を除く）

## Use Cases / 使用场景 / 使用例

### 1. Generated Sort Key Columns / 生成排序键列 / 生成ソートキー列

Create efficient sort keys for tables with complex sorting requirements:

为具有复杂排序要求的表创建高效的排序键：

複雑なソート要件を持つテーブル用の効率的なソートキーを作成：

```sql
-- English
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score DESC, created_date)
    )
) ORDER BY (sort_key);

-- Chinese
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score DESC, created_date)
    )
) ORDER BY (sort_key);

-- Japanese
CREATE TABLE user_analytics (
    user_id INT,
    region VARCHAR(50),
    score DOUBLE,
    created_date DATE,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(region, score DESC, created_date)
    )
) ORDER BY (sort_key);
```

### 2. JSON Field Extraction and Sorting / JSON字段提取和排序 / JSONフィールド抽出とソート

Extract and combine JSON fields for efficient sorting:

提取并组合JSON字段以进行高效排序：

効率的なソートのためにJSONフィールドを抽出して結合：

```sql
-- English
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

-- Chinese
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

-- Japanese
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

### 3. Multi-Column Sorting Optimization / 多列排序优化 / 複数列ソート最適化

Optimize queries that sort by multiple columns:

优化按多列排序的查询：

複数列でソートするクエリを最適化：

```sql
-- English
-- Instead of: ORDER BY region, score DESC, created_date
-- Use: ORDER BY sort_key
SELECT * FROM user_analytics 
WHERE sort_key BETWEEN encode_sort_key('US', 100.0, '2023-01-01') 
                  AND encode_sort_key('US', 0.0, '2023-12-31')
ORDER BY sort_key;

-- Chinese
-- 替代: ORDER BY region, score DESC, created_date
-- 使用: ORDER BY sort_key
SELECT * FROM user_analytics 
WHERE sort_key BETWEEN encode_sort_key('US', 100.0, '2023-01-01') 
                  AND encode_sort_key('US', 0.0, '2023-12-31')
ORDER BY sort_key;

-- Japanese
-- 代わりに: ORDER BY region, score DESC, created_date
-- 使用: ORDER BY sort_key
SELECT * FROM user_analytics 
WHERE sort_key BETWEEN encode_sort_key('US', 100.0, '2023-01-01') 
                  AND encode_sort_key('US', 0.0, '2023-12-31')
ORDER BY sort_key;
```

### 4. Range Queries / 范围查询 / 範囲クエリ

Enable efficient range queries on composite keys:

在复合键上实现高效的范围查询：

複合キーでの効率的な範囲クエリを可能にする：

```sql
-- English
-- Find all records with region='US' and score between 80 and 90
SELECT * FROM user_analytics
WHERE sort_key >= encode_sort_key('US', 90.0, '1900-01-01')
  AND sort_key < encode_sort_key('US', 80.0, '1900-01-01');

-- Chinese
-- 查找region='US'且score在80到90之间的所有记录
SELECT * FROM user_analytics
WHERE sort_key >= encode_sort_key('US', 90.0, '1900-01-01')
  AND sort_key < encode_sort_key('US', 80.0, '1900-01-01');

-- Japanese
-- region='US'でscoreが80から90の間のすべてのレコードを検索
SELECT * FROM user_analytics
WHERE sort_key >= encode_sort_key('US', 90.0, '1900-01-01')
  AND sort_key < encode_sort_key('US', 80.0, '1900-01-01');
```

## Limitations / 限制 / 制限

### 1. Data Type Restrictions / 数据类型限制 / データ型制限

**Unsupported Complex Types / 不支持复杂类型 / サポートされない複雑型:**

- **JSON, ARRAY, MAP, STRUCT**: These complex types cannot be directly encoded
- **JSON, ARRAY, MAP, STRUCT**: 这些复杂类型无法直接编码
- **JSON, ARRAY, MAP, STRUCT**: これらの複雑型は直接エンコードできません

**Workaround / 解决方案 / 回避策:**
```sql
-- English: Extract primitive values from complex types
encode_sort_key(
    get_json_int(json_col, '$.field1'),
    get_json_string(json_col, '$.field2')
)

-- Chinese: 从复杂类型中提取原始值
encode_sort_key(
    get_json_int(json_col, '$.field1'),
    get_json_string(json_col, '$.field2')
)

-- Japanese: 複雑型からプリミティブ値を抽出
encode_sort_key(
    get_json_int(json_col, '$.field1'),
    get_json_string(json_col, '$.field2')
)
```

### 2. Performance Considerations / 性能考虑 / パフォーマンス考慮事項

**Encoding Overhead / 编码开销 / エンコーディングオーバーヘッド:**
- Each call to `encode_sort_key` requires encoding all input columns
- 每次调用`encode_sort_key`都需要编码所有输入列
- `encode_sort_key`の各呼び出しで、すべての入力列のエンコーディングが必要

**Memory Usage / 内存使用 / メモリ使用量:**
- Binary keys can be significantly larger than original data
- 二进制键可能比原始数据大得多
- バイナリキーは元のデータよりも大幅に大きくなる可能性がある

**Recommendation / 建议 / 推奨事項:**
```sql
-- English: Use generated columns to avoid repeated encoding
CREATE TABLE optimized_table (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);

-- Chinese: 使用生成列避免重复编码
CREATE TABLE optimized_table (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);

-- Japanese: 生成列を使用してエンコーディングの繰り返しを避ける
CREATE TABLE optimized_table (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);
```

### 3. Key Size Limitations / 键大小限制 / キーサイズ制限

**Maximum Key Size / 最大键大小 / 最大キーサイズ:**
- Binary keys have practical size limits for efficient comparison
- 二进制键在高效比较方面有实际大小限制
- バイナリキーには効率的な比較のための実用的なサイズ制限がある

**Best Practices / 最佳实践 / ベストプラクティス:**
- Keep the number of columns reasonable (typically < 10)
- 保持列数合理（通常< 10）
- 列数を合理的に保つ（通常< 10）

- Prefer shorter string columns when possible
- 尽可能使用较短的字符串列
- 可能な限り短い文字列列を優先する

- Consider using hash functions for very long strings
- 对于很长的字符串考虑使用哈希函数
- 非常に長い文字列にはハッシュ関数の使用を検討する

### 4. NULL Handling Complexity / NULL处理复杂性 / NULL処理の複雑さ

**Consistent NULL Markers / 一致的NULL标记 / 一貫したNULLマーカー:**
- NULL markers are added to all columns for consistency
- 为了一致性，所有列都添加NULL标记
- 一貫性のためにすべての列にNULLマーカーが追加される

- This increases the size of the binary key
- 这增加了二进制键的大小
- これによりバイナリキーのサイズが増加する

**Impact / 影响 / 影響:**
- Binary keys are larger than necessary for non-nullable columns
- 对于非空列，二进制键比必要的大
- 非NULL列ではバイナリキーが不要に大きくなる

### 5. Version Compatibility / 版本兼容性 / バージョン互換性

**Encoding Format / 编码格式 / エンコーディング形式:**
- The internal encoding format may change between StarRocks versions
- StarRocks版本间的内部编码格式可能发生变化
- StarRocksバージョン間で内部エンコーディング形式が変更される可能性がある

**Recommendation / 建议 / 推奨事項:**
- Avoid storing encoded keys for long-term persistence
- 避免为长期持久化存储编码键
- 長期永続化のためにエンコードされたキーを保存しない

- Regenerate keys when upgrading StarRocks versions
- 升级StarRocks版本时重新生成键
- StarRocksバージョンをアップグレードする際にキーを再生成する

## Examples / 示例 / 例

### Basic Usage / 基本用法 / 基本的な使用法

```sql
-- English
SELECT 
    id,
    name,
    score,
    encode_sort_key(name, score) as sort_key
FROM students
ORDER BY sort_key;

-- Chinese
SELECT 
    id,
    name,
    score,
    encode_sort_key(name, score) as sort_key
FROM students
ORDER BY sort_key;

-- Japanese
SELECT 
    id,
    name,
    score,
    encode_sort_key(name, score) as sort_key
FROM students
ORDER BY sort_key;
```

### Advanced Usage with Generated Columns / 生成列的高级用法 / 生成列での高度な使用法

```sql
-- English
CREATE TABLE product_catalog (
    product_id INT,
    category VARCHAR(50),
    price DECIMAL(10,2),
    rating DOUBLE,
    created_at TIMESTAMP,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(category, price, rating DESC, created_at)
    )
) ORDER BY (sort_key);

-- Chinese
CREATE TABLE product_catalog (
    product_id INT,
    category VARCHAR(50),
    price DECIMAL(10,2),
    rating DOUBLE,
    created_at TIMESTAMP,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(category, price, rating DESC, created_at)
    )
) ORDER BY (sort_key);

-- Japanese
CREATE TABLE product_catalog (
    product_id INT,
    category VARCHAR(50),
    price DECIMAL(10,2),
    rating DOUBLE,
    created_at TIMESTAMP,
    sort_key VARBINARY(1024) AS (
        encode_sort_key(category, price, rating DESC, created_at)
    )
) ORDER BY (sort_key);
```

## Performance Tips / 性能提示 / パフォーマンスのヒント

### 1. Use Generated Columns / 使用生成列 / 生成列を使用

```sql
-- English: Pre-compute sort keys to avoid repeated encoding
CREATE TABLE optimized (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);

-- Chinese: 预计算排序键以避免重复编码
CREATE TABLE optimized (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);

-- Japanese: エンコーディングの繰り返しを避けるためにソートキーを事前計算
CREATE TABLE optimized (
    col1 INT,
    col2 VARCHAR(50),
    sort_key VARBINARY(1024) AS (encode_sort_key(col1, col2))
);
```

### 2. Optimize Column Order / 优化列顺序 / 列順序の最適化

```sql
-- English: Put most selective columns first
encode_sort_key(high_cardinality_col, medium_cardinality_col, low_cardinality_col)

-- Chinese: 将选择性最高的列放在前面
encode_sort_key(high_cardinality_col, medium_cardinality_col, low_cardinality_col)

-- Japanese: 最も選択性の高い列を最初に配置
encode_sort_key(high_cardinality_col, medium_cardinality_col, low_cardinality_col)
```

### 3. Consider Key Size / 考虑键大小 / キーサイズを考慮

```sql
-- English: Use shorter representations when possible
encode_sort_key(LEFT(long_string, 50), numeric_id)

-- Chinese: 尽可能使用较短的表示
encode_sort_key(LEFT(long_string, 50), numeric_id)

-- Japanese: 可能な限り短い表現を使用
encode_sort_key(LEFT(long_string, 50), numeric_id)
```

## Related Functions / 相关函数 / 関連関数

- `decode_sort_key()`: Decode a binary sort key back to original values (if available)
- `decode_sort_key()`: 将二进制排序键解码回原始值（如果可用）
- `decode_sort_key()`: バイナリソートキーを元の値にデコード（利用可能な場合）

- `hash()`: Create hash-based keys for equality comparisons
- `hash()`: 为相等比较创建基于哈希的键
- `hash()`: 等価比較用のハッシュベースのキーを作成

- `concat()`: String concatenation for simple composite keys
- `concat()`: 简单复合键的字符串连接
- `concat()`: シンプルな複合キー用の文字列連結

## See Also / 另请参阅 / 関連項目

- [StarRocks Generated Columns Documentation](https://docs.starrocks.io/)
- [StarRocks 生成列文档](https://docs.starrocks.io/)
- [StarRocks 生成列ドキュメント](https://docs.starrocks.io/)

- [StarRocks ORDER BY Clause Documentation](https://docs.starrocks.io/)
- [StarRocks ORDER BY 子句文档](https://docs.starrocks.io/)
- [StarRocks ORDER BY 句ドキュメント](https://docs.starrocks.io/)