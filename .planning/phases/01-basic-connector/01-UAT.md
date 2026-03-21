---
status: diagnosed
phase: 01-basic-connector
source: [01-SUMMARY.md]
started: 2026-03-18
updated: 2026-03-21
---

## Current Test

[Verification in progress - issues found]

### Issues Found

1. **大量 SQL 无法执行** - **根因已定位**
   - 症状：查询返回 0 行数据
   - 根因：`EsTablePartitions` 对象未正确序列化传递给优化器
   - 原因：`EsTable.esTablePartitions` 字段缺少 `@SerializedName` 注解
   - 状态：已创建修复补丁，等待远程编译验证

2. **外部机器编译**
   - 当前需要在外部环境进行编译验证
   - 本地开发环境编译配置待完善

## Root Cause Analysis

在 `LogicalEsScanOperator` 构造函数中：
```java
this.esTablePartitions = ((EsTable) table).getEsTablePartitions();
```

但 `EsTable.esTablePartitions` 字段**没有 `@SerializedName` 注解**，因此它**不会被 Gson 序列化**。当查询执行时，`esTablePartitions` 是 `null`，导致无法获取分片信息，查询返回 0 行。

## Fix Applied

### 修复文件清单
1. `fe/fe-core/src/main/java/com/starrocks/catalog/EsTable.java`
2. `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsTablePartitions.java`
3. `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardPartitions.java`
4. `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/EsShardRouting.java`

### 补丁文件
- `fix_serialization.patch` - 自动补丁
- `FIX_INSTRUCTIONS.md` - 补丁应用说明
- `fix_manual.md` - 手动修改指南

## Tests

### 1. Code Compilation
expected: OpenSearch connector code compiles without errors
result: pass

### 2. Connector Registration
expected: OPENSEARCH type is recognized by StarRocks
result: pass

### 3. Catalog Creation
expected: Can create OpenSearch catalog with HTTP host
result: pass
sql: |
  CREATE EXTERNAL CATALOG opensearch_test
  PROPERTIES (
    "type" = "opensearch",
    "hosts" = "http://host.docker.internal:9200"
  );

### 4. Schema Discovery
expected: Can list OpenSearch indices as tables
result: pass
sql: |
  USE opensearch_test.default_db;
  SHOW TABLES;
  -- Result: products, test_products
  
  DESCRIBE products;
  -- Result: _id, _index, name, price, category, in_stock columns

### 5. Basic Query
expected: Can execute SELECT queries on OpenSearch indices
result: pending (after fix) - compilation successful, ready for verification
sql: |
  SELECT _id, name, price FROM products;
  -- Expected: 3 rows of data

## Summary

total: 5
passed: 3
issues: 2 (1 root cause identified, 1 pending fix verification)
pending: 0
skipped: 0
blocked: 0

## Test Environment

| Component | Version |
|-----------|---------|
| StarRocks | 4.0-latest (Java 17) |
| OpenSearch | 2.18.0 |
| OpenSearch Connector | Phase 1 |

## Key Fixes Required

1. **Serialization Fix**: Add `@SerializedName` annotations to enable Gson serialization
   - EsTable.esTablePartitions
   - EsTablePartitions fields
   - EsShardPartitions fields
   - EsShardRouting fields

## Verification Commands (After Fix)

```sql
-- Create catalog
CREATE EXTERNAL CATALOG opensearch_test
PROPERTIES (
  "type" = "opensearch",
  "hosts" = "http://localhost:9200"
);

-- List tables
USE opensearch_test.default_db;
SHOW TABLES;

-- Describe table
DESCRIBE products;

-- Query data - should return 3 rows
SELECT * FROM products;

-- Expected output:
-- +----+-------------+----------+-------------+----------+
-- | _id| name        | price    | category    | in_stock |
-- +----+-------------+----------+-------------+----------+
-- | 1  | iPhone 15   | 999.99   | Electronics | 1        |
-- | 2  | MacBook Pro | 2499.99  | Electronics | 0        |
-- | 3  | AirPods Pro | 249.99   | Electronics | 1        |
-- +----+-------------+----------+-------------+----------+
```

## Status

🚀 Phase 1 HTTP Connector - DEPLOYING

- Code complete ✅
- Issue identified ✅
- Fix implemented ✅
- Remote compilation ✅
- Deploying to container ⏳
- Verification testing ⏳
