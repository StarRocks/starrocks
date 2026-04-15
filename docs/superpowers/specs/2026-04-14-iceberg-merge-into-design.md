# StarRocks Iceberg MERGE INTO 实现设计

**日期**: 2026-04-14
**基础**: HEAD commit `99e40d6980a` (Iceberg UPDATE using RowDelta model)
**参考**: `~/work/doc/AITool/design/StarRocks_Iceberg_UPDATE_MERGE_Technical_Design_en.md`、Iceberg Spec、Trino MERGE INTO 实现

---

## 1. 目标

在已有 Iceberg UPDATE (RowDelta 模型) 基础上，实现 MERGE INTO 语法对 Iceberg V2 表的支持。

### 功能范围

- 完整的 `WHEN MATCHED THEN UPDATE / DELETE` 语义
- 完整的 `WHEN NOT MATCHED THEN INSERT` 语义（含 `INSERT *`）
- 多个 WHEN MATCHED / WHEN NOT MATCHED 子句（带可选 AND 条件，首个匹配生效）
- 重复匹配检测（同一 target 行匹配多个 source 行且触发 UPDATE/DELETE 时报错）
- 冲突检测与 SERIALIZABLE 隔离级别支持
- MERGE 专用 Metrics

### 不在范围内

- 分区列更新
- `WHEN NOT MATCHED BY SOURCE`
- Iceberg V1 / V3
- Equality Delete
- Copy-On-Write 模式

---

## 2. 整体方案

复用已有 UPDATE 的 RowDelta 基础设施：

1. **Analyzer** 将 MERGE INTO 重写为 `SELECT ... FROM source LEFT OUTER JOIN target ON ...`，通过 CASE 表达式计算 `op_code` 和数据列
2. **Planner** 生成物理计划，包含 `EnforceUniqueNode`（重复匹配检测）+ 分区 shuffle + `IcebergRowDeltaSink`
3. **BE Sink** 按 `op_code` 路由到 delete/data sub-sink（现有逻辑，无需改动）
4. **FE Commit** 通过 `commitRowDeltaOperation()` 原子提交（现有逻辑，无需改动）

---

## 3. 语法设计（Grammar & AST）

### 3.1 ANTLR 语法

在 `StarRocks.g4` 的 `statement` 规则 DML 区域新增 `mergeIntoStatement`：

```antlr
mergeIntoStatement
    : explainDesc? MERGE INTO qualifiedName (AS? identifier)?
      USING relation (AS? identifier)?
      ON expression
      mergeWhenClause+
    ;

mergeWhenClause
    : WHEN MATCHED (AND expression)? THEN mergeMatchedAction
    | WHEN NOT MATCHED (AND expression)? THEN mergeNotMatchedAction
    ;

mergeMatchedAction
    : UPDATE SET assignmentList
    | DELETE
    ;

mergeNotMatchedAction
    : INSERT STAR
    | INSERT ('(' identifier (',' identifier)* ')')? VALUES '(' expressionList ')'
    ;
```

支持三种 INSERT 形式：
- `INSERT (col1, col2) VALUES (expr1, expr2)` — 显式列 + 值
- `INSERT VALUES (expr1, expr2)` — 全列，值按 schema 顺序
- `INSERT *` — 按列名匹配 source 列（Spark 风格）

### 3.2 AST 节点

**新建文件**（`fe/fe-core/src/main/java/com/starrocks/sql/ast/`）：

- **`MergeIntoStmt`** extends `DmlStmt`
  - `tableRef`: 目标表引用
  - `targetAlias`: 目标表别名
  - `sourceRelation`: source 表/子查询
  - `sourceAlias`: source 别名
  - `mergeCondition`: ON 条件
  - `whenClauses`: List<MergeWhenClause>
  - `table`: 解析后的目标表对象
  - `queryStatement`: Analyzer 重写后的 QueryStatement
  - `icebergColumnOutputNames`: 列名列表（同 UpdateStmt 模式）

- **`MergeWhenClause`**（抽象基类）：`optionalCondition`
  - **`MergeWhenMatchedUpdateClause`**：`List<ColumnAssignment> assignments`
  - **`MergeWhenMatchedDeleteClause`**：无额外字段
  - **`MergeWhenNotMatchedInsertClause`**：`List<String> targetColumnNames`（可选） + `List<Expr> values` + `boolean isStar`

**修改文件**：
- `AstVisitorExtendInterface.java`：新增 `visitMergeIntoStatement()` 默认方法
- `AstBuilder.java`：新增 `visitMergeIntoStatement()` 构建 AST

---

## 4. Analyzer 设计（MergeIntoAnalyzer）

### 4.1 校验规则

1. 目标表必须是 `IcebergTable` 且 `formatVersion == 2`
2. 不允许 CTE（WITH 子句）
3. 不允许更新分区列（WHEN MATCHED UPDATE 子句）
4. 不允许更新隐藏列（`_file`、`_pos`）
5. 至少一个 WHEN 子句
6. INSERT 列数与 VALUES 数匹配（非 `INSERT *` 时）
7. `INSERT *` 时 source 列名必须覆盖 target 全部列

### 4.2 重写策略

将 MERGE INTO 重写为 `SELECT ... FROM source LEFT OUTER JOIN target ON ...`。

**示例**：

```sql
-- 原始
MERGE INTO orders AS t
USING cdc AS s ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET status = s.status
WHEN NOT MATCHED THEN INSERT (order_id, status) VALUES (s.order_id, s.status)

-- 重写为
SELECT
    t._file,
    t._pos,
    -- 数据列（完整 target schema，CASE 表达式）
    CASE
        WHEN t._file IS NOT NULL THEN
            CASE
                WHEN s.op = 'D' THEN t.order_id
                WHEN s.op = 'U' THEN t.order_id
                ELSE t.order_id
            END
        ELSE s.order_id
    END AS order_id,
    CASE
        WHEN t._file IS NOT NULL THEN
            CASE
                WHEN s.op = 'D' THEN t.status
                WHEN s.op = 'U' THEN s.status
                ELSE t.status
            END
        ELSE s.status
    END AS status,
    -- op_code
    CASE
        WHEN t._file IS NOT NULL AND s.op = 'D' THEN 1   -- DELETE
        WHEN t._file IS NOT NULL AND s.op = 'U' THEN 2   -- UPDATE
        WHEN t._file IS NULL THEN 3                        -- INSERT
        ELSE 0                                             -- NO_OP
    END AS op_code
FROM cdc AS s
LEFT OUTER JOIN orders AS t ON t.order_id = s.order_id
```

### 4.3 列布局

与 UPDATE 完全一致：

```
[_file, _pos, data_col_0, data_col_1, ..., data_col_N, op_code]
```

- 位置 0-1：`_file` (VARCHAR), `_pos` (BIGINT) — target 隐藏列
- 位置 2 到 N+1：完整 target 表 schema（schema 顺序）— CASE 表达式
- 最后一位：`op_code` (TINYINT)

### 4.4 CASE 表达式构造规则

对于每个 target 列 `col_i`：

**MATCHED 分支**（`t._file IS NOT NULL`）：
- 若 col_i 出现在某个 WHEN MATCHED UPDATE 的 SET 中 → 使用 SET 表达式
- 若 col_i 未出现在 SET 中 → 使用 `t.col_i`（保留原值）
- WHEN MATCHED DELETE 子句 → 使用 `t.col_i`（数据列被 sink 忽略，仅用 _file/_pos）
- 多个 MATCHED 子句 → 嵌套 CASE 按声明顺序匹配

**NOT MATCHED 分支**（`t._file IS NULL`）：
- 若 col_i 出现在 INSERT 列表中 → 使用对应 VALUES 表达式
- 若 col_i 未出现在 INSERT 列表中 → 使用列默认值或 NULL
- `INSERT *` → 使用 source 中同名列
- 多个 NOT MATCHED 子句 → 嵌套 CASE 按声明顺序匹配

### 4.5 输出

- `mergeIntoStmt.setQueryStatement(queryStatement)`
- `mergeIntoStmt.setTable(icebergTable)`
- `mergeIntoStmt.setIcebergColumnOutputNames(colNames)`

---

## 5. Planner 设计（MergeIntoPlanner）

### 5.1 整体流程

复用 `UpdatePlanner` 的结构：

1. 将 Analyzer 重写的 QueryStatement 通过 `RelationTransformer` 转为 LogicalPlan
2. 通过 `IcebergPlannerUtils.createShuffleProperty()` 创建分区 shuffle 属性
3. 通过 Query Optimizer 优化逻辑计划
4. 通过 `PlanFragmentBuilder.createPhysicalPlan()` 生成物理计划
5. 在物理计划中**插入 `EnforceUniqueNode`**（Join 后、ExchangeNode 前）
6. 设置 `IcebergRowDeltaSink`（复用 UpdatePlanner 的 setup 逻辑）
7. 通过 `IcebergPlannerUtils.configureIcebergSinkPipeline()` 配置 pipeline

### 5.2 物理计划结构

```
SourceScanNode + IcebergScanNode(target, usedForDelete=true)
         |
    HashJoinNode(LEFT OUTER, on = merge_condition)
         |
    ProjectNode(CASE → op_code, 数据列 CASE 表达式)
         |
    FilterNode(op_code != 0)
         |
    EnforceUniqueNode(key=[_file_idx, _pos_idx])   ← 重复匹配检测
         |
    ExchangeNode(HASH SHUFFLE by partition_cols)
         |
    IcebergRowDeltaSink
        ├─ op_code=1 (DELETE):  → IcebergDeleteSink (position delete)
        ├─ op_code=2 (UPDATE):  → IcebergDeleteSink + IcebergChunkSink
        └─ op_code=3 (INSERT):  → IcebergChunkSink (new data)
```

### 5.3 冲突检测 Filter

对于 UPDATE，`IcebergPlannerUtils.buildIcebergFilterExpr()` 从 IcebergScanNode 提取 WHERE 条件作为冲突检测 filter。对于 MERGE，优化器将 JOIN ON 条件下推到 target scan，因此同一个 `buildIcebergFilterExpr()` 方法自动工作——ON 条件推导出的 Iceberg 表达式成为冲突检测 filter。

### 5.4 EnforceUniqueNode 位置选择

**放在 Join 后、ExchangeNode 前**，原因：

- Hash Join 的 probe 机制保证：同一个 target 行 `(_file, _pos)` 的所有匹配结果在同一个 driver instance 上
- ExchangeNode (partition shuffle) 后，DOP > 1 时无法保证 co-location
- 因此 EnforceUniqueNode 必须在 Exchange 之前

---

## 6. EnforceUniqueNode（通用唯一性约束算子）

### 6.1 定位

通用的数据流唯一性约束算子，不限于 Iceberg MERGE 场景。

### 6.2 Thrift 定义

```thrift
// PlanNodes.thrift
struct TEnforceUniqueNode {
    1: optional list<i32> unique_key_col_indices
}
```

### 6.3 行为

- **透传所有行**，不消费、不过滤、不修改数据
- 对每行：若任意 key 列为 NULL → 跳过检查（SQL 标准中 NULL ≠ NULL）
- 对每行：若所有 key 列非 NULL → 插入 hash set，重复时返回 `RuntimeError("Found duplicate values for unique key constraint")`
- Hash set 生命周期：算子实例级别，跨 chunk 累积

### 6.4 BE 实现

```
be/src/exec/pipeline/enforce_unique_operator.h
be/src/exec/pipeline/enforce_unique_operator.cpp
```

核心逻辑：

```cpp
Status EnforceUniqueOperator::push_chunk(const ChunkPtr& chunk) {
    auto file_col = chunk->get_column_by_index(_file_col_idx);
    auto pos_col = chunk->get_column_by_index(_pos_col_idx);

    for (size_t i = 0; i < chunk->num_rows(); i++) {
        // key 列含 NULL → 跳过
        if (file_col->is_null(i) || pos_col->is_null(i)) continue;

        auto key = std::make_pair(file_col->get(i).get_slice().to_string(),
                                  pos_col->get(i).get_int64());
        if (!_seen.insert(key).second) {
            return Status::RuntimeError(
                "Found duplicate values for unique key constraint");
        }
    }
    _output_chunk = chunk;  // 透传
    return Status::OK();
}
```

### 6.5 FE 计划节点

```
fe/fe-core/src/main/java/com/starrocks/planner/EnforceUniqueNode.java
```

继承 `PlanNode`，持有 `uniqueKeyColIndices`，`toThrift()` 生成 `TEnforceUniqueNode`。

### 6.6 未来可复用场景

| 场景 | key 列 | 说明 |
|------|--------|------|
| Iceberg MERGE 重复匹配检测 | `(_file, _pos)` | INSERT 行 key 为 NULL，自动跳过 |
| 未来 OLAP MERGE INTO | 主键列 | 检测 target 行重复匹配 |
| INSERT 去重校验 | unique key 列 | 全量检查 |

---

## 7. 集成点与改动清单

### 7.1 新建文件

| 文件 | 说明 |
|------|------|
| `fe/.../sql/ast/MergeIntoStmt.java` | AST 节点 |
| `fe/.../sql/ast/MergeWhenClause.java` | 抽象基类 |
| `fe/.../sql/ast/MergeWhenMatchedUpdateClause.java` | MATCHED UPDATE 子句 |
| `fe/.../sql/ast/MergeWhenMatchedDeleteClause.java` | MATCHED DELETE 子句 |
| `fe/.../sql/ast/MergeWhenNotMatchedInsertClause.java` | NOT MATCHED INSERT 子句 |
| `fe/.../sql/analyzer/MergeIntoAnalyzer.java` | 语义校验 + 重写 |
| `fe/.../sql/MergeIntoPlanner.java` | 物理计划生成 |
| `fe/.../planner/EnforceUniqueNode.java` | FE 计划节点 |
| `be/src/exec/pipeline/enforce_unique_operator.h` | BE 算子头文件 |
| `be/src/exec/pipeline/enforce_unique_operator.cpp` | BE 算子实现 |

### 7.2 修改文件

| 文件 | 改动 |
|------|------|
| `StarRocks.g4` | 新增 mergeIntoStatement 语法规则 |
| `StarRocksLex.g4` | 确认 MATCHED 关键字（如需新增） |
| `AstBuilder.java` | 新增 visitMergeIntoStatement |
| `AstVisitorExtendInterface.java` | 新增 visitMergeIntoStatement 默认方法 |
| `Analyzer.java` | 新增 MergeIntoStmt 调度 |
| `DMLStmtAnalyzer.java` | 新增 MergeIntoStmt 调度 |
| `StatementPlanner.java` | 新增 MergeIntoStmt → MergeIntoPlanner 调度 |
| `StmtExecutor.java` | getExecType() 新增 "MergeInto" |
| `PlanNodes.thrift` | 新增 TEnforceUniqueNode |
| `ConnectorMetricsMgr.java` | 新增 MERGE 指标注册与记录方法 |
| `IcebergMetadata.java` | commitRowDeltaOperation 中区分 UPDATE/MERGE 记录 metrics |

### 7.3 无需改动的文件

| 文件 | 原因 |
|------|------|
| `IcebergRowDeltaSink` (BE) | 已支持全部 4 种 op_code 路由 |
| `IcebergTableSink` (BE) | 无需改动 |
| `IcebergRowDeltaSink.java` (FE) | 直接复用 |
| `IcebergPlannerUtils.java` | 直接复用 |
| `commitRowDeltaOperation()` | 现有三路判断自动处理混合文件 |
| `DataSinks.thrift` | 已有 write_mode=ROW_DELTA |

### 7.4 改动量估算

| 类型 | 估算行数 |
|------|---------|
| 新建 FE（AST + Analyzer + Planner + PlanNode） | ~800 行 |
| 新建 BE（EnforceUniqueOperator） | ~200 行 |
| 修改 FE（语法、调度、Metrics） | ~100 行 |
| 修改 Thrift | ~10 行 |
| 测试（FE 单测 + BE 单测 + SQL 集成测试） | ~600 行 |
| **合计** | **~1700 行** |

---

## 8. Metrics

### 8.1 指标定义

| 指标名 | 类型 | Labels | 说明 |
|--------|------|--------|------|
| `iceberg_merge_total` | Counter | `status`, `reason` | MERGE INTO 任务总数 |
| `iceberg_merge_duration_ms_total` | Counter | - | MERGE INTO 操作累计耗时（毫秒） |
| `iceberg_merge_rows` | Counter | `merge_type` | MERGE INTO 处理的行数 |
| `iceberg_merge_bytes` | Counter | `file_type` | MERGE INTO 写入的字节数 |
| `iceberg_merge_files` | Counter | `file_type` | MERGE INTO 写入的文件数 |

### 8.2 Label 取值

**`merge_type`**（行操作类型）：

| 值 | op_code | 说明 |
|----|---------|------|
| `matched_update` | 2 | WHEN MATCHED THEN UPDATE |
| `matched_delete` | 1 | WHEN MATCHED THEN DELETE |
| `not_matched_insert` | 3 | WHEN NOT MATCHED THEN INSERT |

**`file_type`**（文件类型）：

| 值 | 说明 |
|----|------|
| `data` | Data File（UPDATE 新行 + INSERT 新行） |
| `position_delete` | Position Delete File（UPDATE/DELETE 旧行标记） |

**`status` / `reason`**：

| status | reason | 说明 |
|--------|--------|------|
| `success` | - | 成功 |
| `failed` | `conflict` | 快照冲突 |
| `failed` | `dup_match` | 重复匹配 |
| `failed` | `internal_error` | 内部错误 |

### 8.3 埋点位置

- `IcebergMetadata.commitRowDeltaOperation()` 成功后：记录 total(success)、duration、bytes、files
- `IcebergMetadata.commitRowDeltaOperation()` 失败时：记录 total(failed, reason)
- Sink commit 结果中统计行数：记录 rows（按 merge_type 分类）
- 通过 `IcebergSinkExtra` 传递操作类型标识，区分 UPDATE 和 MERGE

---

## 9. 测试设计

### 9.1 FE 单元测试

**MergeIntoAnalyzerIcebergTest.java**：

- 目标表非 Iceberg → SemanticException
- 目标表 Iceberg V1 → SemanticException
- WHEN MATCHED UPDATE 更新分区列 → SemanticException
- WHEN MATCHED UPDATE 更新 _file/_pos → SemanticException
- 单个 WHEN MATCHED UPDATE → 通过，验证重写 QueryStatement
- 单个 WHEN MATCHED DELETE → 通过
- 单个 WHEN NOT MATCHED INSERT → 通过
- 三种 WHEN 子句组合 → 通过，验证 op_code CASE 表达式
- 多个 WHEN NOT MATCHED（不同条件） → 通过
- WHEN NOT MATCHED INSERT * → 通过，验证列展开
- INSERT 列数与 VALUES 数不匹配 → SemanticException
- 无 WHEN 子句 → 解析错误
- WITH (CTE) 子句 → SemanticException

**MergeIntoPlanTest.java**：

- 基本 MERGE 物理计划结构验证（HashJoinNode + EnforceUniqueNode + ExchangeNode + IcebergRowDeltaSink）
- 分区表 shuffle（ExchangeNode 按分区列 HASH 分发）
- 非分区表
- Sink TupleDescriptor 列布局验证
- Sink write_mode = ROW_DELTA
- EnforceUniqueNode key 列索引验证
- 冲突检测 filter 验证

### 9.2 BE 单元测试

**enforce_unique_operator_test.cpp**：

- 无重复行 → 全部透传
- 有重复 key → RuntimeError
- key 列含 NULL → 跳过检查，透传
- 空 chunk → 正常处理
- 跨 chunk 重复检测

### 9.3 SQL 集成测试

**test/sql/test_iceberg/T/test_iceberg_merge_into**：

基本功能：
- WHEN MATCHED THEN UPDATE（仅更新）
- WHEN MATCHED THEN DELETE（仅删除）
- WHEN NOT MATCHED THEN INSERT（仅插入）
- 三种子句组合
- 多个 WHEN MATCHED 子句（不同条件）
- 多个 WHEN NOT MATCHED 子句（不同条件）
- INSERT * 语法
- Source 为子查询
- Source 为另一张 Iceberg 表

数据正确性：
- 每条 MERGE 后 SELECT 全表验证
- UPDATE 后旧值消失、新值正确
- DELETE 后行被删除
- INSERT 后新行出现

分区表：
- 目标表有分区，验证分区路由
- MERGE 只影响部分分区，其余分区不变

语义正确性：
- 重复匹配检测（同一 target 行匹配多个 source 行 → 报错）
- WHEN 子句顺序（首个匹配生效）
- NOT MATCHED 行正确插入
- MERGE 后验证新 snapshot 产生

边界情况：
- Source 为空（无操作）
- Target 为空（全部 INSERT）
- 全部匹配（无 INSERT）
- 全部不匹配（无 UPDATE/DELETE）

错误场景：
- 目标表非 V2 → 报错
- 更新分区列 → 报错
- INSERT 列数不匹配 → 报错
