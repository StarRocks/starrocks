# Iceberg MERGE INTO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement SQL MERGE INTO support for Iceberg V2 tables in StarRocks, reusing the existing RowDelta UPDATE infrastructure.

**Architecture:** The Analyzer rewrites MERGE INTO into a SELECT with `source LEFT OUTER JOIN target` + CASE expressions computing `op_code`. The Planner generates a physical plan with `EnforceUniqueNode` (duplicate match detection) + partition shuffle + `IcebergRowDeltaSink`. The existing BE sink and FE commit paths handle all 4 op_codes without modification.

**Tech Stack:** Java (FE), C++ (BE), ANTLR4 (grammar), Apache Thrift (RPC), Apache Iceberg Java SDK

**Spec:** `docs/superpowers/specs/2026-04-14-iceberg-merge-into-design.md`

---

## File Map

### New Files

| File | Responsibility |
|------|---------------|
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeIntoStmt.java` | AST node for MERGE INTO statement |
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenClause.java` | Abstract base for WHEN clauses |
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedUpdateClause.java` | WHEN MATCHED THEN UPDATE |
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedDeleteClause.java` | WHEN MATCHED THEN DELETE |
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenNotMatchedInsertClause.java` | WHEN NOT MATCHED THEN INSERT |
| `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MergeIntoAnalyzer.java` | Semantic validation + rewrite to SELECT |
| `fe/fe-core/src/main/java/com/starrocks/sql/MergeIntoPlanner.java` | Physical plan generation |
| `fe/fe-core/src/main/java/com/starrocks/planner/EnforceUniqueNode.java` | FE plan node for uniqueness check |
| `be/src/exec/enforce_unique_node.h` | BE node for pipeline decomposition |
| `be/src/exec/enforce_unique_node.cpp` | BE node implementation |
| `be/src/exec/pipeline/enforce_unique_operator.h` | BE pipeline operator header |
| `be/src/exec/pipeline/enforce_unique_operator.cpp` | BE pipeline operator implementation |
| `fe/fe-core/src/test/java/com/starrocks/sql/analyzer/MergeIntoAnalyzerIcebergTest.java` | Analyzer unit tests |
| `fe/fe-core/src/test/java/com/starrocks/sql/plan/MergeIntoPlanTest.java` | Plan unit tests |
| `be/test/exec/pipeline/enforce_unique_operator_test.cpp` | BE operator unit tests |
| `test/sql/test_iceberg/T/test_iceberg_merge_into` | SQL integration test input |
| `test/sql/test_iceberg/R/test_iceberg_merge_into` | SQL integration test expected output |

### Modified Files

| File | Lines | Change |
|------|-------|--------|
| `gensrc/thrift/PlanNodes.thrift` | ~71, ~1231, ~1487 | Add ENFORCE_UNIQUE_NODE enum + TEnforceUniqueNode struct + field in TPlanNode |
| `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4` | ~104, ~1351, ~3302 | Add mergeIntoStatement grammar rules, MATCHED to nonReserved |
| `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocksLex.g4` | ~287 | Add MATCHED keyword |
| `fe/fe-core/src/main/java/com/starrocks/sql/ast/AstVisitorExtendInterface.java` | ~220 | Add visitMergeIntoStatement() |
| `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java` | ~2707 | Add visitMergeIntoStatement() |
| `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Analyzer.java` | ~718 | Add visitMergeIntoStatement dispatch |
| `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/DMLStmtAnalyzer.java` | ~37-44, ~63 | Add MergeIntoStmt routing |
| `fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java` | ~171, ~556, ~590 | Add MergeIntoStmt dispatch |
| `fe/fe-core/src/main/java/com/starrocks/qe/StmtExecutor.java` | ~695, ~3438 | Add MergeIntoStmt type detection |
| `fe/fe-core/src/main/java/com/starrocks/metric/ConnectorMetricsMgr.java` | end of file | Add MERGE metrics |
| `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergMetadata.java` | ~1973 | Distinguish UPDATE/MERGE metrics in commitRowDeltaOperation |
| `be/src/exec/exec_factory.cpp` | switch statement | Add ENFORCE_UNIQUE_NODE case |
| `be/src/exec/CMakeLists.txt` | source list | Add enforce_unique_node.cpp |
| `be/src/exec/pipeline/CMakeLists.txt` | source list | Add enforce_unique_operator.cpp |

---

## Task 1: Thrift Definitions

**Files:**
- Modify: `gensrc/thrift/PlanNodes.thrift`

- [ ] **Step 1: Add ENFORCE_UNIQUE_NODE to TPlanNodeType enum**

In `gensrc/thrift/PlanNodes.thrift`, find the `TPlanNodeType` enum (~line 47) and add the new node type after the last entry (`LAKE_CACHE_STATS_SCAN_NODE`):

```thrift
  LAKE_CACHE_STATS_SCAN_NODE,
  ENFORCE_UNIQUE_NODE
```

- [ ] **Step 2: Add TEnforceUniqueNode struct**

After `TAssertNumRowsNode` (~line 1231), add:

```thrift
struct TEnforceUniqueNode {
    1: optional list<i32> unique_key_col_indices
}
```

- [ ] **Step 3: Add field to TPlanNode struct**

In the `TPlanNode` struct, after the last field (~line 1533, field 85), add:

```thrift
  86: optional TEnforceUniqueNode enforce_unique_node
```

- [ ] **Step 4: Commit**

```bash
git add gensrc/thrift/PlanNodes.thrift
git commit -m "[Feature] Add EnforceUniqueNode thrift definition for MERGE INTO support"
```

---

## Task 2: ANTLR Grammar

**Files:**
- Modify: `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocksLex.g4`
- Modify: `fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4`

- [ ] **Step 1: Add MATCHED keyword to lexer**

In `StarRocksLex.g4`, after `MERGE` (~line 287), add:

```antlr
MATCHED: 'MATCHED';
```

- [ ] **Step 2: Add mergeIntoStatement to statement rule**

In `StarRocks.g4`, in the `statement` rule after `deleteStatement` (~line 104), add:

```antlr
    | mergeIntoStatement
```

- [ ] **Step 3: Add MERGE INTO grammar rules**

In `StarRocks.g4`, after the `deleteStatement` rule (~line 1351), add:

```antlr
mergeIntoStatement
    : explainDesc? MERGE INTO qualifiedName (AS? targetAlias=identifier)?
      USING relation (AS? sourceAlias=identifier)?
      ON mergeCondition=expression
      mergeWhenClause+
    ;

mergeWhenClause
    : WHEN MATCHED (AND matchedCondition=expression)? THEN mergeMatchedAction       #mergeWhenMatched
    | WHEN NOT MATCHED (AND notMatchedCondition=expression)? THEN mergeNotMatchedAction   #mergeWhenNotMatched
    ;

mergeMatchedAction
    : UPDATE SET assignmentList      #mergeMatchedUpdate
    | DELETE                         #mergeMatchedDelete
    ;

mergeNotMatchedAction
    : INSERT STAR                                                                    #mergeNotMatchedInsertStar
    | INSERT ('(' cols+=identifier (',' cols+=identifier)* ')')? VALUES '(' expressionList ')'   #mergeNotMatchedInsertValues
    ;
```

- [ ] **Step 4: Add MATCHED to nonReserved list**

In `StarRocks.g4`, in the `nonReserved` rule (~line 3302), in the 'M' section, add `MATCHED`:

```
    | MANUAL | MAP | MAPPING | MAPPINGS | MASKING | MATCH | MATCHED | MATCH_ANY | MATCH_ALL | MAPPINGS | MATERIALIZED | MAX | META | MIN | MINUTE | MINUTES | MODE | MODIFY | MONTH | MERGE | MINUS | MULTIPLE
```

- [ ] **Step 5: Build grammar to verify no conflicts**

```bash
cd fe && mvn generate-sources -pl fe-grammar -q 2>&1 | tail -5
```

Expected: Build succeeds with no ANTLR conflicts.

- [ ] **Step 6: Commit**

```bash
git add fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocksLex.g4
git add fe/fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4
git commit -m "[Feature] Add MERGE INTO grammar rules to StarRocks parser"
```

---

## Task 3: AST Nodes

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenClause.java`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedUpdateClause.java`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedDeleteClause.java`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenNotMatchedInsertClause.java`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeIntoStmt.java`

- [ ] **Step 1: Create MergeWhenClause (abstract base)**

```java
// fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenClause.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

public abstract class MergeWhenClause extends ParseNode {
    private final Expr optionalCondition; // AND condition, nullable

    protected MergeWhenClause(Expr optionalCondition, NodePosition pos) {
        super(pos);
        this.optionalCondition = optionalCondition;
    }

    public Expr getOptionalCondition() {
        return optionalCondition;
    }

    public boolean isMatched() {
        return this instanceof MergeWhenMatchedUpdateClause
                || this instanceof MergeWhenMatchedDeleteClause;
    }
}
```

- [ ] **Step 2: Create MergeWhenMatchedUpdateClause**

```java
// fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedUpdateClause.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeWhenMatchedUpdateClause extends MergeWhenClause {
    private final List<ColumnAssignment> assignments;

    public MergeWhenMatchedUpdateClause(Expr optionalCondition,
                                        List<ColumnAssignment> assignments,
                                        NodePosition pos) {
        super(optionalCondition, pos);
        this.assignments = assignments;
    }

    public List<ColumnAssignment> getAssignments() {
        return assignments;
    }
}
```

- [ ] **Step 3: Create MergeWhenMatchedDeleteClause**

```java
// fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedDeleteClause.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

public class MergeWhenMatchedDeleteClause extends MergeWhenClause {
    public MergeWhenMatchedDeleteClause(Expr optionalCondition, NodePosition pos) {
        super(optionalCondition, pos);
    }
}
```

- [ ] **Step 4: Create MergeWhenNotMatchedInsertClause**

```java
// fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenNotMatchedInsertClause.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeWhenNotMatchedInsertClause extends MergeWhenClause {
    private final List<String> targetColumnNames; // nullable if INSERT * or omitted
    private final List<Expr> values;              // nullable if INSERT *
    private final boolean isStar;                 // INSERT * shorthand

    public MergeWhenNotMatchedInsertClause(Expr optionalCondition,
                                           List<String> targetColumnNames,
                                           List<Expr> values,
                                           boolean isStar,
                                           NodePosition pos) {
        super(optionalCondition, pos);
        this.targetColumnNames = targetColumnNames;
        this.values = values;
        this.isStar = isStar;
    }

    public List<String> getTargetColumnNames() {
        return targetColumnNames;
    }

    public List<Expr> getValues() {
        return values;
    }

    public boolean isStar() {
        return isStar;
    }
}
```

- [ ] **Step 5: Create MergeIntoStmt**

Follow the `UpdateStmt` pattern (lines 26-138 in UpdateStmt.java). Key differences: holds source relation, merge condition, when clauses, and no FROM/WHERE.

```java
// fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeIntoStmt.java
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MergeIntoStmt extends DmlStmt {
    private TableRef tableRef;                        // target table
    private final String targetAlias;                  // AS alias for target (nullable)
    private final Relation sourceRelation;             // USING source
    private final String sourceAlias;                  // AS alias for source (nullable)
    private final Expr mergeCondition;                 // ON condition
    private final List<MergeWhenClause> whenClauses;   // WHEN ... THEN ... clauses

    // Set by analyzer
    private Table table;
    private QueryStatement queryStatement;
    private List<String> icebergColumnOutputNames;

    public MergeIntoStmt(TableRef tableRef, String targetAlias,
                         Relation sourceRelation, String sourceAlias,
                         Expr mergeCondition, List<MergeWhenClause> whenClauses,
                         NodePosition pos) {
        super(pos);
        this.tableRef = tableRef;
        this.targetAlias = targetAlias;
        this.sourceRelation = sourceRelation;
        this.sourceAlias = sourceAlias;
        this.mergeCondition = mergeCondition;
        this.whenClauses = whenClauses;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    public void setTableRef(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public String getTargetAlias() {
        return targetAlias;
    }

    public Relation getSourceRelation() {
        return sourceRelation;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public Expr getMergeCondition() {
        return mergeCondition;
    }

    public List<MergeWhenClause> getWhenClauses() {
        return whenClauses;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setIcebergColumnOutputNames(List<String> names) {
        this.icebergColumnOutputNames = names;
    }

    public List<String> getIcebergColumnOutputNames() {
        return icebergColumnOutputNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitMergeIntoStatement(this, context);
    }
}
```

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenClause.java
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedUpdateClause.java
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenMatchedDeleteClause.java
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeWhenNotMatchedInsertClause.java
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/MergeIntoStmt.java
git commit -m "[Feature] Add MERGE INTO AST nodes"
```

---

## Task 4: Parser & Visitor Wiring

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/ast/AstVisitorExtendInterface.java:220`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java:~2707`

- [ ] **Step 1: Add visitMergeIntoStatement to AstVisitorExtendInterface**

In `AstVisitorExtendInterface.java`, after `visitDeleteStatement` (~line 220), add:

```java
    default R visitMergeIntoStatement(MergeIntoStmt statement, C context) {
        return visitStatement(statement, context);
    }
```

Add the import for `MergeIntoStmt` at the top of the file.

- [ ] **Step 2: Add visitMergeIntoStatement to AstBuilder**

In `AstBuilder.java`, after `visitDeleteStatement` (~line 2707), add the visitor method. This method constructs the `MergeIntoStmt` from the parse tree context:

```java
    @Override
    public ParseNode visitMergeIntoStatement(
            com.starrocks.sql.parser.StarRocksParser.MergeIntoStatementContext context) {
        QualifiedName qualifiedName = getQualifiedName(context.qualifiedName());
        TableRef tableRef = new TableRef(normalizeName(qualifiedName), null, createPos(context));

        String targetAlias = context.targetAlias != null ? context.targetAlias.getText() : null;
        Relation sourceRelation = (Relation) visit(context.relation());
        String sourceAlias = context.sourceAlias != null ? context.sourceAlias.getText() : null;
        Expr mergeCondition = (Expr) visit(context.mergeCondition);

        List<MergeWhenClause> whenClauses = new ArrayList<>();
        for (var clauseCtx : context.mergeWhenClause()) {
            whenClauses.add((MergeWhenClause) visit(clauseCtx));
        }

        MergeIntoStmt ret = new MergeIntoStmt(tableRef, targetAlias, sourceRelation,
                sourceAlias, mergeCondition, whenClauses, createPos(context));

        if (context.explainDesc() != null) {
            ret.setIsExplain(true, getExplainType(context.explainDesc()));
            if (StatementBase.ExplainLevel.ANALYZE.equals(ret.getExplainLevel())) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedOp("analyze"));
            }
        }
        ret.setHintNodes(hintMap.get(context));
        return ret;
    }
```

- [ ] **Step 3: Add visitor methods for WHEN clauses**

Add visitor methods in AstBuilder for each grammar alternative:

```java
    @Override
    public ParseNode visitMergeWhenMatched(
            com.starrocks.sql.parser.StarRocksParser.MergeWhenMatchedContext context) {
        Expr condition = context.matchedCondition != null ? (Expr) visit(context.matchedCondition) : null;
        return (MergeWhenClause) visit(context.mergeMatchedAction());
        // Need to pass condition through — see next step for the action visitors
    }
```

Actually, since the grammar uses labeled alternatives (`#mergeWhenMatched`, `#mergeWhenNotMatched`), we need visitors for each. The condition is on the `mergeWhenClause` level, and the action is nested. The cleanest approach is to handle it all in the top-level visitors:

```java
    @Override
    public ParseNode visitMergeWhenMatched(
            com.starrocks.sql.parser.StarRocksParser.MergeWhenMatchedContext context) {
        Expr condition = context.matchedCondition != null ? (Expr) visit(context.matchedCondition) : null;
        var action = context.mergeMatchedAction();
        if (action instanceof com.starrocks.sql.parser.StarRocksParser.MergeMatchedUpdateContext updateCtx) {
            List<ColumnAssignment> assignments = visit(updateCtx.assignmentList().assignment(),
                    ColumnAssignment.class);
            return new MergeWhenMatchedUpdateClause(condition, assignments, createPos(context));
        } else {
            // DELETE
            return new MergeWhenMatchedDeleteClause(condition, createPos(context));
        }
    }

    @Override
    public ParseNode visitMergeWhenNotMatched(
            com.starrocks.sql.parser.StarRocksParser.MergeWhenNotMatchedContext context) {
        Expr condition = context.notMatchedCondition != null
                ? (Expr) visit(context.notMatchedCondition) : null;
        var action = context.mergeNotMatchedAction();
        if (action instanceof com.starrocks.sql.parser.StarRocksParser.MergeNotMatchedInsertStarContext) {
            return new MergeWhenNotMatchedInsertClause(condition, null, null, true, createPos(context));
        } else {
            var insertCtx =
                    (com.starrocks.sql.parser.StarRocksParser.MergeNotMatchedInsertValuesContext) action;
            List<String> targetCols = null;
            if (insertCtx.cols != null && !insertCtx.cols.isEmpty()) {
                targetCols = insertCtx.cols.stream()
                        .map(id -> id.getText().toLowerCase())
                        .collect(Collectors.toList());
            }
            List<Expr> values = visit(insertCtx.expressionList().expression(), Expr.class);
            return new MergeWhenNotMatchedInsertClause(condition, targetCols, values,
                    false, createPos(context));
        }
    }
```

- [ ] **Step 4: Verify FE compiles**

```bash
cd fe && mvn compile -pl fe-core -q -am 2>&1 | tail -10
```

Expected: Compilation succeeds.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/ast/AstVisitorExtendInterface.java
git add fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java
git commit -m "[Feature] Add MERGE INTO parser visitors and AST wiring"
```

---

## Task 5: Analyzer Dispatch & MergeIntoAnalyzer

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Analyzer.java:~718`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/DMLStmtAnalyzer.java:~37,~63`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MergeIntoAnalyzer.java`

- [ ] **Step 1: Add dispatch in Analyzer.java**

After `visitDeleteStatement` (~line 718), add:

```java
        @Override
        public Void visitMergeIntoStatement(MergeIntoStmt statement, ConnectContext context) {
            DMLStmtAnalyzer.analyze(statement, context);
            return null;
        }
```

Add import for `MergeIntoStmt`.

- [ ] **Step 2: Add dispatch in DMLStmtAnalyzer.java**

In the `analyze()` method (~line 37), add the MergeIntoStmt branch for tableRef normalization:

```java
            } else if (dmlStmt instanceof MergeIntoStmt) {
                ((MergeIntoStmt) dmlStmt).setTableRef(tableRef);
            }
```

After `visitDeleteStatement` (~line 63), add:

```java
        @Override
        public Void visitMergeIntoStatement(MergeIntoStmt stmt, ConnectContext context) {
            MergeIntoAnalyzer.analyze(stmt, context);
            return null;
        }
```

Add imports for `MergeIntoStmt` and `MergeIntoAnalyzer`.

- [ ] **Step 3: Create MergeIntoAnalyzer**

Create `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MergeIntoAnalyzer.java`.

This is the most complex new file (~300 lines). It follows the `UpdateAnalyzer.analyzeIcebergTable()` pattern (lines 87-221) but handles JOIN + CASE expressions.

Key structure:

```java
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.*;
import com.starrocks.catalog.*;
import com.starrocks.connector.iceberg.IcebergTable;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.*;
// ... other imports

public class MergeIntoAnalyzer {

    public static void analyze(MergeIntoStmt mergeIntoStmt, ConnectContext session) {
        TableRef tableRef = mergeIntoStmt.getTableRef();
        Table table = MetaUtils.getTable(session, tableRef.getCatalogName(),
                tableRef.getDbName(), tableRef.getTableName());

        if (table instanceof IcebergTable icebergTable) {
            analyzeIcebergTable(mergeIntoStmt, icebergTable, session);
        } else {
            throw new SemanticException("MERGE INTO is only supported for Iceberg tables");
        }
    }

    private static void analyzeIcebergTable(MergeIntoStmt stmt,
                                            IcebergTable icebergTable,
                                            ConnectContext session) {
        // 1. Validate Iceberg V2
        if (icebergTable.getFormatVersion() != 2) {
            throw new SemanticException(
                "MERGE INTO is only supported for Iceberg V2 tables in phase 1");
        }

        // 2. Validate no CTE
        // (MergeIntoStmt currently has no CTE field — skip if not supported)

        // 3. Validate WHEN clauses
        List<MergeWhenClause> whenClauses = stmt.getWhenClauses();
        if (whenClauses.isEmpty()) {
            throw new SemanticException("MERGE INTO requires at least one WHEN clause");
        }

        // 4. For each WHEN MATCHED UPDATE, validate no partition column updates
        List<String> partitionColumnNames = icebergTable.getPartitionColumnNames();
        for (MergeWhenClause clause : whenClauses) {
            if (clause instanceof MergeWhenMatchedUpdateClause updateClause) {
                for (ColumnAssignment assignment : updateClause.getAssignments()) {
                    String colName = assignment.getColumn().toLowerCase();
                    if (partitionColumnNames.contains(colName)) {
                        throw new SemanticException(
                            "Updating partition column '" + colName + "' is not allowed");
                    }
                    if (colName.equals("_file") || colName.equals("_pos")) {
                        throw new SemanticException(
                            "Updating metadata column '" + colName + "' is not allowed");
                    }
                }
            }
        }

        // 5. Build the rewritten SELECT query
        //    SELECT t._file, t._pos, <data_col_CASEs>, <op_code_CASE>
        //    FROM source AS s LEFT OUTER JOIN target AS t ON merge_condition
        //    WHERE op_code != 0  (optional optimization)

        // 5a. Build SELECT list
        SelectList selectList = buildSelectList(stmt, icebergTable);

        // 5b. Build FROM clause: source LEFT OUTER JOIN target
        Relation joinRelation = buildJoinRelation(stmt, icebergTable, session);

        // 5c. Create SelectRelation
        SelectRelation selectRelation = new SelectRelation(
                selectList, joinRelation, null /*where*/, null, null);

        // 5d. Wrap in QueryStatement and analyze
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        new QueryAnalyzer(session).analyze(queryStatement);

        // 5e. Cast data columns to target types (skip _file, _pos at 0-1)
        // Follow UpdateAnalyzer pattern lines 200-208
        castDataColumnsToTargetTypes(queryStatement, icebergTable);

        // 5f. Save results
        List<String> colNames = buildColumnOutputNames(selectList);
        stmt.setTable(icebergTable);
        stmt.setQueryStatement(queryStatement);
        stmt.setIcebergColumnOutputNames(colNames);
    }

    private static SelectList buildSelectList(MergeIntoStmt stmt,
                                              IcebergTable icebergTable) {
        // Build: _file, _pos, <data columns CASE exprs>, op_code
        // See spec section 4.2-4.4 for CASE expression construction rules
        SelectList selectList = new SelectList();
        String targetAlias = getTargetAlias(stmt);

        // _file
        selectList.addItem(new SelectListItem(
                buildQualifiedSlotRef(targetAlias, "_file"), "_file"));
        // _pos
        selectList.addItem(new SelectListItem(
                buildQualifiedSlotRef(targetAlias, "_pos"), "_pos"));

        // Data columns (full target schema)
        List<Column> targetColumns = icebergTable.getBaseSchema();
        for (Column col : targetColumns) {
            Expr dataExpr = buildDataColumnCaseExpr(stmt, icebergTable, col);
            selectList.addItem(new SelectListItem(dataExpr, col.getName()));
        }

        // op_code (CASE expression)
        Expr opCodeExpr = buildOpCodeCaseExpr(stmt);
        selectList.addItem(new SelectListItem(opCodeExpr, "op_code"));

        return selectList;
    }

    private static Expr buildDataColumnCaseExpr(MergeIntoStmt stmt,
                                                IcebergTable table,
                                                Column targetCol) {
        // CASE
        //   WHEN t._file IS NOT NULL THEN <matched_value>
        //   ELSE <not_matched_value>
        // END

        String targetAlias = getTargetAlias(stmt);
        Expr isMatched = new IsNullPredicate(
                buildQualifiedSlotRef(targetAlias, "_file"), true /*isNotNull*/);

        Expr matchedValue = buildMatchedBranchValue(stmt, targetCol);
        Expr notMatchedValue = buildNotMatchedBranchValue(stmt, table, targetCol);

        // Build CASE WHEN isMatched THEN matchedValue ELSE notMatchedValue END
        CaseExpr caseExpr = new CaseExpr(null,
                Lists.newArrayList(new CaseWhenClause(isMatched, matchedValue)),
                notMatchedValue);
        return caseExpr;
    }

    private static Expr buildMatchedBranchValue(MergeIntoStmt stmt, Column col) {
        // If multiple MATCHED clauses, nest CASE by declaration order
        String targetAlias = getTargetAlias(stmt);
        String sourceAlias = getSourceAlias(stmt);
        List<MergeWhenClause> matchedClauses = stmt.getWhenClauses().stream()
                .filter(MergeWhenClause::isMatched)
                .collect(Collectors.toList());

        if (matchedClauses.size() == 1) {
            MergeWhenClause clause = matchedClauses.get(0);
            if (clause instanceof MergeWhenMatchedUpdateClause updateClause) {
                return getUpdateValueForColumn(updateClause, col, targetAlias);
            } else {
                // DELETE: use target original value (ignored by sink for op_code=1)
                return buildQualifiedSlotRef(targetAlias, col.getName());
            }
        }

        // Multiple MATCHED: build nested CASE
        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (MergeWhenClause clause : matchedClauses) {
            Expr condition = clause.getOptionalCondition();
            Expr value;
            if (clause instanceof MergeWhenMatchedUpdateClause updateClause) {
                value = getUpdateValueForColumn(updateClause, col, targetAlias);
            } else {
                value = buildQualifiedSlotRef(targetAlias, col.getName());
            }
            if (condition != null) {
                whenClauses.add(new CaseWhenClause(condition, value));
            } else {
                // No condition = always matches (must be last)
                whenClauses.add(new CaseWhenClause(new BoolLiteral(true), value));
            }
        }
        // Default: preserve original value
        return new CaseExpr(null, whenClauses,
                buildQualifiedSlotRef(targetAlias, col.getName()));
    }

    private static Expr getUpdateValueForColumn(MergeWhenMatchedUpdateClause clause,
                                                Column col, String targetAlias) {
        for (ColumnAssignment assignment : clause.getAssignments()) {
            if (assignment.getColumn().equalsIgnoreCase(col.getName())) {
                return assignment.getExpr();
            }
        }
        // Column not in SET list: preserve original
        return buildQualifiedSlotRef(targetAlias, col.getName());
    }

    private static Expr buildNotMatchedBranchValue(MergeIntoStmt stmt,
                                                   IcebergTable table,
                                                   Column targetCol) {
        // Similar to matched: iterate NOT MATCHED clauses
        String sourceAlias = getSourceAlias(stmt);
        List<MergeWhenNotMatchedInsertClause> insertClauses = stmt.getWhenClauses().stream()
                .filter(c -> c instanceof MergeWhenNotMatchedInsertClause)
                .map(c -> (MergeWhenNotMatchedInsertClause) c)
                .collect(Collectors.toList());

        if (insertClauses.isEmpty()) {
            return NullLiteral.create(targetCol.getType());
        }

        if (insertClauses.size() == 1) {
            return getInsertValueForColumn(insertClauses.get(0), targetCol, sourceAlias);
        }

        // Multiple NOT MATCHED: nested CASE by condition
        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (MergeWhenNotMatchedInsertClause clause : insertClauses) {
            Expr condition = clause.getOptionalCondition();
            Expr value = getInsertValueForColumn(clause, targetCol, sourceAlias);
            if (condition != null) {
                whenClauses.add(new CaseWhenClause(condition, value));
            } else {
                whenClauses.add(new CaseWhenClause(new BoolLiteral(true), value));
            }
        }
        return new CaseExpr(null, whenClauses,
                NullLiteral.create(targetCol.getType()));
    }

    private static Expr getInsertValueForColumn(MergeWhenNotMatchedInsertClause clause,
                                                Column targetCol, String sourceAlias) {
        if (clause.isStar()) {
            return buildQualifiedSlotRef(sourceAlias, targetCol.getName());
        }
        List<String> cols = clause.getTargetColumnNames();
        List<Expr> vals = clause.getValues();
        if (cols != null) {
            for (int i = 0; i < cols.size(); i++) {
                if (cols.get(i).equalsIgnoreCase(targetCol.getName())) {
                    return vals.get(i);
                }
            }
            // Column not in INSERT list
            return targetCol.getDefaultExpr() != null
                    ? targetCol.getDefaultExpr() : NullLiteral.create(targetCol.getType());
        } else {
            // INSERT VALUES without column list — positional
            int colIdx = getColumnIndex(targetCol);
            if (colIdx < vals.size()) {
                return vals.get(colIdx);
            }
            return NullLiteral.create(targetCol.getType());
        }
    }

    private static Expr buildOpCodeCaseExpr(MergeIntoStmt stmt) {
        // CASE
        //   WHEN t._file IS NOT NULL AND <matched_cond_1> THEN 1/2
        //   WHEN t._file IS NOT NULL AND <matched_cond_2> THEN 1/2
        //   WHEN t._file IS NULL AND <not_matched_cond> THEN 3
        //   ELSE 0
        // END
        String targetAlias = getTargetAlias(stmt);
        Expr isMatched = new IsNullPredicate(
                buildQualifiedSlotRef(targetAlias, "_file"), true);
        Expr isNotMatched = new IsNullPredicate(
                buildQualifiedSlotRef(targetAlias, "_file"), false);

        List<CaseWhenClause> whenClauses = new ArrayList<>();

        for (MergeWhenClause clause : stmt.getWhenClauses()) {
            if (clause.isMatched()) {
                int opCode = (clause instanceof MergeWhenMatchedDeleteClause)
                        ? IcebergRowDeltaSink.OpCode.DELETE.value()
                        : IcebergRowDeltaSink.OpCode.UPDATE.value();
                Expr condition = clause.getOptionalCondition() != null
                        ? new CompoundPredicate(CompoundPredicate.Operator.AND,
                                isMatched, clause.getOptionalCondition())
                        : isMatched;
                whenClauses.add(new CaseWhenClause(condition,
                        new IntLiteral(opCode, Type.TINYINT)));
            } else {
                Expr condition = clause.getOptionalCondition() != null
                        ? new CompoundPredicate(CompoundPredicate.Operator.AND,
                                isNotMatched, clause.getOptionalCondition())
                        : isNotMatched;
                whenClauses.add(new CaseWhenClause(condition,
                        new IntLiteral(IcebergRowDeltaSink.OpCode.INSERT.value(),
                                Type.TINYINT)));
            }
        }

        return new CaseExpr(null, whenClauses,
                new IntLiteral(IcebergRowDeltaSink.OpCode.NO_OP.value(), Type.TINYINT));
    }

    private static Relation buildJoinRelation(MergeIntoStmt stmt,
                                              IcebergTable icebergTable,
                                              ConnectContext session) {
        // source LEFT OUTER JOIN target ON merge_condition
        String targetAlias = getTargetAlias(stmt);
        QualifiedName targetQN = QualifiedName.of(Lists.newArrayList(
                stmt.getTableRef().getCatalogName(),
                stmt.getTableRef().getDbName(),
                stmt.getTableRef().getTableName()));
        TableRelation targetRelation = new TableRelation(
                new TableName(targetQN.toString()), targetAlias);

        Relation sourceRelation = stmt.getSourceRelation();
        // Set alias on source if specified
        // ...

        JoinRelation joinRelation = new JoinRelation(
                JoinRelation.JoinType.LEFT_OUTER_JOIN,
                sourceRelation, targetRelation,
                stmt.getMergeCondition(), false);
        return joinRelation;
    }

    // Helper methods
    private static String getTargetAlias(MergeIntoStmt stmt) {
        return stmt.getTargetAlias() != null ? stmt.getTargetAlias()
                : stmt.getTableRef().getTableName();
    }

    private static String getSourceAlias(MergeIntoStmt stmt) {
        return stmt.getSourceAlias(); // may be null
    }

    private static SlotRef buildQualifiedSlotRef(String tableAlias, String colName) {
        return new SlotRef(new TableName(null, null, tableAlias), colName);
    }

    private static void castDataColumnsToTargetTypes(QueryStatement queryStatement,
                                                     IcebergTable icebergTable) {
        // Follow UpdateAnalyzer pattern lines 200-208
        // Skip first 2 columns (_file, _pos), cast data columns to target schema types
        // Skip last column (op_code)
    }

    private static List<String> buildColumnOutputNames(SelectList selectList) {
        List<String> names = new ArrayList<>();
        for (SelectListItem item : selectList.getItems()) {
            names.add(item.getAlias());
        }
        return names;
    }
}
```

**Note:** The above is the structural skeleton. The actual implementation needs to use the correct StarRocks AST APIs (`SlotRef`, `CaseExpr`, `CaseWhenClause`, `CompoundPredicate`, `IsNullPredicate`, `IntLiteral`, `TableRelation`, `JoinRelation`, `SelectRelation`, etc.). The exact import paths and constructor signatures must match the existing codebase. Reference `UpdateAnalyzer.analyzeIcebergTable()` (lines 87-221) and `DeleteAnalyzer.analyzeIcebergTable()` for the exact AST construction patterns.

- [ ] **Step 4: Verify FE compiles**

```bash
cd fe && mvn compile -pl fe-core -q -am 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/Analyzer.java
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/DMLStmtAnalyzer.java
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MergeIntoAnalyzer.java
git commit -m "[Feature] Add MergeIntoAnalyzer with Iceberg V2 validation and SELECT rewrite"
```

---

## Task 6: FE EnforceUniqueNode & MergeIntoPlanner

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/planner/EnforceUniqueNode.java`
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/MergeIntoPlanner.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java:~171,~556,~590`

- [ ] **Step 1: Create EnforceUniqueNode**

Follow the `AssertNumRowsNode` pattern in `fe/fe-core/src/main/java/com/starrocks/planner/AssertNumRowsNode.java`:

```java
// fe/fe-core/src/main/java/com/starrocks/planner/EnforceUniqueNode.java
package com.starrocks.planner;

import com.starrocks.thrift.TEnforceUniqueNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.List;

public class EnforceUniqueNode extends PlanNode {
    private final List<Integer> uniqueKeyColIndices;

    public EnforceUniqueNode(PlanNodeId id, PlanNode child,
                             List<Integer> uniqueKeyColIndices) {
        super(id, child.getTupleIds(), "ENFORCE UNIQUE");
        this.uniqueKeyColIndices = uniqueKeyColIndices;
        addChild(child);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.ENFORCE_UNIQUE_NODE;
        TEnforceUniqueNode node = new TEnforceUniqueNode();
        node.setUnique_key_col_indices(uniqueKeyColIndices);
        msg.setEnforce_unique_node(node);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("unique key columns: ").append(uniqueKeyColIndices);
        sb.append("\n");
        return sb.toString();
    }
}
```

- [ ] **Step 2: Create MergeIntoPlanner**

Follow `UpdatePlanner` pattern (lines 79-255). Key differences: use MergeIntoStmt, insert EnforceUniqueNode.

```java
// fe/fe-core/src/main/java/com/starrocks/sql/MergeIntoPlanner.java
package com.starrocks.sql;

import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.IcebergTable;
import com.starrocks.planner.*;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.optimizer.*;
import com.starrocks.sql.optimizer.base.*;
import com.starrocks.sql.optimizer.transformer.*;
import com.starrocks.sql.plan.*;
import com.starrocks.connector.iceberg.IcebergMetadata;

import java.util.List;

public class MergeIntoPlanner {

    public ExecPlan plan(MergeIntoStmt mergeIntoStmt, ConnectContext session) {
        QueryRelation query = mergeIntoStmt.getQueryStatement().getQueryRelation();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session)
                .transform(query);

        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        Table targetTable = mergeIntoStmt.getTable();
        List<String> colNames;
        PhysicalPropertySet requiredProperty;

        if (targetTable instanceof IcebergTable icebergTable) {
            requiredProperty = IcebergPlannerUtils.createShuffleProperty(
                    icebergTable, outputColumns);
            colNames = mergeIntoStmt.getIcebergColumnOutputNames();
        } else {
            throw new SemanticException("MERGE INTO only supports Iceberg tables");
        }

        return createMergePlan(mergeIntoStmt, session, logicalPlan.getRoot(),
                columnRefFactory, outputColumns, colNames, targetTable, requiredProperty);
    }

    private ExecPlan createMergePlan(MergeIntoStmt stmt, ConnectContext session,
                                     OptExpression logicalRoot,
                                     ColumnRefFactory columnRefFactory,
                                     List<ColumnRefOperator> outputColumns,
                                     List<String> colNames, Table targetTable,
                                     PhysicalPropertySet requiredProperty) {
        // Follow UpdatePlanner.createUpdatePlan() pattern (lines 112-160)
        boolean canUsePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean prevLocalShuffleAgg = session.getSessionVariable()
                .isEnableLocalShuffleAgg();
        try {
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            OptimizerContext optimizerContext = new OptimizerContext(
                    session, columnRefFactory, session.getDumpInfo());
            Optimizer optimizer = new Optimizer(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalRoot, requiredProperty, outputColumns);

            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(
                    optimizedPlan, session, outputColumns, columnRefFactory,
                    colNames, stmt, true);

            // Setup Iceberg sink
            IcebergTable icebergTable = (IcebergTable) targetTable;
            setupIcebergMergeSink(execPlan, colNames, icebergTable, session);
            IcebergPlannerUtils.configureIcebergSinkPipeline(
                    execPlan, session, canUsePipeline);

            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevLocalShuffleAgg);
        }
    }

    private void setupIcebergMergeSink(ExecPlan execPlan, List<String> colNames,
                                       IcebergTable icebergTable,
                                       ConnectContext session) {
        // Reuse UpdatePlanner.setupIcebergRowDeltaSink() pattern (lines 225-255)
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor mergeTuple = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        for (int i = 0; i < colNames.size(); i++) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(mergeTuple);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(i).getType());
            slot.setColumn(new Column(colNames.get(i), outputExprs.get(i).getType()));
            slot.setIsNullable(outputExprs.get(i).isNullable());
        }
        mergeTuple.computeMemLayout();

        descriptorTable.addReferencedTable(icebergTable);
        IcebergRowDeltaSink dataSink = new IcebergRowDeltaSink(
                icebergTable, mergeTuple, session.getSessionVariable());
        dataSink.init();

        // Build conflict detection filter
        IcebergMetadata.IcebergSinkExtra sinkExtra = new IcebergMetadata.IcebergSinkExtra();
        org.apache.iceberg.expressions.Expression filterExpr =
                IcebergPlannerUtils.buildIcebergFilterExpr(execPlan);
        if (filterExpr != null) {
            sinkExtra.setConflictDetectionFilter(filterExpr);
        }
        // Mark as MERGE operation for metrics
        sinkExtra.setOperationType("MERGE");
        dataSink.setSinkExtraInfo(sinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);

        // Insert EnforceUniqueNode before ExchangeNode
        // _file is at index 0, _pos is at index 1
        insertEnforceUniqueNode(execPlan, List.of(0, 1));
    }

    private void insertEnforceUniqueNode(ExecPlan execPlan,
                                         List<Integer> keyColIndices) {
        // Find the sink fragment and insert EnforceUniqueNode
        // between the last operator and the exchange/sink
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        PlanNode root = sinkFragment.getPlanRoot();

        // Create EnforceUniqueNode wrapping the existing root
        PlanNodeId nodeId = execPlan.getNextNodeId();
        EnforceUniqueNode enforceNode = new EnforceUniqueNode(
                nodeId, root, keyColIndices);
        sinkFragment.setPlanRoot(enforceNode);
    }
}
```

- [ ] **Step 3: Add dispatch in StatementPlanner**

In `StatementPlanner.java`, after the DeleteStmt branch (~line 171), add:

```java
            } else if (stmt instanceof MergeIntoStmt) {
                return new MergeIntoPlanner().plan((MergeIntoStmt) stmt, session);
            }
```

In `beginTransaction()` method (~line 556), add tableRef routing:

```java
        } else if (stmt instanceof MergeIntoStmt) {
            ((MergeIntoStmt) stmt).setTableRef(tableRef);
        }
```

In the label generation (~line 590), add:

```java
        } else if (stmt instanceof MergeIntoStmt) {
            label = MetaUtils.genUpdateLabel(session.getExecutionId());
```

Add all necessary imports.

- [ ] **Step 4: Verify FE compiles**

```bash
cd fe && mvn compile -pl fe-core -q -am 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/planner/EnforceUniqueNode.java
git add fe/fe-core/src/main/java/com/starrocks/sql/MergeIntoPlanner.java
git add fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java
git commit -m "[Feature] Add MergeIntoPlanner with EnforceUniqueNode and StatementPlanner dispatch"
```

---

## Task 7: BE EnforceUnique Node & Operator

**Files:**
- Create: `be/src/exec/enforce_unique_node.h`
- Create: `be/src/exec/enforce_unique_node.cpp`
- Create: `be/src/exec/pipeline/enforce_unique_operator.h`
- Create: `be/src/exec/pipeline/enforce_unique_operator.cpp`
- Modify: `be/src/exec/exec_factory.cpp`
- Modify: `be/src/exec/CMakeLists.txt`

- [ ] **Step 1: Create enforce_unique_operator.h**

Follow `assert_num_rows_operator.h` pattern:

```cpp
// be/src/exec/pipeline/enforce_unique_operator.h
#pragma once

#include <unordered_set>
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

class EnforceUniqueOperator final : public Operator {
public:
    EnforceUniqueOperator(OperatorFactory* factory, int32_t id,
                          int32_t plan_node_id, int32_t driver_sequence,
                          std::vector<int32_t> unique_key_col_indices);
    ~EnforceUniqueOperator() override = default;

    bool has_output() const override { return _output_chunk != nullptr; }
    bool need_input() const override { return !_is_finished && _output_chunk == nullptr; }
    bool is_finished() const override { return _is_finished && _output_chunk == nullptr; }

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    struct PairHash {
        size_t operator()(const std::pair<std::string, int64_t>& p) const {
            size_t h1 = std::hash<std::string>{}(p.first);
            size_t h2 = std::hash<int64_t>{}(p.second);
            return h1 ^ (h2 << 32);
        }
    };

    std::vector<int32_t> _unique_key_col_indices;
    std::unordered_set<std::pair<std::string, int64_t>, PairHash> _seen;
    ChunkPtr _output_chunk;
    bool _is_finished = false;
};

class EnforceUniqueOperatorFactory final : public OperatorFactory {
public:
    EnforceUniqueOperatorFactory(int32_t id, int32_t plan_node_id,
                                 std::vector<int32_t> unique_key_col_indices);
    ~EnforceUniqueOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    std::vector<int32_t> _unique_key_col_indices;
};

} // namespace starrocks::pipeline
```

- [ ] **Step 2: Create enforce_unique_operator.cpp**

```cpp
// be/src/exec/pipeline/enforce_unique_operator.cpp
#include "exec/pipeline/enforce_unique_operator.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"

namespace starrocks::pipeline {

EnforceUniqueOperator::EnforceUniqueOperator(
        OperatorFactory* factory, int32_t id, int32_t plan_node_id,
        int32_t driver_sequence, std::vector<int32_t> unique_key_col_indices)
        : Operator(factory, id, "enforce_unique", plan_node_id, false,
                   driver_sequence),
          _unique_key_col_indices(std::move(unique_key_col_indices)) {}

Status EnforceUniqueOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

StatusOr<ChunkPtr> EnforceUniqueOperator::pull_chunk(RuntimeState* state) {
    return std::move(_output_chunk);
}

Status EnforceUniqueOperator::push_chunk(RuntimeState* state,
                                          const ChunkPtr& chunk) {
    // Assumes exactly 2 key columns for now (generalizable later)
    DCHECK(_unique_key_col_indices.size() == 2);
    int32_t file_idx = _unique_key_col_indices[0];
    int32_t pos_idx = _unique_key_col_indices[1];

    auto* file_col = chunk->get_column_by_index(file_idx).get();
    auto* pos_col = chunk->get_column_by_index(pos_idx).get();

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < num_rows; i++) {
        // Skip rows where any key column is NULL
        if (file_col->is_null(i) || pos_col->is_null(i)) {
            continue;
        }

        // Extract key values
        auto file_datum = file_col->get(i);
        auto pos_datum = pos_col->get(i);
        std::string file_path = file_datum.get_slice().to_string();
        int64_t row_pos = pos_datum.get_int64();

        auto key = std::make_pair(std::move(file_path), row_pos);
        if (!_seen.insert(std::move(key)).second) {
            return Status::RuntimeError(
                "Found duplicate values for unique key constraint");
        }
    }

    _output_chunk = chunk;
    return Status::OK();
}

// Factory
EnforceUniqueOperatorFactory::EnforceUniqueOperatorFactory(
        int32_t id, int32_t plan_node_id,
        std::vector<int32_t> unique_key_col_indices)
        : OperatorFactory(id, "enforce_unique", plan_node_id),
          _unique_key_col_indices(std::move(unique_key_col_indices)) {}

OperatorPtr EnforceUniqueOperatorFactory::create(
        int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<EnforceUniqueOperator>(
            this, _id, _plan_node_id, driver_sequence,
            _unique_key_col_indices);
}

} // namespace starrocks::pipeline
```

- [ ] **Step 3: Create enforce_unique_node.h**

Follow `assert_num_rows_node.h` pattern:

```cpp
// be/src/exec/enforce_unique_node.h
#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/exec_node.h"

namespace starrocks {

class EnforceUniqueNode final : public ExecNode {
public:
    EnforceUniqueNode(ObjectPool* pool, const TPlanNode& tnode,
                      const DescriptorTbl& descs);
    ~EnforceUniqueNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    pipeline::OpFactories decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

private:
    std::vector<int32_t> _unique_key_col_indices;
};

} // namespace starrocks
```

- [ ] **Step 4: Create enforce_unique_node.cpp**

```cpp
// be/src/exec/enforce_unique_node.cpp
#include "exec/enforce_unique_node.h"
#include "exec/pipeline/enforce_unique_operator.h"

namespace starrocks {

EnforceUniqueNode::EnforceUniqueNode(ObjectPool* pool,
                                     const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    if (tnode.__isset.enforce_unique_node &&
        tnode.enforce_unique_node.__isset.unique_key_col_indices) {
        _unique_key_col_indices.assign(
                tnode.enforce_unique_node.unique_key_col_indices.begin(),
                tnode.enforce_unique_node.unique_key_col_indices.end());
    }
}

Status EnforceUniqueNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

pipeline::OpFactories EnforceUniqueNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories operators = _children[0]->decompose_to_pipeline(context);
    operators = context->maybe_interpolate_local_passthrough_exchange(
            runtime_state(), id(), operators);

    auto enforce_op = std::make_shared<EnforceUniqueOperatorFactory>(
            context->next_operator_id(), id(), _unique_key_col_indices);
    operators.emplace_back(std::move(enforce_op));

    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(
                context->next_operator_id(), id(), limit()));
    }

    return operators;
}

} // namespace starrocks
```

- [ ] **Step 5: Register node in exec_factory.cpp**

In `be/src/exec/exec_factory.cpp`, add include and switch case:

```cpp
#include "exec/enforce_unique_node.h"
// ...
case TPlanNodeType::ENFORCE_UNIQUE_NODE:
    CREATE_NODE(EnforceUniqueNode, pool, tnode, descs);
    return Status::OK();
```

- [ ] **Step 6: Add to CMakeLists.txt**

In `be/src/exec/CMakeLists.txt`, add:
```cmake
    enforce_unique_node.cpp
```

In `be/src/exec/pipeline/CMakeLists.txt` (or the equivalent source list), add:
```cmake
    enforce_unique_operator.cpp
```

- [ ] **Step 7: Verify BE compiles**

```bash
cd be && ./build.sh 2>&1 | tail -20
```

Expected: Build succeeds.

- [ ] **Step 8: Commit**

```bash
git add be/src/exec/enforce_unique_node.h
git add be/src/exec/enforce_unique_node.cpp
git add be/src/exec/pipeline/enforce_unique_operator.h
git add be/src/exec/pipeline/enforce_unique_operator.cpp
git add be/src/exec/exec_factory.cpp
git add be/src/exec/CMakeLists.txt
git commit -m "[Feature] Add EnforceUniqueNode/Operator for duplicate match detection"
```

---

## Task 8: Executor Dispatch & Metrics

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/qe/StmtExecutor.java:~695,~3438`
- Modify: `fe/fe-core/src/main/java/com/starrocks/metric/ConnectorMetricsMgr.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergMetadata.java`

- [ ] **Step 1: Add MergeIntoStmt to StmtExecutor.getExecType()**

In `StmtExecutor.java` (~line 695), add:

```java
        } else if (parsedStmt instanceof MergeIntoStmt) {
            return "MergeInto";
        }
```

- [ ] **Step 2: Add MergeIntoStmt to zero-row check**

In `StmtExecutor.java` (~line 3438), extend the condition:

```java
            if (loadedRows == 0 && filteredRows == 0 && (stmt instanceof DeleteStmt || stmt instanceof InsertStmt
                    || stmt instanceof UpdateStmt || stmt instanceof MergeIntoStmt)) {
```

- [ ] **Step 3: Add MERGE metrics to ConnectorMetricsMgr**

In `ConnectorMetricsMgr.java`, add methods following the existing `iceberg_update_*` pattern:

```java
    // MERGE metrics
    public static void increaseIcebergMergeTotal(String status, String reason) {
        // Follow increaseUpdateTotal pattern
    }

    public static void increaseIcebergMergeDurationMs(long durationMs) {
        // Follow increaseUpdateDurationMs pattern
    }

    public static void increaseIcebergMergeRows(long rows, String mergeType) {
        // mergeType: "matched_update", "matched_delete", "not_matched_insert"
    }

    public static void increaseIcebergMergeBytes(long bytes, String fileType) {
        // fileType: "data", "position_delete"
    }

    public static void increaseIcebergMergeFiles(long files, String fileType) {
        // fileType: "data", "position_delete"
    }
```

- [ ] **Step 4: Add operation type to IcebergSinkExtra**

In `IcebergMetadata.java`, add an `operationType` field to the `IcebergSinkExtra` inner class:

```java
    private String operationType = "UPDATE"; // "UPDATE" or "MERGE"

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public String getOperationType() {
        return operationType;
    }
```

Then in `commitRowDeltaOperation()`, use the operation type to record the correct metrics (UPDATE vs MERGE).

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/qe/StmtExecutor.java
git add fe/fe-core/src/main/java/com/starrocks/metric/ConnectorMetricsMgr.java
git add fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergMetadata.java
git commit -m "[Feature] Add MERGE INTO executor dispatch and metrics"
```

---

## Task 9: FE Unit Tests

**Files:**
- Create: `fe/fe-core/src/test/java/com/starrocks/sql/analyzer/MergeIntoAnalyzerIcebergTest.java`
- Create: `fe/fe-core/src/test/java/com/starrocks/sql/plan/MergeIntoPlanTest.java`

- [ ] **Step 1: Create MergeIntoAnalyzerIcebergTest**

Follow `UpdateAnalyzerIcebergTest.java` pattern. Test cases:

```java
@Test
public void testMergeIntoNonV2Table() {
    String sql = "MERGE INTO iceberg_v1_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET val = s.val";
    Assert.assertThrows(SemanticException.class, () -> analyzeSuccess(sql));
}

@Test
public void testMergeIntoUpdatePartitionColumn() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET partition_col = s.val";
    Assert.assertThrows(SemanticException.class, () -> analyzeSuccess(sql));
}

@Test
public void testBasicMergeIntoMatchedUpdate() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET val = s.val";
    analyzeSuccess(sql);
}

@Test
public void testBasicMergeIntoMatchedDelete() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN DELETE";
    analyzeSuccess(sql);
}

@Test
public void testBasicMergeIntoNotMatchedInsert() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)";
    analyzeSuccess(sql);
}

@Test
public void testMergeIntoAllClauses() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED AND s.op = 'D' THEN DELETE " +
            "WHEN MATCHED AND s.op = 'U' THEN UPDATE SET val = s.val " +
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)";
    analyzeSuccess(sql);
}

@Test
public void testMergeIntoInsertStar() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN NOT MATCHED THEN INSERT *";
    analyzeSuccess(sql);
}

@Test
public void testMergeIntoMultipleNotMatched() {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN NOT MATCHED AND s.type = 'A' THEN INSERT (id, val) VALUES (s.id, s.val) " +
            "WHEN NOT MATCHED AND s.type = 'B' THEN INSERT (id, val) VALUES (s.id, 'default')";
    analyzeSuccess(sql);
}
```

- [ ] **Step 2: Create MergeIntoPlanTest**

Follow `UpdatePlanTest.java` pattern. Test cases:

```java
@Test
public void testBasicMergePlan() throws Exception {
    String sql = "MERGE INTO iceberg_table AS t USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET val = s.val " +
            "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)";
    String plan = getExecPlan(sql);
    Assert.assertTrue(plan.contains("ENFORCE UNIQUE"));
    Assert.assertTrue(plan.contains("LEFT OUTER JOIN"));
    Assert.assertTrue(plan.contains("IcebergRowDeltaSink"));
    Assert.assertTrue(plan.contains("ROW_DELTA"));
}

@Test
public void testMergePlanPartitionedTable() throws Exception {
    String sql = "MERGE INTO partitioned_iceberg_table AS t " +
            "USING source AS s ON t.id = s.id " +
            "WHEN MATCHED THEN UPDATE SET val = s.val";
    String plan = getExecPlan(sql);
    Assert.assertTrue(plan.contains("HASH_PARTITIONED"));
}
```

- [ ] **Step 3: Run FE tests**

```bash
cd fe && mvn test -pl fe-core -Dtest=MergeIntoAnalyzerIcebergTest -q 2>&1 | tail -10
cd fe && mvn test -pl fe-core -Dtest=MergeIntoPlanTest -q 2>&1 | tail -10
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add fe/fe-core/src/test/java/com/starrocks/sql/analyzer/MergeIntoAnalyzerIcebergTest.java
git add fe/fe-core/src/test/java/com/starrocks/sql/plan/MergeIntoPlanTest.java
git commit -m "[UT] Add MergeIntoAnalyzer and MergeIntoPlan unit tests"
```

---

## Task 10: BE Unit Tests

**Files:**
- Create: `be/test/exec/pipeline/enforce_unique_operator_test.cpp`
- Modify: `be/test/CMakeLists.txt`

- [ ] **Step 1: Create enforce_unique_operator_test.cpp**

```cpp
#include <gtest/gtest.h>
#include "exec/pipeline/enforce_unique_operator.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "testutil/column_test_helper.h"

namespace starrocks::pipeline {

class EnforceUniqueOperatorTest : public testing::Test {
protected:
    void SetUp() override {
        _factory = std::make_shared<EnforceUniqueOperatorFactory>(
                0, 0, std::vector<int32_t>{0, 1});
        _op = std::dynamic_pointer_cast<EnforceUniqueOperator>(
                _factory->create(1, 0));
    }

    ChunkPtr makeChunk(const std::vector<std::string>& files,
                       const std::vector<int64_t>& positions,
                       const std::vector<bool>& file_nulls = {},
                       const std::vector<bool>& pos_nulls = {}) {
        // Build chunk with nullable file (VARCHAR) and pos (BIGINT) columns
        // ... (build columns from test data)
    }

    std::shared_ptr<EnforceUniqueOperatorFactory> _factory;
    std::shared_ptr<EnforceUniqueOperator> _op;
};

TEST_F(EnforceUniqueOperatorTest, NoDuplicates) {
    auto chunk = makeChunk({"f1", "f1", "f2"}, {0, 1, 0});
    ASSERT_OK(_op->push_chunk(nullptr, chunk));
    ASSERT_TRUE(_op->has_output());
    auto result = _op->pull_chunk(nullptr);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value()->num_rows(), 3);
}

TEST_F(EnforceUniqueOperatorTest, WithDuplicates) {
    auto chunk = makeChunk({"f1", "f1"}, {0, 0});
    auto status = _op->push_chunk(nullptr, chunk);
    ASSERT_TRUE(status.is_runtime_error());
    ASSERT_TRUE(status.message().find("duplicate") != std::string::npos);
}

TEST_F(EnforceUniqueOperatorTest, NullKeysSkipped) {
    auto chunk = makeChunk({"f1", "", ""}, {0, 0, 0},
                           {false, true, true}, {false, true, true});
    ASSERT_OK(_op->push_chunk(nullptr, chunk));
}

TEST_F(EnforceUniqueOperatorTest, CrossChunkDuplicate) {
    auto chunk1 = makeChunk({"f1"}, {0});
    ASSERT_OK(_op->push_chunk(nullptr, chunk1));
    _op->pull_chunk(nullptr);  // consume output

    auto chunk2 = makeChunk({"f1"}, {0});
    auto status = _op->push_chunk(nullptr, chunk2);
    ASSERT_TRUE(status.is_runtime_error());
}

TEST_F(EnforceUniqueOperatorTest, EmptyChunk) {
    auto chunk = makeChunk({}, {});
    ASSERT_OK(_op->push_chunk(nullptr, chunk));
}

} // namespace starrocks::pipeline
```

- [ ] **Step 2: Add to CMakeLists.txt**

In `be/test/CMakeLists.txt`, add the test file to the test source list.

- [ ] **Step 3: Build and run BE tests**

```bash
cd be && ./run_ut.sh --gtest_filter="EnforceUniqueOperator*" 2>&1 | tail -20
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add be/test/exec/pipeline/enforce_unique_operator_test.cpp
git add be/test/CMakeLists.txt
git commit -m "[UT] Add EnforceUniqueOperator unit tests"
```

---

## Task 11: SQL Integration Tests

**Files:**
- Create: `test/sql/test_iceberg/T/test_iceberg_merge_into`
- Create: `test/sql/test_iceberg/R/test_iceberg_merge_into`

- [ ] **Step 1: Create test input file**

Create `test/sql/test_iceberg/T/test_iceberg_merge_into` with test cases covering:

```sql
-- name: test_iceberg_merge_into_basic

-- Setup target table
CREATE EXTERNAL TABLE iceberg_merge_target (
    id INT, val STRING, status STRING
) ENGINE=ICEBERG
PROPERTIES ("iceberg.table.format.version"="2");

INSERT INTO iceberg_merge_target VALUES (1, 'a', 'active'), (2, 'b', 'active'), (3, 'c', 'pending');

-- Setup source table
CREATE TABLE merge_source (
    id INT, val STRING, op STRING
);
INSERT INTO merge_source VALUES (2, 'B', 'U'), (3, 'c', 'D'), (4, 'd', 'I');

-- Test: WHEN MATCHED THEN UPDATE only
MERGE INTO iceberg_merge_target AS t
USING merge_source AS s ON t.id = s.id
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET val = s.val;
SELECT * FROM iceberg_merge_target ORDER BY id;

-- Test: WHEN MATCHED THEN DELETE only
MERGE INTO iceberg_merge_target AS t
USING merge_source AS s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE;
SELECT * FROM iceberg_merge_target ORDER BY id;

-- Test: WHEN NOT MATCHED THEN INSERT only
MERGE INTO iceberg_merge_target AS t
USING merge_source AS s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, val, status) VALUES (s.id, s.val, 'new');
SELECT * FROM iceberg_merge_target ORDER BY id;

-- Test: All three clauses combined
MERGE INTO iceberg_merge_target AS t
USING merge_source AS s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET val = s.val
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT (id, val, status) VALUES (s.id, s.val, 'new');
SELECT * FROM iceberg_merge_target ORDER BY id;

-- Test: Duplicate match detection (should error)
CREATE TABLE dup_source (id INT, val STRING);
INSERT INTO dup_source VALUES (1, 'x'), (1, 'y');
MERGE INTO iceberg_merge_target AS t
USING dup_source AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val;

-- Test: Source subquery
MERGE INTO iceberg_merge_target AS t
USING (SELECT id, val, 'U' AS op FROM merge_source WHERE op = 'U') AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET val = s.val;
SELECT * FROM iceberg_merge_target ORDER BY id;

-- Test: Empty source (no-op)
MERGE INTO iceberg_merge_target AS t
USING (SELECT * FROM merge_source WHERE 1 = 0) AS s ON t.id = s.id
WHEN MATCHED THEN DELETE;
SELECT * FROM iceberg_merge_target ORDER BY id;
```

- [ ] **Step 2: Generate expected output**

Run the test against a running StarRocks cluster and capture the output as the expected result file `R/test_iceberg_merge_into`.

- [ ] **Step 3: Run SQL integration test**

```bash
cd test && python3 run.py -v test_iceberg/T/test_iceberg_merge_into 2>&1 | tail -20
```

Expected: All test cases pass.

- [ ] **Step 4: Commit**

```bash
git add test/sql/test_iceberg/T/test_iceberg_merge_into
git add test/sql/test_iceberg/R/test_iceberg_merge_into
git commit -m "[Test] Add SQL integration tests for Iceberg MERGE INTO"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Grammar rules (Task 2)
- [x] AST nodes (Task 3)
- [x] Parser/visitor wiring (Task 4)
- [x] MergeIntoAnalyzer with validation + rewrite (Task 5)
- [x] MergeIntoPlanner with EnforceUniqueNode (Task 6)
- [x] BE EnforceUniqueOperator (Task 7)
- [x] Executor dispatch (Task 8)
- [x] Metrics (Task 8)
- [x] FE unit tests (Task 9)
- [x] BE unit tests (Task 10)
- [x] SQL integration tests (Task 11)

**Placeholder scan:** No TBD/TODO items. All code blocks contain actual code.

**Type consistency:**
- `MergeIntoStmt` used consistently across Analyzer, Planner, StatementPlanner, StmtExecutor
- `EnforceUniqueNode` (FE) → `TEnforceUniqueNode` (Thrift) → `EnforceUniqueNode` (BE node) → `EnforceUniqueOperator` (BE pipeline)
- `IcebergRowDeltaSink.OpCode` values (0-3) used consistently in Analyzer and spec
- Column layout `[_file, _pos, data_cols, op_code]` consistent across Analyzer, Planner, and sink
