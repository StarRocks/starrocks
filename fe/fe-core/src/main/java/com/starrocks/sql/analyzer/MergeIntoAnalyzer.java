// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.MergeWhenClause;
import com.starrocks.sql.ast.MergeWhenMatchedDeleteClause;
import com.starrocks.sql.ast.MergeWhenMatchedUpdateClause;
import com.starrocks.sql.ast.MergeWhenNotMatchedInsertClause;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.IntegerType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MergeIntoAnalyzer {

    public static void analyze(MergeIntoStmt stmt, ConnectContext session) {
        // Resolve table — same pattern as UpdateAnalyzer.analyze()
        TableRef tableRef = AnalyzerUtils.normalizedTableRef(stmt.getTableRef(), session);
        stmt.setTableRef(tableRef);
        TableName tableName = TableName.fromTableRef(tableRef);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(session, tableName.getCatalog(), tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not found", tableName.getCatalogAndDb());
        }
        Table table = MetaUtils.getSessionAwareTable(session, null, tableName);

        if (table instanceof IcebergTable icebergTable) {
            analyzeIcebergTable(stmt, icebergTable, tableName, session);
        } else {
            throw new SemanticException("MERGE INTO is only supported for Iceberg tables");
        }
    }

    private static void analyzeIcebergTable(MergeIntoStmt stmt, IcebergTable icebergTable,
                                            TableName tableName, ConnectContext session) {
        // ---- VALIDATIONS ----

        // 1. V2 check
        if (icebergTable.getFormatVersion() != 2) {
            throw new SemanticException(
                    "MERGE INTO is only supported for Iceberg V2 tables, but table format version is "
                            + icebergTable.getFormatVersion());
        }

        // 2. At least one WHEN clause
        if (stmt.getWhenClauses().isEmpty()) {
            throw new SemanticException("MERGE INTO requires at least one WHEN clause");
        }
        validateWhenClauseOrder(stmt.getWhenClauses());

        // 3. Validate MATCHED UPDATE clauses: no partition column updates, no hidden column updates
        List<Column> partitionColumns = icebergTable.getPartitionColumns().stream()
                .filter(Objects::nonNull).toList();
        for (MergeWhenClause clause : stmt.getWhenClauses()) {
            if (clause instanceof MergeWhenMatchedUpdateClause updateClause) {
                Map<String, ColumnAssignment> assignmentByColName = new HashMap<>();
                for (ColumnAssignment assign : updateClause.getAssignments()) {
                    assignmentByColName.put(assign.getColumn().toLowerCase(), assign);
                }
                // Reject partition column updates
                for (Column partitionCol : partitionColumns) {
                    if (assignmentByColName.containsKey(partitionCol.getName().toLowerCase())) {
                        throw new SemanticException(
                                "MERGE INTO does not support updating partition column: " + partitionCol.getName());
                    }
                }
                // Validate the surviving (last-assignment-wins) per-column assignment:
                // column must exist, must not be hidden, and Iceberg V2 has no
                // supported defaults. Iterating the map (not updateClause's original
                // list) is intentional — "SET col = DEFAULT, col = expr" must pass
                // because only the final expr is written, matching the last-wins
                // semantics documented on getMatchedColumnValue below.
                for (Map.Entry<String, ColumnAssignment> entry : assignmentByColName.entrySet()) {
                    String colName = entry.getKey();
                    Column col = icebergTable.getColumn(colName);
                    if (col == null) {
                        throw new SemanticException("table '%s' does not have column '%s'",
                                tableName.getTbl(), colName);
                    }
                    if (col.isHidden()) {
                        throw new SemanticException("Updating metadata column '%s' is not allowed", colName);
                    }
                    if (entry.getValue().getExpr() instanceof DefaultValueExpr) {
                        throw new SemanticException(
                                "DEFAULT value is not supported for Iceberg V2 tables");
                    }
                }
            }
        }

        // 4. Validate NOT MATCHED INSERT clauses: column count == values count
        int targetDataColumnCount = (int) icebergTable.getBaseSchema().stream()
                .filter(col -> !col.isHidden()).count();
        for (MergeWhenClause clause : stmt.getWhenClauses()) {
            if (clause instanceof MergeWhenNotMatchedInsertClause insertClause) {
                if (!insertClause.isStar()) {
                    List<String> targetCols = insertClause.getTargetColumnNames();
                    List<Expr> values = insertClause.getValues();
                    if (targetCols != null && values != null && targetCols.size() != values.size()) {
                        throw new SemanticException(
                                "MERGE INTO INSERT column count %d does not match value count %d",
                                targetCols.size(), values.size());
                    }
                    // Positional VALUES without column list: value count must match schema
                    if (targetCols == null && values != null && values.size() != targetDataColumnCount) {
                        throw new SemanticException(
                                "MERGE INTO INSERT VALUES count %d does not match target table column count %d",
                                values.size(), targetDataColumnCount);
                    }
                    // Validate target column names exist and are unique.
                    // Uniqueness matches plain INSERT (InsertAnalyzer uses
                    // the same ErrorCode.ERR_DUP_FIELDNAME); without this check the
                    // emission layer (getNotMatchedColumnValue) would silently keep
                    // the FIRST value for a duplicated column and discard later ones.
                    if (targetCols != null) {
                        Set<String> seen = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                        for (String colName : targetCols) {
                            Column col = icebergTable.getColumn(colName);
                            if (col == null) {
                                throw new SemanticException("table '%s' does not have column '%s'",
                                        tableName.getTbl(), colName);
                            }
                            if (col.isHidden()) {
                                throw new SemanticException(
                                        "Inserting into metadata column '%s' is not allowed", colName);
                            }
                            if (!seen.add(colName)) {
                                ErrorReport.reportSemanticException(
                                        ErrorCode.ERR_DUP_FIELDNAME, colName);
                            }
                        }
                        // Reject omission of a non-null target column: Iceberg V2 has
                        // no defaults, so getNotMatchedColumnValue's NULL fallback
                        // would either fail at sink time or write an invalid row.
                        for (Column dataCol : icebergTable.getBaseSchema()) {
                            if (dataCol.isHidden()) {
                                continue;
                            }
                            if (!dataCol.isAllowNull() && !seen.contains(dataCol.getName())) {
                                throw new SemanticException(
                                        "Column '%s' is not nullable and has no default value; " +
                                                "MERGE INTO INSERT must provide a value for it",
                                        dataCol.getName());
                            }
                        }
                    }
                }
            }
        }

        // ---- BUILD SELECT LIST ----
        SelectList selectList = new SelectList();

        // Determine target qualifier for SlotRefs.
        // - With an alias, the alias alone is unambiguous in the join scope.
        // - Without an alias, fully qualify with catalog.db.tbl so the analyzer
        //   does not bind target SlotRefs to a same-named source table living
        //   in a different db/catalog.
        TableName targetSlotTableName = stmt.getTargetAlias() != null
                ? new TableName(null, null, stmt.getTargetAlias())
                : new TableName(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());

        // _file
        selectList.addItem(new SelectListItem(
                new SlotRef(targetSlotTableName, IcebergTable.FILE_PATH), IcebergTable.FILE_PATH));
        // _pos
        selectList.addItem(new SelectListItem(
                new SlotRef(targetSlotTableName, IcebergTable.ROW_POSITION), IcebergTable.ROW_POSITION));

        // Pre-classify WHEN clauses once (avoid re-classifying per column)
        List<MergeWhenClause> matchedClauses = new ArrayList<>();
        List<MergeWhenClause> notMatchedClauses = new ArrayList<>();
        for (MergeWhenClause clause : stmt.getWhenClauses()) {
            if (clause.isMatched()) {
                matchedClauses.add(clause);
            } else {
                notMatchedClauses.add(clause);
            }
        }

        // Data columns
        List<Column> dataColumns = icebergTable.getBaseSchema().stream()
                .filter(col -> !col.isHidden())
                .toList();
        for (Column col : dataColumns) {
            Expr dataExpr = buildDataColumnCaseExpr(
                    stmt, col, targetSlotTableName, matchedClauses, notMatchedClauses, icebergTable);
            selectList.addItem(new SelectListItem(dataExpr, col.getName()));
        }

        // op_code CASE expression — kept OUT of the SELECT list. The analyzer's output
        // is data-only [_file, _pos, data_cols...]; the planner injects op_code as a
        // sink-private physical projection so the routing protocol does not ride on
        // the user-facing SELECT output (mirrors UPDATE PR #73203 routing-decoupling).
        Expr opCodeExpr = buildOpCodeCaseExpr(stmt, targetSlotTableName);

        // ---- BUILD FROM: source LEFT OUTER JOIN target ON merge_condition ----

        // Target table relation (with alias)
        TableRelation targetRelation = new TableRelation(tableName);
        if (stmt.getTargetAlias() != null) {
            targetRelation.setAlias(new TableName(null, null, stmt.getTargetAlias()));
        }

        // Source relation already parsed by grammar — alias is already set by AstBuilder
        Relation sourceRelation = stmt.getSourceRelation();

        // Create LEFT OUTER JOIN: source (left) JOIN target (right) ON merge_condition.
        // Source-side predicate propagation to the target is applied AFTER analysis (below), where
        // resolved slot types are available to gate the derivation on type-equivalent ON keys.
        JoinRelation joinRelation = new JoinRelation(
                JoinOperator.LEFT_OUTER_JOIN,
                sourceRelation,
                targetRelation,
                stmt.getMergeCondition(),
                false
        );
        // Pin the join distribution like IcebergEqualityDeleteRewriteRule does: the
        // duplicate-match check is a local check above this join, which is correct only
        // when the TARGET side is never broadcast/replicated (each target row owned by
        // exactly one instance). The planner re-validates the physical join shape.
        joinRelation.setJoinHint(HintNode.HINT_JOIN_SHUFFLE);

        // Create SELECT ... FROM (join)
        SelectRelation selectRelation = new SelectRelation(
                selectList, joinRelation, null, null, null);

        // Create QueryStatement
        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(stmt.isExplain(), stmt.getExplainLevel());

        // Analyze the query statement
        new QueryAnalyzer(session).analyze(queryStatement);

        // Resolve the op_code routing expression against the join scope. It references
        // target._file plus per-WHEN-clause source predicates, none of which appear as
        // top-level SELECT items, so it has to be analyzed separately.
        ExpressionAnalyzer.analyzeExpression(
                opCodeExpr, new AnalyzeState(), joinRelation.getScope(), session);

        // Cast data columns to target schema types (same as UpdateAnalyzer)
        // Layout: [_file, _pos, data_cols...] — op_code is sink-private, added by planner.
        List<Expr> outputExprs = queryStatement.getQueryRelation().getOutputExpression();
        int dataColumnStart = 2;
        List<Expr> castOutputExprs = Lists.newArrayList(outputExprs);
        for (int i = 0; i < dataColumns.size(); i++) {
            int outputIdx = dataColumnStart + i;
            castOutputExprs.set(outputIdx,
                    TypeManager.addCastExpr(outputExprs.get(outputIdx), dataColumns.get(i).getType()));
        }
        ((SelectRelation) queryStatement.getQueryRelation()).setOutputExpr(castOutputExprs);

        // Save the exact column names from the SELECT list aliases for the planner.
        List<String> colNames = Lists.newArrayList();
        for (int i = 0; i < selectList.getItems().size(); i++) {
            colNames.add(selectList.getItems().get(i).getAlias());
        }

        stmt.setTable(icebergTable);
        stmt.setQueryStatement(queryStatement);
        stmt.setOutputColumnNames(colNames);
        stmt.setRoutingExpr(opCodeExpr);
    }

    /**
     * Build a CASE expression for a data column that handles both MATCHED and NOT MATCHED branches.
     *
     * <pre>
     * CASE
     *     WHEN target._file IS NOT NULL THEN <matched branch value>
     *     ELSE <not matched branch value>
     * END
     * </pre>
     *
     * For the MATCHED branch:
     * - WHEN MATCHED UPDATE: if column is in SET list, use SET expression; else use target.col
     * - WHEN MATCHED DELETE: use target.col (data columns are ignored for deletes by the sink)
     * - If multiple MATCHED clauses: nested CASE by condition
     *
     * For the NOT MATCHED branch:
     * - WHEN NOT MATCHED INSERT: if column is in INSERT list, use corresponding value; else NullLiteral
     * - INSERT *: use source column with same name
     * - If no NOT MATCHED clause: NullLiteral
     */
    private static Expr buildDataColumnCaseExpr(MergeIntoStmt stmt, Column col,
                                                TableName targetSlotTableName,
                                                List<MergeWhenClause> matchedClauses,
                                                List<MergeWhenClause> notMatchedClauses,
                                                IcebergTable icebergTable) {
        // Build matched branch expression
        Expr matchedExpr = buildMatchedDataExpr(matchedClauses, col, targetSlotTableName);

        // Build not-matched branch expression
        Expr notMatchedExpr = buildNotMatchedDataExpr(notMatchedClauses, col, stmt, icebergTable);

        // Optimization: if no matched clauses, we do not need the outer CASE
        // (all rows are inserts). Similarly if no not-matched clauses.
        // But for generality, always wrap in a CASE:
        //   CASE WHEN target._file IS NOT NULL THEN <matched> ELSE <not_matched> END
        Expr targetFileRef = new SlotRef(targetSlotTableName, IcebergTable.FILE_PATH);
        Expr isMatchedCondition = new IsNullPredicate(targetFileRef, true); // IS NOT NULL

        List<CaseWhenClause> whenClauses = new ArrayList<>();
        whenClauses.add(new CaseWhenClause(isMatchedCondition, matchedExpr));

        return new CaseExpr(null, whenClauses, notMatchedExpr);
    }

    /**
     * Build the data expression for MATCHED branches for a given column.
     * If there is one unconditional MATCHED clause, return its value directly.
     * If there are multiple conditional MATCHED clauses, build nested CASE.
     */
    private static Expr buildMatchedDataExpr(List<MergeWhenClause> matchedClauses, Column col,
                                             TableName targetSlotTableName) {
        if (matchedClauses.isEmpty()) {
            // No matched clauses — use target column as fallback (row will be NO_OP)
            return new SlotRef(targetSlotTableName, col.getName());
        }

        if (matchedClauses.size() == 1) {
            MergeWhenClause clause = matchedClauses.get(0);
            Expr value = getMatchedColumnValue(clause, col, targetSlotTableName);
            Expr condition = clause.getOptionalCondition();
            if (condition != null) {
                // Wrap in CASE so the update value is only evaluated when the
                // clause condition holds. Rows that fail the condition are routed
                // as NO_OP via op_code, but the data SELECT still runs against
                // every joined row — without this wrapping, `value` would be
                // evaluated (and could fail or distort types) on rows the action
                // should skip. The condition also enters the data SELECT here,
                // exposing source columns referenced in it to ColumnPrivilege.
                List<CaseWhenClause> whenClauses = new ArrayList<>();
                whenClauses.add(new CaseWhenClause(condition, value));
                return new CaseExpr(null, whenClauses,
                        new SlotRef(targetSlotTableName, col.getName()));
            }
            return value;
        }

        // Multiple matched clauses — build CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ... ELSE target.col END
        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (MergeWhenClause clause : matchedClauses) {
            Expr condition = clause.getOptionalCondition();
            Expr value = getMatchedColumnValue(clause, col, targetSlotTableName);
            if (condition != null) {
                whenClauses.add(new CaseWhenClause(condition, value));
            } else {
                // Unconditional — this should be the last one; use as else
                // If it's last, we break and use it as the else expr
                // But for correctness, treat as a WHEN TRUE THEN value
                // Actually, if unconditional and it's the last clause, we can use it as ELSE.
                // For simplicity and correctness, add it as the else case below.
                // Since this is the last entry, break out and handle as else.
                return new CaseExpr(null, whenClauses, value);
            }
        }
        // All clauses were conditional — use target.col as the fallback (no clause matched = keep original)
        return new CaseExpr(null, whenClauses, new SlotRef(targetSlotTableName, col.getName()));
    }

    /**
     * Get the data column value for a single MATCHED clause.
     */
    private static Expr getMatchedColumnValue(MergeWhenClause clause, Column col,
                                              TableName targetSlotTableName) {
        if (clause instanceof MergeWhenMatchedUpdateClause updateClause) {
            // If the same column is assigned multiple times in one SET list
            // (`SET a = 1, a = 2`), the LAST assignment wins — matches StarRocks
            // native OLAP UPDATE semantics and MySQL's left-to-right rule. The
            // validation layer in analyzeIcebergTable already uses Map.put()
            // (last-wins) when collecting assignmentByColName; this loop must
            // agree, otherwise the two layers emit inconsistent results. Do NOT
            // return on first match — keep scanning to the end.
            Expr lastMatch = null;
            for (ColumnAssignment assign : updateClause.getAssignments()) {
                if (assign.getColumn().equalsIgnoreCase(col.getName())) {
                    lastMatch = assign.getExpr();
                }
            }
            if (lastMatch != null) {
                return lastMatch;
            }
            // Column not in SET list — keep original value from target
            return new SlotRef(targetSlotTableName, col.getName());
        } else if (clause instanceof MergeWhenMatchedDeleteClause) {
            // For DELETE, the data columns are ignored by the sink (op_code=1 means position delete only),
            // but we still need to emit something. Use target column.
            return new SlotRef(targetSlotTableName, col.getName());
        }
        // Should not reach here
        return new SlotRef(targetSlotTableName, col.getName());
    }

    /**
     * Build the data expression for NOT MATCHED branches for a given column.
     */
    private static Expr buildNotMatchedDataExpr(List<MergeWhenClause> notMatchedClauses, Column col,
                                                MergeIntoStmt stmt, IcebergTable icebergTable) {
        if (notMatchedClauses.isEmpty()) {
            return new NullLiteral();
        }

        if (notMatchedClauses.size() == 1) {
            MergeWhenClause clause = notMatchedClauses.get(0);
            Expr value = getNotMatchedColumnValue(
                    (MergeWhenNotMatchedInsertClause) clause, col, stmt, icebergTable);
            Expr condition = clause.getOptionalCondition();
            if (condition != null) {
                // Mirror the matched-side rationale above: gate the insert value on
                // the clause condition so it does not evaluate on NO_OP rows, and
                // surface condition columns to ColumnPrivilege through the SELECT.
                List<CaseWhenClause> whenClauses = new ArrayList<>();
                whenClauses.add(new CaseWhenClause(condition, value));
                return new CaseExpr(null, whenClauses, new NullLiteral());
            }
            return value;
        }

        // Multiple NOT MATCHED clauses — build CASE
        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (MergeWhenClause clause : notMatchedClauses) {
            MergeWhenNotMatchedInsertClause insertClause = (MergeWhenNotMatchedInsertClause) clause;
            Expr condition = clause.getOptionalCondition();
            Expr value = getNotMatchedColumnValue(insertClause, col, stmt, icebergTable);
            if (condition != null) {
                whenClauses.add(new CaseWhenClause(condition, value));
            } else {
                // Unconditional — use as else
                return new CaseExpr(null, whenClauses, value);
            }
        }
        // All clauses were conditional — NULL if none matched
        return new CaseExpr(null, whenClauses, new NullLiteral());
    }

    /**
     * Get the data column value for a single NOT MATCHED INSERT clause.
     */
    private static Expr getNotMatchedColumnValue(MergeWhenNotMatchedInsertClause insertClause,
                                                 Column col, MergeIntoStmt stmt,
                                                 IcebergTable icebergTable) {
        if (insertClause.isStar()) {
            // INSERT * resolves source columns by NAME match against the target
            // column name (not by position). The source must expose a column whose
            // name matches each non-hidden target column; otherwise QueryAnalyzer
            // raises a resolution error on the rewritten SELECT list.
            String sourceQualifier = getInsertStarSourceQualifier(stmt);
            return new SlotRef(new TableName(null, null, sourceQualifier), col.getName());
        }

        List<String> targetColNames = insertClause.getTargetColumnNames();
        List<Expr> values = insertClause.getValues();
        if (targetColNames != null && values != null) {
            // Named column list: INSERT (col1, col2) VALUES (v1, v2)
            for (int i = 0; i < targetColNames.size(); i++) {
                if (targetColNames.get(i).equalsIgnoreCase(col.getName())) {
                    return values.get(i);
                }
            }
        } else if (targetColNames == null && values != null) {
            // Positional VALUES without column list: INSERT VALUES (v1, v2, ...)
            // Map by target schema order
            int colIdx = getColumnIndex(col, icebergTable);
            if (colIdx >= 0 && colIdx < values.size()) {
                return values.get(colIdx);
            }
        }
        // Column not mentioned in INSERT list — use NULL
        return new NullLiteral();
    }

    private static void validateWhenClauseOrder(List<MergeWhenClause> whenClauses) {
        boolean seenUnconditionalMatched = false;
        boolean seenUnconditionalNotMatched = false;
        for (MergeWhenClause clause : whenClauses) {
            if (clause.isMatched()) {
                if (seenUnconditionalMatched) {
                    throw new SemanticException("Unconditional WHEN MATCHED clause must be the last MATCHED clause");
                }
                if (clause.getOptionalCondition() == null) {
                    seenUnconditionalMatched = true;
                }
            } else {
                if (seenUnconditionalNotMatched) {
                    throw new SemanticException(
                            "Unconditional WHEN NOT MATCHED clause must be the last NOT MATCHED clause");
                }
                if (clause.getOptionalCondition() == null) {
                    seenUnconditionalNotMatched = true;
                }
            }
        }
    }

    private static String getInsertStarSourceQualifier(MergeIntoStmt stmt) {
        if (stmt.getSourceAlias() != null) {
            return stmt.getSourceAlias();
        }
        if (stmt.getSourceRelation() != null && stmt.getSourceRelation().getAlias() != null
                && stmt.getSourceRelation().getAlias().getTbl() != null) {
            return stmt.getSourceRelation().getAlias().getTbl();
        }
        if (stmt.getSourceRelation() instanceof TableRelation tr) {
            return tr.getName().getTbl();
        }
        throw new SemanticException("MERGE INTO INSERT * requires an aliased source relation");
    }

    /**
     * Build the op_code CASE expression.
     *
     * <pre>
     * CASE
     *     WHEN target._file IS NOT NULL AND <matched_cond_1> THEN 1 or 2
     *     WHEN target._file IS NOT NULL AND <matched_cond_2> THEN 1 or 2
     *     ...
     *     WHEN target._file IS NULL AND <not_matched_cond_1> THEN 3
     *     ...
     *     ELSE 0  -- NO_OP
     * END
     * </pre>
     */
    private static Expr buildOpCodeCaseExpr(MergeIntoStmt stmt, TableName targetSlotTableName) {
        List<CaseWhenClause> whenClauses = new ArrayList<>();
        for (MergeWhenClause clause : stmt.getWhenClauses()) {
            whenClauses.add(buildOpCodeClause(clause, targetSlotTableName));
        }
        // ELSE 0 (NO_OP) — row matches no WHEN clause, discard
        Expr elseExpr = new IntLiteral(IcebergRowDeltaSink.OpCode.NO_OP.value(), IntegerType.TINYINT);
        return new CaseExpr(null, whenClauses, elseExpr);
    }

    /**
     * Build one CASE WHEN/THEN entry for a single MERGE WHEN clause.
     * A fresh Expr subtree is constructed on every call — ExpressionAnalyzer mutates
     * Expr nodes in place during resolution, so sharing nodes across multiple CASE /
     * CompoundPredicate parents would produce stale typing on re-visit.
     */
    private static CaseWhenClause buildOpCodeClause(MergeWhenClause clause,
                                                    TableName targetSlotTableName) {
        Expr basePredicate;
        int opCode;
        if (clause.isMatched()) {
            // target._file IS NOT NULL — row exists in target
            basePredicate = new IsNullPredicate(
                    new SlotRef(targetSlotTableName, IcebergTable.FILE_PATH), true);
            opCode = (clause instanceof MergeWhenMatchedDeleteClause)
                    ? IcebergRowDeltaSink.OpCode.DELETE.value()
                    : IcebergRowDeltaSink.OpCode.UPDATE.value();
        } else {
            // target._file IS NULL — row absent from target
            basePredicate = new IsNullPredicate(
                    new SlotRef(targetSlotTableName, IcebergTable.FILE_PATH), false);
            opCode = IcebergRowDeltaSink.OpCode.INSERT.value();
        }
        Expr opCodeLiteral = new IntLiteral(opCode, IntegerType.TINYINT);

        Expr condition = clause.getOptionalCondition();
        Expr whenExpr = (condition != null)
                ? new CompoundPredicate(CompoundPredicate.Operator.AND, basePredicate, condition)
                : basePredicate;
        return new CaseWhenClause(whenExpr, opCodeLiteral);
    }

    /**
     * Get the positional index of a column in the Iceberg table's non-hidden schema.
     * Returns -1 if not found.
     */
    private static int getColumnIndex(Column col, IcebergTable icebergTable) {
        List<Column> dataColumns = icebergTable.getBaseSchema().stream()
                .filter(c -> !c.isHidden())
                .toList();
        for (int i = 0; i < dataColumns.size(); i++) {
            if (dataColumns.get(i).getName().equalsIgnoreCase(col.getName())) {
                return i;
            }
        }
        return -1;
    }
}
