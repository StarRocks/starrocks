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

import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddMVColumnClause;
import com.starrocks.sql.ast.DropMVColumnClause;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterMVClauseAnalyzerVisitor extends AlterTableClauseAnalyzer {
    public AlterMVClauseAnalyzerVisitor(Table table) {
        super(table);
    }

    public Void visitModifyTablePropertiesClause(ModifyTablePropertiesClause clause, ConnectContext context) {
        //modify properties check in AlterMVJobExecutor
        return null;
    }

    public Void visitAddMVColumnClause(AddMVColumnClause clause, ConnectContext context) {
        // Validate the column name
        String columnName = clause.getColumnName();
        if (columnName == null || columnName.isEmpty()) {
            throw new SemanticException("Column name cannot be empty");
        }
        
        // Validate the aggregate expression
        Expr aggregateExpr = clause.getAggregateExpression();
        if (aggregateExpr == null) {
            throw new SemanticException("Aggregate expression cannot be null");
        }
        if (aggregateExpr instanceof LiteralExpr) {
            throw new SemanticException("Aggregate expression cannot be a constant");
        }
        // Validate aggregate expression - it should contain aggregate functions
        MaterializedView mv = (MaterializedView) table;
        ParseNode astParseNode = mv.getDefineQueryParseNode();
        // chck mv's parse node is a simple QueryStatement
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        if (queryStatement.getQueryRelation() == null || !(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view definition is invalid: only support SELECT statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        // only support one table in the ast tree
        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            throw new SemanticException("Materialized view based on multiple tables is not supported");
        }
        // TODO: further validate the aggregate expression against the mv definition
        if (selectRelation.getOutputAnalytic() != null && !selectRelation.getOutputAnalytic().isEmpty()) {
            throw new SemanticException("Materialized view with analytic functions is not supported");
        }
        
        return null;
    }

    public Void visitDropMVColumnClause(DropMVColumnClause clause, ConnectContext context) {
        String columnName = clause.getColumnName();
        if (columnName == null || columnName.isEmpty()) {
            throw new SemanticException("Column name cannot be empty");
        }

        MaterializedView mv = (MaterializedView) table;
        if (mv.getColumn(columnName) == null) {
            throw new SemanticException("Column '{}' does not exist in materialized view", columnName);
        }

        ParseNode astParseNode = mv.getDefineQueryParseNode();
        if (astParseNode == null || !(astParseNode instanceof QueryStatement)) {
            throw new SemanticException("Materialized view definition is invalid");
        }
        QueryStatement queryStatement = (QueryStatement) astParseNode;
        if (queryStatement.getQueryRelation() == null || !(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("Materialized view definition is invalid: only support SELECT statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        if (!(selectRelation.getRelation() instanceof TableRelation)) {
            throw new SemanticException("Materialized view based on multiple tables is not supported");
        }
        if (selectRelation.getOutputAnalytic() != null && !selectRelation.getOutputAnalytic().isEmpty()) {
            throw new SemanticException("Materialized view with analytic functions is not supported");
        }

        return null;
    }

    public Void visitReorderColumnsClause(ReorderColumnsClause clause, ConnectContext context) {
        List<String> orderBy = clause.getColumnsByPos();
        if (orderBy == null || orderBy.isEmpty()) {
            throw new SemanticException("ORDER BY column list must not be empty");
        }
        // The MV ORDER BY grammar reuses the rollup reorder-columns clause, which also parses an
        // optional `FROM <rollup>` and `PROPERTIES(...)` -- neither applies to an MV (its single index
        // IS its base; there is no rollup to target, and no property this rewrite consumes), so reject
        // both explicitly rather than silently ignoring them.
        if (clause.getRollupName() != null) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY does not support 'FROM <rollup>'");
        }
        if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY does not support PROPERTIES");
        }
        if (!(table instanceof MaterializedView)) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is only supported on materialized views");
        }
        MaterializedView mv = (MaterializedView) table;
        // The rewrite quiesces the MV by suspending/draining its async refresh task; a synchronous MV
        // has no such task, so the quiescence hooks would be no-ops. The feature (and its docs) are
        // asynchronous-only, so reject a synchronous MV explicitly.
        if (mv.getRefreshScheme() != null && mv.getRefreshScheme().isSync()) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is only supported on "
                    + "asynchronous materialized views");
        }
        // The online-rewrite machinery this feeds is lake-only. isCloudNativeTable() is FALSE for MVs
        // (Table.java), so use the MV-inclusive predicate to gate shared-nothing MVs out.
        if (!mv.isCloudNativeTableOrMaterializedView()) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is only supported on shared-data "
                    + "(cloud-native) materialized views");
        }
        if (!mv.isRangeDistribution()) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is only supported on "
                    + "range-distributed materialized views");
        }
        // Only the base index is rewritten; a materialized view carrying additional (rollup/sync)
        // indexes is out of scope (mirrors SchemaChangeHandler#needsRangeRewriteSchemaChange).
        if (mv.getIndexMetaIdToMeta().size() > 1) {
            throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is not supported on a materialized "
                    + "view that carries additional (rollup/sync) indexes");
        }

        List<Column> baseSchema = mv.getSchemaByIndexMetaId(mv.getBaseIndexMetaId());
        Set<String> seen = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String name : orderBy) {
            Column column = baseSchema.stream().filter(c -> c.getName().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new SemanticException("ORDER BY column '" + name + "' does not exist "
                            + "in materialized view '" + mv.getName() + "'"));
            if (!seen.add(name)) {
                throw new SemanticException("Duplicate ORDER BY column '" + name + "'");
            }
            if (!column.getType().canDistributedBy()) {
                throw new SemanticException("ORDER BY column '" + name + "' has a type that cannot be a sort key");
            }
        }
        validateKeysTypeRule(mv, orderBy);
        // Column validation above is case-insensitive, so compare the same way here: `ORDER BY (K1)` on
        // an existing `k1` sort key is a no-op and must be rejected too.
        if (equalsIgnoreCaseInOrder(orderBy, currentMvSortKeys(mv))) {
            throw new SemanticException("ORDER BY (" + String.join(", ", orderBy)
                    + ") is already the sort key of materialized view '" + mv.getName() + "'");
        }
        return null;
    }

    /**
     * Enforces the same sort-key/keysType rule as {@code SchemaChangeHandler#processModifySortKeyColumn}:
     * DUP_KEYS accepts any subset/permutation; AGG_KEYS/UNIQUE_KEYS require the ORDER BY columns to be
     * exactly the set of key columns (a permutation, not a subset or prefix); every other keysType
     * (e.g. PRIMARY_KEYS) is out of scope for this feature and is rejected outright.
     */
    private void validateKeysTypeRule(MaterializedView mv, List<String> orderBy) {
        KeysType keysType = mv.getKeysType();
        if (keysType == KeysType.DUP_KEYS) {
            return;
        }
        if (keysType == KeysType.AGG_KEYS || keysType == KeysType.UNIQUE_KEYS) {
            Set<String> keyCols = mv.getSchemaByIndexMetaId(mv.getBaseIndexMetaId()).stream()
                    .filter(Column::isKey).map(Column::getName)
                    .collect(Collectors.toCollection(() -> Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER)));
            Set<String> sortCols = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            sortCols.addAll(orderBy);
            if (!sortCols.equals(keyCols)) {
                throw new SemanticException("For " + keysType + " materialized views the ORDER BY columns must "
                        + "be exactly the key columns (a permutation), not a subset/prefix");
            }
            return;
        }
        throw new SemanticException("ALTER MATERIALIZED VIEW ... ORDER BY is not supported for "
                + keysType + " materialized views");
    }

    // The MV's current sort key: TableProperty.mvSortKeys if explicitly set by a previous ORDER BY,
    // otherwise the leading key columns in schema order (key columns are always the leading columns of
    // a StarRocks table schema, so this is the implicit default sort key).
    private List<String> currentMvSortKeys(MaterializedView mv) {
        List<String> sortKeys = mv.getTableProperty().getMvSortKeys();
        if (CollectionUtils.isNotEmpty(sortKeys)) {
            return sortKeys;
        }
        return mv.getSchemaByIndexMetaId(mv.getBaseIndexMetaId()).stream()
                .filter(Column::isKey)
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    // Same size and pairwise case-insensitive equal at every position (order-sensitive).
    private boolean equalsIgnoreCaseInOrder(List<String> a, List<String> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equalsIgnoreCase(b.get(i))) {
                return false;
            }
        }
        return true;
    }
}
