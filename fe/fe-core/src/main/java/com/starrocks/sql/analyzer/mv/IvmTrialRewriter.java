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

package com.starrocks.sql.analyzer.mv;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * CREATE-time trial compilation of the IVM refresh plan. Runs the same {@code IvmRewriter}
 * pipeline production refresh uses, but with a mocked target MV (delivered via
 * {@link com.starrocks.sql.optimizer.QueryMaterializationContext#setOverrideTargetMv},
 * never registered in the catalog) and
 * synthetic TVR deltas. Catches rewriter drift the analyzer-level checks can't —
 * unresolved Delta markers from a new operator without a matching {@code IvmDelta*Rule},
 * combinator metadata that no longer matches the BE state-union path, etc.
 *
 * <p>Scope: optimizer rewrite phase only (no fragment build, no thrift serialize).
 */
public final class IvmTrialRewriter {

    private IvmTrialRewriter() {
    }

    // Mock MV lives only inside runTrial and is never compared by id, so fixed sentinels suffice.
    private static final long MOCK_MV_ID = -1L;
    private static final long MOCK_DB_ID = -1L;

    /**
     * Caller must have run {@code IVMAnalyzer.rewriteImpl} first so {@code rewrittenQuery}
     * carries the mutated {@code __ROW_ID__} / {@code __AGG_STATE_*} columns. Throws
     * {@link SemanticException} on rewriter failure so CREATE fails with a clear message.
     */
    public static void runTrial(ConnectContext ctx,
                                CreateMaterializedViewStatement stmt,
                                QueryStatement rewrittenQuery) {
        // Same session-var scoping pattern as MVIVMRefreshProcessor.prepareRefreshPlan
        // (#73004) — leaking enable_ivm_refresh=true would make later optimizer calls run
        // IvmRewriter on non-IVM plans.
        boolean prevIvmEnabled = ctx.getSessionVariable().isEnableIVMRefresh();
        String prevTvrTargetMvId = ctx.getSessionVariable().getTvrTargetMvId();
        try {
            // Bind FunctionRefs/types on the aggregate-state expressions rewriteImpl just
            // introduced; the optimizer below needs a fully-typed AST.
            Analyzer.analyze(rewrittenQuery, ctx);

            MaterializedView mockMv = buildMockMv(stmt, rewrittenQuery);
            applySyntheticTvrDelta(rewrittenQuery);

            ctx.getSessionVariable().setEnableIVMRefresh(true);
            ctx.getSessionVariable().setTvrTargetMvid(GsonUtils.GSON.toJson(mockMv.getMvId()));

            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, ctx)
                    .transformWithSelectLimit(rewrittenQuery.getQueryRelation());

            OptimizerContext optimizerContext = OptimizerFactory.initContext(ctx, columnRefFactory);
            // InsertStmt (not CreateMaterializedViewStatement) so IvmRewriter.isPrimaryKeyTargetMv()
            // fires and appendPkLoadOpColumn runs — mirrors production refresh.
            optimizerContext.setStatement(buildSyntheticInsertStmt(mockMv, rewrittenQuery));
            optimizerContext.getQueryMaterializationContext().setOverrideTargetMv(mockMv);
            // RULE_BASED skips Memo / cost-based; the mock MV has no statistics or partitions.
            optimizerContext.setOptimizerOptions(OptimizerOptions.newRuleBaseOpt());

            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            optimizer.optimize(
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
        } catch (SemanticException e) {
            throw new SemanticException(
                    "Failed to generate IVM refresh plan at CREATE time: " + e.getMessage(), e);
        } catch (RuntimeException e) {
            String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            throw new SemanticException("Failed to generate IVM refresh plan at CREATE time: " + msg, e);
        } finally {
            ctx.getSessionVariable().setEnableIVMRefresh(prevIvmEnabled);
            ctx.getSessionVariable().setTvrTargetMvid(prevTvrTargetMvId);
        }
    }

    // Mirrors MaterializedViewAnalyzer.genMaterializedViewColumns: column types come from
    // the analyzed query's RelationFields; __ROW_ID__ is key/non-null/hidden; __AGG_STATE_*
    // are hidden; AUTO_INCREMENT path appends an explicit __ROW_ID__ since it isn't in the
    // query output.
    private static MaterializedView buildMockMv(CreateMaterializedViewStatement stmt,
                                                 QueryStatement rewrittenQuery) {
        List<Field> relationFields = rewrittenQuery.getQueryRelation().getScope()
                .getRelationFields().getAllFields();
        List<String> columnNames = rewrittenQuery.getQueryRelation()
                .getRelationFields().getAllFields().stream()
                .map(Field::getName)
                .toList();

        List<Column> columns = Lists.newArrayList();
        boolean hasRowId = false;
        for (int i = 0; i < relationFields.size(); i++) {
            Field field = relationFields.get(i);
            Type type = AnalyzerUtils.transformTableColumnType(field.getType(), false);
            String colName = columnNames.get(i);
            Column column = new Column(colName, type, field.isNullable());
            if (IvmOpUtils.COLUMN_ROW_ID.equalsIgnoreCase(colName)) {
                column.setIsKey(true);
                column.setIsAllowNull(false);
                column.setIsHidden(true);
                hasRowId = true;
            } else if (colName.startsWith(IvmOpUtils.COLUMN_AGG_STATE_PREFIX)) {
                column.setIsHidden(true);
            }
            if (!column.isKey()) {
                column.setAggregationType(AggregateType.REPLACE, true);
            }
            columns.add(column);
        }
        // AUTO_INCREMENT path: __ROW_ID__ isn't in the query output, append it explicitly.
        if (!hasRowId) {
            Column rowIdCol = new Column(IvmOpUtils.COLUMN_ROW_ID, IntegerType.BIGINT, false);
            rowIdCol.setIsKey(true);
            rowIdCol.setIsAllowNull(false);
            rowIdCol.setIsHidden(true);
            rowIdCol.setIsAutoIncrement(true);
            columns.add(rowIdCol);
        }

        Column rowIdColumn = columns.stream()
                .filter(c -> IvmOpUtils.COLUMN_ROW_ID.equalsIgnoreCase(c.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "rewrittenQuery must have __ROW_ID__ column after schema construction"));
        HashDistributionInfo distInfo = new HashDistributionInfo(1, List.of(rowIdColumn));
        SinglePartitionInfo partInfo = new SinglePartitionInfo();
        MaterializedView.MvRefreshScheme refreshScheme =
                new MaterializedView.MvRefreshScheme(MaterializedViewRefreshType.INCREMENTAL);

        MaterializedView mockMv = new MaterializedView(
                MOCK_MV_ID,
                MOCK_DB_ID,
                "__ivm_trial_mv",
                columns,
                KeysType.PRIMARY_KEYS,
                partInfo,
                distInfo,
                refreshScheme);
        mockMv.setEncodeRowIdVersion(stmt.getEncodeRowIdVersion());
        return mockMv;
    }

    private static InsertStmt buildSyntheticInsertStmt(MaterializedView mockMv,
                                                       QueryStatement rewrittenQuery) {
        TableRef syntheticRef = new TableRef(
                QualifiedName.of(List.of(mockMv.getName())), null, NodePosition.ZERO);
        InsertStmt insertStmt = new InsertStmt(syntheticRef, rewrittenQuery);
        insertStmt.setTargetTable(mockMv);
        return insertStmt;
    }

    // TvrTableDelta.empty() — IvmDeltaIcebergScanRule's check sees a present + append-only
    // trait, and its transform short-circuits to LogicalValuesOperator without querying the
    // catalog for snapshot IDs. Non-empty fake versions would trigger statistics calculation
    // to call IcebergMetadata with bogus snapshot IDs against the real catalog.
    private static void applySyntheticTvrDelta(QueryStatement rewrittenQuery) {
        Multimap<String, TableRelation> tableRelations =
                AnalyzerUtils.collectAllTableRelation(rewrittenQuery);
        TvrTableDelta delta = TvrTableDelta.empty();
        for (TableRelation rel : tableRelations.values()) {
            rel.setTvrVersionRange(delta);
        }
    }
}
