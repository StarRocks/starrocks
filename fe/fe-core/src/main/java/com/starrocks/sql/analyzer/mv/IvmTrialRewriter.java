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

import com.google.common.collect.Multimap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfoBuilder;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStorageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

            MaterializedView mockMv = buildMockMv(stmt);
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
            // The trial bypasses InsertPlanner, so stash the ordered outputs here as it would
            // (position i writes mock schema[i]) for IvmRewriter.bindStateColumnsForAggregate.
            optimizerContext.getTvrOptContext().setIvmInsertOutputColumns(
                    alignInsertOutputColumns(stmt, logicalPlan.getOutputColumn(), columnRefFactory));
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

    /** Build the trial target from the analyzer's final target schema and distribution. */
    static MaterializedView buildMockMv(CreateMaterializedViewStatement stmt) {
        List<Column> columns = stmt.getMvColumnItems().stream()
                .map(Column::deepCopy)
                .collect(Collectors.toList());
        DistributionInfo distInfo;
        try {
            distInfo = DistributionInfoBuilder.build(stmt.getDistributionDesc(), columns);
        } catch (DdlException e) {
            throw new SemanticException("Failed to build IVM trial target distribution: " + e.getMessage(), e);
        }
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
        // RANGE derives its routing columns from the base index's sort/key metadata. Mirror
        // the catalog target far enough for optimizer property derivation without partitions.
        mockMv.setBaseIndexMetaId(MOCK_MV_ID);
        short shortKeyColumnCount = (short) columns.stream().filter(Column::isKey).count();
        mockMv.setIndexMeta(MOCK_MV_ID, mockMv.getName(), columns, 0, 0,
                shortKeyColumnCount, TStorageType.COLUMN, KeysType.PRIMARY_KEYS);
        mockMv.setEncodeRowIdVersion(stmt.getEncodeRowIdVersion());
        return mockMv;
    }

    /**
     * Align query-projection outputs to target-schema positions. Storage-filled positions receive
     * typed placeholders so aggregate-state binding keeps the same positional contract as INSERT.
     */
    static List<ColumnRefOperator> alignInsertOutputColumns(CreateMaterializedViewStatement stmt,
                                                            List<ColumnRefOperator> queryOutputs,
                                                            ColumnRefFactory columnRefFactory) {
        List<Column> targetColumns = stmt.getMvColumnItems();
        List<Integer> queryOutputIndices = stmt.getQueryOutputIndices();
        if (queryOutputIndices == null || queryOutputIndices.isEmpty()) {
            queryOutputIndices = new ArrayList<>(queryOutputs.size());
            for (int i = 0; i < queryOutputs.size(); i++) {
                queryOutputIndices.add(i);
            }
        }
        if (queryOutputIndices.size() != queryOutputs.size()) {
            throw new SemanticException("IVM trial output mapping size does not match query outputs: %d != %d",
                    queryOutputIndices.size(), queryOutputs.size());
        }

        List<ColumnRefOperator> aligned = new ArrayList<>(
                Collections.nCopies(targetColumns.size(), null));
        for (int queryIndex = 0; queryIndex < queryOutputs.size(); queryIndex++) {
            int targetIndex = queryOutputIndices.get(queryIndex);
            if (targetIndex < 0 || targetIndex >= aligned.size() || aligned.get(targetIndex) != null) {
                throw new SemanticException("Invalid IVM trial output mapping at query position %d: %d",
                        queryIndex, targetIndex);
            }
            aligned.set(targetIndex, queryOutputs.get(queryIndex));
        }
        for (int targetIndex = 0; targetIndex < aligned.size(); targetIndex++) {
            if (aligned.get(targetIndex) == null) {
                Column targetColumn = targetColumns.get(targetIndex);
                aligned.set(targetIndex, columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull()));
            }
        }
        return aligned;
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
