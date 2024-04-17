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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvBaseTableUpdateInfo;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVCompensation;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVPartitionPruner;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getPartitionNamesToRefreshForMv;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;

public class MaterializedViewTransparentRewriteRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(MvPartitionCompensator.class);

    public MaterializedViewTransparentRewriteRule() {
        super(RuleType.TF_MV_TRANSPARENT_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        // To avoid dead-loop rewrite, no rewrite when query extra predicate is not changed
        if (MvUtils.isOpAppliedRule(input.getOp(), Operator.OP_TRANSPARENT_MV_BIT)) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();
        Table table = olapScanOperator.getTable();
        if (!table.isMaterializedView()) {
            return Collections.emptyList();
        }

        MaterializedView mv = (MaterializedView) table;
        if (!mv.isEnableTransparentRewrite()) {
            return Collections.emptyList();
        }

        logMVRewrite(mv.getName(), "Start to generate transparent rewrite plan for mv");
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            logMVRewrite("Skip to generate transparent rewrite plan because connect context is null, mv: {}",
                    mv.getName());
            return Collections.emptyList();
        }
        OptExpression result = doTransform(connectContext, context, olapScanOperator, input, mv.getMvId());
        return Collections.singletonList(result);
    }

    private OptExpression doTransform(ConnectContext connectContext,
                                      OptimizerContext context,
                                      LogicalOlapScanOperator olapScanOperator,
                                      OptExpression input,
                                      MvId mvId) {
        OptExpression mvTransparentPlan = doGetMvTransparentPlan(connectContext, context, mvId, olapScanOperator, input);
        Preconditions.checkState(mvTransparentPlan != null,
                "Build mv transparent plan failed: %s", mvId);
        // merge projection
        Map<ColumnRefOperator, ScalarOperator> originalProjectionMap =
                olapScanOperator.getProjection() == null ? null : olapScanOperator.getProjection().getColumnRefMap();
        OptExpression result = mergeProjection(mvTransparentPlan, originalProjectionMap);

        // merge predicate
        if (olapScanOperator.getPredicate() != null) {
            result = MvUtils.addExtraPredicate(result, olapScanOperator.getPredicate());
        }
        // derive logical property
        deriveLogicalProperty(result);
        // mark transparent rewrite
        setOpRuleMask(result);

        return result;
    }

    public static void setOpRuleMask(OptExpression input) {
        List<LogicalScanOperator> scanOps = MvUtils.getScanOperator(input);
        scanOps.stream().forEach(op -> op.setOpRuleMask(Operator.OP_TRANSPARENT_MV_BIT));
    }

    /**
     * Wrap getTransparentPlan and getDefinedQueryPlan methods to ensure the transparent plan is returned if possible.
     */
    private OptExpression doGetMvTransparentPlan(ConnectContext connectContext,
                                                 OptimizerContext context,
                                                 MvId mvId,
                                                 LogicalOlapScanOperator olapScanOperator,
                                                 OptExpression input) {
        // Fetch mv from catalog again since table from olap scan operator is copied.
        Database db = GlobalStateMgr.getCurrentState().getDb(mvId.getDbId());
        Preconditions.checkState(db != null, "Database not found: %s", mvId.getDbId());
        Table table = db.getTable(mvId.getId());
        Preconditions.checkState(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        MvPlanContext mvPlanContext = MvUtils.getMVPlanContext(connectContext, mv, true);
        Preconditions.checkState(mvPlanContext != null, "MV plan context not found: %s", mv.getName());

        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        Set<Table> queryTables = MvUtils.getAllTables(mvPlan).stream().collect(Collectors.toSet());

        // mv's to refresh partition info
        MvUpdateInfo mvUpdateInfo = getPartitionNamesToRefreshForMv(mv, true);
        logMVRewrite(context, this, "MV to refresh partition info: {}", mvUpdateInfo);
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            logMVRewrite(context, this, "Get mv to refresh partition info failed, and redirect to mv's defined query");
            return getOptExpressionByDefault(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }

        Set<String> partitionNamesToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
        logMVRewrite(context, this, "MV to refresh partitions: {}", partitionNamesToRefresh);

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, ConstantOperator.TRUE, mvUpdateInfo, queryTables);

        Map<Column, ColumnRefOperator> columnToColumnRefMap = olapScanOperator.getColumnMetaToColRefMap();
        List<Column> mvColumns = mv.getBaseSchema();
        List<ColumnRefOperator> expectOutputColumns = mvColumns.stream()
                .map(c -> columnToColumnRefMap.get(c))
                .collect(Collectors.toList());
        OptExpression result = getMvTransparentPlan(mvContext, input, expectOutputColumns);
        if (result != null) {
            logMVRewrite(mvContext, "Success to generate transparent plan");
            return result;
        } else {
            logMVRewrite(mvContext, "Get mv transparent plan failed, and redirect to mv's defined query");
            return getOptExpressionByDefault(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }
    }

    private OptExpression getOptExpressionByDefault(OptimizerContext context,
                                                    MaterializedView mv,
                                                    MvPlanContext mvPlanContext,
                                                    LogicalOlapScanOperator olapScanOperator,
                                                    Set<Table> queryTables) {
        TableProperty.MVTransparentRewriteMode transparentRewriteMode = mv.getTransparentRewriteMode();
        switch (transparentRewriteMode) {
            case TRUE:
                return redirectToMVDefinedQuery(context, mv, mvPlanContext, olapScanOperator, queryTables);
            case TRANSPARENT_OR_DEFAULT:
                return OptExpression.create(olapScanOperator);
            case TRANSPARENT_OR_ERROR:
                throw new RuntimeException("Transparent rewrite is not supported for materialized view:" + mv.getName());
            default:
                throw new IllegalArgumentException("Unknown transparent rewrite mode: " + transparentRewriteMode);
        }
    }

    /**
     * Redirect to mv's defined query.
     */
    private OptExpression redirectToMVDefinedQuery(OptimizerContext context,
                                                   MaterializedView mv,
                                                   MvPlanContext mvPlanContext,
                                                   LogicalOlapScanOperator olapScanOperator,
                                                   Set<Table> queryTables) {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        mvUpdateInfo.getMvToRefreshPartitionNames().addAll(mv.getPartitionNames());

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, ConstantOperator.TRUE, mvUpdateInfo, queryTables);
        logMVRewrite(mvContext, "Get mv transparent plan failed, and redirect to mv's defined query");
        Map<Column, ColumnRefOperator> columnToColumnRefMap = olapScanOperator.getColumnMetaToColRefMap();
        List<Column> mvColumns = mv.getBaseSchema();
        List<ColumnRefOperator> expectOutputColumns = mvColumns.stream()
                .map(c -> columnToColumnRefMap.get(c))
                .collect(Collectors.toList());
        return getMvDefinedQueryPlan(mvPlanContext.getLogicalPlan(), mvContext, expectOutputColumns);
    }

    /**
     * Get transparent plan if possible.
     * What's the transparent plan?
     * see {@link MvPartitionCompensator#getMvTransparentPlan(MaterializationContext, MVCompensation, List)
     */
    private OptExpression getMvTransparentPlan(MaterializationContext mvContext,
                                               OptExpression input,
                                               List<ColumnRefOperator> expectOutputColumns) {
        // NOTE: MV's mvSelectPartitionIds is not trusted in transparent since it is targeted for the whole partitions(refresh
        //  and no refreshed).
        // 1. Decide ref base table partition ids to refresh in optimizer.
        // 2. consider partition prunes for the input olap scan operator
        MvUpdateInfo mvUpdateInfo = mvContext.getMvUpdateInfo();
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            logMVRewrite(mvContext, "Failed to get mv to refresh partition info: {}", mvContext.getMv().getName());
            return null;
        }
        MVCompensation mvCompensation = getMvCompensation(mvContext, mvUpdateInfo);
        if (mvCompensation == null) {
            logMVRewrite(mvContext, "Failed to get mv compensation info: {}", mvContext.getMv().getName());
            return null;
        }
        logMVRewrite(mvContext, "Get mv compensation info: {}", mvCompensation);
        if (mvCompensation.getState().isNoCompensate()) {
            return input;
        }
        if (!mvCompensation.getState().isCompensate()) {
            logMVRewrite(mvContext, "Return directly because mv compensation info cannot compensate: {}",
                    mvCompensation);
            return null;
        }

        OptExpression transparentPlan = MvPartitionCompensator.getMvTransparentPlan(mvContext, mvCompensation,
                expectOutputColumns);
        return transparentPlan;
    }

    /**
     * Get mv compensation info by mvToRefreshPartitionInfo.
     */
    private static MVCompensation getMvCompensation(MaterializationContext mvContext,
                                                    MvUpdateInfo mvUpdateInfo) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();

        // If no partition to refresh, return directly.
        if (mvUpdateInfo.getMvToRefreshPartitionNames().isEmpty()) {
            return MVCompensation.createNoCompensateState(sessionVariable);
        }

        MaterializedView mv = mvContext.getMv();
        Pair<Table, Column> partitionTableAndColumns = mv.getDirectTableAndPartitionColumn();
        if (partitionTableAndColumns == null) {
            logMVRewrite("MV's not partitioned, failed to get partition keys: {}", mv.getName());
            return MVCompensation.createUnkownState(sessionVariable);
        }

        Table refBaseTable = partitionTableAndColumns.first;
        Set<String> refTablePartitionNamesToRefresh = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
        if (refTablePartitionNamesToRefresh == null) {
            return MVCompensation.createUnkownState(sessionVariable);
        }

        if (refBaseTable.isNativeTableOrMaterializedView()) {
            // What if nested mv?
            List<Long> refTablePartitionIdsToRefresh = refTablePartitionNamesToRefresh.stream()
                    .map(name -> refBaseTable.getPartition(name))
                    .map(p -> p.getId())
                    .collect(Collectors.toList());
            return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, refTablePartitionIdsToRefresh, null);
        } else if (MvPartitionCompensator.isTableSupportedPartitionCompensate(refBaseTable)) {
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo =
                    mvUpdateInfo.getBaseTableUpdateInfos().get(refBaseTable);
            if (mvBaseTableUpdateInfo == null) {
                return null;
            }
            Map<String, Range<PartitionKey>> refTablePartitionNameWithRanges =
                    mvBaseTableUpdateInfo.getPartitionNameWithRanges();
            List<PartitionKey> partitionKeys = Lists.newArrayList();
            try {
                for (String partitionName : refTablePartitionNamesToRefresh) {
                    Preconditions.checkState(refTablePartitionNameWithRanges.containsKey(partitionName));
                    Range<PartitionKey> partitionKeyRange = refTablePartitionNameWithRanges.get(partitionName);
                    partitionKeys.add(partitionKeyRange.lowerEndpoint());
                }
            } catch (Exception e) {
                logMVRewrite("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                        DebugUtil.getStackTrace(e));
                LOG.warn("Failed to get partition keys for ref base table: {}", refBaseTable.getName(),
                        DebugUtil.getStackTrace(e));
                return MVCompensation.createUnkownState(sessionVariable);
            }
            return new MVCompensation(sessionVariable, MVTransparentState.COMPENSATE, null, partitionKeys);
        } else {
            return MVCompensation.createUnkownState(sessionVariable);
        }
    }

    /**
     * If transparent plan is not available, get defined query plan and reset its selected partitions for further partition prune.
     */
    private OptExpression getMvDefinedQueryPlan(OptExpression mvPlan,
                                                MaterializationContext mvContext,
                                                List<ColumnRefOperator> originalOutputColumns) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvQueryPlan = duplicator.duplicate(mvPlan);
        newMvQueryPlan = MVPartitionPruner.resetSelectedPartitions(newMvQueryPlan, true);

        List<ColumnRefOperator> orgMvQueryOutputColumnRefs = mvContext.getMvOutputColumnRefs();
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(orgMvQueryOutputColumnRefs);
        Map<ColumnRefOperator, ScalarOperator> newProjectionMap = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            newProjectionMap.put(originalOutputColumns.get(i), newQueryOutputColumns.get(i));
        }
        return mergeProjection(newMvQueryPlan, newProjectionMap);
    }

    private OptExpression mergeProjection(OptExpression input, Map<ColumnRefOperator, ScalarOperator> newProjectionMap) {
        if (newProjectionMap == null || newProjectionMap.isEmpty()) {
            return input;
        }
        Operator newOp = input.getOp();
        if (newOp.getProjection() == null) {
            newOp.setProjection(new Projection(newProjectionMap));
        } else {
            // merge two projections
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(newOp.getProjection().getColumnRefMap());
            Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : newProjectionMap.entrySet()) {
                ScalarOperator result = rewriter.rewrite(entry.getValue());
                resultMap.put(entry.getKey(), result);
            }
            newOp.setProjection(new Projection(resultMap));
        }
        return input;
    }
}
