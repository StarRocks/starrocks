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

import com.google.api.client.util.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionNames;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.MvRewritePreprocessor.MvCopyFailurePolicy;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptConstFoldRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_TRANSPARENT_REWRITE;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.deriveLogicalProperty;

public class MaterializedViewTransparentRewriteRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(MvPartitionCompensator.class);

    public MaterializedViewTransparentRewriteRule() {
        super(RuleType.TF_MV_TRANSPARENT_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        // To avoid dead-loop rewrite, no rewrite when query extra predicate is not changed
        if (input.getOp().isOpRuleBitSet(OP_MV_TRANSPARENT_REWRITE)) {
            return false;
        }
        return true;
    }

    /**
     * A transparent plan reads the fresh part of the MV and the base tables at their current state for the rest. A
     * scan narrowed by PARTITION(...), tablet or replica hints or a sample clause, or reading a snapshot (BEFORE), has
     * no equivalent in it.
     */
    private static boolean isRestrictedScan(LogicalOlapScanOperator scan) {
        return scan.hasTableHints() || scan.getSample() != null || scan.getGtid() != 0;
    }

    /**
     * Whether a restricted scan reads only fresh data of the MV, so that scanning it as written is what the
     * transparent plan would return: the whole MV is fresh, or the scan is narrowed by PARTITION(...) alone to
     * partitions that are all fresh.
     * <p>
     * The check runs on the MV in the catalog, as the transparent plan does: the timeliness it computes may be cached
     * across queries, and the scan's copy may predate a partition drop. A base table without any recorded version
     * (never refreshed against, or cleared by a reset) proves nothing about the data the MV holds, even when the
     * check finds nothing to refresh: it ignores partitions that were never written to.
     */
    private static boolean readsOnlyFreshData(OptimizerContext context, MvId mvId, LogicalOlapScanOperator scan) {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(mvId.getDbId(), mvId.getId());
        if (!(table instanceof MaterializedView)) {
            return false;
        }
        MaterializedView mv = (MaterializedView) table;
        if (!recordsVersionsOfAllBaseTables(mv)) {
            return false;
        }
        MvUpdateInfo mvUpdateInfo = context.getQueryMaterializationContext().getOrInitMVTimelinessInfos(mv,
                MVTimelinessArbiter.QueryRewriteParams.ofQueryRewrite(context));
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            return false;
        }
        PCellSortedSet stalePartitions = mvUpdateInfo.getMVToRefreshPCells();
        if (stalePartitions == null || stalePartitions.isEmpty()) {
            return true;
        }
        PartitionNames partitionNames = scan.getPartitionNames();
        boolean narrowedByPartitionsOnly = partitionNames != null && !partitionNames.isTemp()
                && CollectionUtils.isEmpty(scan.getHintsTabletIds()) && CollectionUtils.isEmpty(scan.getHintsReplicaIds())
                && scan.getSample() == null && scan.getGtid() == 0;
        if (!narrowedByPartitionsOnly) {
            return false;
        }
        Set<String> stale = stalePartitions.getPartitionNames().stream()
                .map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        return partitionNames.getPartitionNames().stream()
                .noneMatch(name -> stale.contains(name.toLowerCase(Locale.ROOT)));
    }

    private static boolean recordsVersionsOfAllBaseTables(MaterializedView mv) {
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        if (CollectionUtils.isEmpty(baseTableInfos)) {
            return false;
        }
        MaterializedView.AsyncRefreshContext refreshContext = mv.getRefreshScheme().getAsyncRefreshContext();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Map<String, MaterializedView.BasePartitionInfo> versions = baseTableInfo.isInternalCatalog()
                    ? refreshContext.getBaseTableVisibleVersionMap().get(baseTableInfo.getTableId())
                    : refreshContext.getBaseTableInfoVisibleVersionMap().get(baseTableInfo);
            if (versions == null || versions.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether a base table of the MV is known to be dropped. A lookup that fails (an HMS timeout, say) proves nothing,
     * so it does not count: the defined query may still succeed.
     */
    private static boolean hasDroppedBaseTable(MaterializedView mv) {
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        if (CollectionUtils.isEmpty(baseTableInfos)) {
            return false;
        }
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            try {
                if (MvUtils.getTable(baseTableInfo).isEmpty()) {
                    return true;
                }
            } catch (Exception e) {
                LOG.warn("Failed to get base table: {} of mv: {}, error: {}",
                        baseTableInfo, mv.getName(), e.getMessage());
            }
        }
        return false;
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
        // if this mv comes from mv rewritten, skip it
        if (olapScanOperator.isOpAppliedMV(mv.getId())) {
            return Collections.emptyList();
        }

        // A restricted scan that reads only fresh data is scanned as written. One that reads stale data still takes the
        // transparent plan, which compensates it but does not keep the restriction.
        boolean restrictedScan = isRestrictedScan(olapScanOperator);
        if (restrictedScan) {
            try {
                if (readsOnlyFreshData(context, mv.getMvId(), olapScanOperator)) {
                    logMVRewrite(mv.getName(), "Restricted scan of mv reads only fresh data, scan it as written");
                    return Collections.emptyList();
                }
            } catch (Exception e) {
                LOG.warn("Failed to check the timeliness of mv: {} for a restricted scan, error: {}",
                        mv.getName(), e.getMessage(), e);
            }
        }

        logMVRewrite(mv.getName(), "Start to generate transparent rewrite plan for mv");
        ConnectContext connectContext = context.getConnectContext() == null ?
                new ConnectContext() : context.getConnectContext();
        try {
            OptExpression result = doTransform(connectContext, context, olapScanOperator, input, mv.getMvId(), false);
            return Collections.singletonList(result);
        } catch (Exception e) {
            LOG.warn("Failed to generate transparent rewrite plan for mv: {}, error: {}",
                    mv.getName(), e.getMessage(), e);
            TableProperty.MVTransparentRewriteMode transparentRewriteMode = mv.getTransparentRewriteMode();
            switch (transparentRewriteMode) {
                case TRUE:
                    // Scanning the MV as it is would return data the timeliness check may have found stale: the
                    // failure can be a metadata race (a partition dropped, a refresh publishing) mid-check. The
                    // defined query reads the base tables at their current state and needs no timeliness check, so try
                    // it before that, unless the scan is restricted: the defined query has no equivalent of that.
                    if (restrictedScan) {
                        return Collections.emptyList();
                    }
                    // A dropped base table fails the defined query too, so keep serving the data the MV holds.
                    if (hasDroppedBaseTable(mv)) {
                        LOG.warn("A base table of mv: {} no longer exists, scan the mv as it is", mv.getName());
                        return Collections.emptyList();
                    }
                    try {
                        return Collections.singletonList(
                                doTransform(connectContext, context, olapScanOperator, input, mv.getMvId(), true));
                    } catch (Exception redirectFailure) {
                        LOG.warn("Failed to redirect mv: {} to its defined query, scan it as it is, error: {}",
                                mv.getName(), redirectFailure.getMessage(), redirectFailure);
                        return Collections.emptyList();
                    }
                case TRANSPARENT_OR_DEFAULT:
                    return Collections.emptyList();
                case TRANSPARENT_OR_ERROR:
                    throw new RuntimeException("Failed to generate transparent rewrite plan for mv: " + mv.getName(), e);
                default:
                    throw new IllegalArgumentException("Unknown transparent rewrite mode: " + transparentRewriteMode);
            }
        }
    }

    private OptExpression doTransform(ConnectContext connectContext,
                                      OptimizerContext context,
                                      LogicalOlapScanOperator olapScanOperator,
                                      OptExpression input,
                                      MvId mvId,
                                      boolean definedQueryOnly) {
        OptExpression mvTransparentPlan = doGetMvTransparentPlan(connectContext, context, mvId, olapScanOperator, input,
                definedQueryOnly);
        if (mvTransparentPlan == null) {
            throw new RuntimeException("Build mv transparent plan failed: " + mvId);
        }
        // merge projection
        Map<ColumnRefOperator, ScalarOperator> originalProjectionMap =
                olapScanOperator.getProjection() == null ? null : olapScanOperator.getProjection().getColumnRefMap();
        OptExpression result = Utils.mergeProjection(mvTransparentPlan, originalProjectionMap);

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
        scanOps.stream().forEach(op -> op.setOpRuleBit(OP_MV_TRANSPARENT_REWRITE));
    }

    /**
     * Wrap getTransparentPlan and getDefinedQueryPlan methods to ensure the transparent plan is returned if possible.
     * With {@code definedQueryOnly} the plan is always the MV's defined query, without checking the MV's timeliness.
     */
    private OptExpression doGetMvTransparentPlan(ConnectContext connectContext,
                                                 OptimizerContext context,
                                                 MvId mvId,
                                                 LogicalOlapScanOperator olapScanOperator,
                                                 OptExpression input,
                                                 boolean definedQueryOnly) {
        // Fetch mv from catalog again since table from olap scan operator is copied.
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        Preconditions.checkState(db != null, "Database not found: %s", mvId.getDbId());
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
        Preconditions.checkState(table instanceof MaterializedView);
        MaterializedView mv = (MaterializedView) table;
        final MvPlanContext mvPlanContext = MvUtils.getMVPlanContext(connectContext, mv, true, false);
        if (mvPlanContext == null) {
            throw new RuntimeException("Cannot get mv plan context: " + mv.getName());
        }
        OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        if (mvPlan == null) {
            throw new RuntimeException("Cannot get mv plan: " + mv.getName());
        }
        // do const fold if mv contains non-deterministic functions
        if (mvPlanContext.isContainsNDFunctions()) {
            mvPlan = OptConstFoldRewriter.rewrite(mvPlan);
        }

        Set<Table> queryTables = MvUtils.getAllTables(mvPlan).stream().collect(Collectors.toSet());
        if (definedQueryOnly) {
            return redirectToMVDefinedQuery(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }

        // mv's to refresh partition info
        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        MVTimelinessArbiter.QueryRewriteParams queryRewriteParams =
                MVTimelinessArbiter.QueryRewriteParams.ofQueryRewrite(context);
        MvUpdateInfo mvUpdateInfo = queryMaterializationContext.getOrInitMVTimelinessInfos(mv, queryRewriteParams);
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            logMVRewrite(context, this, "Get mv to refresh partition info failed, and redirect to mv's defined query");
            return getOptExpressionByDefault(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }
        logMVRewrite(context, this, "MV to refresh partition info: {}", mvUpdateInfo);

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, mvUpdateInfo, queryTables, 0, MvCopyFailurePolicy.FALLBACK_TO_LIVE_MV);
        if (mvContext == null) {
            logMVRewrite(context, this,
                    "Failed to build transparent rewrite context for {}, redirect to default transparent behavior",
                    mv.getName());
            return getOptExpressionByDefault(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }

        Map<Column, ColumnRefOperator> columnToColumnRefMap = olapScanOperator.getColumnMetaToColRefMap();
        // use ordered output columns to ensure the order of output columns if the mv contains order-by clause.
        List<Column> mvColumns = mv.getOrderedOutputColumns();
        List<ColumnRefOperator> expectOutputColumns = mvColumns.stream()
                .map(c -> columnToColumnRefMap.get(c))
                .collect(Collectors.toList());
        OptExpression result = MvPartitionCompensator.getMvTransparentPlan(mvContext, input, expectOutputColumns);
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
            case TRANSPARENT_OR_ERROR:
                return redirectToMVDefinedQuery(context, mv, mvPlanContext, olapScanOperator, queryTables);
            case TRANSPARENT_OR_DEFAULT:
                return OptExpression.create(olapScanOperator);
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
        MvUpdateInfo mvUpdateInfo = MvUpdateInfo.fullRefresh(mv);
        mvUpdateInfo.addMVToRefreshPartitionNames(mv.getPartitionCells(Optional.empty()));

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, mvUpdateInfo, queryTables, 0, MvCopyFailurePolicy.FALLBACK_TO_LIVE_MV);
        if (mvContext == null) {
            logMVRewrite(context, this,
                    "Failed to build defined-query redirect context for {}", mv.getName());
            throw new RuntimeException("Failed to build defined-query redirect context for mv: " + mv.getName());
        }
        logMVRewrite(mvContext, "Get mv transparent plan failed, and redirect to mv's defined query");
        Map<Column, ColumnRefOperator> columnToColumnRefMap = olapScanOperator.getColumnMetaToColRefMap();
        List<Column> mvColumns = mv.getOrderedOutputColumns();
        List<ColumnRefOperator> expectOutputColumns = mvColumns.stream()
                .map(c -> columnToColumnRefMap.get(c))
                .collect(Collectors.toList());
        return getMvDefinedQueryPlan(mvPlanContext.getLogicalPlan(), mvContext, expectOutputColumns);
    }

    /**
     * If transparent plan is not available, get defined query plan and reset its selected partitions for further partition prune.
     */
    private OptExpression getMvDefinedQueryPlan(OptExpression mvPlan,
                                                MaterializationContext mvContext,
                                                List<ColumnRefOperator> originalOutputColumns) {
        OptExpressionDuplicator duplicator = new OptExpressionDuplicator(mvContext);
        OptExpression newMvQueryPlan = duplicator.duplicate(mvPlan, mvContext.getMvColumnRefFactory(),
                true, true);

        List<ColumnRefOperator> orgMvQueryOutputColumnRefs = mvContext.getMvOutputColumnRefs();
        List<ColumnRefOperator> newQueryOutputColumns = duplicator.getMappedColumns(orgMvQueryOutputColumnRefs);
        Map<ColumnRefOperator, ScalarOperator> newProjectionMap = Maps.newHashMap();
        for (int i = 0; i < originalOutputColumns.size(); i++) {
            newProjectionMap.put(originalOutputColumns.get(i), newQueryOutputColumns.get(i));
        }
        return Utils.mergeProjection(newMvQueryPlan, newProjectionMap);
    }
}
