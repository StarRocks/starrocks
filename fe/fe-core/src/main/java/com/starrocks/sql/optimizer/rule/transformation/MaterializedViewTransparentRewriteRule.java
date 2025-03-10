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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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
     */
    private OptExpression doGetMvTransparentPlan(ConnectContext connectContext,
                                                 OptimizerContext context,
                                                 MvId mvId,
                                                 LogicalOlapScanOperator olapScanOperator,
                                                 OptExpression input) {
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

        // mv's to refresh partition info
        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        MvUpdateInfo mvUpdateInfo = queryMaterializationContext.getOrInitMVTimelinessInfos(mv);
        if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
            logMVRewrite(context, this, "Get mv to refresh partition info failed, and redirect to mv's defined query");
            return getOptExpressionByDefault(context, mv, mvPlanContext, olapScanOperator, queryTables);
        }
        logMVRewrite(context, this, "MV to refresh partition info: {}", mvUpdateInfo);

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, mvUpdateInfo, queryTables, 0);

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
        MvUpdateInfo mvUpdateInfo = MvUpdateInfo.fullRefresh(mv);
        mvUpdateInfo.addMvToRefreshPartitionNames(mv.getPartitionNames());

        MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                mv, mvPlanContext, mvUpdateInfo, queryTables, 0);
        logMVRewrite(mvContext, "Get mv transparent plan failed, and redirect to mv's defined query");
        Map<Column, ColumnRefOperator> columnToColumnRefMap = olapScanOperator.getColumnMetaToColRefMap();
        List<Column> mvColumns = mv.getBaseSchemaWithoutGeneratedColumn();
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
