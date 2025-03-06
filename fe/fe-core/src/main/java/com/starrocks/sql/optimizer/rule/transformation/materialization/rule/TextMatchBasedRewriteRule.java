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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter.REWRITE_SUCCESS;

/**
 * A rule that performs text match based rewrite on a query using materialized views.
 */
public class TextMatchBasedRewriteRule extends Rule {
    private static final Logger LOG = LogManager.getLogger(TextMatchBasedRewriteRule.class);

    // Supported rewrite operator types in the sub-query to match with the specified operator types
    public static final Set<OperatorType> SUPPORTED_REWRITE_OPERATOR_TYPES = ImmutableSet.of(
            OperatorType.LOGICAL_PROJECT,
            OperatorType.LOGICAL_UNION,
            OperatorType.LOGICAL_LIMIT,
            OperatorType.LOGICAL_FILTER
    );
    private final ConnectContext connectContext;
    private final StatementBase stmt;
    private final MVTransformerContext mvTransformerContext;

    // To avoid text match costing too much time, use parameters below to limit it.
    // limit for sub-query text match(default 4), no match when it <= 0
    private final int mvSubQueryTextMatchMaxCount;
    // limit for mvs which matched input query(default 64)
    private final long mvRewriteRelatedMVsLimit;

    public TextMatchBasedRewriteRule(ConnectContext connectContext,
                                     StatementBase stmt,
                                     MVTransformerContext mvTransformerContext) {
        super(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE, Pattern.create(OperatorType.PATTERN));

        this.connectContext = connectContext;
        this.stmt = stmt;
        this.mvTransformerContext = mvTransformerContext;
        this.mvRewriteRelatedMVsLimit =
                connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
        this.mvSubQueryTextMatchMaxCount =
                connectContext.getSessionVariable().getMaterializedViewSubQueryTextMatchMaxCount();

    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression rewritten = doTransform(context, input, stmt);
        return (rewritten != null) ? ImmutableList.of(rewritten) : ImmutableList.of(input);
    }

    private OptExpression doTransform(OptimizerContext context,
                                      OptExpression input,
                                      ParseNode parseNode) {
        if (context.getOptimizerOptions().isRuleDisable(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE)) {
            return null;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isEnableMaterializedViewRewrite() ||
                !sessionVariable.isEnableMaterializedViewTextMatchRewrite()) {
            logMVRewrite(context, this, "Materialized view text based rewrite is disabled");
            return null;
        }
        if (stmt == null || stmt.getOrigStmt() == null || stmt.getOrigStmt().originStmt == null) {
            logMVRewrite(context, this, "Materialized view text based rewrite is disabled: stmt is null");
            return null;
        }
        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        if (CollectionUtils.isEmpty(queryMaterializationContext.getRelatedMVs())) {
            return null;
        }

        CachingMvPlanContextBuilder.AstKey astKey = new CachingMvPlanContextBuilder.AstKey(parseNode);
        OptExpression rewritten = rewriteByTextMatch(input, context, astKey);
        if (rewritten != null) {
            return rewritten;
        }
        // try to rewrite sub-query again if exact-match failed.
        if (mvTransformerContext == null || mvTransformerContext.isOpASTEmpty()) {
            logMVRewrite(context, this, "OptToAstMap is empty, no try to rewrite sub-query again");
            return null;
        }
        logMVRewrite(context, this, "Materialized view text based rewrite success, " +
                "try to rewrite sub-query again");
        return input.getOp().accept(new TextBasedRewriteVisitor(context, mvTransformerContext), input, connectContext);
    }

    private boolean isSupportForTextBasedRewrite(OptExpression input) {
        if (input.getOp().getOpType() == OperatorType.LOGICAL_PROJECT) {
            // This is not supported yet:
            // eg: select user_id, time, sum(tag_id) from user_tags group by user_id, time order by user_id + 1, time;";
            List<OptExpression> children = input.getInputs();
            if (children.size() == 1 && children.get(0).getOp().getOpType() == OperatorType.LOGICAL_TOPN) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get materialized views by ast.
     * @param input: the input of mv rewrite
     * @param ast
     * @return
     */
    public Set<MaterializedView> getMaterializedViewsByAst(OptExpression input, CachingMvPlanContextBuilder.AstKey ast) {
        CachingMvPlanContextBuilder instance = CachingMvPlanContextBuilder.getInstance();
        Set<MaterializedView> mvs = instance.getMvsByAst(ast);
        if (mvs != null) {
            return mvs;
        }

        // for debug use.
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().getQueryDebugOptions()
                .isEnableQueryTraceLog()) {
            try {
                Set<Table> queryTables = MvUtils.getAllTables(input).stream().collect(Collectors.toSet());
                int maxLevel = connectContext.getSessionVariable().getNestedMvRewriteMaxLevel();
                Set<MaterializedView> relatedMvs = MvUtils.getRelatedMvs(connectContext, maxLevel, queryTables);
                String mvNames = Joiner.on(",").join(relatedMvs.stream()
                        .map(mv -> mv.getName()).collect(Collectors.toList()));
                LOG.warn("Related MVs: {}", mvNames);
                LOG.warn("Query Key: {}", ast);
                List<CachingMvPlanContextBuilder.AstKey> candidates = instance.getAstsOfRelatedMvs(relatedMvs);
                for (CachingMvPlanContextBuilder.AstKey cacheKey : candidates) {
                    LOG.warn("Cached Key: {}", cacheKey);
                }
            } catch (Exception ignored) {
                LOG.warn("Get related mvs failed: {}", DebugUtil.getStackTrace(ignored));
            }
        }
        return Sets.newHashSet();
    }

    private OptExpression rewriteByTextMatch(OptExpression input,
                                             OptimizerContext context,
                                             CachingMvPlanContextBuilder.AstKey ast) {
        if (!isSupportForTextBasedRewrite(input)) {
            logMVRewrite(context, this, "TEXT_BASED_REWRITE is not supported for this input");
            return null;
        }

        QueryMaterializationContext queryMaterializationContext = context.getQueryMaterializationContext();
        try {
            Set<MaterializedView> candidateMvs = getMaterializedViewsByAst(input, ast);
            logMVRewrite(context, this, "TEXT_BASED_REWRITE matched mvs: {}",
                    candidateMvs.stream().map(mv -> mv.getName()).collect(Collectors.toList()));
            if (candidateMvs.isEmpty()) {
                return null;
            }
            int mvRelatedCount = 0;
            for (MaterializedView mv : candidateMvs) {
                Pair<Boolean, String> status = isValidForTextBasedRewrite(context, mv);
                if (!status.first) {
                    logMVRewrite(context, this, "MV {} cannot be used for rewrite, {}", mv.getName(), status.second);
                    continue;
                }
                if (!queryMaterializationContext.getRelatedMVs().contains(mv)) {
                    continue;
                }
                if (mvRelatedCount++ > mvRewriteRelatedMVsLimit) {
                    return null;
                }
                MvUpdateInfo mvUpdateInfo = queryMaterializationContext.getOrInitMVTimelinessInfos(mv);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    logMVRewrite(context, this, "MV {} cannot be used for rewrite, " +
                            "stale partitions {}", mv.getName(), mvUpdateInfo);
                    continue;
                }
                OptimizerTraceUtil.logMVRewrite(context, this, "TEXT_BASED_REWRITE: text matched with {}",
                        mv.getName());
                final MvPlanContext mvPlanContext = MvUtils.getMVPlanContext(connectContext, mv, true, true);
                if (mvPlanContext == null) {
                    logMVRewrite(context, this, "MV {} plan context is invalid", mv.getName());
                    continue;
                }

                final OptExpression mvPlan = mvPlanContext.getLogicalPlan();
                if (mvPlan == null) {
                    logMVRewrite(context, this, "MV {} plan context is null", mv.getName());
                    continue;
                }

                final Set<Table> queryTables = MvUtils.getAllTables(mvPlan).stream().collect(Collectors.toSet());
                final MaterializationContext mvContext = MvRewritePreprocessor.buildMaterializationContext(context,
                        mv, mvPlanContext, mvUpdateInfo, queryTables);
                final LogicalOlapScanOperator mvScanOperator = mvContext.getScanMvOperator();
                final List<ColumnRefOperator>  mvScanOutputColumns = MvUtils.getMvScanOutputColumnRefs(mv, mvScanOperator);

                // if mv is partitioned, and some partitions are outdated, then compensate it
                final Set<String> partitionNamesToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
                OptExpression mvCompensatePlan = null;
                if (CollectionUtils.isEmpty(partitionNamesToRefresh)) {
                    mvCompensatePlan = OptExpression.create(mvScanOperator);
                } else {
                    // if mv's query rewrite consistency mode is FORCE_MV, then do not compensate it because its
                    // partition compensation is not exactly by union rewrite, see {@class PartitionRetentionTableCompensation}.
                    if (mvUpdateInfo.getQueryRewriteConsistencyMode() == TableProperty.QueryRewriteConsistencyMode.FORCE_MV) {
                        return null;
                    }
                    logMVRewrite(context, this, "Partitioned MV {} is outdated which " +
                                    "contains some partitions to be refreshed: {}, compensate it with union rewrite",
                            mv.getName(), partitionNamesToRefresh);
                    mvCompensatePlan = MvPartitionCompensator.getMvTransparentPlan(mvContext, input, mvScanOutputColumns);
                }

                // do text based rewrite
                OptExpression rewritten = doTextMatchBasedRewrite(context, mvPlanContext, mvPlan,
                        mvScanOutputColumns, mvCompensatePlan, input);
                if (rewritten != null) {
                    IMaterializedViewMetricsEntity mvEntity =
                            MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                    mvEntity.increaseQueryTextBasedMatchedCount(1L);
                    OptimizerTraceUtil.logMVRewrite(context, this, "TEXT_BASED_REWRITE: {}", REWRITE_SUCCESS);
                    context.getQueryMaterializationContext().markRewriteSuccess(true);
                    return rewritten;
                }
            }
        } catch (Exception e) {
            logMVRewrite(context, this, "TEXT_BASED_REWRITE rewrite failed:{}", DebugUtil.getStackTrace(e));
            return null;
        }
        return null;
    }

    public Pair<Boolean, String> isValidForTextBasedRewrite(OptimizerContext context,
                                                            MaterializedView mv) {
        if (!mv.isActive()) {
            logMVRewrite(context, this, "MV is not active: {}", mv.getName());
            return Pair.create(false, "MV is not active");
        }

        if (!mv.isEnableRewrite()) {
            String message = PropertyAnalyzer.PROPERTY_MV_ENABLE_QUERY_REWRITE + "=" +
                    mv.getTableProperty().getMvQueryRewriteSwitch();
            logMVRewrite(context, this, message);
            return Pair.create(false, message);
        }
        return Pair.create(true, "");
    }

    // MV and query must have the same output order, otherwise match will fail.
    // TODO: support more patterns later between mv and query:
    //  - support different output orders
    //  - support different aliases
    //  - support query is subset of mv's output
    private OptExpression doTextMatchBasedRewrite(OptimizerContext context,
                                                  MvPlanContext mvPlanContext,
                                                  OptExpression mvPlan,
                                                  List<ColumnRefOperator> mvScanOutputColumns,
                                                  OptExpression rewrittenPlan,
                                                  OptExpression input) {
        MvUtils.deriveLogicalProperty(rewrittenPlan);
        MvUtils.deriveLogicalProperty(input);

        // mv's output column refs may be not the same with query's output column refs.
        // TODO: How to determine OptExpression's define outputs better?
        final List<ColumnRefOperator> mvPlanOutputColumns =
                mvPlan.getOutputColumns().getColumnRefOperators(mvPlanContext.getRefFactory());
        final List<ColumnRefOperator> queryPlanOutputColumns =
                input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        Preconditions.checkState(queryPlanOutputColumns.size() == mvPlanOutputColumns.size());

        final List<Integer> indexes = getOrderedOutputIndexes(mvPlanContext);
        final LogicalProjectOperator logicalProjectOperator = getReorderProjection(mvScanOutputColumns,
                queryPlanOutputColumns, indexes);
        final OptExpression mvProjectExpression = OptExpression.create(logicalProjectOperator, rewrittenPlan);

        if (input.getOp().getOpType() == OperatorType.LOGICAL_TOPN) {
            final LogicalTopNOperator queryTopNOperator = (LogicalTopNOperator) input.getOp();
            final OptExpression newTopNOptExp = OptExpression.create(new LogicalTopNOperator.Builder()
                    .withOperator(queryTopNOperator)
                    .setOrderByElements(queryTopNOperator.getOrderByElements())
                    .build(), mvProjectExpression);
            return newTopNOptExp;
        } else {
            return mvProjectExpression;
        }
    }

    private List<Integer> getOrderedOutputIndexes(MvPlanContext mvPlanContext) {
        final OptExpression mvLogicalPlan = mvPlanContext.getLogicalPlan();

        // mv's real output columns, reorder output columns by user's define
        // eg: 2, 1, 5 order by user's define but may contain duplicated outputs
        final List<ColumnRefOperator> mvPlanRealOutputColumns = mvPlanContext.getOutputColumns();
        final Map<ColumnRefOperator, Integer> mvPlanColRefOrderMap = IntStream.range(0, mvPlanRealOutputColumns.size())
                .boxed()
                .collect(Collectors.toMap(i -> mvPlanRealOutputColumns.get(i), i -> i, (oldV, newV) -> oldV));
        // eg: 1, 2, 5 order by col-ref id
        final List<ColumnRefOperator> mvPlanOutputColumns =
                mvLogicalPlan.getOutputColumns().getColumnRefOperators(mvPlanContext.getRefFactory());
        Preconditions.checkArgument(mvPlanOutputColumns.size() == mvPlanColRefOrderMap.size());
        return mvPlanOutputColumns.stream()
                .map(colRef -> mvPlanColRefOrderMap.get(colRef))
                .distinct()
                .collect(Collectors.toList());
    }

    private LogicalProjectOperator getReorderProjection(List<ColumnRefOperator> mvPlanOutputColumns,
                                                        List<ColumnRefOperator> queryPlanOutputColumns,
                                                        List<Integer> indexes) {
        final Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = IntStream.range(0, indexes.size())
                .boxed()
                .map(i -> Pair.create(indexes.get(i), i))
                .map(p -> Pair.create(queryPlanOutputColumns.get(p.second), mvPlanOutputColumns.get(p.first)))
                .collect(Collectors.toMap(p -> p.first, p -> p.second));
        return new LogicalProjectOperator(newColumnRefMap);
    }

    class TextBasedRewriteVisitor extends OptExpressionVisitor<OptExpression, ConnectContext> {
        private final OptimizerContext optimizerContext;
        private final MVTransformerContext mvTransformerContext;
        // sub-query text match count
        private int subQueryTextMatchCount = 0;

        public TextBasedRewriteVisitor(OptimizerContext optimizerContext,
                                       MVTransformerContext mvTransformerContext) {
            this.optimizerContext = optimizerContext;
            this.mvTransformerContext = mvTransformerContext;
        }

        private boolean isReachLimit() {
            return subQueryTextMatchCount > mvSubQueryTextMatchMaxCount;
        }

        private List<OptExpression> visitChildren(OptExpression optExpression, ConnectContext connectContext) {
            List<OptExpression> children = com.google.common.collect.Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                children.add(child.getOp().accept(this, child, null));
            }
            return children;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ConnectContext connectContext) {
            LogicalOperator op = (LogicalOperator) optExpression.getOp();
            if (SUPPORTED_REWRITE_OPERATOR_TYPES.contains(op.getOpType())) {
                OptExpression rewritten = doRewrite(optExpression);
                if (rewritten != null) {
                    return rewritten;
                }
            }
            List<OptExpression> children = visitChildren(optExpression, connectContext);
            return OptExpression.create(optExpression.getOp(), children);
        }

        private OptExpression doRewrite(OptExpression input) {
            Operator op = input.getOp();
            if (!mvTransformerContext.hasOpAST(op) || isReachLimit()) {
                return null;
            }

            // if op is in the AST map, which means op is a sub-query, then rewrite it.
            subQueryTextMatchCount += 1;

            // try to rewrite by text match
            ParseNode parseNode = mvTransformerContext.getOpAST(op);
            OptExpression rewritten = rewriteByTextMatch(input, optimizerContext,
                    new CachingMvPlanContextBuilder.AstKey(parseNode));
            if (rewritten != null) {
                return rewritten;
            }
            return null;
        }
    }
}