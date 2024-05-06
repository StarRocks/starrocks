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

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.metric.IMaterializedViewMetricsEntity;
import com.starrocks.metric.MaterializedViewMetricsRegistry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.MvRefreshArbiter.getPartitionNamesToRefreshForMv;
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
    private final Map<Operator, ParseNode> optToAstMap;

    // To avoid text match costing too much time, use parameters below to limit it.
    // limit for sub-query text match(default 4), no match when it <= 0
    private final int mvSubQueryTextMatchMaxCount;
    // limit for mvs which matched input query(default 64)
    private final long mvRewriteRelatedMVsLimit;

    private int subQueryTextMatchCount = 1;

    public TextMatchBasedRewriteRule(ConnectContext connectContext,
                                     StatementBase stmt,
                                     Map<Operator, ParseNode> optToAstMap) {
        super(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE, Pattern.create(OperatorType.PATTERN));

        this.connectContext = connectContext;
        this.stmt = stmt;
        this.optToAstMap = optToAstMap;
        this.mvSubQueryTextMatchMaxCount =
                connectContext.getSessionVariable().getMaterializedViewSubQueryTextMatchMaxCount();
        this.mvRewriteRelatedMVsLimit =
                connectContext.getSessionVariable().getCboMaterializedViewRewriteRelatedMVsLimit();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression rewritten = doTransform(context, input, stmt);
        return (rewritten != null) ? ImmutableList.of(rewritten) : ImmutableList.of(input);
    }

    private OptExpression doTransform(OptimizerContext context,
                                      OptExpression input,
                                      ParseNode parseNode) {
        if (context.getOptimizerConfig().isRuleDisable(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE)) {
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

        OptExpression rewritten = rewriteByTextMatch(input, context, parseNode);
        if (rewritten != null) {
            logMVRewrite(context, this, "Materialized view text based rewrite failed, " +
                    "try to rewrite sub-query again");
            return rewritten;
        }
        // try to rewrite sub-query again if exact-match failed.
        if (optToAstMap == null || optToAstMap.isEmpty()) {
            logMVRewrite(context, this, "OptToAstMap is empty, no try to rewrite sub-query again");
            return null;
        }
        return input.getOp().accept(new TextBasedRewriteVisitor(context, optToAstMap), input, connectContext);
    }

    /**
     * Since @{LocalMetastore#createMaterializedView} uses
     * {@code statement.setInlineViewDef(AstToSQLBuilder.toSQL(queryStatement));} to store user's define query,
     * and {@link AstToSQLBuilder} is not reentrant for now, so needs to normalize input query as mv's define query.
     * TODO: This is expensive, remove this if {@link AstToSQLBuilder} is reentrant.
     * @param queryAst : input query parse node.
     */
    private ParseNode normalizeAst(ParseNode queryAst) {
        String query = AstToSQLBuilder.toSQL(queryAst);
        return MvUtils.getQueryAst(query);
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
    public Set<MaterializedView> getMaterializedViewsByAst(OptExpression input, ParseNode ast) {
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
                CachingMvPlanContextBuilder.AstKey astKey = new CachingMvPlanContextBuilder.AstKey(ast);
                LOG.warn("Query Key: {}", astKey);
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
                                             ParseNode queryAst) {
        if (!isSupportForTextBasedRewrite(input)) {
            logMVRewrite(context, this, "TEXT_BASED_REWRITE is not supported for this input");
            return null;
        }

        try {
            ParseNode normalizedAst = normalizeAst(queryAst);
            Set<MaterializedView> candidateMvs = getMaterializedViewsByAst(input, normalizedAst);
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
                if (mvRelatedCount++ > mvRewriteRelatedMVsLimit) {
                    return null;
                }
                MvUpdateInfo mvUpdateInfo = getPartitionNamesToRefreshForMv(mv, true);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    logMVRewrite(context, this, "MV {} cannot be used for rewrite, " +
                            "stale partitions {}", mv.getName(), mvUpdateInfo);
                    continue;
                }
                Set<String> partitionNamesToRefresh = mvUpdateInfo.getMvToRefreshPartitionNames();
                if (!partitionNamesToRefresh.isEmpty()) {
                    logMVRewrite(context, this, "Partitioned MV {} is outdated which " +
                                    "contains some partitions to be refreshed: {}, and cannot compensate it to predicate",
                            mv.getName(), partitionNamesToRefresh);
                    continue;
                }
                OptimizerTraceUtil.logMVRewrite(context, this, "TEXT_BASED_REWRITE: text matched with {}",
                        mv.getName());

                MvPlanContext mvPlanContext = MvUtils.getMVPlanContext(connectContext, mv, true);
                if (mvPlanContext == null) {
                    logMVRewrite(context, this, "MV {} plan context is invalid", mv.getName());
                    continue;
                }

                OptExpression mvPlan = mvPlanContext.getLogicalPlan();
                if (mvPlan == null) {
                    logMVRewrite(context, this, "MV {} plan context is null", mv.getName());
                    continue;
                }

                // do: text match based mv rewrite
                OptExpression rewritten = doTextMatchBasedRewrite(context, mvPlanContext, mv, input);
                if (rewritten != null) {
                    IMaterializedViewMetricsEntity mvEntity =
                            MaterializedViewMetricsRegistry.getInstance().getMetricsEntity(mv.getMvId());
                    mvEntity.increaseQueryTextBasedMatchedCount(1L);
                    OptimizerTraceUtil.logMVRewrite(context, this, "TEXT_BASED_REWRITE: {}", REWRITE_SUCCESS);
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

    private OptExpression doTextMatchBasedRewrite(OptimizerContext context,
                                                  MvPlanContext mvPlanContext,
                                                  MaterializedView mv,
                                                  OptExpression input) {
        final OptExpression mvPlan = mvPlanContext.getLogicalPlan();
        final LogicalOlapScanOperator mvScanOperator = MvRewritePreprocessor.createScanMvOperator(mv,
                context.getColumnRefFactory(), Sets.newHashSet());

        // MV and query must have the same output order, otherwise match will fail.
        // TODO: support more patterns later between mv and query:
        //  - support different output orders
        //  - support different aliases
        //  - support query is subset of mv's output
        List<Column> mvColumns = MvRewritePreprocessor.getMvOutputColumns(mv);
        Map<String, ColumnRefOperator> mvColRefNameColRefMapping = Maps.newHashMap();
        mvScanOperator.getOutputColumns().stream().forEach(colRef ->
                mvColRefNameColRefMapping.put(colRef.getName(), colRef));
        Preconditions.checkState(mvColRefNameColRefMapping.size() == mvColumns.size());

        // Determine mv's output column refs by mv's define query
        List<ColumnRefOperator> mvColumnRefs = Lists.newArrayList();
        for (Column col : mvColumns) {
            if (!mvColRefNameColRefMapping.containsKey(col.getName())) {
                logMVRewrite(context, this, "MV column name {} is not found in the mapping {}",
                        col.getName(), mvColRefNameColRefMapping.keySet().stream().collect(Collectors.toList()));
                return null;
            }
            mvColumnRefs.add(mvColRefNameColRefMapping.get(col.getName()));
        }

        MvUtils.deriveLogicalProperty(input);

        // eg: 1, 2, 5 order by col-ref id
        final List<ColumnRefOperator> mvPlanOutputColumns =
                mvPlan.getOutputColumns().getColumnRefOperators(mvPlanContext.getRefFactory());
        Map<ColumnRefOperator, Integer> mvPlanColRefOrderMap = Maps.newHashMap();
        for (int i = 0; i < mvPlanOutputColumns.size(); i++) {
            mvPlanColRefOrderMap.put(mvPlanOutputColumns.get(i), i);
        }
        // eg: 2, 1, 5 order by user's define
        final List<ColumnRefOperator> mvPlanRealOutputColumns = mvPlanContext.getOutputColumns();
        List<Integer> outputOrderIndices = mvPlanRealOutputColumns.stream()
                .map(colRef -> mvPlanColRefOrderMap.get(colRef))
                .collect(Collectors.toList());

        // mv's output column refs may be not the same with query's output column refs.
        // TODO: How to determine OptExpression's define outputs better?
        final List<ColumnRefOperator> queryPlanOutputColumns =
                input.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory());
        // use mvPlanOutputColumns instead of mvPlanRealOutputColumns because one column may repeat multi times.
        Preconditions.checkState(queryPlanOutputColumns.size() == mvPlanOutputColumns.size());
        List<ColumnRefOperator> queryColumnRefs = outputOrderIndices.stream()
                .map(i -> queryPlanOutputColumns.get(i))
                .collect(Collectors.toList());

        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = Maps.newHashMap();
        Preconditions.checkState(mvColumnRefs.size() == queryColumnRefs.size());
        for (int i = 0; i < mvColumnRefs.size(); i++) {
            newColumnRefMap.put(queryColumnRefs.get(i), mvColumnRefs.get(i));
        }
        LogicalProjectOperator logicalProjectOperator = new LogicalProjectOperator(newColumnRefMap);

        final LogicalOlapScanOperator logicalOlapScanOperator = LogicalOlapScanOperator.builder()
                .withOperator(mvScanOperator)
                .build();
        final OptExpression mvScanOptExpression = OptExpression.create(logicalOlapScanOperator);
        OptExpression mvProjectExpression = OptExpression.create(logicalProjectOperator, mvScanOptExpression);

        if (input.getOp().getOpType() == OperatorType.LOGICAL_TOPN) {
            LogicalTopNOperator queryTopNOperator = (LogicalTopNOperator) input.getOp();
            OptExpression newTopNOptExp = OptExpression.create(new LogicalTopNOperator.Builder()
                    .withOperator(queryTopNOperator)
                    .setOrderByElements(queryTopNOperator.getOrderByElements())
                    .build(), mvProjectExpression);
            return newTopNOptExp;
        } else {
            return mvProjectExpression;
        }
    }

    class TextBasedRewriteVisitor extends OptExpressionVisitor<OptExpression, ConnectContext> {
        private final OptimizerContext optimizerContext;
        private final Map<Operator, ParseNode> optToAstMap;
        public TextBasedRewriteVisitor(OptimizerContext optimizerContext,
                                       Map<Operator, ParseNode> optToAstMap) {
            this.optimizerContext = optimizerContext;
            this.optToAstMap = optToAstMap;
        }

        private List<OptExpression> visitChildren(OptExpression optExpression, ConnectContext connectContext) {
            List<OptExpression> children = com.google.common.collect.Lists.newArrayList();
            for (OptExpression child : optExpression.getInputs()) {
                children.add(child.getOp().accept(this, child, null));
            }
            return children;
        }

        private boolean isReachLimit() {
            return subQueryTextMatchCount++ > mvSubQueryTextMatchMaxCount;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, ConnectContext connectContext) {
            LogicalOperator op = (LogicalOperator) optExpression.getOp();
            if (SUPPORTED_REWRITE_OPERATOR_TYPES.contains(op.getOpType())) {
                if (isReachLimit()) {
                    return optExpression;
                }
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
            if (!optToAstMap.containsKey(op)) {
                return null;
            }
            ParseNode parseNode = optToAstMap.get(op);
            OptExpression rewritten = rewriteByTextMatch(input, optimizerContext, parseNode);
            if (rewritten != null) {
                return rewritten;
            }
            return null;
        }
    }
}