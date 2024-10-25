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
import com.google.common.base.Predicate;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewPushDownRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;
import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_MV_AGG_PUSH_DOWN_REWRITE;

/**
 * Support to push down aggregate functions below join operator and rewrite the query by mv transparently.
 * eg:
 *
 * MV:
 *  create materialized view mv0
 *  distributed by random as
 *  select a.id, a.dt, a.col, array_agg_distinct(a.user_id) as count_distinct_im_uv
 *  from a group by a.id, a.dt, a.cal;
 *
 * Query:
 *  select a.dt, a.col, array_agg_distinct(a.user_id) as count_distinct_im_uv
 *  from a join b on a.id = b.id group by a.dt, a.cal;
 *
 * Rewrite it by:
 * select a.dt, a.col, cardinality(array_agg_unique(a.count_distinct_im_uv)) as count_distinct_im_uv
 * from
 *  ( select id, dt, col, array_agg_unique(count_distinct_im_uv) as count_distinct_im_uv from mv0 group by id, dt, col
 *  ) as a join b on a.id = b.id
 * group by a.dt, a.cal;
 *
 * Rewrite result:
 * select a.dt, a.col, cardinality(array_agg_unique(a.count_distinct_im_uv)) as count_distinct_im_uv
 * from mv0 as a join b on a.id = b.id group by a.dt, a.cal;
 */
public class AggregateJoinPushDownRule extends BaseMaterializedViewRewriteRule {
    private static AggregateJoinPushDownRule INSTANCE = new AggregateJoinPushDownRule();

    public AggregateJoinPushDownRule() {
        super(RuleType.TF_MV_AGGREGATE_JOIN_PUSH_DOWN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTIJOIN)));
    }

    public static AggregateJoinPushDownRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableMaterializedViewPushDownRewrite()) {
            return false;
        }
        if (Utils.isOptHasAppliedRule(input, OP_MV_AGG_PUSH_DOWN_REWRITE)) {
            return false;
        }
        return super.check(input, context);
    }

    /**
     * MV can only contain LogicalScanOperator, LogicalProjectOperator, LogicalAggregationOperator for mv push down rewrite.
     */
    public static boolean isLogicalSPG(OptExpression root) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalAggregationOperator)) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPG(child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Why do we override this method? since queryExpression's tables are greater than mv's, so it's QUERY_DELTA mv match mode
     * which is not supported in the base class.
     *
     * Now AggregatedMaterializedViewPushDownRewriter only supports rewrite aggregation plans which are pushed down in the table
     * scan operator, so filter mv candidates which only contains ScanOperator, ProjectOperator, AggregationOperator.
     *
     * @param queryExpression: query opt expression.
     * @param context: optimizer context.
     * @param mvCandidateContexts: materialized view candidates prepared in the mv preprocessor.
     * @return: pruned materialized view candidates.
     */
    @Override
    public List<MaterializationContext> doPrune(OptExpression queryExpression,
                                                OptimizerContext context,
                                                List<MaterializationContext> mvCandidateContexts) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryExpression);
        List<MaterializationContext> validCandidateContexts = Lists.newArrayList();
        for (MaterializationContext mvContext : mvCandidateContexts) {
            if (isLogicalSPG(mvContext.getMvExpression()) && validMv(mvContext, scanOperators)) {
                validCandidateContexts.add(mvContext);
            } else {
                logMVRewrite(mvContext, "mv pruned");
            }
        }
        return validCandidateContexts;
    }

    private boolean validMv(MaterializationContext mvContext, List<LogicalScanOperator> scanOperators) {
        // mv is SPG, so there is only one baseTable
        long baseTableId = mvContext.getBaseTables().get(0).getId();
        Set<ColumnRefOperator> mvUsedColRefs = MvUtils.collectScanColumn(mvContext.getMvExpression());
        Set<String> mvUsedColNames = mvUsedColRefs.stream()
                .map(ColumnRefOperator::getName)
                .collect(Collectors.toSet());
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (scanOperator.getTable().getId() != baseTableId) {
                continue;
            }
            // mv should contain all columns that used in at least one query
            if (mvContainsAllColumnsUsedInScan(mvUsedColNames, scanOperator)) {
                return true;
            }
        }
        return false;
    }

    private boolean mvContainsAllColumnsUsedInScan(Set<String> mvUsedColNames, LogicalScanOperator scanOperator) {
        return scanOperator.getColRefToColumnMetaMap().values().stream().allMatch(
                (Predicate<Column>) c -> mvUsedColNames.contains(c.getName()));
    }

    @Override
    public IMaterializedViewRewriter createRewriter(OptimizerContext optimizerContext,
                                                    MvRewriteContext mvContext) {
        return new AggregatedMaterializedViewPushDownRewriter(mvContext, this);
    }
}