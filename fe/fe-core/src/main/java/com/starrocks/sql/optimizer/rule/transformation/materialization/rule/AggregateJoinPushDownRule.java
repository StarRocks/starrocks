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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewPushDownRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.IMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static final class QueryColumnCache {
        final Map<Table, Set<String>> groupByColumnsByTable;
        final Map<Table, Set<String>> predicateColumnsByTable;

        QueryColumnCache(Map<Table, Set<String>> groupByColumnsByTable,
                         Map<Table, Set<String>> predicateColumnsByTable) {
            this.groupByColumnsByTable = groupByColumnsByTable;
            this.predicateColumnsByTable = predicateColumnsByTable;
        }
    }

    // Identity-keyed cache: one entry per distinct queryExpression object.
    private final Cache<OptExpression, QueryColumnCache> queryColumnCache = CacheBuilder.newBuilder()
            .maximumSize(Config.mv_aggregate_join_push_down_query_column_cache_max_size)
            .weakKeys()
            .build();

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

    @Override
    public boolean isChooseBestMVBasedDataLayout(OptExpression expression) {
        return false;
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
        List<LogicalScanOperator> scanOperators = getScanOperator(queryExpression);
        QueryColumnCache cached = queryColumnCache.getIfPresent(queryExpression);
        Map<Table, Set<String>> queryGroupByColumnsByTable;
        Map<Table, Set<String>> queryPredicateColumnsByTable;
        if (cached != null) {
            queryGroupByColumnsByTable = cached.groupByColumnsByTable;
            queryPredicateColumnsByTable = cached.predicateColumnsByTable;
        } else {
            queryGroupByColumnsByTable = new HashMap<>();
            MvUtils.collectGroupByColumnsByTable(queryExpression, queryGroupByColumnsByTable);
            queryPredicateColumnsByTable = new HashMap<>();
            MvUtils.collectPredicateColumnsByTable(queryExpression, queryPredicateColumnsByTable);
            queryColumnCache.put(queryExpression, new QueryColumnCache(queryGroupByColumnsByTable, queryPredicateColumnsByTable));
        }
        List<MaterializationContext> validCandidateContexts = Lists.newArrayList();
        for (MaterializationContext mvContext : mvCandidateContexts) {
            if (!MvUtils.isLogicalSPG(mvContext.getMvExpression())) {
                logMVRewrite(mvContext, this, "mv pruned: not logical SPG");
            } else if (validMv(mvContext, scanOperators, queryGroupByColumnsByTable, queryPredicateColumnsByTable)) {
                validCandidateContexts.add(mvContext);
            }
        }
        return validCandidateContexts;
    }

    private boolean validMv(MaterializationContext mvContext, List<LogicalScanOperator> scanOperators,
                            Map<Table, Set<String>> queryGroupByColumnsByTable,
                            Map<Table, Set<String>> queryPredicateColumnsByTable) {
        // mv is SPG, so there is only one baseTable
        // Use Table.equals rather than getId() so external tables (e.g. IcebergTable) that
        // override equals() by catalog/db/tableIdentifier match correctly across plan rebuilds,
        // where CONNECTOR_ID_GENERATOR may assign different numeric ids to the same logical table.
        Table mvBaseTable = mvContext.getBaseTables().get(0);
        Set<String> mvUsedColNames = MvUtils.collectSPGScanColumnNames(mvContext.getMvExpression());
        boolean baseTableFoundInMv = false;
        boolean checkStage1 = false;
        for (LogicalScanOperator scanOperator : scanOperators) {
            if (!mvBaseTable.equals(scanOperator.getTable())) {
                continue;
            }
            baseTableFoundInMv = true;
            // mv should contain all columns that used in at least one query
            if (mvContainsAllColumnsUsedInScan(mvUsedColNames, scanOperator)) {
                checkStage1 = true;
                break;
            }
        }
        if (!checkStage1) {
            if (baseTableFoundInMv) {
                logMVRewrite(mvContext, this, "mv pruned: not contain all columns used in scan");
            }
            return false;
        }
        // no group by or predicate columns in query
        if (!queryGroupByColumnsByTable.containsKey(mvBaseTable)
                && !queryPredicateColumnsByTable.containsKey(mvBaseTable)) {
            return true;
        }
        if (mvContext.validSPGMvGroupByAndPredicateColumns(queryGroupByColumnsByTable.get(mvBaseTable),
                queryPredicateColumnsByTable.get(mvBaseTable))) {
            return true;
        }
        logMVRewrite(mvContext, "mv pruned: mv group by does not cover query group by + predicate columns");
        return false;
    }

    private List<LogicalScanOperator> getScanOperator(OptExpression root) {
        List<LogicalScanOperator> scanOperators = Lists.newArrayList();
        getScanOperator(root, scanOperators);
        return scanOperators;
    }

    private void getScanOperator(OptExpression root, List<LogicalScanOperator> scanOperators) {
        if (root.getOp() instanceof LogicalScanOperator) {
            scanOperators.add((LogicalScanOperator) root.getOp());
        } else {
            for (OptExpression child : root.getInputs()) {
                if (child.getOp() instanceof LogicalAggregationOperator) {
                    return;
                }
                getScanOperator(child, scanOperators);
            }
        }
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