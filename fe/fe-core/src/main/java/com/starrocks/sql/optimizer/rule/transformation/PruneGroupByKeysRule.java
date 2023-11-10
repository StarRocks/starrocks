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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// for aggregate queries with group by keys like `group by col,expr(col),constant` or `group by constant`,
// these expr and constant won't affect the effect of aggregation grouping, we can remove them
public class PruneGroupByKeysRule extends TransformationRule {
    public PruneGroupByKeysRule() {
        super(RuleType.TF_PRUNE_GROUP_BY_KEYS, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) input.getOp();
        List<ColumnRefOperator> groupingKeys = aggOperator.getGroupingKeys();
        if (groupingKeys == null || groupingKeys.isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) input.getOp();
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.getInputs().get(0).getOp();

        List<ColumnRefOperator> groupingKeys = aggOperator.getGroupingKeys();
        Map<ColumnRefOperator, CallOperator> aggregations = aggOperator.getAggregations();
        Map<ColumnRefOperator, ScalarOperator> projections = projectOperator.getColumnRefMap();

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        List<ColumnRefOperator> newGroupingKeys = Lists.newArrayList();

        Map<ColumnRefOperator, ScalarOperator> newProjections = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> newPostAggProjections = Maps.newHashMap();
        Set<ColumnRefOperator> removedGroupingKeys = new HashSet<>();
        Pair<ColumnRefOperator, ScalarOperator> firstConstantGroupingKey = null;

        Set<Integer> existedColumnIds = new HashSet<>();
        for (ColumnRefOperator groupingKey : groupingKeys) {
            ScalarOperator groupingExpr = projections.get(groupingKey);
            Preconditions.checkState(groupingExpr != null,
                    "cannot find grouping key from projections");
            if (groupingExpr.isColumnRef()) {
                int columnId = ((ColumnRefOperator) groupingExpr).getId();
                // if this column already exists, ignore it, otherwise, add it into new grouping key
                if (!existedColumnIds.contains(columnId)) {
                    newGroupingKeys.add(groupingKey);
                    existedColumnIds.add(columnId);
                    newProjections.put(groupingKey, groupingExpr);
                    newPostAggProjections.put(groupingKey, groupingKey);
                    continue;
                }
            } else if (groupingExpr.isConstant()) {
                if (firstConstantGroupingKey == null) {
                    firstConstantGroupingKey = new Pair<>(groupingKey, groupingExpr);
                }
            } else {
                ColumnRefSet usedColumns = groupingExpr.getUsedColumns();
                // if this expr contains only one column that already exists in the grouping key,
                // it won't affect the grouping result, just remove it.
                // Otherwise, we should reserve it.
                if (usedColumns.size() == 1 && !Utils.hasNonDeterministicFunc(groupingExpr)) {
                    int columnId = usedColumns.getColumnIds()[0];
                    if (!existedColumnIds.contains(columnId)) {
                        newGroupingKeys.add(groupingKey);
                        newProjections.put(groupingKey, groupingExpr);
                        newPostAggProjections.put(groupingKey, groupingKey);
                        continue;
                    }
                } else {
                    newGroupingKeys.add(groupingKey);
                    newProjections.put(groupingKey, groupingExpr);
                    newPostAggProjections.put(groupingKey, groupingKey);
                    continue;
                }
            }
            removedGroupingKeys.add(groupingKey);
            newPostAggProjections.put(groupingKey, groupingExpr);
        }

        if (newGroupingKeys.isEmpty() && !aggregations.isEmpty()) {
            // if all group by keys are pruned, there must be all constant key.
            Preconditions.checkState(firstConstantGroupingKey != null);
            // if all group by keys are constant and the query has aggregations, in order to ensure the correct result,
            // we can't prune all group by keys.
            // for example, suppose table is empty,
            // `select 'a','b',count(*) from table group by 'a','b'` will return an empty set,
            // but `select 'a','b',count(*) from table` will return one row.
            // In this case, we should reserve at least one key,
            // here we choose the first constant key
            newGroupingKeys.add(firstConstantGroupingKey.first);
            newProjections.put(firstConstantGroupingKey.first, firstConstantGroupingKey.second);

            removedGroupingKeys.remove(firstConstantGroupingKey.first);
        }

        if (newGroupingKeys.size() == groupingKeys.size()) {
            return Lists.newArrayList();
        }

        if (newGroupingKeys.isEmpty() && aggregations.isEmpty()) {
            // If agg's predicate is not null, cannot prune it.
            // eg: select 1 from t group by null having 1=0, it returns empty rather than input + limit 1.
            if (aggOperator.getPredicate() != null) {
                return Lists.newArrayList();
            } else {
                // for queries with all constant in project and group by keys,
                // like `select 'a','b' from table group by 'c','d'`,
                // we can remove agg node and rewrite it to `select 'a','b' from table limit 1`
                OptExpression result = OptExpression.create(
                        LogicalLimitOperator.init(1),
                        OptExpression.create(
                                projectOperator, input.getInputs().get(0).getInputs()));
                return Lists.newArrayList(result);
            }
        }
        // update projection by aggregation
        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation : aggregations.entrySet()) {
            CallOperator aggExpr = aggregation.getValue();
            ColumnRefSet usedColumns = aggExpr.getUsedColumns();
            Set<ColumnRefOperator> columnRefOperators = columnRefFactory.getColumnRefs(usedColumns);
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                ScalarOperator scalarOperator = projections.get(columnRefOperator);
                Preconditions.checkState(scalarOperator != null, "cannot find column ref");
                newProjections.put(columnRefOperator, scalarOperator);
            }
            newPostAggProjections.put(aggregation.getKey(), aggregation.getKey());
        }
        // add a post agg project, all removed group by key should be here
        LogicalProjectOperator newPostAggProjectOperator = new LogicalProjectOperator(newPostAggProjections);

        List<ColumnRefOperator> newPartitionColumns = aggOperator.getPartitionByColumns()
                .stream().filter(columnRefOperator -> !removedGroupingKeys.contains(columnRefOperator))
                .collect(Collectors.toList());

        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator.Builder().withOperator(aggOperator)
                .setType(AggType.GLOBAL)
                .setGroupingKeys(newGroupingKeys)
                .setPartitionByColumns(newPartitionColumns).build();

        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(newProjections);

        OptExpression result = OptExpression.create(newPostAggProjectOperator,
                OptExpression.create(newAggOperator,
                        OptExpression.create(newProjectOperator, input.getInputs().get(0).getInputs())));

        return Lists.newArrayList(result);
    }
}
