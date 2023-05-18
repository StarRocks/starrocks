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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarEquivalenceExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Deprecated
// Can't promise output unique columns
//
// Push down Join
// Before:
//        Join
//      /      \
//  LEFT     Aggregation
//               \
//                ....
//
//
// After:
//     Aggregation
//          |
//         Join
//        /    \
//     LEFT           ....
//
// Requirements:
// 1. Must be Inner-Join or Left-Outer-Join
// 2. Aggregate isn't split and the functions can't contain COUNT(*)
// 3. Promise left output columns is unique
//
public class PushDownJoinAggRule extends TransformationRule {
    private static final Map<String, List<String>> MOCK_TPCH_PRIMARY_KEYS = ImmutableMap.<String, List<String>>builder()
            .put("region", Lists.newArrayList("r_regionkey"))
            .put("supplier", Lists.newArrayList("s_suppkey"))
            .put("partsupp", Lists.newArrayList("ps_partkey"))
            .put("orders", Lists.newArrayList("o_orderkey"))
            .put("customer", Lists.newArrayList("c_custkey"))
            .put("nation", Lists.newArrayList("n_nationkey"))
            .put("part", Lists.newArrayList("p_partkey"))
            .put("lineitem", Lists.newArrayList("l_orderkey"))
            .build();

    private PushDownJoinAggRule() {
        super(RuleType.TF_PUSH_DOWN_JOIN_AGG, Pattern.create(OperatorType.LOGICAL_JOIN).addChildren(
                Pattern.create(OperatorType.PATTERN_LEAF),
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF)));
    }

    private static final PushDownJoinAggRule INSTANCE = new PushDownJoinAggRule();

    public static PushDownJoinAggRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator ljo = (LogicalJoinOperator) input.getOp();
        LogicalAggregationOperator rightAggOperator = (LogicalAggregationOperator) input.getInputs().get(1).getOp();

        // Can't resolve COUNT(*)
        return (ljo.getJoinType().isInnerJoin() || ljo.getJoinType().isLeftOuterJoin())
                && rightAggOperator.getType().isGlobal()
                && !rightAggOperator.isSplit()
                && rightAggOperator.getAggregations().values().stream().noneMatch(CallOperator::isCountStar);
    }

    private List<String> getTpchMockPrimaryKeys(String table) {
        return MOCK_TPCH_PRIMARY_KEYS.getOrDefault(table.toLowerCase(), Collections.emptyList());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression leftExpression = input.getInputs().get(0);
        OptExpression rightAggExpression = input.getInputs().get(1);

        LogicalAggregationOperator rightAggOperator = (LogicalAggregationOperator) input.getInputs().get(1).getOp();
        LogicalJoinOperator inputJoinOperator = (LogicalJoinOperator) input.getOp();

        ColumnRefFactory factory = context.getColumnRefFactory();
        List<ColumnRefOperator> leftOutput =
                Arrays.stream(leftExpression.getOutputColumns().getColumnIds()).mapToObj(factory::getColumnRef).collect(
                        Collectors.toList());

        if (!checkIsUnique(leftOutput, leftExpression)) {
            return Collections.emptyList();
        }

        List<ScalarOperator> newJoinOnPredicate = Lists.newArrayList();
        List<ScalarOperator> newJoinFilterPredicate = Lists.newArrayList();
        List<ScalarOperator> newAggFilterPredicate = Lists.newArrayList();

        for (ScalarOperator on : Utils.extractConjuncts(inputJoinOperator.getOnPredicate())) {
            if (Collections.disjoint(rightAggOperator.getAggregations().keySet(), Utils.extractColumnRef(on))) {
                newJoinOnPredicate.add(on);
            } else {
                newAggFilterPredicate.add(on);
            }
        }

        for (ScalarOperator filter : Utils.extractConjuncts(inputJoinOperator.getPredicate())) {
            if (Collections.disjoint(rightAggOperator.getAggregations().keySet(), Utils.extractColumnRef(filter))) {
                newJoinFilterPredicate.add(filter);
            } else {
                newAggFilterPredicate.add(filter);
            }
        }

        List<ScalarOperator> requiredOutput =
                Arrays.stream(context.getTaskContext().getRequiredColumns().getColumnIds())
                        .mapToObj(factory::getColumnRef).collect(Collectors.toList());

        ScalarEquivalenceExtractor equivalenceExtractor = new ScalarEquivalenceExtractor();
        equivalenceExtractor.union(Utils.extractConjuncts(inputJoinOperator.getOnPredicate()));

        for (ColumnRefOperator gk : rightAggOperator.getGroupingKeys()) {
            if (requiredOutput.contains(gk)) {
                leftOutput.add(gk);
            } else {
                Set<ScalarOperator> equivalences = equivalenceExtractor.getEquivalentColumnRefs(gk);

                if (Collections.disjoint(equivalences, leftOutput)) {
                    leftOutput.add(gk);
                }
            }
        }

        ColumnRefSet newJoinPruneOutPutColumns = new ColumnRefSet(leftOutput);
        newJoinPruneOutPutColumns.union(rightAggExpression.inputAt(0).getOutputColumns());
        LogicalJoinOperator.Builder newJoinBuilder = new LogicalJoinOperator.Builder().withOperator(inputJoinOperator)
                .setOnPredicate(Utils.compoundAnd(newJoinOnPredicate))
                .setPredicate(Utils.compoundAnd(newJoinFilterPredicate));

        ColumnRefSet inputColumns = new ColumnRefSet();
        inputColumns.union(input.inputAt(0).getOutputColumns());
        inputColumns.union(rightAggExpression.inputAt(0).getOutputColumns());
        if (newJoinPruneOutPutColumns.equals(inputColumns)) {
            newJoinBuilder.setProjection(null);
        } else {
            newJoinBuilder.setProjection(
                    new Projection(newJoinPruneOutPutColumns.getStream().map(factory::getColumnRef)
                            .collect(Collectors.toMap(Function.identity(), Function.identity()))));
        }

        OptExpression newJoinExpression = new OptExpression(newJoinBuilder.build());
        newJoinExpression.getInputs().add(input.getInputs().get(0));
        newJoinExpression.getInputs().addAll(rightAggExpression.getInputs());

        LogicalAggregationOperator newAgg = new LogicalAggregationOperator(rightAggOperator.getType(), leftOutput,
                rightAggOperator.getAggregations());
        newAggFilterPredicate.add(rightAggOperator.getPredicate());
        newAgg.setPredicate(Utils.compoundAnd(newAggFilterPredicate));

        OptExpression newAggExpression = new OptExpression(newAgg);
        newAggExpression.getInputs().add(newJoinExpression);
        return Lists.newArrayList(newAggExpression);
    }

    // now just check isn't primary key
    private boolean checkIsUnique(List<ColumnRefOperator> leftOutput, OptExpression left) {
        return checkContainsAllPrimaryKey(leftOutput, left.getGroupExpression());
    }

    // Requirements:
    // 1. Scan node return columns must contains all primary key
    private boolean checkContainsAllPrimaryKey(List<ColumnRefOperator> leftOutput,
                                               GroupExpression groupExpression) {
        if (OperatorType.LOGICAL_OLAP_SCAN.equals(groupExpression.getOp().getOpType())) {
            LogicalOlapScanOperator loso = (LogicalOlapScanOperator) groupExpression.getOp();
            Map<ColumnRefOperator, Column> askColumnsMap = loso.getColRefToColumnMetaMap();

            List<String> keyColumns = getTpchMockPrimaryKeys(loso.getTable().getName());

            if (keyColumns.isEmpty()) {
                return false;
            }

            List<ColumnRefOperator> keyColumnRefs = askColumnsMap.entrySet().stream()
                    .filter(e -> keyColumns.contains(e.getValue().getName().toLowerCase())).map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            return leftOutput.containsAll(keyColumnRefs);
        }

        List<Group> groups = groupExpression.getInputs();
        for (Group group : groups) {
            GroupExpression expression = group.getFirstLogicalExpression();

            if (!checkContainsAllPrimaryKey(leftOutput, expression)) {
                return false;
            }
        }

        return true;
    }
}
