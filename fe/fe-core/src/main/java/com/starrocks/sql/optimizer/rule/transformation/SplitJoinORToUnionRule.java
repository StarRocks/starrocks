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

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.transformer.QueryTransformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/*
 * Rule Objective:
 *
 * Transform the following SQL:
 *   SELECT *
 *   FROM left_table l
 *   JOIN right_table r
 *     ON l.k1 = r.v1 OR l.k1 = r.v2
 *
 * Into:
 *   SELECT *
 *   FROM left_table l
 *   JOIN right_table r
 *     ON l.k1 = r.v1
 *   UNION ALL
 *   SELECT *
 *   FROM left_table l
 *   JOIN right_table r
 *   ON l.k1 = r.v2
 *   WHERE l.k1 != r.v1 OR l.k1 IS NULL OR r.v1 IS NULL
 *
 *
 *                                  +------+
 *                                  | JOIN |
 *                                  +------+
 *                                     |
 *                                     v
 *                         +-------------------------+
 *                         | CompoundPredicate(OR)   |
 *                         +-------------------------+
 *                              /         \
 *                             /           \
 *         +-------------------------+    +-------------------------+
 *         | BinaryPredicateOperator |    | BinaryPredicateOperator |
 *         | (l.k1 = r.v1)           |    | (l.k1 = r.v2)           |
 *         +-------------------------+    +-------------------------+
 *
 *                                       |
 *                                       v
 *
 *                                   +--------+
 *                                   | UNION  |
 *                                   +--------+
 *                                      /   \
 *                                     /     \
 *                            +------+       +------+
 *                            | JOIN |       | JOIN |
 *                            +------+       +------+
 *                               /                  \
 *                              /               +-------------------------+
 *                             /                | CompoundPredicate(AND)  |
 *                            /                 +-------------------------+
 *                           /                      /                    \
 *       +-------------------------+   +------------------------+  +------------------------+
 *       | BinaryPredicateOperator |   | BinaryPredicateOperator|  |    CompoundPredicate   |
 *       | (l.k1 = r.v1)           |   | (l.k1 = r.v2)          |  |         (OR)           |
 *       +-------------------------+   +------------------------+  +------------------------+
 *                                                                    /               \
 *                                          +------------------------+       +------------------------+
 *                                          |    CompoundPredicate   |       |    IsNullPredicate     |
 *                                          |           (OR)         |       |      (r.v1 IS NULL)    |
 *                                          +------------------------+       +------------------------+
 *                                             /                    \
 *                                +------------------------+    +------------------------+
 *                                | BinaryPredicateOperator|    |    IsNullPredicate     |
 *                                |     (l.k1 != r.v1)     |    |     (l.k1 IS NULL)     |
 *                                +------------------------+    +------------------------+
 *
 *
 */

public class SplitJoinORToUnionRule extends TransformationRule {

    private static final SplitJoinORToUnionRule INSTANCE = new SplitJoinORToUnionRule();

    private SplitJoinORToUnionRule() {
        super(RuleType.TF_OR_TO_UNION_ALL_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN));
    }

    public static SplitJoinORToUnionRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnabledRewriteOrToUnionAllJoin()) {
            return false;
        }

        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.getOp();
        JoinOperator joinType = joinOp.getJoinType();
        if (joinType != JoinOperator.INNER_JOIN && joinType != JoinOperator.CROSS_JOIN) {
            return false;
        }

        ScalarOperator predicate = joinOp.getOnPredicate();
        if (!(predicate instanceof CompoundPredicateOperator)) {
            return false;
        }

        CompoundPredicateOperator compoundPredicate = predicate.cast();
        if (compoundPredicate.getCompoundType() != CompoundPredicateOperator.CompoundType.OR) {
            return false;
        }

        if (containsNonDeterministicFunction(input)) {
            return false;
        }

        for (OptExpression child : input.getInputs()) {
            if (containsUnsupportedOperators(child)) {
                return false;
            }
        }

        List<ScalarOperator> disjunctivePredicates = Utils.extractDisjunctive(predicate);
        if (disjunctivePredicates.isEmpty()) {
            return false;
        }

        int maxOrPredicates = context.getSessionVariable().getMaxOrToUnionAllPredicates();
        if (disjunctivePredicates.size() > maxOrPredicates) {
            return false;
        }

        ColumnRefSet leftColumns = input.inputAt(0).getOutputColumns();
        ColumnRefSet rightColumns = input.inputAt(1).getOutputColumns();
        List<BinaryPredicateOperator> equalPredicates =
                JoinHelper.getEqualsPredicate(leftColumns, rightColumns, disjunctivePredicates);

        if (equalPredicates.size() != disjunctivePredicates.size()) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.getOp();
        CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) joinOp.getOnPredicate();

        List<BinaryPredicateOperator> binaryPredicateList = Utils.extractDisjunctive(compoundPredicate)
                .stream()
                .filter(p -> p instanceof BinaryPredicateOperator)
                .map(p -> (BinaryPredicateOperator) p)
                .collect(Collectors.toList());

        if (binaryPredicateList.isEmpty()) {
            return Lists.newArrayList(input);
        }

        Map<Integer, List<ScalarOperator>> cumulativePredicateMap =
                createCumulativePredicateMap(binaryPredicateList, context);

        for (int j = 0; j < cumulativePredicateMap.size(); j++) {
            List<ScalarOperator> branchPredicates = cumulativePredicateMap.get(j);
            int branchSize = branchPredicates.size();
            if (branchSize > 1) {
                for (int k = 0; k < branchSize - 1; k++) {
                    ScalarOperator predicate = branchPredicates.get(k);
                    if (predicate instanceof BinaryPredicateOperator eqPredicate) {
                        BinaryPredicateOperator nePredicate =
                                new BinaryPredicateOperator(BinaryType.NE, eqPredicate.getChildren());

                        ScalarOperator leftChild = new IsNullPredicateOperator(eqPredicate.getChild(0));
                        ScalarOperator rightChild = new IsNullPredicateOperator(eqPredicate.getChild(1));

                        List<ScalarOperator> orConditions = Lists.newArrayList(nePredicate, leftChild, rightChild);
                        ScalarOperator orPredicate =
                                Utils.createCompound(CompoundPredicateOperator.CompoundType.OR, orConditions);
                        branchPredicates.set(k, orPredicate);
                    }
                }
            }
        }

        List<LogicalJoinOperator> joinBranchList = new ArrayList<>();
        for (int i = 0; i < cumulativePredicateMap.size(); i++) {
            List<ScalarOperator> branchPredicates = cumulativePredicateMap.get(i);
            LogicalJoinOperator branchJoin;
            if (branchPredicates.size() == 1) {
                ScalarOperator predicate0 = branchPredicates.get(0);
                LogicalJoinOperator.Builder builder =
                        new LogicalJoinOperator.Builder().withOperator(joinOp).setOnPredicate(predicate0);
                branchJoin = builder.build();
            } else {
                ScalarOperator combinedPredicate =
                        Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, branchPredicates);
                LogicalJoinOperator.Builder builder = new LogicalJoinOperator.Builder()
                        .withOperator(joinOp)
                        .setOnPredicate(combinedPredicate);
                branchJoin = builder.build();
            }
            joinBranchList.add(branchJoin);
        }

        List<OptExpression> unionChildren = new ArrayList<>();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        boolean isFirst = true;
        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        for (LogicalJoinOperator branchJoin : joinBranchList) {
            if (isFirst) {
                OptExpression branchOpt = OptExpression.create(branchJoin, input.getInputs());
                unionChildren.add(branchOpt);
                outputColumns = context.getColumnRefFactory().getColumnRefs(input.getOutputColumns()).stream().toList();
                childOutputColumns.add(outputColumns);
                isFirst = false;
                continue;
            }

            OptExpression branchOpt = OptExpression.create(branchJoin, input.getInputs());
            OptExpressionDuplicator duplicator =
                    new OptExpressionDuplicator(context.getColumnRefFactory(), context);
            branchOpt = duplicator.duplicate(branchOpt);
            List<ColumnOutputInfo> outputInfo = input.getRowOutputInfo().getColumnOutputInfo();

            List<ColumnRefOperator> newOutputColumns = new ArrayList<>();
            for (ColumnOutputInfo colInfo : outputInfo) {
                newOutputColumns.add(duplicator.getColumnMapping().get(colInfo.getColumnRef()));
            }

            unionChildren.add(branchOpt);
            childOutputColumns.add(newOutputColumns);
        }

        LogicalUnionOperator.Builder builder =
                new LogicalUnionOperator.Builder().isUnionAll(true)
                        .setChildOutputColumns(childOutputColumns).setOutputColumnRefOp(outputColumns);

        if (joinOp.hasLimit()) {
            builder.setLimit(joinOp.getLimit());
        }

        LogicalUnionOperator unionOp = builder.build();
        OptExpression result = OptExpression.create(unionOp, unionChildren);

        return Lists.newArrayList(result);
    }

    private boolean containsUnsupportedOperators(OptExpression expr) {
        if (expr == null) {
            return false;
        }

        if (expr.getOp().getOpType() == OperatorType.LOGICAL_LIMIT) {
            return true;
        }

        if (expr.getOp() instanceof LogicalJoinOperator joinOp) {
            JoinOperator joinType = joinOp.getJoinType();
            if (joinType.isOuterJoin()) {
                return true;
            }
        }

        if (expr.getOp() instanceof LogicalAggregationOperator aggOp) {
            AggType type = aggOp.getType();
            if (type.name().contains("GROUPING_SETS") ||
                    type.name().contains("CUBE") ||
                    type.name().contains("ROLLUP")) {
                return true;
            }

            for (CallOperator call : aggOp.getAggregations().values()) {
                if (call.getFnName().equalsIgnoreCase(QueryTransformer.GROUPING) ||
                        call.getFnName().equalsIgnoreCase(QueryTransformer.GROUPING_ID)) {
                    return true;
                }
            }
        }

        for (OptExpression child : expr.getInputs()) {
            if (containsUnsupportedOperators(child)) {
                return true;
            }
        }

        return false;
    }

    private boolean containsNonDeterministicFunction(OptExpression expr) {
        if (expr.getOp() instanceof LogicalOperator logicalOp) {

            if (logicalOp.getPredicate() != null &&
                    containsNonDeterministicFn(logicalOp.getPredicate())) {
                return true;
            }

            if (logicalOp.getProjection() != null) {
                for (ScalarOperator projExpr : logicalOp.getProjection().getColumnRefMap().values()) {
                    if (containsNonDeterministicFn(projExpr)) {
                        return true;
                    }
                }
            }
        }

        for (OptExpression child : expr.getInputs()) {
            if (containsNonDeterministicFunction(child)) {
                return true;
            }
        }

        return false;
    }

    private boolean containsNonDeterministicFn(ScalarOperator expr) {
        if (expr instanceof CallOperator callOp) {
            String functionName = callOp.getFnName();
            if (functionName != null &&
                    FunctionSet.allNonDeterministicFunctions.contains(functionName)) {
                return true;
            }
        }

        for (ScalarOperator child : expr.getChildren()) {
            if (containsNonDeterministicFn(child)) {
                return true;
            }
        }

        return false;
    }

    private Map<Integer, List<ScalarOperator>> createCumulativePredicateMap(
            List<BinaryPredicateOperator> binaryPredicateList, OptimizerContext context) {

        Map<Integer, List<ScalarOperator>> cumulativePredicateMap = new HashMap<>();
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();

        for (int i = 0; i < binaryPredicateList.size(); i++) {
            List<ScalarOperator> currentBranchPredicates = new ArrayList<>();
            for (int j = 0; j <= i; j++) {
                BinaryPredicateOperator originalPredicate = binaryPredicateList.get(j);

                OptExpressionDuplicator duplicator = new OptExpressionDuplicator(columnRefFactory, context);
                ScalarOperator rewrittenPredicate = duplicator.rewriteAfterDuplicate(originalPredicate);

                currentBranchPredicates.add(rewrittenPredicate);
            }
            cumulativePredicateMap.put(i, currentBranchPredicates);
        }

        return cumulativePredicateMap;
    }
}
