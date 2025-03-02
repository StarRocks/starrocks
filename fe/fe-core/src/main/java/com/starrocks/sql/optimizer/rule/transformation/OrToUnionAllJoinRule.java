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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

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
 *     ON l.k1 = r.v2
 *   WHERE l.k1 != r.v1;
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
 *       | BinaryPredicateOperator |   | BinaryPredicateOperator|  | BinaryPredicateOperator|
 *       | (l.k1 = r.v1)           |   | (l.k1 = r.v2)          |  | (l.k1 != r.v1)         |
 *       +-------------------------+   +------------------------+  +------------------------+
 *
 *
 */

public class OrToUnionAllJoinRule extends TransformationRule {

    private static final OrToUnionAllJoinRule INSTANCE = new OrToUnionAllJoinRule();

    private OrToUnionAllJoinRule() {
        super(RuleType.TF_OR_TO_UNION_ALL_JOIN, Pattern.create(OperatorType.LOGICAL_JOIN));
    }

    public static OrToUnionAllJoinRule getInstance() {
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

        ScalarOperator predicate = joinOp.getOriginalOnPredicate();
        if (!(predicate instanceof CompoundPredicateOperator)) {
            return false;
        }

        CompoundPredicateOperator compoundPredicate = predicate.cast();
        if (compoundPredicate.getCompoundType() != CompoundPredicateOperator.CompoundType.OR) {
            return false;
        }

        if (containsLimitOrNonDeterministicFunction(input)) {
            return false;
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

        return equalPredicates.size() == disjunctivePredicates.size();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.getOp();
        CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) joinOp.getOriginalOnPredicate();

        List<BinaryPredicateOperator> binaryPredicateList = Utils.extractDisjunctive(compoundPredicate)
                .stream()
                .filter(p -> p instanceof BinaryPredicateOperator)
                .map(p -> (BinaryPredicateOperator) p)
                .collect(Collectors.toList());

        if (binaryPredicateList.isEmpty()) {
            return Lists.newArrayList(input);
        }

        Map<Integer, List<BinaryPredicateOperator>> cumulativePredicateMap =
                createCumulativePredicateMap(binaryPredicateList);
        
        for (int j = 0; j < cumulativePredicateMap.size(); j++) {
            List<BinaryPredicateOperator> branchPredicates = cumulativePredicateMap.get(j);
            int branchSize = branchPredicates.size();
            if (branchSize > 1) {
                for (int k = 0; k < branchSize - 1; k++) {
                    BinaryPredicateOperator eqPredicate = branchPredicates.get(k);
                    BinaryPredicateOperator nePredicate =
                            new BinaryPredicateOperator(BinaryType.NE, eqPredicate.getChildren());
                    branchPredicates.set(k, nePredicate);
                }
            }
        }

        List<LogicalJoinOperator> joinBranchList = new ArrayList<>();
        for (int i = 0; i < cumulativePredicateMap.size(); i++) {
            List<BinaryPredicateOperator> branchPredicates = cumulativePredicateMap.get(i);
            LogicalJoinOperator branchJoin;
            if (branchPredicates.size() == 1) {
                BinaryPredicateOperator predicate0 = branchPredicates.get(0);
                branchJoin = new LogicalJoinOperator.Builder().withOperator(joinOp).setOnPredicate(predicate0)
                        .setOriginalOnPredicate(predicate0).build();
            } else {
                List<ScalarOperator> scalarOps = new ArrayList<>(branchPredicates);
                ScalarOperator combinedPredicate = buildAndPredicate(scalarOps);
                ScalarOperator lastPredicate = branchPredicates.get(branchPredicates.size() - 1);
                branchJoin = new LogicalJoinOperator.Builder()
                        .withOperator(joinOp)
                        .setOnPredicate(combinedPredicate)
                        .setOriginalOnPredicate(lastPredicate)
                        .build();
            }
            joinBranchList.add(branchJoin);
        }

        List<OptExpression> unionChildren = new ArrayList<>();
        List<ColumnRefOperator> outputColumns;
        if (joinOp.getProjection() != null) {
            outputColumns = joinOp.getProjection().getOutputColumns();
        } else {
            outputColumns = new ArrayList<>();
            for (OptExpression child : input.getInputs()) {
                ColumnRefSet childColumns = child.getOutputColumns();
                childColumns.getColumnRefOperators(context.getColumnRefFactory()).forEach(outputColumns::add);
            }
        }

        List<List<ColumnRefOperator>> childOutputColumns = new ArrayList<>();
        for (LogicalJoinOperator branchJoin : joinBranchList) {
            unionChildren.add(OptExpression.create(branchJoin, input.getInputs()));
            childOutputColumns.add(outputColumns);
        }

        LogicalUnionOperator unionOp =
                new LogicalUnionOperator.Builder().isUnionAll(true).setProjection(joinOp.getProjection())
                        .setChildOutputColumns(childOutputColumns).setOutputColumnRefOp(outputColumns).build();

        OptExpression result = OptExpression.create(unionOp, unionChildren);
        return Lists.newArrayList(result);
    }

    private boolean containsLimitOrNonDeterministicFunction(OptExpression expr) {
        if (expr.getOp().getOpType() == OperatorType.LOGICAL_LIMIT) {
            return true;
        }

        if (expr.getOp() instanceof LogicalOperator) {
            LogicalOperator logicalOp = (LogicalOperator) expr.getOp();

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
            if (containsLimitOrNonDeterministicFunction(child)) {
                return true;
            }
        }

        return false;
    }

    private boolean containsNonDeterministicFn(ScalarOperator expr) {
        if (expr instanceof CallOperator) {
            CallOperator callOp = (CallOperator) expr;
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

    private Map<Integer, List<BinaryPredicateOperator>> createCumulativePredicateMap(
            List<BinaryPredicateOperator> binaryPredicateList) {

        Map<Integer, List<BinaryPredicateOperator>> cumulativePredicateMap = new HashMap<>();

        for (int i = 0; i < binaryPredicateList.size(); i++) {
            List<BinaryPredicateOperator> currentBranchPredicates = new ArrayList<>();


            for (int j = 0; j <= i; j++) {
                BinaryPredicateOperator originalPredicate = binaryPredicateList.get(j);
                Map<ColumnRefOperator, ColumnRefOperator> columnMapping = new HashMap<>();
                collectColumnRefs(originalPredicate, columnMapping);

                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(columnMapping);
                ScalarOperator rewrittenPredicate = rewriter.rewrite(originalPredicate);
                currentBranchPredicates.add((BinaryPredicateOperator) rewrittenPredicate);

            }
            cumulativePredicateMap.put(i, currentBranchPredicates);
        }

        return cumulativePredicateMap;
    }

    private void collectColumnRefs(ScalarOperator expr,
                                   Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        if (expr instanceof ColumnRefOperator) {
            ColumnRefOperator columnRef = (ColumnRefOperator) expr;
            if (!columnMapping.containsKey(columnRef)) {
                columnMapping.put(columnRef, columnRef);
            }
        }
        for (ScalarOperator child : expr.getChildren()) {
            collectColumnRefs(child, columnMapping);
        }
    }

    private ScalarOperator buildAndPredicate(List<ScalarOperator> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return ConstantOperator.createBoolean(true);
        }
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        ScalarOperator result =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, predicates.get(0),
                        predicates.get(1));
        for (int i = 2; i < predicates.size(); i++) {
            result = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, result,
                    predicates.get(i));
        }
        return result;
    }
}
