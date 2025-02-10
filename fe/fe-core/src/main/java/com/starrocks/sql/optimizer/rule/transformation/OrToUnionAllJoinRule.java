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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
        if (!context.getSessionVariable().isEnabledOrToUnionAllJoin()) {
            return false;
        }

        if (!super.check(input, context)) {
            return false;
        }
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.getOp();
        ScalarOperator predicate = joinOp.getOriginalOnPredicate();
        if (!(predicate instanceof CompoundPredicateOperator)) {
            return false;
        }

        CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
        if (compoundPredicate.getCompoundType() != CompoundPredicateOperator.CompoundType.OR) {
            return false;
        }

        for (ScalarOperator child : compoundPredicate.getChildren()) {
            if (!(child instanceof BinaryPredicateOperator)) {
                return false;
            }
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) child;
            if (binaryPredicate.getBinaryType() != BinaryType.EQ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.getOp();
        CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) joinOp.getOriginalOnPredicate();

        List<BinaryPredicateOperator> binaryPredicateList = new ArrayList<>();
        for (ScalarOperator scalarOperator : compoundPredicate.getChildren()) {
            binaryPredicateList.add((BinaryPredicateOperator) scalarOperator);
        }

        Map<Integer, List<BinaryPredicateOperator>> cumulativePredicateMap = new HashMap<>();
        List<BinaryPredicateOperator> cumulativeList = new ArrayList<>();
        for (int i = 0; i < binaryPredicateList.size(); i++) {
            cumulativeList.add(binaryPredicateList.get(i));
            cumulativePredicateMap.put(i, new ArrayList<>(cumulativeList));
        }

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
                branchJoin = new LogicalJoinOperator.Builder().withOperator(joinOp).setOriginalOnPredicate(predicate0)
                        .setOnPredicate(predicate0).build();
            } else {
                List<ScalarOperator> scalarOps = new ArrayList<>(branchPredicates);
                CompoundPredicateOperator combinedPredicate =
                        new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                                scalarOps.toArray(new ScalarOperator[0]));
                ScalarOperator lastPredicate = branchPredicates.get(branchPredicates.size() - 1);
                branchJoin = new LogicalJoinOperator.Builder().withOperator(joinOp).setOnPredicate(combinedPredicate)
                        .setOriginalOnPredicate(lastPredicate).build();
            }
            joinBranchList.add(branchJoin);
        }

        List<OptExpression> unionChildren = new ArrayList<>();
        List<ColumnRefOperator> outputColumns = joinOp.getProjection().getOutputColumns();
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
}
