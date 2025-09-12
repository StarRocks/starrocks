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
package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * for distinct(t1.c1) from( t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1)
 * t1 is the driving table, t2 and t3 is used to filter t1
 * so we can rewrite it into t1 join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1
 */
public class JoinReorderDrivingTable extends JoinOrder {
    private Optional<OptExpression> bestPlanRoot = Optional.empty();
    // for t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1
    // if t1 is driving table, then expressionMap will be: {t2.c1: t1.c1} after handle t1 join t2
    // when t3 join (t1 join t2), we will rewrite t2.c1 = t3.c1 into t1.c1 = t3.c1 using expressionMap firstly
    // then insert {t3:c1: t1.c1} into expressionMap
    private Map<ColumnRefOperator, ScalarOperator> expressionMap = new HashMap<>();

    public JoinReorderDrivingTable(OptimizerContext context) {
        super(context);
    }


    private boolean containsOutputColumns(GroupInfo group) {
        ColumnRefSet outputColumns = group.bestExprInfo.expr.getOutputColumns();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();
        return outputColumns.containsAll(requiredOutputColumns);
    }

    @Override
    protected void enumerate() {
        List<GroupInfo> atoms = joinLevels.get(1).groups;
        if (atoms.size() > context.getSessionVariable().getJoinReorderDrivingTableMaxElement() || atoms.size() <= 2) {
            return;
        }

        // 1. find the driving table
        GroupInfo drivingTable = null;
        int drivingTableIndex = -1;
        for (int i = 0; i < atoms.size(); i++) {
            if (containsOutputColumns(atoms.get(i))) {
                drivingTable = atoms.get(i);
                drivingTableIndex = i;
                break;
            }
        }

        if (drivingTable == null) {
            return;
        }

        // 2.move driving table to the first place
        atoms.remove(drivingTableIndex);
        atoms.add(0, drivingTable);

        // 3. construct join tree
        boolean[] used = new boolean[atomSize];
        int usedNum = 1;
        GroupInfo leftGroup = atoms.get(0); // driving table
        used[0] = true;

        while (true) {
            int nextIndex = -1;
            // find the first atom which can be joined with leftGroup
            for (int i = 1; i < atomSize; i++) {
                if (!used[i] && canBuildInnerJoinPredicate(leftGroup, atoms.get(i))) {
                    nextIndex = i;
                    break;
                }
            }

            // if all used or no join predicate can be built, break
            if (nextIndex == -1) {
                break;
            }

            used[nextIndex] = true;
            GroupInfo rightGroup = atoms.get(nextIndex);

            // if right table output more then one column, like t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c2= t3.c2
            // right table is t2, it will output 2 columns, we can't do this optimization
            if (rightGroup.bestExprInfo.expr.getOutputColumns().size() != 1) {
                return;
            }

            // construct join expr
            Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
            if (!joinExpr.isPresent()) {
                return;
            }

            LogicalJoinOperator newJoinOperator = (LogicalJoinOperator) joinExpr.get().expr.getOp();
            if (newJoinOperator.getJoinType() == JoinOperator.CROSS_JOIN) {
                return;
            }

            List<BinaryPredicateOperator> onPredicate =
                    JoinHelper.getEqualsPredicate(leftGroup.bestExprInfo.expr.getOutputColumns(),
                            rightGroup.bestExprInfo.expr.getOutputColumns(),
                            Utils.extractConjuncts(newJoinOperator.getOnPredicate()));
            if (onPredicate.size() != 1) {
                return;
            }

            ColumnRefOperator left = null;
            ColumnRefOperator right = null;
            for (ScalarOperator child : onPredicate.get(0).getChildren()) {
                // just support col-op-col right now for simply
                if (!(child instanceof ColumnRefOperator colRef)) {
                    return;
                }
                // if col is replaced already, use replaced column
                // like case above, when dealing with left group join t3  on t2.c1 = t3.c1
                // t2.c1 should be replaced with t1.c1
                if (expressionMap.containsKey(colRef)) {
                    colRef = (ColumnRefOperator) expressionMap.get(colRef);
                }

                if (leftGroup.bestExprInfo.expr.getOutputColumns().contains(colRef)) {
                    left = colRef;
                } else {
                    right = colRef;
                }
            }

            // rewrite on-predicate
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionMap, false);
            ScalarOperator newOnPredicate = rewriter.rewrite(newJoinOperator.getOnPredicate());

            LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, newOnPredicate);
            OptExpression newJoinExpr =
                    OptExpression.create(joinOperator, leftGroup.bestExprInfo.expr, rightGroup.bestExprInfo.expr);
            joinExpr.get().expr = newJoinExpr;

            // replace right table's column with left table's column
            // every iteration, we use map first, after create new join op, then we insert new kv into map
            expressionMap.put(right, left);

            // calculate logical property, statistic, cost
            joinExpr.get().expr.deriveLogicalPropertyItself();
            calculateStatistics(joinExpr.get().expr);
            computeCost(joinExpr.get());

            // let new join as leftGroup
            BitSet joinBitSet = new BitSet();
            joinBitSet.or(leftGroup.atoms);
            joinBitSet.or(rightGroup.atoms);

            leftGroup = new GroupInfo(joinBitSet);
            leftGroup.bestExprInfo = joinExpr.get();
            leftGroup.lowestExprCost = joinExpr.get().cost;

            usedNum++;
        }

        if (usedNum == atoms.size()) {
            bestPlanRoot = Optional.of(leftGroup.bestExprInfo.expr);
        }
    }

    @Override
    public List<OptExpression> getResult() {
        return bestPlanRoot.map(Collections::singletonList).orElse(Collections.emptyList());
    }
} 