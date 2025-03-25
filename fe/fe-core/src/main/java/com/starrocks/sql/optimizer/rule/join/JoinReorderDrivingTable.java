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

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * for distinct(t1.c1) from( t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1)
 * t1 is the driving table, t2 and t3 is used to filter t1
 * so we can rewrite it into t1 join t3 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1
 */
public class JoinReorderDrivingTable extends JoinOrder {
    private Optional<OptExpression> bestPlanRoot = Optional.empty();
    // for t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1
    // if t1 is driving table, then expressionMap will be: {t2.c1: t1.c1} after handle t1 join t2
    // when t3 join (t1 join t2), we will rewrite t2.c1 = t3.c1 into t1.c1 = t3.c1 using expressionMap
    // then expressionMap will be: {t3:c1: t1.c1}, {t2.c1: t1:c1}
    private Map<ColumnRefOperator, ScalarOperator> expressionMap;

    public JoinReorderDrivingTable(OptimizerContext context) {
        super(context);
    }

    /**
     * 检查是否是同一个表的join
     */
    private boolean isSameTableJoin(GroupInfo left, GroupInfo right) {
        if (!(left.bestExprInfo.expr.getOp() instanceof LogicalScanOperator l)) {
            return false;
        }
        if (!(right.bestExprInfo.expr.getOp() instanceof LogicalScanOperator r)) {
            return false;
        }
        return l.getTable().getId() == r.getTable().getId();
    }

    /**
     * 检查表是否包含查询所需的输出列
     */
    private boolean containsOutputColumns(GroupInfo group) {
        ColumnRefSet outputColumns = group.bestExprInfo.expr.getOutputColumns();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();
        return outputColumns.containsAll(requiredOutputColumns);
    }

    /**
     * 检查是否可以与driving table进行join
     * 通过检查是否存在直接的join条件或可以通过等价关系推导出的join条件
     */
    private boolean canJoinWithDrivingTable(GroupInfo leftGroup, GroupInfo rightGroup) {
        BitSet leftBitSet = leftGroup.atoms;
        BitSet rightBitSet = new BitSet();
        rightBitSet.set(0); // driving table的位置是0

        List<ScalarOperator> joinPredicates = buildInnerJoinPredicate(leftBitSet, rightBitSet);
        return !joinPredicates.isEmpty();
    }

    @Override
    protected void enumerate() {
        List<GroupInfo> atoms = joinLevels.get(1).groups;

        // 1. find the driving table（包含输出列的表）
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

        // 3. order other atmos based on srot
        atoms.subList(1, atoms.size()).sort((a, b) -> {
            double diff = b.bestExprInfo.cost - a.bestExprInfo.cost;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
        });

        // 4. construct join tree
        boolean[] used = new boolean[atomSize];
        int usedNum = 1;
        GroupInfo leftGroup = atoms.get(0); // driving table
        used[0] = true;
        int next = 1;

        while (next < atomSize) {
            if (used[next]) {
                next++;
                continue;
            }

            int bestIndex = -1;
            for (int i = next; i < atomSize; i++) {
                if (!used[i] && canJoinWithDrivingTable(leftGroup, atoms.get(i))) {
                    bestIndex = i;
                    break;
                }
            }

            if (bestIndex == -1) {
                break;
            }

            used[bestIndex] = true;
            GroupInfo rightGroup = atoms.get(bestIndex);

            // avoid same table join
            if (isSameTableJoin(leftGroup, rightGroup)) {
                continue;
            }

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
                if (!(child instanceof ColumnRefOperator colRef)) {
                    return;
                }
                // if col is replaced already, use replaced column
                if (expressionMap.containsKey(colRef)) {
                    colRef = (ColumnRefOperator) expressionMap.get(colRef);
                }

                if (leftGroup.bestExprInfo.expr.getOutputColumns().contains(colRef)) {
                    left = colRef;
                } else {
                    right = colRef;
                }
            }
            // replace right table's column with left table's column
            expressionMap.put(right, left);

            // remove right table's output column from new join's output, because we don't need it anymore
            Projection projection = joinExpr.get().expr.getOp().getProjection();
            if (projection != null) {
                rightGroup.bestExprInfo.expr.getOutputColumns().getColumnRefOperators(context.getColumnRefFactory())
                        .forEach(col -> {
                            projection.getColumnRefMap().remove(col);

                        });
            }

            // add left table's on predicate column into new join's projection
            projection.getColumnRefMap().values().stream().forEach(col -> {
                projection.getColumnRefMap().putIfAbsent((ColumnRefOperator) col, col);
            });

            // rewrite on predicate
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(expressionMap, false);
            rewriter.rewrite(newJoinOperator.getOnPredicate());

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
            next = 1;
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