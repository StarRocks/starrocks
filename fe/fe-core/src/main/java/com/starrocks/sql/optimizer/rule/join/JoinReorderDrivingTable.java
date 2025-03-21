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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 基于driving table的join重排序算法
 * driving table是包含查询输出列的表，会被放在最左边
 * 其他表优先通过与driving table的join条件来join
 */
public class JoinReorderDrivingTable extends JoinOrder {
    private Optional<OptExpression> bestPlanRoot = Optional.empty();

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

        // 1. 找到driving table（包含输出列的表）
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
            // 如果没有找到driving table，返回空结果
            return;
        }

        // 2. 将driving table移到第一位
        atoms.remove(drivingTableIndex);
        atoms.add(0, drivingTable);

        // 3. 其他表按照代价排序
        atoms.subList(1, atoms.size()).sort((a, b) -> {
            double diff = b.bestExprInfo.cost - a.bestExprInfo.cost;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
        });

        // 4. 开始构建join树
        boolean[] used = new boolean[atomSize];
        GroupInfo leftGroup = atoms.get(0); // driving table
        used[0] = true;
        int next = 1;

        while (next < atomSize) {
            if (used[next]) {
                next++;
                continue;
            }

            // 优先选择可以直接与driving table join的表
            int bestIndex = -1;
            for (int i = next; i < atomSize; i++) {
                if (!used[i] && canJoinWithDrivingTable(leftGroup, atoms.get(i))) {
                    bestIndex = i;
                    break;
                }
            }

            // 如果没有找到可以直接join的表，就使用下一个可用的表
            if (bestIndex == -1) {
                bestIndex = next;
            }

            used[bestIndex] = true;
            GroupInfo rightGroup = atoms.get(bestIndex);

            // 避免同表join
            if (next == 1 && isSameTableJoin(leftGroup, rightGroup)) {
                continue;
            }

            // 构建join表达式
            Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
            if (!joinExpr.isPresent()) {
                return;
            }

            // 计算统计信息和代价
            joinExpr.get().expr.deriveLogicalPropertyItself();
            calculateStatistics(joinExpr.get().expr);
            computeCost(joinExpr.get());

            // 更新左子树为新的join结果
            BitSet joinBitSet = new BitSet();
            joinBitSet.or(leftGroup.atoms);
            joinBitSet.or(rightGroup.atoms);

            leftGroup = new GroupInfo(joinBitSet);
            leftGroup.bestExprInfo = joinExpr.get();
            leftGroup.lowestExprCost = joinExpr.get().cost;
        }

        bestPlanRoot = Optional.of(leftGroup.bestExprInfo.expr);
    }

    @Override
    public List<OptExpression> getResult() {
        return bestPlanRoot.map(Collections::singletonList).orElse(Collections.emptyList());
    }
} 