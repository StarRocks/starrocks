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

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class JoinReorderHelper {

    private JoinReorderHelper() {
        //not called
    }

    /*
     * split join output cols into three parts:
     * 1. cols only ref left child cols
     * 2. cols only ref right child cols
     * 3. cols ref both left and right children cols
     * Actually there may be some constants cols, we don't need to consider them here.
     */
    public static List<ColumnRefSet> splitJoinOutputCols(OptExpression joinExpr) {
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) joinExpr.getOp();
        OptExpression left = joinExpr.inputAt(0);
        OptExpression right = joinExpr.inputAt(1);
        ColumnRefSet leftCols = new ColumnRefSet();
        ColumnRefSet rightCols = new ColumnRefSet();
        ColumnRefSet refBothChildCols = new ColumnRefSet();

        Projection projection = joinOperator.getProjection();
        if (projection == null) {
            leftCols.union(left.getOutputColumns());
            if (!joinOperator.getJoinType().isLeftSemiAntiJoin()) {
                rightCols.union(right.getOutputColumns());
            }
        } else {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                ColumnRefOperator columnRefOperator = entry.getKey();
                ScalarOperator scalarOperator = entry.getValue();
                if (scalarOperator.getUsedColumns().isEmpty()) {
                    // do nothing
                } else if (left.getOutputColumns().containsAll(scalarOperator.getUsedColumns())) {
                    leftCols.union(columnRefOperator.getId());
                } else if (right.getOutputColumns().containsAll(scalarOperator.getUsedColumns())) {
                    rightCols.union(columnRefOperator.getId());
                } else {
                    refBothChildCols.union(columnRefOperator.getId());
                }
            }
        }
        return Lists.newArrayList(leftCols, rightCols, refBothChildCols);
    }


    public static boolean isAssoc(OptExpression bottomJoinExpr, OptExpression topJoinExpr) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) topJoinExpr.getOp();
        ScalarOperator topJoinOnCondition = topJoin.getOnPredicate();

        List<ColumnRefSet> splitCols = splitJoinOutputCols(bottomJoinExpr);

        if (!splitCols.get(2).isEmpty()) {
            return false;
        }
        if (topJoinOnCondition == null) {
            return false;
        }

        ColumnRefSet topJoinOnConditionCols = topJoinOnCondition.getUsedColumns();
        if (topJoin.getJoinType() == JoinOperator.INNER_JOIN
                && topJoinOnConditionCols.isIntersect(splitCols.get(1))) {
            // when topJoin is inner join, it's on condition must ref cols from right child of bottom join
            // to avoid cross join transformation.
            return true;
        } else {
            // when topJoin is other type join, it's on condition must only ref cols from right child of bottom join
            // to avoid cross join transformation and ref null generating cols
            return topJoinOnConditionCols.isIntersect(splitCols.get(1))
                    && !topJoinOnConditionCols.isIntersect(splitCols.get(0));
        }
    }


    public static boolean isLeftAsscom(OptExpression bottomJoinExpr, OptExpression topJoinExpr, boolean isInnerMode) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) topJoinExpr.getOp();
        ScalarOperator topJoinOnCondition = topJoin.getOnPredicate();

        List<ColumnRefSet> splitCols = splitJoinOutputCols(bottomJoinExpr);
        if (!splitCols.get(2).isEmpty()) {
            return false;
        }

        Projection botJoinProjection = bottomJoinExpr.getOp().getProjection();
        if (!isInnerMode && botJoinProjection != null && !splitCols.get(1).isEmpty()) {
            ColumnRefSet rightCols = splitCols.get(1);
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : botJoinProjection.getColumnRefMap().entrySet()) {
                if (rightCols.contains(entry.getKey())) {
                    // don't reorder join like select ifnull(t1.v1,0) from t0 left join t1 on t0.v2 = t1.v2
                    // left join t3 on t0.v3 = t3.v3
                    if (Utils.isNotAlwaysNullResultWithNullScalarOperator(entry.getValue())) {
                        return false;
                    }
                }
            }
        }

        if (topJoinOnCondition == null) {
            return false;
        }

        ColumnRefSet topJoinOnConditionCols = topJoinOnCondition.getUsedColumns();

        // top join on condition must only ref cols from left child of bottom join
        // because of the null generating behavior when bottom is left outer join
        return !topJoinOnConditionCols.isIntersect(splitCols.get(1))
                && topJoinOnConditionCols.isIntersect(splitCols.get(0));
    }
}
