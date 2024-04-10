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


package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.Pair;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.EQ_FOR_NULL;

public class JoinHelper {
    private final JoinOperator type;
    private final ColumnRefSet leftChildColumns;
    private final ColumnRefSet rightChildColumns;

    private final ScalarOperator onPredicate;
    private final String hint;

    private List<BinaryPredicateOperator> equalsPredicate;

    private List<DistributionCol> leftOnCols;

    private List<DistributionCol> rightOnCols;

    public static JoinHelper of(Operator join, ColumnRefSet leftInput, ColumnRefSet rightInput) {
        JoinHelper helper = new JoinHelper(join, leftInput, rightInput);
        helper.init();
        return helper;
    }

    private JoinHelper(Operator join, ColumnRefSet leftInput, ColumnRefSet rightInput) {
        this.leftChildColumns = leftInput;
        this.rightChildColumns = rightInput;

        if (join instanceof LogicalJoinOperator) {
            LogicalJoinOperator ljo = ((LogicalJoinOperator) join);
            type = ljo.getJoinType();
            onPredicate = ljo.getOnPredicate();
            hint = ljo.getJoinHint();
        } else if (join instanceof PhysicalJoinOperator) {
            PhysicalJoinOperator phjo = ((PhysicalJoinOperator) join);
            type = phjo.getJoinType();
            onPredicate = phjo.getOnPredicate();
            hint = phjo.getJoinHint();
        } else if (join instanceof PhysicalStreamJoinOperator) {
            PhysicalStreamJoinOperator operator = (PhysicalStreamJoinOperator) join;
            type = operator.getJoinType();
            onPredicate = operator.getOnPredicate();
            hint = operator.getJoinHint();
        } else {
            type = null;
            onPredicate = null;
            hint = null;
            Preconditions.checkState(false, "Operator must be join operator");
        }
    }

    private void init() {
        equalsPredicate = getEqualsPredicate(leftChildColumns, rightChildColumns, Utils.extractConjuncts(onPredicate));
        leftOnCols = Lists.newArrayList();
        rightOnCols = Lists.newArrayList();

        boolean leftTableAggStrict = type.isLeftOuterJoin() || type.isFullOuterJoin();
        boolean rightTableAggStrict = type.isRightOuterJoin() || type.isFullOuterJoin();

        for (BinaryPredicateOperator binaryPredicate : equalsPredicate) {
            boolean nullStrict = binaryPredicate.getBinaryType() == EQ_FOR_NULL;
            leftTableAggStrict = leftTableAggStrict || nullStrict;
            rightTableAggStrict = rightTableAggStrict || nullStrict;
            ColumnRefSet leftUsedColumns = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rightUsedColumns = binaryPredicate.getChild(1).getUsedColumns();
            // Join on expression had pushed down to project node, so there must be one column
            if (leftUsedColumns.cardinality() > 1 || rightUsedColumns.cardinality() > 1) {
                throw new StarRocksPlannerException(
                        "we do not support equal on predicate have multi columns in left or right",
                        ErrorType.UNSUPPORTED);
            }

            if (leftChildColumns.containsAll(leftUsedColumns) && rightChildColumns.containsAll(rightUsedColumns)) {
                leftOnCols.add(new DistributionCol(leftUsedColumns.getFirstId(), nullStrict, leftTableAggStrict));
                rightOnCols.add(new DistributionCol(rightUsedColumns.getFirstId(), nullStrict, rightTableAggStrict));
            } else if (leftChildColumns.containsAll(rightUsedColumns) && rightChildColumns.containsAll(leftUsedColumns)) {
                leftOnCols.add(new DistributionCol(rightUsedColumns.getFirstId(), nullStrict, leftTableAggStrict));
                rightOnCols.add(new DistributionCol(leftUsedColumns.getFirstId(), nullStrict, rightTableAggStrict));
            } else {
                Preconditions.checkState(false, "shouldn't reach here");
            }
        }
    }

    public List<Integer> getLeftOnColumnIds() {
        return leftOnCols.stream().map(DistributionCol::getColId).collect(Collectors.toList());
    }

    public List<Integer> getRightOnColumnIds() {
        return rightOnCols.stream().map(DistributionCol::getColId).collect(Collectors.toList());
    }

    public List<DistributionCol> getLeftCols() {
        return leftOnCols;
    }

    public List<DistributionCol> getRightCols() {
        return rightOnCols;
    }

    public boolean isCrossJoin() {
        return type.isCrossJoin() || equalsPredicate.isEmpty();
    }

    public boolean onlyBroadcast() {
        // Cross join only support broadcast join
        return onlyBroadcast(type, equalsPredicate, hint);
    }

    public boolean onlyShuffle() {
        return type.isRightJoin() || type.isFullOuterJoin() || JoinOperator.HINT_SHUFFLE.equals(hint) ||
                JoinOperator.HINT_BUCKET.equals(hint) || JoinOperator.HINT_SKEW.equals(hint);
    }

    public static List<BinaryPredicateOperator> getEqualsPredicate(ColumnRefSet leftColumns, ColumnRefSet rightColumns,
                                                                   List<ScalarOperator> conjunctList) {
        List<BinaryPredicateOperator> eqConjuncts = Lists.newArrayList();
        for (ScalarOperator predicate : conjunctList) {
            if (isEqualBinaryPredicate(leftColumns, rightColumns, predicate)) {
                eqConjuncts.add((BinaryPredicateOperator) predicate);
            }
        }
        return eqConjuncts;
    }

    public static Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> separateEqualPredicatesFromOthers(
            OptExpression optExpression) {
        Preconditions.checkArgument(optExpression.getOp() instanceof LogicalJoinOperator);
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<ScalarOperator> onPredicates = Utils.extractConjuncts(joinOp.getOnPredicate());

        ColumnRefSet leftChildColumns = optExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = optExpression.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                leftChildColumns, rightChildColumns, onPredicates);

        onPredicates.removeAll(eqOnPredicates);
        List<BinaryPredicateOperator> lhsEqRhsOnPredicates = Lists.newArrayList();
        for (BinaryPredicateOperator s : eqOnPredicates) {
            if (!leftChildColumns.containsAll(s.getChild(0).getUsedColumns())) {
                lhsEqRhsOnPredicates.add(new BinaryPredicateOperator(s.getBinaryType(), s.getChild(1), s.getChild(0)));
            } else {
                lhsEqRhsOnPredicates.add(s);
            }
        }
        return Pair.create(lhsEqRhsOnPredicates, onPredicates);
    }

    /**
     * Conditions should contain:
     * 1. binary predicate operator is EQ or EQ_FOR_NULL type
     * 2. operands in each side of operator should totally belong to each side of join's input
     *
     * @param leftColumns
     * @param rightColumns
     * @param predicate
     * @return
     */
    private static boolean isEqualBinaryPredicate(ColumnRefSet leftColumns, ColumnRefSet rightColumns,
                                                  ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            if (!binaryPredicate.getBinaryType().isEquivalence()) {
                return false;
            }

            ColumnRefSet leftUsedColumns = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rightUsedColumns = binaryPredicate.getChild(1).getUsedColumns();

            // Constant predicate
            if (leftUsedColumns.isEmpty() || rightUsedColumns.isEmpty()) {
                return false;
            }

            return leftColumns.containsAll(leftUsedColumns) && rightColumns.containsAll(rightUsedColumns) ||
                    leftColumns.containsAll(rightUsedColumns) && rightColumns.containsAll(leftUsedColumns);
        }
        return false;
    }

    public static boolean onlyBroadcast(JoinOperator type, List<BinaryPredicateOperator> equalOnPredicate,
                                        String hint) {
        // Cross join only support broadcast join
        return type.isCrossJoin() || JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN.equals(type) ||
                (type.isInnerJoin() && equalOnPredicate.isEmpty()) || JoinOperator.HINT_BROADCAST.equals(hint);
    }
}
