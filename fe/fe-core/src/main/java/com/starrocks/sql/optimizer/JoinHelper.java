// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class JoinHelper {
    private final JoinOperator type;
    private final ColumnRefSet leftChildColumns;
    private final ColumnRefSet rightChildColumns;

    private final ScalarOperator onPredicate;
    private final String hint;

    private List<BinaryPredicateOperator> equalsPredicate;

    private List<Integer> leftOnColumns;
    private List<Integer> rightOnColumns;

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
        } else {
            type = null;
            onPredicate = null;
            hint = null;
            Preconditions.checkState(false, "Operator must be join operator");
        }
    }

    private void init() {
        equalsPredicate = getEqualsPredicate(leftChildColumns, rightChildColumns, Utils.extractConjuncts(onPredicate));
        leftOnColumns = Lists.newArrayList();
        rightOnColumns = Lists.newArrayList();

        for (BinaryPredicateOperator binaryPredicate : equalsPredicate) {
            ColumnRefSet leftUsedColumns = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rightUsedColumns = binaryPredicate.getChild(1).getUsedColumns();
            // Join on expression had pushed down to project node, so there must be one column
            if (leftUsedColumns.cardinality() > 1 || rightUsedColumns.cardinality() > 1) {
                throw new StarRocksPlannerException(
                        "we do not support equal on predicate have multi columns in left or right",
                        ErrorType.UNSUPPORTED);
            }

            if (leftChildColumns.containsAll(leftUsedColumns) && rightChildColumns.containsAll(rightUsedColumns)) {
                leftOnColumns.add(leftUsedColumns.getColumnIds()[0]);
                rightOnColumns.add(rightUsedColumns.getColumnIds()[0]);
            } else if (leftChildColumns.containsAll(rightUsedColumns) &&
                    rightChildColumns.containsAll(leftUsedColumns)) {
                leftOnColumns.add(rightUsedColumns.getColumnIds()[0]);
                rightOnColumns.add(leftUsedColumns.getColumnIds()[0]);
            } else {
                Preconditions.checkState(false, "shouldn't reach here");
            }
        }
    }

    public List<Integer> getLeftOnColumns() {
        return leftOnColumns;
    }

    public List<Integer> getRightOnColumns() {
        return rightOnColumns;
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
                JoinOperator.HINT_BUCKET.equals(hint);
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
<<<<<<< HEAD
=======
    }

    public static boolean validateJoinExpr(OptExpression joinExpr) {
        ColumnRefSet requiredCols = ((LogicalJoinOperator) joinExpr.getOp()).getRequiredChildInputColumns();
        joinExpr.inputAt(0).deriveLogicalPropertyItself();
        joinExpr.inputAt(1).deriveLogicalPropertyItself();

        ColumnRefSet left = joinExpr.inputAt(0).getOutputColumns();
        ColumnRefSet right = joinExpr.inputAt(1).getOutputColumns();
        requiredCols.except(left);
        requiredCols.except(right);
        return requiredCols.isEmpty();
>>>>>>> 2.5.18
    }
}
