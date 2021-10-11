// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class SubqueryUtils {

    // ApplyNode doesn't need to check the number of subquery's return rows
    // when the correlation predicate meets these requirements:
    // 1. All predicate is Binary.EQ
    // 2. Only a child contains outer table's column
    public static boolean checkAllIsBinaryEQ(List<ScalarOperator> correlationPredicate,
                                             List<ColumnRefOperator> correlationColumnRefs) {
        for (ScalarOperator predicate : correlationPredicate) {
            if (!OperatorType.BINARY.equals(predicate.getOpType())) {
                return false;
            }

            BinaryPredicateOperator bpo = ((BinaryPredicateOperator) predicate);
            if (!BinaryPredicateOperator.BinaryType.EQ.equals(bpo.getBinaryType())) {
                return false;
            }

            ScalarOperator left = bpo.getChild(0);
            ScalarOperator right = bpo.getChild(1);

            boolean correlationLeft = Utils.containAnyColumnRefs(correlationColumnRefs, left);
            boolean correlationRight = Utils.containAnyColumnRefs(correlationColumnRefs, right);

            if (correlationLeft == correlationRight) {
                return false;
            }
        }

        return true;
    }

    //
    // extract expression as a columnRef when the parameter of correlation predicate is expression
    // e.g.
    // correlation predicate: a = abs(b)
    // return: <a = c, c: abs(b)>
    public static Pair<List<ScalarOperator>, Map<ColumnRefOperator, ScalarOperator>> rewritePredicateAndExtractColumnRefs(
            List<ScalarOperator> correlationPredicate, List<ColumnRefOperator> correlationColumnRefs,
            OptimizerContext context) {
        List<ScalarOperator> newPredicate = Lists.newArrayList();
        Map<ColumnRefOperator, ScalarOperator> groupRefs = Maps.newHashMap();

        for (ScalarOperator predicate : correlationPredicate) {
            BinaryPredicateOperator bpo = ((BinaryPredicateOperator) predicate);

            ScalarOperator left = bpo.getChild(0);
            ScalarOperator right = bpo.getChild(1);

            if (Utils.containAnyColumnRefs(correlationColumnRefs, left)) {
                if (right.isColumnRef()) {
                    newPredicate.add(bpo);
                    groupRefs.put((ColumnRefOperator) right, right);
                } else {
                    ColumnRefOperator ref =
                            context.getColumnRefFactory().create(right, right.getType(), right.isNullable());
                    newPredicate.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, left, ref));
                    groupRefs.put(ref, right);
                }
            } else {
                if (left.isColumnRef()) {
                    newPredicate.add(bpo);
                    groupRefs.put((ColumnRefOperator) left, left);
                } else {
                    ColumnRefOperator ref =
                            context.getColumnRefFactory().create(left, left.getType(), left.isNullable());
                    newPredicate.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, ref, right));
                    groupRefs.put(ref, left);
                }
            }
        }

        return new Pair<>(newPredicate, groupRefs);
    }

    public static CallOperator createCountRowsOperator() {
        Function count = Expr.getBuiltinFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT},
                Function.CompareMode.IS_IDENTICAL);
        return new CallOperator("count", Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(1)), count,
                false);
    }
}
