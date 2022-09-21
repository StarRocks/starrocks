// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialize;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;

/*
 * SPJG materialized view rewrite rule, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 * The first step is to realize Project - Filter - Scan
 *
 */
public class RewriteUtils {
    public static boolean isLogicalSPJG(OptExpression root) {
        Operator operator = root.getOp();
        if (!(operator instanceof LogicalOperator)) {
            return false;
        }
        if (!(operator instanceof LogicalScanOperator)
                && !(operator instanceof LogicalProjectOperator)
                && !(operator instanceof LogicalFilterOperator)
                && !(operator instanceof LogicalJoinOperator)) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPJG(child)) {
                return false;
            }
        }
        return true;
    }

    // get all ref tables within and below root
    public static List<Table> getAllTables(OptExpression root) {
        List<Table> tables = Lists.newArrayList();
        getAllTables(root, tables);
        return tables;
    }

    private static void getAllTables(OptExpression root, List<Table> tables) {
        if (root.getOp() instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) root.getOp();
            tables.add(scanOperator.getTable());
        } else {
            for (OptExpression child : root.getInputs()) {
                getAllTables(child, tables);
            }
        }
    }

    // get all predicates within and below root
    public static List<ScalarOperator> getAllPredicates(OptExpression root) {
        List<ScalarOperator> predicates = Lists.newArrayList();
        getAllPredicates(root, predicates);
        return predicates;
    }

    private static void getAllPredicates(OptExpression root, List<ScalarOperator> predicates) {
        if (root.getOp().getPredicate() != null) {
            predicates.add(root.getOp().getPredicate());
        }
        for (OptExpression child : root.getInputs()) {
            getAllPredicates(child, predicates);
        }
    }

    // split predicate into two parts: equal columns predicates and residual predicates
    // the result pair's left is equal columns predicates, right is residual predicates
    public static Pair<ScalarOperator, ScalarOperator> splitPredicate(ScalarOperator predicate) {
        List<ScalarOperator> predicateConjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
        List<ScalarOperator> residualPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : predicateConjuncts) {
            if (scalarOperator instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) scalarOperator).getBinaryType() == BinaryType.EQ) {
                ScalarOperator leftChild = scalarOperator.getChild(0);
                ScalarOperator rightChild = scalarOperator.getChild(1);
                if (leftChild.isColumnRef() && rightChild.isColumnRef()) {
                    columnEqualityPredicates.add(scalarOperator);
                } else {
                    residualPredicates.add(scalarOperator);
                }
            } else {
                residualPredicates.add(scalarOperator);
            }
        }
        return Pair.create(
                Utils.compoundAnd(columnEqualityPredicates),
                Utils.compoundAnd(residualPredicates));
    }
}
