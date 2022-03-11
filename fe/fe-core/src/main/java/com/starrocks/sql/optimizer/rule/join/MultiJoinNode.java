// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * This class represents a set of inner joins that can be executed in any order.
 */
public class MultiJoinNode {
    // Atom: A child of the Multi join. This could be a table or some
    // other operator like a group by or a full outer join.
    private final LinkedHashSet<OptExpression> atoms;
    private final List<ScalarOperator> predicates;
    private Map<ColumnRefOperator, ScalarOperator> expressionMap;

    public MultiJoinNode(LinkedHashSet<OptExpression> atoms, List<ScalarOperator> predicates,
                         Map<ColumnRefOperator, ScalarOperator> expressionMap) {
        this.atoms = atoms;
        this.predicates = predicates;
        this.expressionMap = expressionMap;
    }

    public LinkedHashSet<OptExpression> getAtoms() {
        return atoms;
    }

    public List<ScalarOperator> getPredicates() {
        return predicates;
    }

    public Map<ColumnRefOperator, ScalarOperator> getExpressionMap() {
        return expressionMap;
    }

    public static MultiJoinNode toMultiJoinNode(OptExpression node) {
        LinkedHashSet<OptExpression> atoms = new LinkedHashSet<>();
        List<ScalarOperator> predicates = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> proMap = new HashMap<>();

        flattenJoinNode(node, atoms, predicates, proMap);

        return new MultiJoinNode(atoms, predicates, proMap);
    }

    private static void flattenJoinNode(OptExpression node, LinkedHashSet<OptExpression> atoms,
                                        List<ScalarOperator> predicates,
                                        Map<ColumnRefOperator, ScalarOperator> expressionMap) {
        Operator operator = node.getOp();
        if (!(operator instanceof LogicalJoinOperator)) {
            atoms.add(node);
            return;
        }

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
        if (!joinOperator.isInnerOrCrossJoin()) {
            atoms.add(node);
            return;
        }

        ScalarOperator joinPredicate = joinOperator.getPredicate();

        /*
         * If the predicate is equal binary predicate and wasn't push down to child, it's
         * means the predicate can't bind to any child, join reorder can't handle the predicate
         *
         * */
        if (JoinPredicateUtils.isEqualBinaryPredicate(joinPredicate)) {
            atoms.add(node);
            return;
        }

        if (joinOperator.getProjection() != null) {
            Projection projection = joinOperator.getProjection();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (!entry.getValue().isColumnRef()
                        && entry.getValue().getUsedColumns().isIntersect(node.inputAt(0).getOutputColumns())
                        && entry.getValue().getUsedColumns().isIntersect(node.inputAt(1).getOutputColumns())) {
                    atoms.add(node);
                    return;
                }

                if (!entry.getKey().equals(entry.getValue())) {
                    expressionMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        flattenJoinNode(node.inputAt(0), atoms, predicates, expressionMap);
        flattenJoinNode(node.inputAt(1), atoms, predicates, expressionMap);
        predicates.addAll(Utils.extractConjuncts(joinOperator.getOnPredicate()));
        Preconditions.checkState(!JoinPredicateUtils.isEqualBinaryPredicate(joinPredicate));
        predicates.addAll(Utils.extractConjuncts(joinPredicate));
    }
}
