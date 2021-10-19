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

    public MultiJoinNode(LinkedHashSet<OptExpression> atoms, List<ScalarOperator> predicates) {
        this.atoms = atoms;
        this.predicates = predicates;
    }

    public LinkedHashSet<OptExpression> getAtoms() {
        return atoms;
    }

    public List<ScalarOperator> getPredicates() {
        return predicates;
    }

    public static MultiJoinNode toMultiJoinNode(OptExpression node) {
        LinkedHashSet<OptExpression> atoms = new LinkedHashSet<>();
        List<ScalarOperator> predicates = new ArrayList<>();

        flattenJoinNode(node, atoms, predicates);

        return new MultiJoinNode(atoms, predicates);
    }

    private static void flattenJoinNode(OptExpression node, LinkedHashSet<OptExpression> atoms,
                                        List<ScalarOperator> predicates) {
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

        if (joinOperator.getProjection() != null) {
            Projection projection = joinOperator.getProjection();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (!entry.getValue().isColumnRef()) {
                    atoms.add(node);
                    return;
                }

                if (!entry.getKey().equals(entry.getValue())) {
                    atoms.add(node);
                    return;
                }
            }
        }

        flattenJoinNode(node.inputAt(0), atoms, predicates);
        flattenJoinNode(node.inputAt(1), atoms, predicates);
        predicates.addAll(Utils.extractConjuncts(joinOperator.getOnPredicate()));
        ScalarOperator joinPredicate = joinOperator.getPredicate();
        Preconditions.checkState(!JoinPredicateUtils.isEqualBinaryPredicate(joinPredicate));
        predicates.addAll(Utils.extractConjuncts(joinPredicate));
    }
}
