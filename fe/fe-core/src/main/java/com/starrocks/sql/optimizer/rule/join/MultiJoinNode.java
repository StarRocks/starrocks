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

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

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
    private final Map<ColumnRefOperator, ScalarOperator> expressionMap;

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

    public boolean checkDependsPredicate() {
        // check join on-predicate depend on child join result. e.g.
        // select * from t2 join (select abs(t0.v1) x1 from t0 join t1 on t0.v1 = t1.v1) on abs(t2.v1) = x1
        ColumnRefSet checkColumns = new ColumnRefSet(this.expressionMap.keySet());

        for (ScalarOperator value : expressionMap.values()) {
            if (checkColumns.isIntersect(value.getUsedColumns())) {
                return false;
            }
        }
        return true;
    }

    public static MultiJoinNode toMultiJoinNode(OptExpression node) {
        LinkedHashSet<OptExpression> atoms = new LinkedHashSet<>();
        List<ScalarOperator> predicates = new ArrayList<>();
        Map<ColumnRefOperator, ScalarOperator> proMap = new HashMap<>();

        flattenJoinNode(node, atoms, predicates, proMap, true);

        return new MultiJoinNode(atoms, predicates, proMap);
    }

    private static void flattenJoinNode(OptExpression node, LinkedHashSet<OptExpression> atoms,
                                        List<ScalarOperator> predicates,
                                        Map<ColumnRefOperator, ScalarOperator> expressionMap,
                                        boolean isMultiJoinRoot) {
        Operator operator = node.getOp();
        if (!(operator instanceof LogicalJoinOperator)) {
            atoms.add(node);
            return;
        }

        LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
        if (!joinOperator.isInnerOrCrossJoin() || !joinOperator.getJoinHint().isEmpty()) {
            atoms.add(node);
            return;
        }

        ScalarOperator joinPredicate = joinOperator.getPredicate();

        /*
         * If the predicate is equal binary predicate and wasn't push down to child, it's
         * means the predicate can't bind to any child, join reorder can't handle the predicate
         *
         * */
        if (Utils.isEqualBinaryPredicate(joinPredicate)) {
            atoms.add(node);
            return;
        }

        if (joinOperator.getProjection() != null) {
            Projection projection = joinOperator.getProjection();
            if (hasUnsafeProjectionForJoinReorder(node, isMultiJoinRoot)) {
                atoms.add(node);
                return;
            }
            projection.getColumnRefMap().forEach((columnRef, expression) -> {
                if (!columnRef.equals(expression)) {
                    expressionMap.put(columnRef, expression);
                }
            });
        }

        flattenJoinNode(node.inputAt(0), atoms, predicates, expressionMap, false);
        flattenJoinNode(node.inputAt(1), atoms, predicates, expressionMap, false);
        predicates.addAll(Utils.extractConjuncts(joinOperator.getOnPredicate()));
        Preconditions.checkState(!Utils.isEqualBinaryPredicate(joinPredicate));
        predicates.addAll(Utils.extractConjuncts(joinPredicate));
    }

    static boolean hasProjectRelyOnTwoChildren(OptExpression node) {
        if (!(node.getOp() instanceof LogicalJoinOperator)) {
            return false;
        }
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) node.getOp();
        if (joinOperator.getProjection() == null) {
            return false;
        }
        ColumnRefSet leftChildColumns = node.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = node.inputAt(1).getOutputColumns();
        return joinOperator.getProjection().getColumnRefMap().values().stream().anyMatch(expression -> {
            ColumnRefSet usedColumns = expression.getUsedColumns();
            return !expression.isColumnRef()
                    && usedColumns.isIntersect(leftChildColumns)
                    && usedColumns.isIntersect(rightChildColumns);
        });
    }

    // multi join node tree's root can have projection that rely on two children
    // since projection's output won't be used by any atom of this join tree
    // so it's safe to reorder this join tree
    private static boolean hasUnsafeProjectionForJoinReorder(OptExpression node, boolean isMultiJoinRoot) {
        return !isMultiJoinRoot && hasProjectRelyOnTwoChildren(node);
    }
}
