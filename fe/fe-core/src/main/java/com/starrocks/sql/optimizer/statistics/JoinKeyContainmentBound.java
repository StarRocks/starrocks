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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashMap;
import java.util.Map;

/**
 * An upper bound on the number of distinct values of equi-join key columns, for keys whose statistics are unknown
 * on both sides of a join.
 *
 * <p>Columns equated anywhere in the query (join and filter equalities, plain column projections, CTE outputs) form
 * a class. Each scan in a class bounds the class by its estimated output rows: assuming, as the join estimate does
 * for a key known on one side, that the values of an equi-join key are contained in the other side's, no column of
 * the class has more distinct values than the smallest table it is equated with. TPC-DS Q72 joins inventory and
 * catalog_sales on item; with both tables' column statistics still unknown the join was estimated one-to-one
 * (max(L, R)) and planned first, while their item keys are bounded by the 300K rows of item.
 *
 * <p>The bound only ever replaces an unknown NDV, and the join estimate keeps the unknown-statistics row count as a
 * floor, so it can raise an estimate but never lower it.
 */
public final class JoinKeyContainmentBound {
    private JoinKeyContainmentBound() {
    }

    public static Map<Integer, Double> compute(OptExpression tree, OptimizerContext context) {
        Classes classes = new Classes();
        collect(tree, context, classes);
        Map<Integer, Double> result = new HashMap<>();
        for (Integer column : classes.parent.keySet()) {
            int root = classes.find(column);
            Double bound = classes.rows.get(root);
            if (bound != null && classes.size.getOrDefault(root, 1) > 1) {
                result.put(column, bound);
            }
        }
        return result;
    }

    private static void collect(OptExpression expr, OptimizerContext context, Classes classes) {
        Operator op = expr.getOp();
        if (op instanceof LogicalScanOperator) {
            // estimate on the side: the statistics must not stay on the tree that the memo is built from
            Statistics previous = expr.getStatistics();
            Utils.calculateStatistics(expr, context);
            Statistics statistics = expr.getStatistics();
            expr.setStatistics(previous);
            double rows = statistics == null ? -1 : statistics.getOutputRowCount();
            if (rows > 0) {
                for (ColumnRefOperator column : ((LogicalScanOperator) op).getColRefToColumnMetaMap().keySet()) {
                    classes.addRows(column.getId(), rows);
                }
            }
        }
        if (op instanceof LogicalJoinOperator) {
            addEqualities(((LogicalJoinOperator) op).getOnPredicate(), classes);
        }
        addEqualities(op.getPredicate(), classes);
        if (op instanceof LogicalProjectOperator) {
            addProjection(((LogicalProjectOperator) op).getColumnRefMap(), classes);
        }
        if (op.getProjection() != null) {
            addProjection(op.getProjection().getColumnRefMap(), classes);
        }
        if (op instanceof LogicalCTEConsumeOperator) {
            ((LogicalCTEConsumeOperator) op).getCteOutputColumnRefMap()
                    .forEach((consumer, producer) -> classes.union(consumer.getId(), producer.getId()));
        }
        for (OptExpression child : expr.getInputs()) {
            collect(child, context, classes);
        }
    }

    private static void addEqualities(ScalarOperator predicate, Classes classes) {
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (conjunct instanceof BinaryPredicateOperator &&
                    ((BinaryPredicateOperator) conjunct).getBinaryType() == BinaryType.EQ &&
                    conjunct.getChild(0).isColumnRef() && conjunct.getChild(1).isColumnRef()) {
                classes.union(((ColumnRefOperator) conjunct.getChild(0)).getId(),
                        ((ColumnRefOperator) conjunct.getChild(1)).getId());
            }
        }
    }

    private static void addProjection(Map<ColumnRefOperator, ScalarOperator> projection, Classes classes) {
        projection.forEach((output, value) -> {
            if (value.isColumnRef() && !output.equals(value)) {
                classes.union(output.getId(), ((ColumnRefOperator) value).getId());
            }
        });
    }

    // union-find over column ids; per root: the smallest scan rows and the number of members
    private static final class Classes {
        final Map<Integer, Integer> parent = new HashMap<>();
        final Map<Integer, Double> rows = new HashMap<>();
        final Map<Integer, Integer> size = new HashMap<>();

        int find(int column) {
            parent.putIfAbsent(column, column);
            int root = column;
            while (parent.get(root) != root) {
                root = parent.get(root);
            }
            int current = column;
            while (current != root) {
                int next = parent.get(current);
                parent.put(current, root);
                current = next;
            }
            return root;
        }

        void union(int a, int b) {
            int rootA = find(a);
            int rootB = find(b);
            if (rootA == rootB) {
                return;
            }
            parent.put(rootB, rootA);
            size.put(rootA, size.getOrDefault(rootA, 1) + size.getOrDefault(rootB, 1));
            Double rowsB = rows.remove(rootB);
            if (rowsB != null) {
                rows.merge(rootA, rowsB, Math::min);
            }
        }

        void addRows(int column, double value) {
            rows.merge(find(column), value, Math::min);
        }
    }
}
