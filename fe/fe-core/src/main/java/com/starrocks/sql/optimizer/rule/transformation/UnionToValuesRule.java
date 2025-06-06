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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// Transform all constant UNION ALLs into a single Values operator
//
// Pattern:
//
//      Union
//      /   \
//     X     Y
//
// Transform:
//
//           Union                              Union
//         /   |   \          ===>             /     \
//   Values1 Child2 Values3               Child2     Values(1, 3)
//
//
//            UnionAll
//         /     |     \          ===>      Values(1, 2, 3)
//   Values1  Value2   Values3
//
//
// Requirements:
// 1. The operators for x and y must be of the LOGICAL_VALUES type, and must not contain limit and Predicate

public class UnionToValuesRule extends TransformationRule {

    private UnionToValuesRule() {
        super(RuleType.TF_MERGE_CONSTANT_UNION, Pattern.create(OperatorType.LOGICAL_UNION)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF)));
    }

    public static UnionToValuesRule getInstance() {
        return INSTANCE;
    }

    private static final UnionToValuesRule INSTANCE = new UnionToValuesRule();

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return input.getInputs().stream()
                .filter(UnionToValuesRule::isMergeable)
                .skip(1)
                .findFirst()
                .isPresent();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator unionOp = (LogicalUnionOperator) input.getOp();

        List<List<ScalarOperator>> newRows = new ArrayList<>();
        List<OptExpression> otherChildren = new ArrayList<>();
        List<List<ColumnRefOperator>> newChildOutputs = Lists.newArrayList();

        final int numChildren = input.getInputs().size();
        for (int i = 0; i < numChildren; i++) {
            OptExpression child = input.getInputs().get(i);
            if (isMergeable(child)) {
                LogicalValuesOperator valuesOp = (LogicalValuesOperator) child.getOp();
                List<List<ScalarOperator>> rows = valuesOp.getRows();
                if (isConstantUnion(valuesOp)) {
                    List<ScalarOperator> scalarOperators = unionOp.getChildOutputColumns().get(i)
                            .stream()
                            .map(valuesOp.getProjection().getColumnRefMap()::get)
                            .collect(Collectors.toList());
                    newRows.add(scalarOperators);
                } else {
                    newRows.addAll(rows);
                }
            } else {
                newChildOutputs.add(unionOp.getChildOutputColumns().get(i));
                otherChildren.add(child);
            }
        }

        if (otherChildren.isEmpty()) {
            LogicalValuesOperator newValuesOperator = new LogicalValuesOperator.Builder()
                    .setColumnRefSet(unionOp.getOutputColumnRefOp())
                    .setRows(newRows)
                    .setLimit(unionOp.getLimit())
                    .setPredicate(unionOp.getPredicate())
                    .setProjection(unionOp.getProjection())
                    .build();
            return List.of(OptExpression.create(newValuesOperator));
        } else {
            List<OptExpression> inputs = new ArrayList<>(otherChildren);
            if (!newRows.isEmpty()) {
                // use new ColumnRefOperator for the new child output columns to avoid conflicts
                // eg:
                // SELECT 'test1' AS c1, 'test1' AS c2, 'test1' AS c3
                // UNION ALL
                // SELECT 'test1' AS c1, 'test2' AS c2, 'test3' AS c3
                // 1th child's original output only contain one element because of the same name 'test1',
                // use new ColumnRefOperator to avoid the conflict.
                final ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
                final List<ColumnRefOperator> newColRefs = unionOp.getChildOutputColumns().get(0)
                        .stream()
                        .map(c -> columnRefFactory.create(c, c.getType(), c.isNullable()))
                        .collect(Collectors.toUnmodifiableList());
                final LogicalValuesOperator newValuesOperator = new LogicalValuesOperator.Builder()
                        .setColumnRefSet(newColRefs)
                        .setRows(newRows)
                        .setPredicate(null)
                        .build();
                inputs.add(OptExpression.create(newValuesOperator));
                newChildOutputs.add(newValuesOperator.getColumnRefSet());
            }

            LogicalUnionOperator newUnionOp = new LogicalUnionOperator.Builder()
                    .withOperator(unionOp)
                    .setChildOutputColumns(newChildOutputs)
                    .build();
            OptExpression newUnionExpr = OptExpression.create(newUnionOp, inputs);

            return List.of(newUnionExpr);
        }
    }

    private static boolean isMergeable(OptExpression input) {
        if (input.getOp().getOpType() != OperatorType.LOGICAL_VALUES) {
            return false;
        }

        if (input.getOp().hasLimit() || input.getOp().getPredicate() != null) {
            return false;
        }

        LogicalValuesOperator values = (LogicalValuesOperator) input.getOp();
        return isConstantValues(values) || isConstantUnion(values);
    }

    private static boolean isConstantUnion(LogicalValuesOperator valuesOp) {
        if (valuesOp.getProjection() == null ||
                valuesOp.getProjection().getColumnRefMap().values().stream().anyMatch(expr -> !expr.isConstant())) {
            return false;
        }

        List<List<ScalarOperator>> rows = valuesOp.getRows();
        if (rows.size() != 1 || rows.get(0).size() != 1) {
            return false;
        }

        ScalarOperator value = rows.get(0).get(0);
        return value.equals(ConstantOperator.createNull(value.getType()));
    }

    private static boolean isConstantValues(LogicalValuesOperator valuesOp) {
        return valuesOp.getProjection() == null &&
                valuesOp.getRows().stream().flatMap(List::stream).allMatch(ScalarOperator::isConstant);
    }
}
