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

import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

public class DrivingTableSelection extends TransformationRule {

    public DrivingTableSelection() {
        super(RuleType.TF_DRIVING_TABLE_SELECTION, Pattern.create(OperatorType.LOGICAL_JOIN));
    }

    private class Node {
        OptExpression parent;
        OptExpression child;
        Integer childIndex;

        public Node(OptExpression parent, OptExpression child, Integer childIndex) {
            this.parent = parent;
            this.child = child;
            this.childIndex = childIndex;
        }
    }

    private Optional<Long> getSourceTableId(OptExpression parent, int childIdx, OptExpression node, List<Node> projections) {
        Operator operator = node.getOp();
        if (operator instanceof LogicalScanOperator) {
            return Optional.of(((LogicalScanOperator) operator).getTable().getId());
        }
        if (operator instanceof LogicalJoinOperator) {
            return Optional.empty();
        }
        for (int i = 0; i < node.getInputs().size(); ++i) {
            OptExpression child = node.inputAt(i);
            Optional<Long> tableId = getSourceTableId(node, i, child, projections);
            if (tableId.isPresent()) {
                return tableId;
            }
        }
        if (operator instanceof LogicalProjectOperator) {
            projections.add(new Node(parent, node, childIdx));
        }
        return Optional.empty();
    }

    private void extractJoins(OptExpression parent, int childIdx, OptExpression root, List<Pair<Long, OptExpression>> joinTables,
                              List<Pair<Node, Integer>> joinWithTableIdx, List<Node> projections) {
        if (root.getOp() instanceof LogicalJoinOperator joinOperator &&
                (joinOperator.getJoinType().isCrossJoin() || joinOperator.getJoinType().isInnerJoin() && childIdx == -1)) {
            Optional<Integer> tableIdx = Optional.empty();
            for (int i = 0; i < root.getInputs().size(); i++) {
                OptExpression child = root.inputAt(i);
                Optional<Long> sourceTableId = getSourceTableId(root, i, child, projections);
                if (sourceTableId.isPresent()) {
                    OptExpression tableChild = root.inputAt(i);
                    joinTables.add(new Pair<>(sourceTableId.get(), tableChild));
                    joinWithTableIdx.add(new Pair<>(new Node(parent, root, childIdx), i));

                    if (tableIdx.isPresent()) {
                        tableIdx = Optional.empty();
                    } else {
                        tableIdx = Optional.of(i);
                    }
                }
            }
            if (tableIdx.isPresent()) {
                int joinIdx = tableIdx.get() == 1 ? 0 : 1;
                extractJoins(root, joinIdx, root.inputAt(joinIdx), joinTables, joinWithTableIdx, projections);
            }
        } else if (root.getOp() instanceof LogicalProjectOperator) {
            extractJoins(root, 0, root.inputAt(0), joinTables, joinWithTableIdx, projections);
        }
    }

    boolean isCrossJoin(OptExpression root) {
        if (root.getOp() instanceof LogicalJoinOperator joinOp && joinOp.getJoinType().isCrossJoin()) {
            return true;
        } else if (root.getOp() instanceof LogicalProjectOperator) {
            return isCrossJoin(root.inputAt(0));
        } else {
            return false;
        }
    }

    private void extractJoinOutputColumnMapping(OptExpression root, Map<Integer, Integer> columnMapping, boolean isRoot) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalJoinOperator joinOperator) {
            JoinOperator joinType = joinOperator.getJoinType();

            if (!joinType.isCrossJoin() && !isRoot) {
                return;
            }

        } else if (operator instanceof LogicalProjectOperator projectOperator) {
            projectOperator.getRowOutputInfo(root.getInputs()).getColumnRefMap().forEach((columnOp, scalarOp) -> {
                if (scalarOp instanceof ColumnRefOperator columnRefOperator) {
                    if (columnOp.getId() != columnRefOperator.getId()) {
                        columnMapping.put(columnOp.getId(), columnRefOperator.getId());
                    }
                }
            });
        } else if (!(operator instanceof LogicalAssertOneRowOperator)) {
            return;
        }

        for (int i = 0; i < root.getInputs().size(); ++i) {
            extractJoinOutputColumnMapping(root.inputAt(i), columnMapping, false);
        }
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator inputOp = input.getOp();

        boolean isInnerJoin =
                inputOp instanceof LogicalJoinOperator && ((LogicalJoinOperator) inputOp).getJoinType().isInnerJoin();
        if (!isInnerJoin) {
            return false;
        }
        if (input.getInputs().stream().noneMatch(this::isCrossJoin)) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        LogicalJoinOperator rootJoinOp = (LogicalJoinOperator) input.getOp();
        ScalarOperator innerOnPredicate = rootJoinOp.getOnPredicate();

        Map<Integer, Integer> outputColumnMapping = new HashMap<>();
        extractJoinOutputColumnMapping(input, outputColumnMapping, true);

        Map<Long, HashMap<Long, Integer>> tableRelations = new HashMap<>();
        List<Pair<Integer, CompoundPredicateOperator.CompoundType>> compoundTypes = new ArrayList<>();
        if (rootJoinOp.getJoinType().isInnerJoin() && rootJoinOp.getOnPredicate() != null) {
            if (binaryRelation(rootJoinOp.getOnPredicate(), columnRefFactory, tableRelations, outputColumnMapping, compoundTypes,
                    0, null)) {
                return Collections.emptyList();
            }
        }
        if (tableRelations.size() <= 1) {
            return Collections.emptyList();
        }
        // all tables have a relationship with only the same table.
        // e.g. select * from t1, t2, t3 inner join t4 on t4.c1 = t1.c1 or t4.c1 = t2.c1 or t4.c1 = t3.c1
        Optional<Map<Long, Integer>> tableDepthMap = Optional.empty();
        for (Map.Entry<Long, HashMap<Long, Integer>> entry : tableRelations.entrySet()) {
            HashMap<Long, Integer> map = entry.getValue();
            if (map.size() > 1) {
                if (tableDepthMap.isPresent()) {
                    return Collections.emptyList();
                }
                // driving table first
                map.put(entry.getKey(), 0);
                tableDepthMap = Optional.of(map);
            }
        }
        if (tableDepthMap.isPresent()) {
            List<Pair<Long, OptExpression>> joinTables = new ArrayList<>();
            List<Pair<Node, Integer>> joinWithTableIdx = new ArrayList<>();
            List<Node> projections = new ArrayList<>();
            extractJoins(null, -1, input, joinTables, joinWithTableIdx, projections);
            if (joinWithTableIdx.isEmpty()) {
                return Collections.emptyList();
            }
            int joinsHash = joinTables.hashCode();
            // JoinReorder
            Map<Long, Integer> finalTableDepthMap = tableDepthMap.get();
            joinTables.sort(Comparator.comparingInt((Pair<Long, OptExpression> pair) -> finalTableDepthMap.get(pair.first)));
            // If the join order does not change, then skip
            if (joinTables.hashCode() == joinsHash) {
                return Collections.emptyList();
            }
            for (int i = 0; i < joinTables.size(); i++) {
                Pair<Node, Integer> joinPair = joinWithTableIdx.get(i);
                Node join = joinPair.first;
                OptExpression joinRoot = join.child;
                int tableChildId = joinPair.second;
                Pair<Long, OptExpression> tablePair = joinTables.get(i);
                OptExpression tableChild = tablePair.second;

                joinRoot.setChild(tableChildId, tableChild);
            }

            // Projection
            Collections.reverse(projections);
            for (Node node : projections) {
                OptExpression child = node.child;
                List<OptExpression> childInputs = node.parent.inputAt(node.childIndex).getInputs();

                Map<ColumnRefOperator, ScalarOperator> newMap = new HashMap<>();
                for (OptExpression projectionChild : childInputs) {
                    newMap.putAll(projectionChild.getRowOutputInfo().getColumnRefMap());
                }

                LogicalProjectOperator projectionOp = (LogicalProjectOperator) child.getOp();
                node.parent.setChild(node.childIndex, OptExpression.create(
                        LogicalProjectOperator.builder().withOperator(projectionOp).setColumnRefMap(newMap).build(),
                        childInputs));
            }
            ScalarOperator newOnPredicate =
                    innerOnPredicate.accept(new ColumnMappingRewriter(outputColumnMapping, columnRefFactory), null);

            compoundTypes.sort(Comparator.comparingInt((pair -> pair.first)));
            for (int i = 0; i < joinWithTableIdx.size(); i++) {
                Pair<Node, Integer> joinPair = joinWithTableIdx.get(i);
                Node join = joinPair.first;
                OptExpression joinRoot = join.child;

                if (join.parent != null) {
                    Pair<Integer, CompoundPredicateOperator.CompoundType> compoundTypePair = compoundTypes.get(i - 1);
                    if (!compoundTypePair.second.equals(CompoundPredicateOperator.CompoundType.OR)) {
                        continue;
                    }
                    // rewrite to union all
                    List<ColumnRefOperator> result = new ArrayList<>();
                    List<List<ColumnRefOperator>> childOutputColumns = List.of(new ArrayList<>(), new ArrayList<>());

                    Map<ColumnRefOperator, ScalarOperator> leftMap = new HashMap<>();
                    Map<ColumnRefOperator, ScalarOperator> rightMap = new HashMap<>();
                    OptExpression leftInput = joinRoot.inputAt(0);
                    OptExpression rightInput = joinRoot.inputAt(1);

                    extractUnionInput(result, childOutputColumns, leftMap, rightMap, leftInput);
                    extractUnionInput(result, childOutputColumns, rightMap, leftMap, rightInput);
                    List<OptExpression> newInputs = new ArrayList<>();
                    newInputs.add(
                            OptExpression.create(LogicalProjectOperator.builder().setColumnRefMap(leftMap).build(), leftInput));
                    newInputs.add(
                            OptExpression.create(LogicalProjectOperator.builder().setColumnRefMap(rightMap).build(), rightInput));

                    join.parent.setChild(join.childIndex,
                            OptExpression.create(new LogicalUnionOperator(result, childOutputColumns, true), newInputs));
                }
            }

            return List.of(OptExpression.create(
                    LogicalJoinOperator.builder().withOperator(rootJoinOp).setOnPredicate(newOnPredicate).build(),
                    input.getInputs()));
        }
        return Collections.emptyList();
    }

    private void extractUnionInput(List<ColumnRefOperator> result, List<List<ColumnRefOperator>> childOutputColumns,
                                   Map<ColumnRefOperator, ScalarOperator> leftMap,
                                   Map<ColumnRefOperator, ScalarOperator> rightMap, OptExpression input) {
        input.getRowOutputInfo().getColumnRefMap().forEach((columnOp, scalarOp) -> {
            ColumnRefOperator nullableColumnOp =
                    new ColumnRefOperator(columnOp.getId(), columnOp.getType(), columnOp.getName(), true);
            result.add(nullableColumnOp);
            childOutputColumns.get(0).add(nullableColumnOp);
            childOutputColumns.get(1).add(nullableColumnOp);
            leftMap.put(nullableColumnOp, scalarOp);
            rightMap.put(nullableColumnOp, ConstantOperator.createNull(scalarOp.getType()));
        });
    }

    private Long getTableIdByColumnId(int columnId, ColumnRefFactory columnRefFactory, Map<Integer, Integer> columnMapping) {
        Table table = columnRefFactory.getTableForColumn(columnId);

        if (null == table) {
            if (columnMapping.containsKey(columnId)) {
                return getTableIdByColumnId(columnMapping.get(columnId), columnRefFactory, columnMapping);
            } else {
                return null;
            }
        } else {
            return table.getId();
        }
    }

    private boolean binaryRelation(ScalarOperator onPredicate, ColumnRefFactory columnRefFactory,
                                   Map<Long, HashMap<Long, Integer>> tableRelations, Map<Integer, Integer> columnMapping,
                                   List<Pair<Integer, CompoundPredicateOperator.CompoundType>> compoundTypes,
                                   int depth, CompoundPredicateOperator.CompoundType compoundType) {
        if (onPredicate instanceof CompoundPredicateOperator compoundPredicate) {
            for (ScalarOperator scalarOperator : compoundPredicate.normalizeChildren()) {
                if (binaryRelation(scalarOperator, columnRefFactory, tableRelations, columnMapping, compoundTypes, depth + 1,
                        compoundPredicate.getCompoundType())) {
                    return true;
                }
            }
        } else if (onPredicate instanceof BinaryPredicateOperator && depth != 0) {
            int[] columnIds = onPredicate.getUsedColumns().getColumnIds();
            if (columnIds.length == 2) {
                int leftIdx = columnIds[0];
                int rightIdx = columnIds[1];

                Long leftTableId = getTableIdByColumnId(leftIdx, columnRefFactory, columnMapping);
                Long rightTableId = getTableIdByColumnId(rightIdx, columnRefFactory, columnMapping);

                if (leftTableId != null && rightTableId != null) {
                    BiFunction<Long, Integer, Integer> function =
                            (tableId, tableDepth) -> tableDepth == null ? depth : Math.min(depth, tableDepth);
                    tableRelations.computeIfAbsent(leftTableId, k -> new HashMap<>()).compute(rightTableId, function);
                    tableRelations.computeIfAbsent(rightTableId, k -> new HashMap<>()).compute(leftTableId, function);
                    compoundTypes.add(new Pair<>(depth, compoundType));
                }
            } else {
                // e,g, `t1.c1 = t2.c1 + t3.c1`
                return columnIds.length > 2;
            }
        }
        return false;
    }

    private class ColumnMappingRewriter extends BaseScalarOperatorShuttle {
        Map<Integer, Integer> columnMapping;
        ColumnRefFactory columnRefFactory;

        public ColumnMappingRewriter(Map<Integer, Integer> columnMapping, ColumnRefFactory columnRefFactory) {
            this.columnMapping = columnMapping;
            this.columnRefFactory = columnRefFactory;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            if (columnMapping.containsKey(variable.getId())) {
                Integer mappingColumnId = columnMapping.get(variable.getId());
                return columnRefFactory.getColumnRef(mappingColumnId);
            } else {
                return super.visitVariableReference(variable, context);
            }
        }
    }
}
