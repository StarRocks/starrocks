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
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private Optional<Pair<Node, Integer>> findDrivingTable(OptExpression parent, int childIdx, OptExpression root,
                                                           Long drivingTableId, Integer rootChildIdx, List<Node> projections) {
        Optional<Long> sourceTableId = getSourceTableId(parent, childIdx, root, projections);
        if (sourceTableId.isPresent() && sourceTableId.get().equals(drivingTableId)) {
            return Optional.of(Pair.create(new Node(parent, root, childIdx), rootChildIdx));
        }
        for (int i = 0; i < root.getInputs().size(); ++i) {
            OptExpression child = root.inputAt(i);
            if (childIdx == -1) {
                rootChildIdx = i;
            }
            Optional<Pair<Node, Integer>> newChild = findDrivingTable(root, i, child, drivingTableId, rootChildIdx, projections);
            if (newChild.isPresent()) {
                return newChild;
            }
        }
        return Optional.empty();
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
                for (int columnId : scalarOp.getUsedColumns().getColumnIds()) {
                    if (columnOp.getId() != columnId) {
                        columnMapping.put(columnOp.getId(), columnId);
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

        Map<Long, HashSet<Long>> tableRelations = new HashMap<>();
        if (rootJoinOp.getJoinType().isInnerJoin() && rootJoinOp.getOnPredicate() != null) {
            binaryRelation(rootJoinOp.getOnPredicate(), columnRefFactory, tableRelations, outputColumnMapping);
        }
        // all tables have a relationship with only the same table.
        // e.g. select * from t1, t2, t3 inner join t4 on t4.c1 = t1.c1 or t4.c1 = t2.c1 or t4.c1 = t3.c1
        Optional<Long> drivingTableId = Optional.empty();
        for (Map.Entry<Long, HashSet<Long>> entry : tableRelations.entrySet()) {
            if (entry.getValue().size() > 1) {
                if (drivingTableId.isPresent()) {
                    return Collections.emptyList();
                }
                drivingTableId = Optional.of(entry.getKey());
            }
        }
        if (drivingTableId.isPresent()) {
            List<Node> projections = new ArrayList<>();
            Optional<Pair<Node, Integer>> pair = findDrivingTable(null, -1, input, drivingTableId.get(), null, projections);
            if (pair.isEmpty()) {
                return Collections.emptyList();
            }
            Node drivingTableNode = pair.get().first;
            Integer rootChildIdx = pair.get().second;

            if (drivingTableNode.childIndex != -1 && drivingTableNode.parent != input) {
                OptExpression drivingTableRoot = drivingTableNode.child;
                int drivingTableChildIndex = drivingTableNode.childIndex;
                OptExpression drivingTableParent = drivingTableNode.parent;

                int replaceChildIdx = rootChildIdx == 0 ? 1 : 0;
                OptExpression replace = input.getInputs().get(replaceChildIdx);
                if (replace == drivingTableRoot) {
                    return Collections.emptyList();
                }
                drivingTableParent.setChild(drivingTableChildIndex, replace);
                ScalarOperator newOnPredicate =
                        innerOnPredicate.accept(new ColumnMappingRewriter(outputColumnMapping, columnRefFactory), null);
                OptExpression newRoot = OptExpression.create(LogicalJoinOperator.builder().withOperator(rootJoinOp)
                        .setOnPredicate(newOnPredicate).build(), input.getInputs());
                newRoot.setChild(replaceChildIdx, drivingTableRoot);

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
                            LogicalProjectOperator.builder().withOperator(projectionOp)
                                    .setColumnRefMap(newMap).build(), childInputs));
                }
                return List.of(newRoot);
            }
        }
        return Collections.emptyList();
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

    private void binaryRelation(ScalarOperator onPredicate, ColumnRefFactory columnRefFactory,
                                Map<Long, HashSet<Long>> tableRelations, Map<Integer, Integer> columnMapping) {
        if (onPredicate instanceof CompoundPredicateOperator) {
            for (ScalarOperator scalarOperator : ((CompoundPredicateOperator) onPredicate).normalizeChildren()) {
                binaryRelation(scalarOperator, columnRefFactory, tableRelations, columnMapping);
            }
        } else if (onPredicate instanceof BinaryPredicateOperator) {
            int[] columnIds = onPredicate.getUsedColumns().getColumnIds();
            if (columnIds.length == 2) {
                int leftIdx = columnIds[0];
                int rightIdx = columnIds[1];

                Long leftTableId = getTableIdByColumnId(leftIdx, columnRefFactory, columnMapping);
                Long rightTableId = getTableIdByColumnId(rightIdx, columnRefFactory, columnMapping);

                if (rightTableId != null && leftTableId != null) {
                    tableRelations.computeIfAbsent(leftTableId, k -> new HashSet<>()).add(rightTableId);
                    tableRelations.computeIfAbsent(rightTableId, k -> new HashSet<>()).add(leftTableId);
                }
            }
        }
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
