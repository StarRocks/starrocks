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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PruneSubfieldsForComplexType implements TreeRewriteRule {

    private static final MarkSubfieldsOptVisitor MARK_SUBFIELDS_OPT_VISITOR = new MarkSubfieldsOptVisitor();

    private static final PruneSubfieldsOptVisitor PRUNE_SUBFIELDS_OPT_VISITOR =
            new PruneSubfieldsOptVisitor();

    private static final Set<OperatorType> SUPPORTED_OPERATOR = new HashSet<>();

    public PruneSubfieldsForComplexType() {
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_OLAP_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_HIVE_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_FILE_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_ICEBERG_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_HUDI_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_DELTALAKE_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_SCHEMA_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_MYSQL_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_ES_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_JDBC_SCAN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_PROJECT);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_HASH_AGG);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_TOPN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_DISTRIBUTION);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_HASH_JOIN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_MERGE_JOIN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_NESTLOOP_JOIN);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_ASSERT_ONE_ROW);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_UNION);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_VALUES);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_FILTER);
        SUPPORTED_OPERATOR.add(OperatorType.PHYSICAL_LIMIT);
    }

    private static class Context {
        // The same ColumnRefOperator may have multiple access path for complex type
        private final Map<ColumnRefOperator, List<ImmutableList<Integer>>> accessPaths;

        private boolean enablePruneComplexTypes;

        public Context() {
            accessPaths = new HashMap<>();
            this.enablePruneComplexTypes = true;
        }

        public void setEnablePruneComplexTypes(boolean enablePruneComplexTypes) {
            this.enablePruneComplexTypes = enablePruneComplexTypes;
        }

        public boolean getEnablePruneComplexTypes() {
            return this.enablePruneComplexTypes;
        }

        public void addPath(ColumnRefOperator columnRefOperator, ImmutableList<Integer> path) {
            accessPaths.putIfAbsent(columnRefOperator, new ArrayList<>());
            accessPaths.get(columnRefOperator).add(path);
        }

        public List<ImmutableList<Integer>> getPath(ColumnRefOperator columnRefOperator) {
            return accessPaths.get(columnRefOperator);
        }
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        boolean canPrune = taskContext.getOptimizerContext().getSessionVariable().getEnablePruneComplexTypes();
        if (!canPrune) {
            return root;
        }
        Context context = new Context();
        root.getOp().accept(MARK_SUBFIELDS_OPT_VISITOR, root, context);
        if (context.getEnablePruneComplexTypes()) {
            // Still do prune
            root.getOp().accept(PRUNE_SUBFIELDS_OPT_VISITOR, root, context);
        }
        return root;
    }

    private static class MarkSubfieldsOptVisitor extends OptExpressionVisitor<Void, Context> {

        private static final MarkSubfieldsVisitor MARK_SUBFIELDS_VISITOR = new MarkSubfieldsVisitor();

        @Override
        public Void visit(OptExpression optExpression, Context context) {
            // If we face an unknown operator, just stop prune complex type
            if (!SUPPORTED_OPERATOR.contains(optExpression.getOp().getOpType())) {
                context.setEnablePruneComplexTypes(false);
                return null;
            }

            ScalarOperator predicate = optExpression.getOp().getPredicate();
            Projection projection = optExpression.getOp().getProjection();

            if (projection != null) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    entry.getValue().accept(MARK_SUBFIELDS_VISITOR, context);
                }

                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getCommonSubOperatorMap()
                        .entrySet()) {
                    entry.getValue().accept(MARK_SUBFIELDS_VISITOR, context);
                }
            }

            if (predicate != null) {
                predicate.accept(MARK_SUBFIELDS_VISITOR, context);
            }

            for (OptExpression opt : optExpression.getInputs()) {
                opt.getOp().accept(this, opt, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, Context context) {
            PhysicalHashAggregateOperator physicalHashAggregateOperator =
                    (PhysicalHashAggregateOperator) optExpression.getOp();
            if (physicalHashAggregateOperator.getAggregations() != null) {
                for (Map.Entry<ColumnRefOperator, CallOperator> entry : physicalHashAggregateOperator.getAggregations()
                        .entrySet()) {
                    entry.getValue().accept(MARK_SUBFIELDS_VISITOR, context);
                }
            }
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalJoin(OptExpression optExpression, Context context) {
            PhysicalJoinOperator physicalJoinOperator = (PhysicalJoinOperator) optExpression.getOp();
            ScalarOperator predicate = physicalJoinOperator.getOnPredicate();
            if (predicate != null) {
                predicate.accept(MARK_SUBFIELDS_VISITOR, context);
            }
            return visit(optExpression, context);
        }
    }

    private static class MarkSubfieldsVisitor extends ScalarOperatorVisitor<Void, Context> {

        private final Deque<Integer> usedSubFieldPos = new ArrayDeque<>();

        @Override
        public Void visit(ScalarOperator scalarOperator, Context context) {
            if (scalarOperator.getType().isMapType() || scalarOperator.getType().isStructType()) {
                // New expression maybe added, it's not be handled when go here, so we need disable prune subfields
                context.setEnablePruneComplexTypes(false);
            }
            for (ScalarOperator child : scalarOperator.getChildren()) {
                child.accept(this, context);
            }
            return null;
        }


        @Override
        public Void visitSubfield(SubfieldOperator subfieldOperator, Context context) {
            List<String> fieldNames = subfieldOperator.getFieldNames();

            Type tmpType = subfieldOperator.getChild(0).getType();
            List<Integer> tmpPos = new ArrayList<>(fieldNames.size());
            for (String field : fieldNames) {
                Preconditions.checkArgument(tmpType.isStructType());
                StructType structType = (StructType) tmpType;
                int pos = structType.getFieldPos(field);
                tmpPos.add(pos);
                tmpType = structType.getField(pos).getType();
            }
            // Reverse tmpPos
            for (int i = tmpPos.size() - 1; i >= 0; i--) {
                usedSubFieldPos.push(tmpPos.get(i));
            }

            subfieldOperator.getChild(0).accept(this, context);

            for (int i = 0; i < fieldNames.size(); i++) {
                usedSubFieldPos.pop();
            }
            return null;
        }

        @Override
        public Void visitVariableReference(ColumnRefOperator variable, Context context) {
            if (variable.getType().isComplexType()) {
                context.addPath(variable, ImmutableList.copyOf(usedSubFieldPos));
            }
            return null;
        }

        @Override
        public Void visitCollectionElement(CollectionElementOperator collectionElementOp, Context context) {
            if (collectionElementOp.getChild(0).getType().isMapType()) {
                usedSubFieldPos.push(-1);
            }
            collectionElementOp.getChild(0).accept(this, context);
            collectionElementOp.getChild(1).accept(this, context);

            if (collectionElementOp.getChild(0).getType().isMapType()) {
                usedSubFieldPos.pop();
            }
            return null;
        }

        @Override
        public Void visitCall(CallOperator call, Context context) {

            if (call.getFnName().equalsIgnoreCase("map_keys") || call.getFnName().equalsIgnoreCase("map_size")) {
                usedSubFieldPos.push(0);
            } else if (call.getFnName().equalsIgnoreCase("map_values")) {
                usedSubFieldPos.push(1);
            }

            for (ScalarOperator child : call.getChildren()) {
                child.accept(this, context);
            }

            if (call.getFnName().equalsIgnoreCase("map_keys") || call.getFnName().equalsIgnoreCase("map_size") ||
                    call.getFnName().equalsIgnoreCase("map_values")) {
                usedSubFieldPos.pop();
            }

            return null;
        }
    }

    private static class PruneSubfieldsOptVisitor extends OptExpressionVisitor<Void, Context> {

        @Override
        public Void visit(OptExpression optExpression, Context context) {
            Projection projection = optExpression.getOp().getProjection();

            if (projection != null) {
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                    if (entry.getKey().getType().isComplexType()) {
                        Preconditions.checkArgument(entry.getKey().getType().equals(entry.getValue().getType()),
                                "ColumnRefOperator and ScalarOperator should has the same type.");
                        pruneForComplexType(entry.getKey(), context);
                        entry.getValue().setType(entry.getKey().getType());
                    }
                }

                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getCommonSubOperatorMap()
                        .entrySet()) {
                    if (entry.getKey().getType().isComplexType()) {
                        Preconditions.checkArgument(entry.getKey().getType().equals(entry.getValue().getType()),
                                "ColumnRefOperator and ScalarOperator should has the same type.");
                        pruneForComplexType(entry.getKey(), context);
                        entry.getValue().setType(entry.getKey().getType());
                    }
                }
            }

            for (OptExpression opt : optExpression.getInputs()) {
                opt.getOp().accept(this, opt, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, Context context) {
            PhysicalScanOperator physicalScanOperator = (PhysicalScanOperator) optExpression.getOp();
            for (Map.Entry<ColumnRefOperator, Column> entry : physicalScanOperator.getColRefToColumnMetaMap()
                    .entrySet()) {
                if (entry.getKey().getType().isComplexType()) {
                    pruneForComplexType(entry.getKey(), context);
                }
            }
            return visit(optExpression, context);
        }

        private void pruneForComplexType(ColumnRefOperator columnRefOperator, Context context) {
            List<ImmutableList<Integer>> listGroup = context.getPath(columnRefOperator);
            if (listGroup == null) {
                return;
            }
            // Clone a new type for prune
            Type cloneType = columnRefOperator.getType().clone();
            Util.setUsedSubfieldPosGroup(cloneType, listGroup);
            cloneType.pruneUnusedSubfields();
            columnRefOperator.setType(cloneType);
        }
    }

    public static class Util {
        public static void setUsedSubfieldPosGroup(Type type, List<ImmutableList<Integer>> usedSubfieldPosGroup) {
            Preconditions.checkArgument(type.isComplexType());
            // type should be cloned from originType!
            if (usedSubfieldPosGroup.isEmpty()) {
                type.selectAllFields();
                return;
            }

            for (List<Integer> usedSubfieldPos : usedSubfieldPosGroup) {
                if (usedSubfieldPos.isEmpty()) {
                    type.selectAllFields();
                    return;
                }
                Type tmpType = type;
                for (int i = 0; i < usedSubfieldPos.size(); i++) {
                    // we will always select the ItemType of ArrayType, so we don't mark it and skip it.
                    while (tmpType.isArrayType()) {
                        tmpType = ((ArrayType) tmpType).getItemType();
                    }
                    int pos = usedSubfieldPos.get(i);
                    if (i == usedSubfieldPos.size() - 1) {
                        // last one, select children's all subfields
                        tmpType.setSelectedField(pos, true);
                    } else {
                        tmpType.setSelectedField(pos, false);
                        if (tmpType.isStructType()) {
                            tmpType = ((StructType) tmpType).getField(pos).getType();
                        } else if (tmpType.isMapType()) {
                            tmpType = pos == 0 ? ((MapType) tmpType).getKeyType() : ((MapType) tmpType).getValueType();
                        }
                    }
                }
            }
        }
    }
}
