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
import com.starrocks.catalog.ComplexTypeAccessGroup;
import com.starrocks.catalog.ComplexTypeAccessPath;
import com.starrocks.catalog.ComplexTypeAccessPathType;
import com.starrocks.catalog.ComplexTypeAccessPaths;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PruneComplexTypeUtil {
    private static final Logger LOG = LogManager.getLogger(PruneComplexTypeUtil.class);

    // For example, we have a column col: MAP<INT, STRUCT<a INT, b STRUCT<c INT, d INT>>>
    // And we have a sql: SELECT map_values(col).b.d, map_keys(col) from TABLE;
    // Column col has a specific ColumnRefOperator
    // map_values(col).b.d has three access paths:
    // --- Access path 1: ComplexTypeAccessPath(MAP_VALUES)
    // --- Access path 2: ComplexTypeAccessPath(STRUCT_SUBFIELD, "b")
    // --- Access path 3: ComplexTypeAccessPath(STRUCT_SUBFIELD, "d")
    // So it's access paths is: [Access path 1, Access path 2, Access path 3]
    // map_keys(col) has one access paths:
    // --- Access path 4: ComplexTypeAccessPath(MAP_KEYS)
    // So it's access paths is: [Access path 4]
    // Summary: ColumnRefOperator(col) has two access paths, we use access group to represent it.
    // ColumnRefOperator(col):
    // --- Access group: [[Access path 1, Access path 2, Access path 3], [Access path 4]]
    protected static class Context {
        // The same ColumnRefOperator may have multiple access paths for complex type
        private final Map<ColumnRefOperator, ComplexTypeAccessGroup> accessGroups;

        private boolean enablePruneComplexTypes;

        public Context() {
            this.accessGroups = new HashMap<>();
            this.enablePruneComplexTypes = true;
        }

        public void setEnablePruneComplexTypes(boolean enablePruneComplexTypes) {
            this.enablePruneComplexTypes = enablePruneComplexTypes;
        }

        public boolean getEnablePruneComplexTypes() {
            return this.enablePruneComplexTypes;
        }

        public void addAccessPaths(ColumnRefOperator columnRefOperator, ComplexTypeAccessPaths accessPaths) {
            accessGroups.putIfAbsent(columnRefOperator, new ComplexTypeAccessGroup());
            accessGroups.get(columnRefOperator).addAccessPaths(accessPaths);
        }

        public void addAccessPaths(ColumnRefOperator columnRefOperator,
                                   ComplexTypeAccessPaths curAccessPaths,
                                   ComplexTypeAccessGroup visitedAccessGroup) {
            int size = visitedAccessGroup.size();
            // We should we below loop to avoid ConcurrentModificationException
            for (int i = 0; i < size; i++) {
                addAccessPaths(columnRefOperator, concatAccessPaths(curAccessPaths, visitedAccessGroup.get(i)));
            }
        }

        public void add(ColumnRefOperator outputColumnRefOperator, ScalarOperator scalarOperator) {
            // If outputColumnRefOperator's type is not equal to scalarOperator's type,
            // we just stop pruning in case.
            if (!checkCanPrune(outputColumnRefOperator, scalarOperator)) {
                setEnablePruneComplexTypes(false);
                return;
            }

            ComplexTypeAccessGroup visitedAccessGroup = null;
            if (outputColumnRefOperator != null) {
                // If outputColumnRefOperator is not null, it means it may have visited access group,
                // we get it for later merge access path.
                visitedAccessGroup = getVisitedAccessGroup(outputColumnRefOperator);
            }

            MarkSubfieldsVisitor markSubfieldsVisitor = new MarkSubfieldsVisitor(visitedAccessGroup);
            scalarOperator.accept(markSubfieldsVisitor, this);
        }

        public ComplexTypeAccessGroup getVisitedAccessGroup(ColumnRefOperator columnRefOperator) {
            return accessGroups.get(columnRefOperator);
        }

        private boolean checkCanPrune(ColumnRefOperator columnRefOperator, ScalarOperator scalarOperator) {
            if (columnRefOperator == null) {
                return true;
            }

            if (!columnRefOperator.getType().isComplexType() && !scalarOperator.getType().isComplexType()) {
                // If columnRefOperator and scalarOperator both are not complex type, don't need to check
                return true;
            }

            if (!columnRefOperator.getType().equals(scalarOperator.getType())) {
                LOG.warn("Complex type columnRefOperator and scalarOperator should has the same type.");
                return false;
            }
            return true;
        }
    }

    private static ComplexTypeAccessPaths concatAccessPaths(
            ComplexTypeAccessPaths curAccessPaths,
            ComplexTypeAccessPaths parentVisitedAccessPaths) {
        ImmutableList.Builder<ComplexTypeAccessPath> builder = new ImmutableList.Builder<>();
        // Add cur access paths first.
        // For example: select a.b.c.d from tbl; ".c.d" is parent visited access paths, ".b" is cur access paths.
        // We need put cur access paths first, then put parent visited access paths.
        builder.addAll(curAccessPaths.getAccessPaths());
        builder.addAll(parentVisitedAccessPaths.getAccessPaths());
        return new ComplexTypeAccessPaths(builder.build());
    }

    public static void setAccessGroup(Type type, ComplexTypeAccessGroup accessGroup) {
        Preconditions.checkArgument(type.isComplexType());

        for (ComplexTypeAccessPaths accessPaths : accessGroup.getAccessGroup()) {
            // If a ColumnRefOperator has an empty access path, means select all subfields.
            if (accessPaths.isEmpty()) {
                type.selectAllFields();
                return;
            }
            Type tmpType = type;
            for (int i = 0; i < accessPaths.size(); i++) {
                // we will always select the ItemType of ArrayType, so we don't mark it and skip it.
                while (tmpType.isArrayType()) {
                    tmpType = ((ArrayType) tmpType).getItemType();
                }
                ComplexTypeAccessPath accessPath = accessPaths.get(i);
                if (i == accessPaths.size() - 1) {
                    // last one, select children's all subfields
                    tmpType.setSelectedField(accessPath, true);
                } else {
                    tmpType.setSelectedField(accessPath, false);
                    if (tmpType.isStructType()) {
                        tmpType = ((StructType) tmpType).getField(accessPath.getStructSubfieldName()).getType();
                    } else if (tmpType.isMapType()) {
                        tmpType = accessPath.getAccessPathType() == ComplexTypeAccessPathType.MAP_KEY ?
                                ((MapType) tmpType).getKeyType() : ((MapType) tmpType).getValueType();
                    }
                }
            }
        }
    }

    private static class MarkSubfieldsVisitor extends ScalarOperatorVisitor<Void, Context> {

        // For example, complexTypeAccessPaths first push A, then push B.
        // In the last, we convert it to a LinkedList, this list is [B, A].
        private final Deque<ComplexTypeAccessPath> complexTypeAccessPaths = new LinkedList<>();

        private final ComplexTypeAccessGroup visitedAccessGroup;

        public MarkSubfieldsVisitor(ComplexTypeAccessGroup visitedAccessGroup) {
            this.visitedAccessGroup = visitedAccessGroup;
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, Context context) {
            if (!context.getEnablePruneComplexTypes()) {
                return null;
            }

            if (scalarOperator.getType().isMapType() || scalarOperator.getType().isStructType()) {
                // New expression maybe added, and it's not be handled when go here, so we need disable prune subfields
                // to prevent error prune.
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

            // Add STRUCT_SUBFIELD access path from end to front, because complexTypeAccessPaths is a stack.
            for (int i = fieldNames.size() - 1; i >= 0; i--) {
                ComplexTypeAccessPath accessPath =
                        new ComplexTypeAccessPath(ComplexTypeAccessPathType.STRUCT_SUBFIELD, fieldNames.get(i));
                complexTypeAccessPaths.push(accessPath);
            }

            subfieldOperator.getChild(0).accept(this, context);

            for (int i = 0; i < fieldNames.size(); i++) {
                complexTypeAccessPaths.pop();
            }
            return null;
        }

        @Override
        public Void visitVariableReference(ColumnRefOperator variable, Context context) {
            if (variable.getType().isComplexType()) {
                ComplexTypeAccessPaths accessPaths = new ComplexTypeAccessPaths(ImmutableList.copyOf(complexTypeAccessPaths));
                if (visitedAccessGroup == null) {
                    context.addAccessPaths(variable, accessPaths);
                } else {
                    /*
                     * If specific ColumnRefOperator has visited access group, we should merge it.
                     */
                    context.addAccessPaths(variable, accessPaths, visitedAccessGroup);
                }
            }
            return null;
        }

        @Override
        public Void visitCollectionElement(CollectionElementOperator collectionElementOp, Context context) {
            if (collectionElementOp.getChild(0).getType().isMapType()) {
                // Consider for select col["map-key"] from tbl; This sql needs load both key and value columns
                complexTypeAccessPaths.push(new ComplexTypeAccessPath(ComplexTypeAccessPathType.ALL_SUBFIELDS));
            }

            collectionElementOp.getChild(0).accept(this, context);
            collectionElementOp.getChild(1).accept(this, context);

            if (collectionElementOp.getChild(0).getType().isMapType()) {
                complexTypeAccessPaths.pop();
            }
            return null;
        }

        @Override
        public Void visitCall(CallOperator call, Context context) {
            if (call.getFnName().equals(FunctionSet.MAP_KEYS) || call.getFnName().equals(FunctionSet.MAP_SIZE)) {
                complexTypeAccessPaths.push(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_KEY));
            } else if (call.getFnName().equals(FunctionSet.MAP_VALUES)) {
                complexTypeAccessPaths.push(new ComplexTypeAccessPath(ComplexTypeAccessPathType.MAP_VALUE));
            }

            for (ScalarOperator child : call.getChildren()) {
                child.accept(this, context);
            }

            if (call.getFnName().equals(FunctionSet.MAP_KEYS) || call.getFnName().equals(FunctionSet.MAP_SIZE) ||
                    call.getFnName().equals(FunctionSet.MAP_VALUES)) {
                complexTypeAccessPaths.pop();
            }

            return null;
        }
    }
}

