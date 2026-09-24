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
import com.google.common.collect.Lists;
import com.starrocks.catalog.ComplexTypeAccessGroup;
import com.starrocks.catalog.ComplexTypeAccessPaths;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.ComplexTypeAccessPath;
import com.starrocks.type.ComplexTypeAccessPathType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
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
        private final Map<ColumnRefOperator, ColumnRefOperator> unnestColRefMap;
        private final List<ColumnRefOperator> scanRefs;
        private boolean enablePruneComplexTypesInUnnest;
        private boolean enablePruneComplexTypes;

        public Context(boolean enablePruneComplexTypesInUnnest) {
            this.accessGroups = new HashMap<>();
            this.enablePruneComplexTypes = true;
            this.unnestColRefMap = new HashMap<>();
            this.scanRefs = Lists.newArrayList();
            this.enablePruneComplexTypesInUnnest = enablePruneComplexTypesInUnnest;
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

            ColumnRefOperator oriColRefOperator = getOriginalColRef(columnRefOperator);
            if (oriColRefOperator != columnRefOperator) {
                accessGroups.putIfAbsent(oriColRefOperator, new ComplexTypeAccessGroup());
                accessGroups.get(oriColRefOperator).addAccessPaths(accessPaths);
            }
        }

        public void addAccessPaths(ColumnRefOperator columnRefOperator,
                                   ComplexTypeAccessPaths curAccessPaths,
                                   ComplexTypeAccessGroup visitedAccessGroup) {
            // We should copy it first to avoid ConcurrentModificationException
            ImmutableList<ComplexTypeAccessPaths> accessGroup = ImmutableList.<ComplexTypeAccessPaths>builder().addAll(
                    visitedAccessGroup.getAccessGroup()).build();
            for (ComplexTypeAccessPaths complexTypeAccessPaths : accessGroup) {
                addAccessPaths(columnRefOperator, concatAccessPaths(curAccessPaths, complexTypeAccessPaths));
            }
        }

        public void addScan(ColumnRefOperator columnRefOperator) {
            scanRefs.add(columnRefOperator);
        }

        public List<ColumnRefOperator> getScanRefs() {
            return scanRefs;
        }

        public void add(ColumnRefOperator outputColumnRefOperator, ScalarOperator scalarOperator) {
            ComplexTypeAccessGroup visitedAccessGroup = null;
            if (outputColumnRefOperator != null) {
                // If outputColumnRefOperator is not null, it means it may have visited access group,
                // we get it for later merge access path.
                visitedAccessGroup = getVisitedAccessGroup(outputColumnRefOperator);
            }

            MarkSubfieldsVisitor markSubfieldsVisitor = new MarkSubfieldsVisitor(visitedAccessGroup);
            scalarOperator.accept(markSubfieldsVisitor, this);
        }

        public void setUnnest(PhysicalTableFunctionOperator operator) {
            for (int i = 0; i < operator.getFnResultColRefs().size(); i++) {
                ColumnRefOperator output = operator.getFnResultColRefs().get(i);
                ColumnRefOperator input = operator.getFnParamColumnRefs().get(i);
                unnestColRefMap.put(output, input);
                ComplexTypeAccessGroup outputGroup = getVisitedAccessGroup(output);
                if (outputGroup != null) {
                    // Merge access paths into the input column's access group instead of overwriting.
                    // Multiple UNNEST operators may share the same input array column (e.g. UNNEST(a), UNNEST(a)),
                    // and each one's downstream consumers may need different subfields of the array element.
                    // A hard put would drop access paths recorded by earlier UNNESTs and cause the scan to
                    // prune subfields that are still needed, leading to BE crashes when reading missing fields.
                    ComplexTypeAccessGroup existing = accessGroups.get(input);
                    if (existing == null) {
                        accessGroups.put(input, outputGroup);
                    } else if (existing != outputGroup) {
                        for (ComplexTypeAccessPaths paths : outputGroup.getAccessGroup()) {
                            existing.addAccessPaths(paths);
                        }
                    }
                    if (operator.getProjection() == null && operator.getOutputColRefs().contains(output)) {
                        add(input, input);
                    }
                }
            }
        }

        public boolean isEnablePruneComplexTypesInUnnest() {
            return enablePruneComplexTypesInUnnest;
        }

        public boolean hasUnnestColRefMapValue(ColumnRefOperator columnRefOperator) {
            return unnestColRefMap.containsValue(columnRefOperator);
        }

        public boolean hasUnnestColRefMapKey(ColumnRefOperator columnRefOperator) {
            return unnestColRefMap.containsKey(columnRefOperator);
        }

        // Returns the input array column that an UNNEST output column was produced from, or null if
        // the given column is not an UNNEST output. Used to walk stacked UNNESTs (UNNEST of an
        // UNNEST output) when deciding whether an output can be pruned in lockstep with its input.
        public ColumnRefOperator getUnnestInput(ColumnRefOperator output) {
            return unnestColRefMap.get(output);
        }

        public ComplexTypeAccessGroup getVisitedAccessGroup(ColumnRefOperator columnRefOperator) {
            return accessGroups.get(columnRefOperator);
        }

        private ColumnRefOperator getOriginalColRef(ColumnRefOperator col) {
            if (unnestColRefMap.containsKey(col)) {
                return unnestColRefMap.get(col);
            }
            return col;
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
                /// If origin type is Array, the item type may not be complex type anymore
                if (!tmpType.isComplexType()) {
                    break;
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

        // True while we are inside an expression we cannot reason about. The paths recorded downstream
        // for the column this expression defines describe the expression's *result*, so they must not
        // be attributed to the columns it reads - see visit() and visitVariableReference().
        private boolean inUnknownExpr = false;

        public MarkSubfieldsVisitor(ComplexTypeAccessGroup visitedAccessGroup) {
            this.visitedAccessGroup = visitedAccessGroup;
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, Context context) {
            if (!context.getEnablePruneComplexTypes()) {
                return null;
            }

            if (scalarOperator.getType().isComplexType() || scalarOperator.getType().isFunctionType()) {
                // An expression we cannot reason about that yields a complex type - a CASE WHEN whose
                // branches return MAP, array_map/array_filter, or the LambdaFunctionOperator inside
                // map_filter/map_apply, for example. We do not know which subfields of its inputs it
                // needs, so the columns it reads must not be pruned.
                //
                // ARRAY counts here just like MAP and STRUCT: a lambda reads its argument's subfields
                // through its own ColumnRefOperators, which never reach a scan column, so those reads
                // are invisible to this visitor. select array_filter(x -> x.user = 'official', name)[1].family
                // would otherwise narrow name to struct<family> and the BE would fail evaluating x.user.
                //
                // Clearing the access path stack for this subtree is what enforces that: a column that
                // registers with an empty ComplexTypeAccessPaths means "select all subfields" (see
                // markComplexTypeSelectedFields above). Clearing is required rather than merely skipping
                // the paths - leaving an outer path in place would misattribute it to this subtree's
                // columns, e.g. map_values(unknown_expr(col)) would prune col down to its map values and
                // drop the keys unknown_expr may need.
                //
                // This is exactly as conservative as disabling pruning outright for the columns this
                // expression touches, but it is scoped to them. Disabling the flag instead turns one such
                // expression anywhere in the query into a query-wide opt-out, so an unrelated wide struct
                // elsewhere in the same statement gets read in full.
                Deque<ComplexTypeAccessPath> savedAccessPaths = new LinkedList<>(complexTypeAccessPaths);
                boolean savedInUnknownExpr = inUnknownExpr;
                complexTypeAccessPaths.clear();
                inUnknownExpr = true;
                try {
                    for (ScalarOperator child : scalarOperator.getChildren()) {
                        child.accept(this, context);
                    }
                } finally {
                    inUnknownExpr = savedInUnknownExpr;
                    complexTypeAccessPaths.clear();
                    complexTypeAccessPaths.addAll(savedAccessPaths);
                }
                return null;
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
                if (visitedAccessGroup == null || inUnknownExpr) {
                    /*
                     * Clearing the local stack is not enough inside an unknown expression: visitedAccessGroup
                     * carries the paths that downstream operators recorded for the column this expression
                     * defines, and they describe its result, not what it reads. Appending them here would
                     * narrow the read columns by a path the expression never took - e.g. an upper
                     * `map_values(m)` over a lower `m = map_filter(lambda, col_map)` would drop col_map's
                     * keys, which the lambda evaluates. Dropping the suffix only ever widens what is read,
                     * so it stays on the conservative side. Paths pushed below the boundary (a SubfieldOperator
                     * between the expression and the column) are still in the stack and still apply.
                     */
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
            ComplexTypeAccessPathType pathType = accessPathTypeOf(call);
            if (pathType == null) {
                // A function whose effect on its arguments' subfields we cannot describe - map_filter and
                // map_apply, for example. Route it through visit() so that, when it yields a complex type,
                // the subtree is walked with a cleared access path stack. This has to happen before we
                // descend: a path pushed by an enclosing map_keys/map_values describes this call's result,
                // not the columns it reads, and attributing it to them would prune away subfields the call
                // needs.
                return visit(call, context);
            }

            complexTypeAccessPaths.push(new ComplexTypeAccessPath(pathType));
            try {
                for (ScalarOperator child : call.getChildren()) {
                    child.accept(this, context);
                }
            } finally {
                complexTypeAccessPaths.pop();
            }

            return null;
        }

        // The access path a function takes into its map argument, or null when we cannot describe it.
        private static ComplexTypeAccessPathType accessPathTypeOf(CallOperator call) {
            String name = call.getFnName();
            if (FunctionSet.MAP_KEYS.equals(name) || FunctionSet.MAP_SIZE.equals(name)) {
                return ComplexTypeAccessPathType.MAP_KEY;
            }
            if (FunctionSet.MAP_VALUES.equals(name)) {
                return ComplexTypeAccessPathType.MAP_VALUE;
            }
            if (FunctionSet.MAP_ENTRIES.equals(name)) {
                // map_entries returns array<struct<key, value>>, so it needs both key and value
                return ComplexTypeAccessPathType.ALL_SUBFIELDS;
            }
            return null;
        }
    }
}

