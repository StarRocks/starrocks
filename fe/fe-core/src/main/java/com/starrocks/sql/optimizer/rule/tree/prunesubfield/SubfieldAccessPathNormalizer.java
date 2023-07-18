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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.StructType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.thrift.TAccessPathType;

import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/*
 * normalize expression to ColumnAccessPath
 */
public class SubfieldAccessPathNormalizer extends ScalarOperatorVisitor<Void, Void> {
    private final Deque<AccessPath> allAccessPaths = Lists.newLinkedList();

    private AccessPath currentPath = null;

    private static class AccessPath {
        private final ScalarOperator root;
        private final List<String> paths = Lists.newArrayList();
        private final List<TAccessPathType> pathTypes = Lists.newArrayList();

        public AccessPath(ScalarOperator root) {
            this.root = root;
        }

        public AccessPath appendPath(String path, TAccessPathType pathType) {
            paths.add(path);
            pathTypes.add(pathType);
            return this;
        }

        public ScalarOperator root() {
            return root;
        }
    }

    public ColumnAccessPath normalizePath(ColumnRefOperator root, String columnName) {
        List<AccessPath> paths = allAccessPaths.stream().filter(path -> path.root().equals(root))
                .sorted((o1, o2) -> Integer.compare(o2.paths.size(), o1.paths.size()))
                .collect(Collectors.toList());

        ColumnAccessPath rootPath = new ColumnAccessPath(TAccessPathType.ROOT, columnName);
        for (AccessPath accessPath : paths) {
            ColumnAccessPath parentPath = rootPath;
            for (int i = 0; i < accessPath.paths.size(); i++) {
                if (parentPath.hasChildPath(accessPath.paths.get(i))) {
                    ColumnAccessPath childPath = parentPath.getChildPath(accessPath.paths.get(i));
                    TAccessPathType pathType = accessPath.pathTypes.get(i);
                    if (childPath.getType() != accessPath.pathTypes.get(i)) {
                        // if the path same but type different, must be PATH_PLACEHOLDER, the Type must be
                        // INDEX, OFFSET, KEY, ALL, and we can merge them
                        // 1. when contains OFFSET and KEY, we set the type to KEY
                        // 2. other (OFFSET-ALL, OFFSET-INDEX, INDEX-KEY, KEY-ALL), we set the type to ALL
                        boolean isOffsetOrKey =
                                childPath.getType() == TAccessPathType.OFFSET && pathType == TAccessPathType.KEY;
                        isOffsetOrKey = isOffsetOrKey ||
                                (childPath.getType() == TAccessPathType.KEY && pathType == TAccessPathType.OFFSET);
                        childPath.setType(isOffsetOrKey ? TAccessPathType.KEY : TAccessPathType.ALL);
                    }
                    parentPath = childPath;
                } else {
                    ColumnAccessPath childPath =
                            new ColumnAccessPath(accessPath.pathTypes.get(i), accessPath.paths.get(i));
                    parentPath.addChildPath(childPath);
                    parentPath = childPath;
                }
            }
            parentPath.clearChildPath();
        }

        return rootPath;
    }

    public boolean hasPath(ColumnRefOperator root) {
        return allAccessPaths.stream().anyMatch(path -> path.root().equals(root));
    }

    public void add(ScalarOperator operator) {
        if (operator == null) {
            return;
        }
        operator.accept(this, null);
    }

    @Override
    public Void visit(ScalarOperator scalarOperator, Void context) {
        for (ScalarOperator child : scalarOperator.getChildren()) {
            child.accept(this, context);
            if (currentPath != null) {
                allAccessPaths.push(currentPath);
                currentPath = null;
            }
        }
        return null;
    }

    @Override
    public Void visitVariableReference(ColumnRefOperator variable, Void context) {
        if (variable.getType().isComplexType()) {
            currentPath = new AccessPath(variable);
            allAccessPaths.push(currentPath);
        }
        return null;
    }

    @Override
    public Void visitSubfield(SubfieldOperator subfieldOperator, Void context) {
        subfieldOperator.getChild(0).accept(this, context);
        if (currentPath != null) {
            subfieldOperator.getFieldNames()
                    .forEach(p -> currentPath.appendPath(p, TAccessPathType.FIELD));
        }
        return null;
    }

    @Override
    public Void visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
        if (!collectionElementOp.getChild(1).isConstant()) {
            collectionElementOp.getChild(1).accept(this, context);
        }

        collectionElementOp.getChild(0).accept(this, context);
        if (currentPath == null) {
            return null;
        }

        if (!collectionElementOp.getChild(1).isConstant()) {
            currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.ALL);
            return null;
        }
        currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.INDEX);
        return null;
    }

    @Override
    public Void visitCall(CallOperator call, Void context) {
        if (!PruneSubfieldRule.SUPPORT_FUNCTIONS.contains(call.getFnName())) {
            return visit(call, context);
        }

        if (call.getFnName().equals(FunctionSet.MAP_KEYS)) {
            call.getChild(0).accept(this, context);
            if (currentPath != null) {
                currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.KEY);
            }
            return null;
        } else if (FunctionSet.MAP_SIZE.equals(call.getFnName())
                || FunctionSet.CARDINALITY.equals(call.getFnName())
                || FunctionSet.ARRAY_LENGTH.equals(call.getFnName())) {
            call.getChild(0).accept(this, context);
            if (currentPath != null) {
                currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.OFFSET);
            }
        }

        return null;
    }

    @Override
    public Void visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        predicate.getChild(0).accept(this, context);
        if (currentPath != null) {
            if (predicate.getChild(0).getType().isMapType()) {
                currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.OFFSET);
            } else if (predicate.getChild(0).getType().isStructType()) {
                StructType type = (StructType) predicate.getChild(0).getType();
                currentPath.appendPath(type.getFields().get(0).getName(), TAccessPathType.FIELD);
            } else if (predicate.getChild(0).getType().isArrayType()) {
                currentPath.appendPath(ColumnAccessPath.PATH_PLACEHOLDER, TAccessPathType.OFFSET);
            }
        }
        currentPath = null;
        return null;
    }
}
