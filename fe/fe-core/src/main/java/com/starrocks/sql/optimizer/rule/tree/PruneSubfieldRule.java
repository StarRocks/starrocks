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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.thrift.TAccessPathType;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PruneSubfieldRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SubfieldAccessPathComputer computer = new SubfieldAccessPathComputer();
        return root.getOp().accept(computer, root, null);
    }

    /*
     * collect all complex expressions, such as: MAP_KEYS, MAP_VALUES, map['key'], struct.a.b.c ...
     */
    private static class ComplexExpressionCollector extends ScalarOperatorVisitor<Void, Void> {
        private static final List<String> SUPPORT_FUNCTIONS = ImmutableList.<String>builder()
                .add(FunctionSet.MAP_KEYS, FunctionSet.MAP_SIZE, FunctionSet.MAP_VALUES)
                .build();

        private final List<ScalarOperator> complexExpressions = Lists.newArrayList();

        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            for (ScalarOperator child : scalarOperator.getChildren()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitVariableReference(ColumnRefOperator variable, Void context) {
            if (variable.getType().isComplexType()) {
                complexExpressions.add(variable);
            }
            return null;
        }

        @Override
        public Void visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            complexExpressions.add(collectionElementOp);
            return null;
        }

        @Override
        public Void visitSubfield(SubfieldOperator subfieldOperator, Void context) {
            complexExpressions.add(subfieldOperator);
            return null;
        }

        @Override
        public Void visitCall(CallOperator call, Void context) {
            if (SUPPORT_FUNCTIONS.contains(call.getFnName())) {
                complexExpressions.add(call);
                return null;
            }
            return visit(call, context);
        }
    }

    /*
     * compute all complex column access path in whole plan, we only need check the expression
     * which one in project and predicate (the rule execute order promise this)
     */
    private static class SubfieldAccessPathComputer extends OptExpressionVisitor<OptExpression, Void> {
        private final Map<ScalarOperator, ColumnRefSet> allComplexColumns = Maps.newHashMap();

        public void visitPredicate(OptExpression optExpression) {
            if (optExpression.getOp().getPredicate() == null) {
                return;
            }

            ComplexExpressionCollector collector = new ComplexExpressionCollector();
            optExpression.getOp().getPredicate().accept(collector, null);
            for (ScalarOperator expr : collector.complexExpressions) {
                allComplexColumns.put(expr, expr.getUsedColumns());
            }
        }

        @Override
        public OptExpression visit(OptExpression optExpression, Void context) {
            visitPredicate(optExpression);

            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                OptExpression child = optExpression.inputAt(i);
                optExpression.setChild(i, child.getOp().accept(this, child, context));
            }

            return optExpression;
        }

        @Override
        public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
            LogicalProjectOperator lpo = optExpression.getOp().cast();

            // check & rewrite nest expression
            Set<ColumnRefOperator> keys = lpo.getColumnRefMap().keySet();

            // avoid concurrent modify
            List<ScalarOperator> complexColumns = Lists.newArrayList(allComplexColumns.keySet());
            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(lpo.getColumnRefMap(), true);
            for (ScalarOperator expr : complexColumns) {
                ColumnRefSet usedColumns = allComplexColumns.get(expr);

                // need rewrite
                if (usedColumns.containsAny(keys)) {
                    allComplexColumns.remove(expr);
                    expr = rewriter.rewrite(expr);
                    allComplexColumns.put(expr, expr.getUsedColumns());
                }
            }

            // collect complex column
            ComplexExpressionCollector collector = new ComplexExpressionCollector();
            for (ScalarOperator value : lpo.getColumnRefMap().values()) {
                value.accept(collector, null);
            }

            for (ScalarOperator expr : collector.complexExpressions) {
                allComplexColumns.put(expr, expr.getUsedColumns());
            }

            return visit(optExpression, context);
        }

        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
            visit(optExpression, context);

            // normalize access path
            LogicalScanOperator scan = optExpression.getOp().cast();
            Set<ColumnRefOperator> scanColumns = scan.getColRefToColumnMetaMap().keySet();

            SubfieldAccessPathNormalizer normalizer = new SubfieldAccessPathNormalizer();

            allComplexColumns.forEach((k, v) -> {
                if (v.containsAny(scanColumns)) {
                    normalizer.add(k);
                }
            });

            List<ColumnAccessPath> accessPaths = Lists.newArrayList();

            for (ColumnRefOperator ref : scanColumns) {
                if (!normalizer.hasPath(ref)) {
                    continue;
                }
                String columnName = scan.getColRefToColumnMetaMap().get(ref).getName();
                ColumnAccessPath p = normalizer.normalizePath(ref, columnName);

                if (p.hasChildPath()) {
                    accessPaths.add(p);
                }
            }

            if (accessPaths.isEmpty()) {
                return optExpression;
            }

            LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scan);
            Operator newScan = builder.withOperator(scan).setColumnAccessPaths(accessPaths).build();
            return OptExpression.create(newScan, optExpression.getInputs());
        }
    }

    /*
     * normalize expression to ColumnAccessPath
     */
    private static class SubfieldAccessPathNormalizer extends ScalarOperatorVisitor<Void, Void> {
        private static final List<String> NORMALIZE_FUNCTIONS =
                ImmutableList.of(FunctionSet.MAP_KEYS, FunctionSet.MAP_SIZE, FunctionSet.MAP_VALUES);

        private final Deque<AccessPath> allAccessPaths = Lists.newLinkedList();

        private AccessPath currentPath = null;

        private ColumnAccessPath normalizePath(ColumnRefOperator root, String columnName) {
            List<AccessPath> paths = allAccessPaths.stream().filter(path -> path.root().equals(root))
                    .sorted((o1, o2) -> Integer.compare(o2.paths.size(), o1.paths.size()))
                    .collect(Collectors.toList());

            ColumnAccessPath rootPath = new ColumnAccessPath(TAccessPathType.ROOT, columnName);
            for (AccessPath accessPath : paths) {
                ColumnAccessPath parentPath = rootPath;
                for (int i = 0; i < accessPath.paths.size(); i++) {
                    if (parentPath.hasChildPath(accessPath.paths.get(i))) {
                        parentPath = parentPath.getChildPath(accessPath.paths.get(i));
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
        public Void visitCall(CallOperator call, Void context) {
            if (!NORMALIZE_FUNCTIONS.contains(call.getFnName())) {
                return visit(call, context);
            }

            if (call.getFnName().equals(FunctionSet.MAP_KEYS)) {
                call.getChild(0).accept(this, context);
                if (currentPath != null) {
                    currentPath.appendPath(AccessPath.PATH_KEY, TAccessPathType.KEY);
                }
                return null;
            } else if (call.getFnName().equals(FunctionSet.MAP_SIZE)) {
                call.getChild(0).accept(this, context);
                if (currentPath != null) {
                    currentPath.appendPath(AccessPath.PATH_OFFSET, TAccessPathType.OFFSET);
                }
            }

            return null;
        }
    }

    private static class AccessPath {
        private static final String PATH_KEY = "KEY";
        private static final String PATH_OFFSET = "OFFSET";

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
}
