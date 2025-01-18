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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// extract table scan predicates to two parts: pushed-down to scan node, keep into filter node
// extract the predicates of table scan and divide it into two part:
// 1. pushedPredicates: simple single-column predicates that can use the optimizations of storage layer,
// these should be evaluated inside ScanNode;
// 2. reservedPredicates: other predicates which can be evaluated outside ScanNode;
public class TableScanPredicateExtractor {
    private Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
    private List<ScalarOperator> pushedPredicates = new LinkedList<>();
    private List<ScalarOperator> reservedPredicates = new LinkedList<>();

    public TableScanPredicateExtractor(Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        this.colRefToColumnMetaMap = colRefToColumnMetaMap;
    }

    public ScalarOperator getPushedPredicates() {
        return Utils.compoundAnd(pushedPredicates);
    }

    public ScalarOperator getReservedPredicates() {
        return Utils.compoundAnd(reservedPredicates);
    }

    public void extract(ScalarOperator op) {
        if (op.getOpType().equals(OperatorType.COMPOUND)) {
            CompoundPredicateOperator operator = (CompoundPredicateOperator) op;
            switch (operator.getCompoundType()) {
                case AND: {
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(operator);
                    for (ScalarOperator conjunct : conjuncts) {
                        if (conjunct.accept(new CanFullyPushDownVisitor(colRefToColumnMetaMap),
                                new CanFullyPushDownVisitorContext())) {
                            pushedPredicates.add(conjunct);
                        } else {
                            reservedPredicates.add(conjunct);
                        }
                    }
                    return;
                }
                case OR: {
                    for (ScalarOperator child : operator.getChildren()) {
                        if (!child.accept(new CanFullyPushDownVisitor(colRefToColumnMetaMap),
                                new CanFullyPushDownVisitorContext())) {
                            reservedPredicates.add(op);
                            return;
                        }
                    }
                    pushedPredicates.add(op);
                    return;
                }
                case NOT: {
                    if (op.getChild(0).accept(new CanFullyPushDownVisitor(colRefToColumnMetaMap),
                            new CanFullyPushDownVisitorContext())) {
                        pushedPredicates.add(op);
                    } else {
                        reservedPredicates.add(op);
                    }
                    return;
                }
            }
            return;
        }
        if (op.accept(new CanFullyPushDownVisitor(colRefToColumnMetaMap), new CanFullyPushDownVisitorContext())) {
            pushedPredicates.add(op);
        } else {
            reservedPredicates.add(op);
        }
    }

    private class CanFullyPushDownVisitorContext {
        public ColumnRefOperator usedColumn = null;
    }

    // CanFullyPushDownVisitor is used to check whether a predicate can be pushed down into ScanNode.
    // currently, we only allow single-column predicates that not contain lambda expressions to be pushed down.
    private class CanFullyPushDownVisitor extends ScalarOperatorVisitor<Boolean, CanFullyPushDownVisitorContext> {
        private final Map<ColumnRefOperator, Column> columnRefOperatorColumnMap;

        public CanFullyPushDownVisitor(Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
            this.columnRefOperatorColumnMap = columnRefOperatorColumnMap;
        }

        private Boolean visitAllChildren(ScalarOperator op, CanFullyPushDownVisitorContext context) {
            for (ScalarOperator child : op.getChildren()) {
                if (!child.accept(this, context)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(ScalarOperator op, CanFullyPushDownVisitorContext context) {
            return visitAllChildren(op, context);
        }

        @Override
        public Boolean visitConstant(ConstantOperator op, CanFullyPushDownVisitorContext context) {
            return true;
        }

        @Override
        public Boolean visitVariableReference(ColumnRefOperator op, CanFullyPushDownVisitorContext context) {
            if (columnRefOperatorColumnMap.containsKey(op)) {
                if (context.usedColumn == null) {
                    context.usedColumn = op;
                } else if (!context.usedColumn.equals(op)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visitCompoundPredicate(CompoundPredicateOperator op, CanFullyPushDownVisitorContext context) {
            for (ScalarOperator child : op.getChildren()) {
                context.usedColumn = null;
                if (!child.accept(this, context)) {
                    return false;
                }
            }
            return true;
        }

        private Boolean isTableColumn(ScalarOperator op) {
            if (op instanceof ColumnRefOperator && columnRefOperatorColumnMap.containsKey((ColumnRefOperator) op)) {
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator op, CanFullyPushDownVisitorContext context) {
            // we allow two column comparison
            if (isTableColumn(op.getChild(0)) && isTableColumn(op.getChild(1))) {
                return true;
            }
            return visit(op, context);
        }

        @Override
        public Boolean visitLambdaFunctionOperator(LambdaFunctionOperator op, CanFullyPushDownVisitorContext context) {
            return false;
        }
    }
}
