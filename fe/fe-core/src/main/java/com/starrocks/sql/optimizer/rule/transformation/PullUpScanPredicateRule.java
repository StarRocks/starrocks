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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rewrite.TableScanPredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.tree.prunesubfield.PruneSubfieldRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// PullUpScanPredicateRule is used to extract predicates that cannot use the optimizations of storage layer from ScanOperator,
// so that there is an opportunity to use expression reuse optimization later.
public class PullUpScanPredicateRule extends TransformationRule {
    public static final PullUpScanPredicateRule OLAP_SCAN = new PullUpScanPredicateRule(OperatorType.LOGICAL_OLAP_SCAN);

    public PullUpScanPredicateRule(OperatorType type) {
        super(RuleType.TF_PULL_UP_PREDICATE_SCAN, Pattern.create(type));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        ScalarOperator predicates = input.getOp().getPredicate();
        if (!context.getSessionVariable().isEnableScanPredicateExprReuse() || predicates == null) {
            return false;
        }
        return true;
    }

    private ScalarOperator replaceScalarOperator(ScalarOperator root,
                                                 Map<ScalarOperator, ColumnRefOperator> columnRefMap) {
        if (columnRefMap.containsKey(root)) {
            return columnRefMap.get(root);
        }

        List<ScalarOperator> children = root.getChildren();
        for (int i = 0; i < children.size(); i++) {
            root.setChild(i, replaceScalarOperator(children.get(i), columnRefMap));
        }
        return root;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator logicalScanOperator = (LogicalScanOperator) input.getOp();
        ScalarOperator predicates = logicalScanOperator.getPredicate();
        TableScanPredicateExtractor tableScanPredicateExtractor =
                new TableScanPredicateExtractor(logicalScanOperator.getColRefToColumnMetaMap());
        tableScanPredicateExtractor.extract(predicates);
        ScalarOperator pushedPredicates = tableScanPredicateExtractor.getPushedPredicates();
        ScalarOperator reservedPredicates = tableScanPredicateExtractor.getReservedPredicates();
        boolean newScanPredicateIsSame = Objects.equals(pushedPredicates, predicates);
        if (newScanPredicateIsSame || reservedPredicates == null) {
            return Lists.newArrayList();
        }

        Operator.Builder builder = OperatorBuilderFactory.build(logicalScanOperator);
        LogicalScanOperator newScanOperator = (LogicalScanOperator) builder.withOperator(logicalScanOperator)
                .setPredicate(pushedPredicates).build();
        // these columns must keep in the final projections
        List<ColumnRefOperator> outputColumns = newScanOperator.getOutputColumns();

        // After applying some rules, the ScanOperator may generate some projection columns, e.g. `RemoveAggregationFromAggTableRule`
        // In order to reuse existing columns as much as possible,
        // we need to replace the expressions in the predicate with the column that appears in the projection.
        Map<ScalarOperator, ColumnRefOperator> translatingMap = new HashMap<>();
        if (logicalScanOperator.getProjection() != null) {
            logicalScanOperator.getProjection().getColumnRefMap().forEach((k, v) -> {
                if (!translatingMap.containsKey(v) && !k.equals(v)) {
                    translatingMap.put(v, k);
                }
            });
        }

        Map<ColumnRefOperator, ScalarOperator> newProjections = new HashMap();
        // collect all expressions that can be used to prune subfield,
        // we should replace these expressions in the predicate so that subfield pruning still works.
        // for example, `cardinality(col1) + cardinality(col2) > 10`,
        // we should make `cardinality(col1)` and `cardinality(col2)` appear in the scan projections
        // instead of `col1` and `col2` so that we can have a correct column access path.
        SubfieldCollector collector = new SubfieldCollector(context.getColumnRefFactory());
        collector.visit(reservedPredicates, null);

        collector.getExpressionMapping().forEach((k, v) -> {
            if (!translatingMap.containsKey(k)) {
                translatingMap.put(k, v);
                newProjections.put(v, k);
            }
        });


        if (!translatingMap.isEmpty()) {
            reservedPredicates = replaceScalarOperator(reservedPredicates, translatingMap);
        }

        ColumnRefSet predicateUsedColumnRefSet = reservedPredicates.getUsedColumns();
        newScanOperator.buildColumnFilters(pushedPredicates);

        List<ColumnRefOperator> predicateUsedColumns = predicateUsedColumnRefSet
                .getColumnRefOperators(context.getColumnRefFactory());
        boolean allPredicatesColumnFromTableColumn = predicateUsedColumns.stream().allMatch(
                columnRefOperator -> newScanOperator.getColRefToColumnMetaMap().containsKey(columnRefOperator));

        Map<ColumnRefOperator, ScalarOperator> scanProjectionMap;
        if (newScanOperator.getProjection() != null) {
            // since pulled-up predicates must use some columns, we no longer need auto fill column.
            scanProjectionMap = newScanOperator.getProjection().getColumnRefMap();
            scanProjectionMap.keySet().removeIf(columnRefOperator -> columnRefOperator.getName().equals("auto_fill_col"));
            if (scanProjectionMap.isEmpty()) {
                scanProjectionMap = null;
                newScanOperator.setProjection(null);
            }
        }

        if (newScanOperator.getProjection() == null && newProjections.isEmpty() && allPredicatesColumnFromTableColumn) {
            // if all predicate used columns are table columns and projection is null, we don't need to set projection too
        } else {
            if (newScanOperator.getProjection() == null) {
                newScanOperator.setProjection(new Projection(new HashMap<>()));
            }
            scanProjectionMap = newScanOperator.getProjection().getColumnRefMap();
            for (Map.Entry<ScalarOperator, ColumnRefOperator> entry : translatingMap.entrySet()) {
                if (!scanProjectionMap.containsKey(entry.getValue())) {
                    scanProjectionMap.put(entry.getValue(), entry.getKey());
                }
            }
            for (ColumnRefOperator columnRefOperator : predicateUsedColumns) {
                if (!scanProjectionMap.containsKey(columnRefOperator)) {
                    scanProjectionMap.put(columnRefOperator, columnRefOperator);
                }
            }
            for (ColumnRefOperator columnRefOperator : outputColumns) {
                if (!scanProjectionMap.containsKey(columnRefOperator)) {
                    scanProjectionMap.put(columnRefOperator, columnRefOperator);
                }
            }

        }

        LogicalFilterOperator newFilterOperator = new LogicalFilterOperator(reservedPredicates);
        if (newScanOperator.hasLimit()) {
            newFilterOperator.setLimit(newScanOperator.getLimit());
            newScanOperator.setLimit(Operator.DEFAULT_LIMIT);
        }

        OptExpression filter = OptExpression.create(newFilterOperator, OptExpression.create(newScanOperator));
        return Lists.newArrayList(filter);
    }

    // collect all expressions that can be used to prune subfield and record them in expressionMap
    private static class SubfieldCollector extends ScalarOperatorVisitor<Boolean, Void> {
        private ColumnRefFactory columnRefFactory;
        private Map<ScalarOperator, ColumnRefOperator> expressionMapping = new HashMap();
        public SubfieldCollector(ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
        }
        public Map<ScalarOperator, ColumnRefOperator> getExpressionMapping() {
            return expressionMapping;
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            boolean result = true;
            for (ScalarOperator child : scalarOperator.getChildren()) {
                if (!child.accept(this, context)) {
                    result = false;
                }
            }
            return result;
        }

        @Override
        public Boolean visitVariableReference(ColumnRefOperator op, Void context) {
            if (op.getType().isComplexType() || op.getType().isJsonType()) {
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitCollectionElement(CollectionElementOperator op, Void context) {
            if (op.getUsedColumns().isEmpty()) {
                return false;
            }
            if (op.getChild(1).isConstant() && visit(op.getChild(0), context)) {
                if (!expressionMapping.containsKey(op)) {
                    ColumnRefOperator columnRefOperator = columnRefFactory.create("element_at", op.getType(), op.isNullable());
                    expressionMapping.put(op, columnRefOperator);
                }
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitSubfield(SubfieldOperator op, Void context) {
            if (op.getUsedColumns().isEmpty()) {
                return false;
            }
            if (visit(op.getChild(0), context)) {
                if (!expressionMapping.containsKey(op)) {
                    ColumnRefOperator columnRefOperator = columnRefFactory.create("subfield", op.getType(), op.isNullable());
                    expressionMapping.put(op, columnRefOperator);
                }
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitCall(CallOperator op, Void context) {
            if (!PruneSubfieldRule.PRUNE_FUNCTIONS.contains(op.getFnName())) {
                visit(op, context);
                return false;
            }
            List<ScalarOperator> children = op.getChildren();
            boolean allConstArgs = true;
            for (int i = 1; i < children.size() && allConstArgs; i++) {
                allConstArgs &= children.get(i).isConstant();
            }
            if (allConstArgs && visit(children.get(0), context)) {
                if (!expressionMapping.containsKey(op)) {
                    ColumnRefOperator columnRefOperator =
                            columnRefFactory.create(op.getFnName(), op.getType(), op.isNullable());
                    expressionMapping.put(op, columnRefOperator);
                }
                return true;
            }
            return false;
        }

        @Override
        public Boolean visitLambdaFunctionOperator(LambdaFunctionOperator op, Void context) {
            return false;
        }
    }
}
