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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import autovalue.shaded.com.google.common.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

// Add columnsToEnforce into LogicalScanOperator's colRefToColumnMetaMap.
// enforce() will return a new OptExpression based on old OptExpression instead of
// modifying it locally.
public class ColumnEnforcer {
    private OptExpression optExpression;
    private List<ColumnRefOperator> columnsToEnforce;
    private EnforceContext enforceContext;

    public ColumnEnforcer(
            OptExpression optExpression, List<ColumnRefOperator> columnsToEnforce) {
        this.optExpression = optExpression;
        this.columnsToEnforce = columnsToEnforce;
        this.enforceContext = new EnforceContext();
    }

    public OptExpression enforce() {
        ColumnEnforcerVisitor visitor = new ColumnEnforcerVisitor();
        OptExpression rootOptExpression = optExpression.getOp().accept(visitor, optExpression, enforceContext);
        return rootOptExpression;
    }

    public List<ColumnRefOperator> getEnforcedColumns() {
        return enforceContext.enforcedColumns;
    }

    public List<ColumnRefOperator> getEnforcedNonExistedColumns() {
        return enforceContext.enforcedNonExistedColumns;
    }

    private class EnforceContext {
        // Columns which are enforced no matter it is existed in operator's output columns.
        private final List<ColumnRefOperator> enforcedColumns = Lists.newArrayList();
        // Columns which are enforced and are not existed before in operator's output columns which should be
        // removed after rewrite.
        private final List<ColumnRefOperator> enforcedNonExistedColumns = Lists.newArrayList();

        public EnforceContext() {
        }
    }

    private class ColumnEnforcerVisitor extends OptExpressionVisitor<OptExpression, EnforceContext> {
        @Override
        public OptExpression visitLogicalTableScan(OptExpression optExpression, EnforceContext context) {
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scanOperator);
            builder.withOperator(scanOperator);
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = Maps.newHashMap(scanOperator.getColRefToColumnMetaMap());
            for (ColumnRefOperator columnRef : columnsToEnforce) {
                for (Map.Entry<Column, ColumnRefOperator> entry : scanOperator.getColumnMetaToColRefMap().entrySet()) {
                    ColumnRefOperator toEnforceColumnRefOp = entry.getValue();
                    // Only add to-enforce column ref operator into enforce columns when it's not in the output columns.
                    if (toEnforceColumnRefOp.equals(columnRef)) {
                        if (!columnRefOperatorColumnMap.containsKey(toEnforceColumnRefOp)) {
                            columnRefOperatorColumnMap.put(columnRef, entry.getKey());
                            context.enforcedNonExistedColumns.add(columnRef);
                        }
                        context.enforcedColumns.add(columnRef);
                    }
                }
            }
            builder.setColRefToColumnMetaMap(columnRefOperatorColumnMap);
            OptExpression newScan = new OptExpression(builder.build());
            newScan.deriveLogicalPropertyItself();
            return newScan;
        }

        @Override
        public OptExpression visit(OptExpression optExpression, EnforceContext context) {
            List<OptExpression> inputs = Lists.newArrayList();
            EnforceContext localContext = new EnforceContext();
            for (OptExpression child : optExpression.getInputs()) {
                inputs.add(child.getOp().accept(this, child, localContext));
            }

            LogicalOperator.Builder builder = OperatorBuilderFactory.build(optExpression.getOp());
            builder.withOperator(optExpression.getOp());

            if (optExpression.getOp().getProjection() != null) {
                Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                        Maps.newHashMap(optExpression.getOp().getProjection().getColumnRefMap());
                for (ColumnRefOperator columnRef : localContext.enforcedColumns) {
                    if (!columnRefMap.containsKey(columnRef)) {
                        columnRefMap.put(columnRef, columnRef);
                        context.enforcedNonExistedColumns.add(columnRef);
                    }
                    context.enforcedColumns.add(columnRef);
                }
                builder.setProjection(new Projection(columnRefMap));
            }
            OptExpression newOptExpression = OptExpression.create(builder.build(), inputs);
            newOptExpression.deriveLogicalPropertyItself();
            return newOptExpression;
        }
    }
}
