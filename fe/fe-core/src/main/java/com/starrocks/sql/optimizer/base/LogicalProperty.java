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

package com.starrocks.sql.optimizer.base;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalProperty implements Property {
    // Operator's output columns
    private ColumnRefSet outputColumns;

    // The flag for execute upon less than or equal one tablet
    private OneTabletMeta isExecuteInOneTablet;

    public ColumnRefSet getOutputColumns() {
        return outputColumns;
    }

    public void setOutputColumns(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public OneTabletMeta isExecuteInOneTablet() {
        return isExecuteInOneTablet;
    }

    public LogicalProperty() {
        this.outputColumns = new ColumnRefSet();
    }

    public LogicalProperty(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public LogicalProperty(LogicalProperty other) {
        outputColumns = other.outputColumns.clone();
        isExecuteInOneTablet = other.isExecuteInOneTablet;
    }

    public void derive(ExpressionContext expressionContext) {
        LogicalOperator op = (LogicalOperator) expressionContext.getOp();
        outputColumns = op.getOutputColumns(expressionContext);
        isExecuteInOneTablet = op.accept(new OneTabletExecutorVisitor(), expressionContext);
    }

    public static final class OneTabletMeta {
        /*
         * For instance, see the below example
         *        Agg(group by v1)
         *             |
         *             v
         *      Analytic(partition by b2)
         *             |
         *             v
         *        Scan (bucket: v1, only one tablet)
         * For Analytic(self=true, asChild=false):
         *     Can perform one tablet optimization(no exchange needed).
         *
         * For Agg(self=false, asChild=false):
         *     Cannot perform one tablet optimization(exchange needed), because it's child, i.e. Analytic, cannot
         * offer one tablet guarantee because distribution has been changed.
         */
        public final boolean self;
        public final boolean asChild;
        public final ColumnRefSet bucketColumns;

        private OneTabletMeta(boolean self, boolean asChild, ColumnRefSet bucketColumns) {
            this.self = self;
            this.asChild = asChild;
            this.bucketColumns = bucketColumns;
        }

        private static OneTabletMeta onlySelf(ColumnRefSet bucketColumns) {
            return new OneTabletMeta(true, false, bucketColumns);
        }

        private static OneTabletMeta both(ColumnRefSet bucketColumns) {
            return new OneTabletMeta(true, true, bucketColumns);
        }

        private static OneTabletMeta neither() {
            return new OneTabletMeta(false, false, null);
        }
    }

    static class OneTabletExecutorVisitor extends OperatorVisitor<OneTabletMeta, ExpressionContext> {
        @Override
        public OneTabletMeta visitOperator(Operator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() != 0);
            return context.isExecuteInOneTablet(0);
        }

        @Override
        public OneTabletMeta visitMockOperator(MockOperator node, ExpressionContext context) {
            return OneTabletMeta.both(new ColumnRefSet());
        }

        @Override
        public OneTabletMeta visitLogicalTableScan(LogicalScanOperator node, ExpressionContext context) {
            if (node instanceof LogicalOlapScanOperator) {
                if (((LogicalOlapScanOperator) node).getSelectedTabletId().size() <= 1) {
                    Set<String> distributionColumnNames = node.getTable().getDistributionColumnNames();
                    List<ColumnRefOperator> bucketColumns = Lists.newArrayList();
                    for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                        if (distributionColumnNames.contains(entry.getValue().getName())) {
                            bucketColumns.add(entry.getKey());
                        }
                    }
                    return OneTabletMeta.both(new ColumnRefSet(bucketColumns));
                }
                return OneTabletMeta.neither();
            } else if (node instanceof LogicalMysqlScanOperator || node instanceof LogicalJDBCScanOperator) {
                return OneTabletMeta.both(new ColumnRefSet());
            }
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalValues(LogicalValuesOperator node, ExpressionContext context) {
            return OneTabletMeta.both(new ColumnRefSet());
        }

        @Override
        public OneTabletMeta visitLogicalAnalytic(LogicalWindowOperator node, ExpressionContext context) {
            OneTabletMeta isExecuteInOneTablet = context.isExecuteInOneTablet(0);
            if (isExecuteInOneTablet.asChild) {
                List<Integer> partitionColumnRefSet = new ArrayList<>();
                node.getPartitionExpressions().forEach(e -> partitionColumnRefSet
                        .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));
                ColumnRefSet partitionColumns = ColumnRefSet.createByIds(partitionColumnRefSet);
                if (partitionColumns.isSame(isExecuteInOneTablet.bucketColumns)) {
                    return isExecuteInOneTablet;
                }
                return OneTabletMeta.onlySelf(isExecuteInOneTablet.bucketColumns);
            }
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalAggregation(LogicalAggregationOperator node,
                                                     ExpressionContext context) {
            OneTabletMeta isExecuteInOneTablet = context.isExecuteInOneTablet(0);
            if (isExecuteInOneTablet.asChild) {
                ColumnRefSet groupByColumns = new ColumnRefSet(node.getGroupingKeys());
                if (groupByColumns.isSame(isExecuteInOneTablet.bucketColumns)) {
                    return isExecuteInOneTablet;
                }
                return OneTabletMeta.onlySelf(isExecuteInOneTablet.bucketColumns);
            }
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalJoin(LogicalJoinOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalUnion(LogicalUnionOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalExcept(LogicalExceptOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalIntersect(LogicalIntersectOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalTableFunction(LogicalTableFunctionOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }

        @Override
        public OneTabletMeta visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            return context.isExecuteInOneTablet(1);
        }

        @Override
        public OneTabletMeta visitLogicalCTEConsume(LogicalCTEConsumeOperator node, ExpressionContext context) {
            return OneTabletMeta.neither();
        }
    }
}
