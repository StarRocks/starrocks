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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.OperatorType.LOGICAL_CTE_ANCHOR;
import static com.starrocks.sql.optimizer.operator.OperatorType.LOGICAL_CTE_CONSUME;
import static com.starrocks.sql.optimizer.operator.OperatorType.LOGICAL_CTE_PRODUCE;

public class LogicalProperty implements Property {
    // Operator's output columns
    private ColumnRefSet outputColumns;

    // The flag for execute upon less than or equal one tablet
    private OneTabletProperty oneTabletProperty;

    // save the used cte collection of this group
    private CTEProperty usedCTEs;

    public ColumnRefSet getOutputColumns() {
        return outputColumns;
    }

    public CTEProperty getUsedCTEs() {
        return usedCTEs;
    }

    public void setOutputColumns(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
    }

    public OneTabletProperty oneTabletProperty() {
        return oneTabletProperty;
    }

    public LogicalProperty() {
        this.outputColumns = new ColumnRefSet();
        this.usedCTEs = EmptyCTEProperty.INSTANCE;
    }

    public LogicalProperty(ColumnRefSet outputColumns) {
        this.outputColumns = outputColumns;
        this.usedCTEs = EmptyCTEProperty.INSTANCE;
    }

    public LogicalProperty(LogicalProperty other) {
        outputColumns = other.outputColumns.clone();
        oneTabletProperty = other.oneTabletProperty;
        usedCTEs = other.usedCTEs;
    }

    public void derive(ExpressionContext expressionContext) {
        LogicalOperator op = (LogicalOperator) expressionContext.getOp();
        outputColumns = op.getOutputColumns(expressionContext);
        oneTabletProperty = op.accept(new OneTabletExecutorVisitor(), expressionContext);
        if (expressionContext.isGroupExprContext()) {
            // only derived after entering memo
            deriveUsedCTEs(expressionContext);
        }
    }

    private void deriveUsedCTEs(ExpressionContext expressionContext) {
        OperatorType type = expressionContext.getOp().getOpType();
        Set<Integer> cteIds = Sets.newHashSet();

        if (type == LOGICAL_CTE_ANCHOR) {
            LogicalCTEAnchorOperator anchorOperator = (LogicalCTEAnchorOperator) expressionContext.getOp();
            cteIds.addAll(expressionContext.getChildLogicalProperty(0).getUsedCTEs().getCteIds());
            cteIds.addAll(expressionContext.getChildLogicalProperty(1).getUsedCTEs().getCteIds());
            cteIds.remove(anchorOperator.getCteId());
        } else if (type == LOGICAL_CTE_PRODUCE) {
            cteIds.addAll(expressionContext.getChildLogicalProperty(0).getUsedCTEs().getCteIds());
        } else if (type == LOGICAL_CTE_CONSUME) {
            LogicalCTEConsumeOperator consumeOperator = (LogicalCTEConsumeOperator) expressionContext.getOp();
            if (expressionContext.arity() > 0) {
                cteIds.addAll(expressionContext.getChildLogicalProperty(0).getUsedCTEs().getCteIds());
            }
            cteIds.add(consumeOperator.getCteId());
        } else {
            for (int i = 0; i < expressionContext.arity(); i++) {
                cteIds.addAll(expressionContext.getChildLogicalProperty(i).getUsedCTEs().getCteIds());
            }
        }

        usedCTEs = new CTEProperty(cteIds);
    }

    public static final class OneTabletProperty {
        public final boolean supportOneTabletOpt;
        public final boolean distributionIntact;
        public final ColumnRefSet bucketColumns;

        private OneTabletProperty(boolean supportOneTabletOpt, boolean distributionIntact,
                                  ColumnRefSet bucketColumns) {
            this.supportOneTabletOpt = supportOneTabletOpt;
            this.distributionIntact = distributionIntact;
            this.bucketColumns = bucketColumns;
        }

        private static OneTabletProperty supportButChangeDistribution(ColumnRefSet bucketColumns) {
            return new OneTabletProperty(true, false, bucketColumns);
        }

        private static OneTabletProperty supportWithoutChangeDistribution(ColumnRefSet bucketColumns) {
            return new OneTabletProperty(true, true, bucketColumns);
        }

        private static OneTabletProperty notSupport() {
            return new OneTabletProperty(false, false, null);
        }
    }

    static class OneTabletExecutorVisitor extends OperatorVisitor<OneTabletProperty, ExpressionContext> {
        @Override
        public OneTabletProperty visitOperator(Operator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() != 0);
            return context.oneTabletProperty(0);
        }

        @Override
        public OneTabletProperty visitMockOperator(MockOperator node, ExpressionContext context) {
            return OneTabletProperty.supportWithoutChangeDistribution(new ColumnRefSet());
        }

        @Override
        public OneTabletProperty visitLogicalTableScan(LogicalScanOperator node, ExpressionContext context) {
            if (node instanceof LogicalOlapScanOperator) {
                if (((LogicalOlapScanOperator) node).getSelectedTabletId().size() <= 1) {
                    Set<String> distributionColumnNames = node.getTable().getDistributionColumnNames();
                    List<ColumnRefOperator> bucketColumns = Lists.newArrayList();
                    for (Map.Entry<ColumnRefOperator, Column> entry : node.getColRefToColumnMetaMap().entrySet()) {
                        if (distributionColumnNames.contains(entry.getValue().getName())) {
                            bucketColumns.add(entry.getKey());
                        }
                    }
                    return OneTabletProperty.supportWithoutChangeDistribution(new ColumnRefSet(bucketColumns));
                }
                return OneTabletProperty.notSupport();
            } else if (node instanceof LogicalMysqlScanOperator || node instanceof LogicalJDBCScanOperator) {
                return OneTabletProperty.supportWithoutChangeDistribution(new ColumnRefSet());
            }
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalValues(LogicalValuesOperator node, ExpressionContext context) {
            return OneTabletProperty.supportWithoutChangeDistribution(new ColumnRefSet());
        }

        @Override
        public OneTabletProperty visitLogicalAnalytic(LogicalWindowOperator node, ExpressionContext context) {
            OneTabletProperty isExecuteInOneTablet = context.oneTabletProperty(0);
            if (isExecuteInOneTablet.distributionIntact) {
                List<Integer> partitionColumnRefSet = new ArrayList<>();
                node.getPartitionExpressions().forEach(e -> partitionColumnRefSet
                        .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));
                ColumnRefSet partitionColumns = ColumnRefSet.createByIds(partitionColumnRefSet);
                if (partitionColumns.isSame(isExecuteInOneTablet.bucketColumns)) {
                    return isExecuteInOneTablet;
                }
                return OneTabletProperty.supportButChangeDistribution(isExecuteInOneTablet.bucketColumns);
            }
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalAggregation(LogicalAggregationOperator node,
                                                         ExpressionContext context) {
            OneTabletProperty isExecuteInOneTablet = context.oneTabletProperty(0);
            if (isExecuteInOneTablet.distributionIntact) {
                ColumnRefSet groupByColumns = new ColumnRefSet(node.getGroupingKeys());
                if (groupByColumns.isSame(isExecuteInOneTablet.bucketColumns)) {
                    return isExecuteInOneTablet;
                }
                return OneTabletProperty.supportButChangeDistribution(isExecuteInOneTablet.bucketColumns);
            }
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalJoin(LogicalJoinOperator node, ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalUnion(LogicalUnionOperator node, ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalExcept(LogicalExceptOperator node, ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalIntersect(LogicalIntersectOperator node, ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalTableFunction(LogicalTableFunctionOperator node,
                                                           ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }

        @Override
        public OneTabletProperty visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            return context.oneTabletProperty(1);
        }

        @Override
        public OneTabletProperty visitLogicalCTEConsume(LogicalCTEConsumeOperator node, ExpressionContext context) {
            return OneTabletProperty.notSupport();
        }
    }
}
