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

package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaterializedColumnRule extends Rule {
    private final List<LogicalOlapScanOperator> scanOperators = Lists.newArrayList();
    private ColumnRefFactory factory;

    public MaterializedColumnRule() {
        super(RuleType.TF_MATERIALIZED_COLUMN,  Pattern.create(OperatorType.LOGICAL_PROJECT).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    private boolean matches(Expr materializedExpr, ScalarOperator operator) {
        if (materializedExpr instanceof ArithmeticExpr && operator instanceof CallOperator) {
            ArithmeticExpr arithmeticExpr = (ArithmeticExpr) materializedExpr;
            CallOperator callOp = (CallOperator) operator;
            if (arithmeticExpr.getOp().getName() != callOp.getFnName()) {
                return false;
            }
            if (arithmeticExpr.getChildren().size() != operator.getChildren().size()) {
                return false;
            }
            for (int i = 0; i < arithmeticExpr.getChildren().size(); ++i) {
                if (!matches(arithmeticExpr.getChild(i), operator.getChild(i))) {
                    return false;
                }
            }
            return true;
        }

        if (operator instanceof CastOperator) {
            return matches(materializedExpr, operator.getChild(0));
        }

        if (materializedExpr instanceof SlotRef && operator instanceof ColumnRefOperator) {
            SlotRef slot = (SlotRef) materializedExpr;
            ColumnRefOperator columnRef = (ColumnRefOperator) operator;
            if (columnRef.getName().equals(slot.getColumnName())) {
                return true;
            }
        }

        if (materializedExpr instanceof LiteralExpr && operator instanceof ConstantOperator) {
            LiteralExpr literalExpr = (LiteralExpr) materializedExpr;
            ConstantOperator constOp = (ConstantOperator) operator;
            Object v1 = constOp.getValue();
            try {
                LiteralExpr castedExpr = (LiteralExpr) literalExpr.castTo(operator.getType());
                Object v2 = castedExpr.getRealObjectValue();
                if (v1.equals(v2)) {
                    return true;
                }
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        this.factory = context.getColumnRefFactory();
        OptExpression optExpression = input;
        if (!hasMaterializedColumn(input)) {
            return Lists.newArrayList(optExpression);
        }
        init(input);

        LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) optExpression.getInputs().get(0).getOp();
        OlapTable table = (OlapTable) scanOperator.getTable();
        Map<ColumnRefOperator, ScalarOperator> newProjectColumnRefMap = new HashMap<>();
        Map<ColumnRefOperator, Column> newScanColumnRefScan = new HashMap<>(scanOperator.getColRefToColumnMetaMap());

        List<Column> materializedColumns = table.getMaterializedColumns();
        boolean hasMaterializedColumns = false;
        for (ColumnRefOperator column : project.getColumnRefMap().keySet()) {
            ScalarOperator val = project.getColumnRefMap().get(column);
            Column foundMetaColumn = null;
            if (val instanceof CallOperator) {
                CallOperator callOp = (CallOperator) val;

                for (Column col : materializedColumns) {
                    if (matches(col.materializedColumnExpr(), callOp)) {
                        newScanColumnRefScan.put(scanOperator.getColumnMetaToColRefMap().get(col), col);
                        hasMaterializedColumns = true;
                        foundMetaColumn = col;
                        break;

                    }
                }

            }
            if (foundMetaColumn != null) {
                newProjectColumnRefMap.put(column, scanOperator.getColumnMetaToColRefMap().get(foundMetaColumn));
            } else {
                newProjectColumnRefMap.put(column, val);
            }
        }

        if (hasMaterializedColumns) {
            LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                    scanOperator.getTable(),
                    newScanColumnRefScan,
                    scanOperator.getColumnMetaToColRefMap(),
                    scanOperator.getDistributionSpec(),
                    scanOperator.getLimit(),
                    scanOperator.getPredicate(),
                    scanOperator.getSelectedIndexId(),
                    scanOperator.getSelectedPartitionId(),
                    scanOperator.getPartitionNames(),
                    scanOperator.getSelectedTabletId(),
                    scanOperator.getHintsTabletIds());

            LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(newProjectColumnRefMap, project.getLimit());
            return Lists.newArrayList(OptExpression.create(newProjectOperator, new OptExpression(newScanOperator)));
        }
        return Lists.newArrayList(optExpression);
    }


    private void init(OptExpression root) {
        collectTableScans(root);
    }

    private void collectTableScans(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            collectTableScans(child);
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalProjectOperator) {
            Operator childOperator = root.getInputs().get(0).getOp();
            if (!(childOperator instanceof LogicalOlapScanOperator)) {
                return;
            }
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) childOperator;
            if (((OlapTable) scanOperator.getTable()).hasMaterializedColumn()) {
                scanOperators.add(scanOperator);
            }

        }


    }

    public static boolean hasMaterializedColumn(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            if (hasMaterializedColumn(child)) {
                return true;
            }
        }

        Operator operator = root.getOp();
        if (operator instanceof LogicalOlapScanOperator) {
            LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) operator;
            OlapTable olapTable = (OlapTable) scanOperator.getTable();
            return olapTable.hasMaterializedColumn();
        }
        return false;
    }

}
