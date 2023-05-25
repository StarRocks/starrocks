// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Before:
//      ApplyNode
//      /      \
//  LEFT     TABLE-FUNCTION
//               \
//               ....
//
// After:
//    TABLE-FUNCTION
//         |
//        LEFT

public class MergeApplyWithTableFunction extends TransformationRule {
    public MergeApplyWithTableFunction() {
        super(RuleType.TF_MERGE_APPLY_WITH_TABLE_FUNCTION,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF,
                        OperatorType.LOGICAL_TABLE_FUNCTION));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        OptExpression expression = input.inputAt(1);
        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) expression.getOp();

        OptExpression childOptExpression = input.inputAt(0);

        Map<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        for (Pair<ColumnRefOperator, ScalarOperator> pair : tableFunctionOperator.getFnParamColumnProject()) {
            projectMap.put(pair.first, pair.second);
        }
        if (!projectMap.values().stream().allMatch(ScalarOperator::isColumnRef)) {
            for (int columnId : childOptExpression.getOutputColumns().getColumnIds()) {
                ColumnRefOperator columnRefOperator = context.getColumnRefFactory().getColumnRef(columnId);
                projectMap.put(columnRefOperator, columnRefOperator);
            }

            LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
            childOptExpression = OptExpression.create(projectOperator, input.inputAt(0));
        }

        LogicalTableFunctionOperator newTableFunctionOperator =
                new LogicalTableFunctionOperator(tableFunctionOperator.getFnResultColumnRefSet(),
                        tableFunctionOperator.getFn(), tableFunctionOperator.getFnParamColumnProject(),
                        input.inputAt(0).getOutputColumns());
        newTableFunctionOperator.setLimit(tableFunctionOperator.getLimit());

        return Lists.newArrayList(OptExpression.create(newTableFunctionOperator, childOptExpression));
    }
}