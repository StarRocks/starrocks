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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class RewriteUnnestBitmapRule extends TransformationRule {
    private static final RewriteUnnestBitmapRule INSTANCE = new RewriteUnnestBitmapRule();

    private RewriteUnnestBitmapRule() {
        super(RuleType.TF_REWRITE_UNNEST_BITMAP_RULE, Pattern.create(OperatorType.LOGICAL_TABLE_FUNCTION)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)));
    }

    public static RewriteUnnestBitmapRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableRewriteUnnestBitmapToArray()) {
            return false;
        }

        LogicalTableFunctionOperator tableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        if (tableFunctionOperator.getFn() != null &&
                !tableFunctionOperator.getFn().functionName().equals(FunctionSet.UNNEST)) {
            return false;
        }

        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.inputAt(0).getOp();

        boolean existBitmapToArray = projectOperator.getColumnRefMap().values().stream().filter(scalarOperator -> {
            if (scalarOperator instanceof CallOperator) {
                CallOperator callOperator = (CallOperator) scalarOperator;
                return callOperator.getFnName().equals(FunctionSet.BITMAP_TO_ARRAY);
            }
            return false;
        }).count() == 1;

        return existBitmapToArray;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTableFunctionOperator originalTableFunctionOperator = (LogicalTableFunctionOperator) input.getOp();
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) input.inputAt(0).getOp();

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOperator.getColumnRefMap();
        Pair<ColumnRefOperator, ScalarOperator> bitmapToArray = columnRefMap.entrySet().stream().filter(entry -> {
            if (entry.getValue() instanceof CallOperator) {
                CallOperator callOperator = (CallOperator) entry.getValue();
                return callOperator.getFnName().equals(FunctionSet.BITMAP_TO_ARRAY);
            }
            return false;
        }).map(entry -> new Pair<>(entry.getKey(), entry.getValue())).findFirst().get();

        // if bitmap_to_array's output are used by upper nodes, it's not safe to rewrite
        if (originalTableFunctionOperator.getOuterColRefs().contains(bitmapToArray.first)) {
            return Lists.newArrayList();
        }

        Preconditions.checkArgument(bitmapToArray.second.getChildren().size() == 1);
        // use bitmapToArrayArg to replace bitmapColumn
        // eg:
        //  with r1 as (select b1 as b2 from test_agg),
        //       r2 as (select sub_bitmap(b1, 0, 10) as b2 from test_agg),
        //       r3 as (select bitmap_and(t0.b2, t1.b2) as b2 from r1 t0 join r2 t1)
        //  select unnest as r1 from test_agg, unnest(bitmap_to_array(b2)) order by r1;
        //  use bitmap_and(t0.b2, t1.b2) as b2 to replace bitmap_to_array(b2)
        final ScalarOperator bitmapToArrayArg = bitmapToArray.second.getChild(0);
        final ColumnRefOperator bitmapColumn = bitmapToArray.first;
        columnRefMap.put(bitmapColumn, bitmapToArrayArg);

        TableFunction unnestBitmapFn =
                (TableFunction) Expr.getBuiltinFunction(FunctionSet.UNNEST_BITMAP, new Type[] {Type.BITMAP},
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        List<Pair<ColumnRefOperator, ScalarOperator>> fnParamColumnProject =
                Lists.newArrayList(Pair.create(bitmapColumn, bitmapColumn));

        LogicalTableFunctionOperator newTableFunctionOperator =
                new LogicalTableFunctionOperator(originalTableFunctionOperator.getFnResultColRefs(), unnestBitmapFn,
                        fnParamColumnProject, originalTableFunctionOperator.getOuterColRefs());

        return Lists.newArrayList(OptExpression.create(newTableFunctionOperator, input.inputAt(0)));
    }
}
