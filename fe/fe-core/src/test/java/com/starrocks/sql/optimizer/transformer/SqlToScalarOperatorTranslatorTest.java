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


package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.SystemFunctionCallExpr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SqlToScalarOperatorTranslatorTest {

    @Test
    public void testTranslateConstant() {
        DateLiteral literal = new DateLiteral(2000, 12, 1);
        ScalarOperator so =
                SqlToScalarOperatorTranslator.translate(literal, new ExpressionMapping(null, Collections.emptyList()),
                        new ColumnRefFactory());

        assertEquals(OperatorType.CONSTANT, so.getOpType());
        assertEquals(LocalDateTime.of(2000, 12, 1, 0, 0), ((ConstantOperator) so).getDate());
    }

    @Test
    public void testTranslateComplexFunction() {
        StringLiteral test = new StringLiteral("test");
        StringLiteral defaultStr = new StringLiteral("default");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, defaultStr, test);
        FunctionCallExpr baseFunc = new FunctionCallExpr("if", ImmutableList.of(predicate, test, defaultStr));
        FunctionCallExpr complexFunc = baseFunc;
        for (int i = 0; i < 100; i++) {
            complexFunc = new FunctionCallExpr("if",  ImmutableList.of(predicate, test, complexFunc));
        }
        CallOperator so = (CallOperator) SqlToScalarOperatorTranslator.translate(complexFunc,
                new ExpressionMapping(null, Collections.emptyList()), new ColumnRefFactory());
        assertEquals("if", so.getFnName());
    }

    @Test
    public void testTranslateSystemFunction() {
        FunctionName fnName = FunctionName.createFnName("system$cbo_stats_add_exclusion");
        StringLiteral test = new StringLiteral("catalog.db.table");
        SystemFunctionCallExpr systemFunctionCallExpr = new SystemFunctionCallExpr(fnName,
                new FunctionParams(false, ImmutableList.of(test)), NodePosition.ZERO);

        CallOperator call = (CallOperator) SqlToScalarOperatorTranslator.translate(systemFunctionCallExpr,
                new ExpressionMapping(null, Collections.emptyList()), new ColumnRefFactory());
        assertEquals("system$cbo_stats_add_exclusion", call.getFnName());
        assertEquals(OperatorType.CONSTANT, call.getChild(0).getOpType());
    }
}
