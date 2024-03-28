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
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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
    public void testTranslateIfsFunction() {
        StringLiteral column = new StringLiteral("c1");
        StringLiteral firstWhen = new StringLiteral("str1");
        IntLiteral firstThen = new IntLiteral(1);
        BinaryPredicate firstPredicate = new BinaryPredicate(BinaryType.EQ, column, firstWhen);
        StringLiteral secondWhen = new StringLiteral("str2");
        IntLiteral secondThen = new IntLiteral(2);
        BinaryPredicate secondPredicate = new BinaryPredicate(BinaryType.EQ, column, secondWhen);
        IntLiteral elseResult = new IntLiteral(0);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr(FunctionSet.IFS,
                ImmutableList.of(firstPredicate, firstThen, secondPredicate, secondThen, elseResult));
        CallOperator co = (CallOperator) SqlToScalarOperatorTranslator.translate(functionCallExpr);
        assertEquals("CaseWhen", co.getFnName());
    }
}
