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

package com.starrocks.sql.optimizer.rule;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AIFunctionIdentityTest {
    @Test
    public void testResolvedNonAIFunctionDoesNotInheritAINameSemantics() {
        Function fn = new Function(new FunctionName("ai_complete"), List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        CallOperator call = new CallOperator("ai_complete", VarcharType.VARCHAR,
                List.of(ConstantOperator.createVarchar("text")), fn);
        assertTrue(call.isConstant());
        assertFalse(Utils.hasNonDeterministicFunc(call));
        FunctionCallExpr expression = new FunctionCallExpr("ai_complete", List.of(new StringLiteral("text")));
        expression.setFn(fn);
        expression.setType(VarcharType.VARCHAR);
        assertFalse(expression.isNondeterministicBuiltinFnName());
        assertEquals(1, SqlToScalarOperatorTranslator.translate(expression).getChildren().size());
    }
    @Test
    public void testSameNameUdfExecutionKeepsDeclaredArity() {
        Function fn = new Function(new FunctionName("ai_decide"), List.of(VarcharType.VARCHAR), VarcharType.VARCHAR, false);
        fn.setBinaryType(TFunctionBinaryType.SRJAR);
        FunctionCallExpr expression = new FunctionCallExpr("ai_decide", List.of(new StringLiteral("text")));
        expression.setFn(fn);
        expression.setType(VarcharType.VARCHAR);
        Expr execution = ScalarOperatorToExpr.buildExprIgnoreSlot(SqlToScalarOperatorTranslator.translate(expression),
                new ScalarOperatorToExpr.FormatterContext(new HashMap<>()));
        assertEquals(1, execution.getChildren().size());
    }

}
