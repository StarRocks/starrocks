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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ReplaceColumnRefRewriterTest {
    @Test
    public void testRecursiveWithoutChildren() {
        Map<ColumnRefOperator, ScalarOperator> operatorMap = Maps.newHashMap();
        ColumnRefOperator columnRef1 = createColumnRef(1);
        ColumnRefOperator columnRef2 = createColumnRef(2);
        ColumnRefOperator columnRef3 = createColumnRef(3);
        operatorMap.put(columnRef1, columnRef2);
        operatorMap.put(columnRef2, columnRef3);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap, true);
        ColumnRefOperator source = createColumnRef(1);
        ScalarOperator target = rewriter.rewrite(source);
        Assertions.assertTrue(target instanceof ColumnRefOperator);
        ColumnRefOperator rewritten = (ColumnRefOperator) target;
        Assertions.assertEquals(3, rewritten.getId());
    }

    @Test
    public void testRecursiveWithChildren() {
        Map<ColumnRefOperator, ScalarOperator> operatorMap = Maps.newHashMap();
        ColumnRefOperator columnRef1 = createColumnRef(1);
        ColumnRefOperator columnRef2 = createColumnRef(2);
        ColumnRefOperator columnRef3 = createColumnRef(3);

        BinaryPredicateOperator binary = new BinaryPredicateOperator(BinaryType.EQ, columnRef2,
                ConstantOperator.createInt(1));

        operatorMap.put(columnRef1, binary);
        operatorMap.put(columnRef2, columnRef3);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap, true);
        ColumnRefOperator source = createColumnRef(1);
        ScalarOperator target = rewriter.rewrite(source);
        Assertions.assertTrue(target instanceof BinaryPredicateOperator);
        BinaryPredicateOperator rewritten = (BinaryPredicateOperator) target;

        BinaryPredicateOperator result = new BinaryPredicateOperator(BinaryType.EQ, columnRef3,
                ConstantOperator.createInt(1));
        Assertions.assertEquals(result, rewritten);

        Map<ColumnRefOperator, ScalarOperator> operatorMap2 = Maps.newHashMap();
        operatorMap.put(columnRef1, columnRef1);
        ReplaceColumnRefRewriter rewriter2 = new ReplaceColumnRefRewriter(operatorMap2, true);
        ScalarOperator result2 = rewriter2.rewrite(columnRef1);
        Assertions.assertEquals(columnRef1, result2);
    }

    @Test
    public void testLambdaFunctionWithRecursiveRewrite() {
        // Reproduces StackOverflowError when lambda parameter is in operatorMap with isRecursively=true
        // Scenario: array_map(x -> named_struct('my_field', x.my_field), ...)
        ColumnRefOperator lambdaParam = createColumnRef(100, "x");
        SubfieldOperator subfield = new SubfieldOperator(lambdaParam, VarcharType.VARCHAR,
                Lists.newArrayList("my_field"));

        StructType structType = new StructType(Lists.newArrayList(
                new StructField("my_field", VarcharType.VARCHAR)
        ));
        CallOperator namedStruct = new CallOperator(
                FunctionSet.NAMED_STRUCT,
                structType,
                Lists.newArrayList(
                        ConstantOperator.createVarchar("my_field"),
                        subfield
                )
        );

        LambdaFunctionOperator lambda = new LambdaFunctionOperator(
                Lists.newArrayList(lambdaParam),
                namedStruct,
                ArrayType.ARRAY_VARCHAR);

        // Map lambda parameter to subfield expression containing the lambda param
        Map<ColumnRefOperator, ScalarOperator> operatorMap = Maps.newHashMap();
        operatorMap.put(lambdaParam, subfield);

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(operatorMap, true);

        try {
            ScalarOperator rewritten = rewriter.rewrite(lambda);

            // Verify lambda parameter is NOT replaced
            Assertions.assertInstanceOf(LambdaFunctionOperator.class, rewritten);
            LambdaFunctionOperator rewrittenLambda = (LambdaFunctionOperator) rewritten;
            Assertions.assertEquals(1, rewrittenLambda.getRefColumns().size());
            Assertions.assertEquals(lambdaParam.getId(), rewrittenLambda.getRefColumns().get(0).getId());

            ScalarOperator rewrittenExpr = rewrittenLambda.getLambdaExpr();
            Assertions.assertInstanceOf(CallOperator.class, rewrittenExpr);
            CallOperator rewrittenNamedStruct = (CallOperator) rewrittenExpr;
            ScalarOperator secondArg = rewrittenNamedStruct.getChild(1);
            Assertions.assertInstanceOf(SubfieldOperator.class, secondArg);
        } catch (StackOverflowError e) {
            Assertions.fail("StackOverflowError occurred - lambda parameter was incorrectly replaced");
        }
    }

    ColumnRefOperator createColumnRef(int id) {
        return new ColumnRefOperator(id, IntegerType.INT, "ref" + id, false);
    }

    ColumnRefOperator createColumnRef(int id, String name) {
        return new ColumnRefOperator(id, IntegerType.INT, name, false);
    }
}
