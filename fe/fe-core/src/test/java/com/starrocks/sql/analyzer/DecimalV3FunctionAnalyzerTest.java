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


package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

public class DecimalV3FunctionAnalyzerTest {

    @Test
    public void testGetFnOfTruncateForDouble() throws AnalysisException {
        List<Expr> params = Lists.newArrayList();
        params.add(new FloatLiteral(18450.76));
        params.add(new IntLiteral(1));
        FunctionCallExpr node = new FunctionCallExpr(FunctionSet.TRUNCATE, params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.DOUBLE);
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction(FunctionSet.TRUNCATE, paramTypes.toArray(new Type[0]),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertNotNull(function);
        Function newFn = DecimalV3FunctionAnalyzer.getFunctionOfRound(node, function, paramTypes);
        Type returnType = newFn.getReturnType();
        Assert.assertTrue(returnType.isDouble());
    }

    @Test
    public void testGetFnOfTruncateForDecimalAndIntLiteral() {
        List<Expr> params = Lists.newArrayList();
        params.add(new DecimalLiteral(new BigDecimal(new BigInteger("1845076"), 2)));
        params.add(new IntLiteral(1));
        FunctionCallExpr node = new FunctionCallExpr(FunctionSet.TRUNCATE, params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction(FunctionSet.TRUNCATE, paramTypes.toArray(new Type[0]),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertNotNull(function);
        Function newFn = DecimalV3FunctionAnalyzer.getFunctionOfRound(node, function, paramTypes);
        Type returnType = newFn.getReturnType();
        Assert.assertTrue(returnType.isDecimalV3());
        Assert.assertEquals(Integer.valueOf(38), returnType.getPrecision());
    }

    @Test
    public void testGetFnOfTruncateForDecimalAndSlotRef() {
        List<Expr> params = Lists.newArrayList();
        params.add(new DecimalLiteral(new BigDecimal(new BigInteger("1845076"), 2)));
        TableName tableName = new TableName("db", "table");
        SlotRef slotRef = new SlotRef(tableName, "v1");
        params.add(slotRef);
        FunctionCallExpr node = new FunctionCallExpr(FunctionSet.TRUNCATE, params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction(FunctionSet.TRUNCATE, paramTypes.toArray(new Type[0]),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertNotNull(function);
        Function newFn = DecimalV3FunctionAnalyzer.getFunctionOfRound(node, function, paramTypes);
        Type returnType = newFn.getReturnType();
        Assert.assertTrue(returnType.isDecimalV3());
        Assert.assertEquals(Integer.valueOf(38), returnType.getPrecision());
    }

    @Test
    public void testGetFnOfTruncateForDecimalAndConstantExpression() {
        List<Expr> params = Lists.newArrayList();
        params.add(new DecimalLiteral(new BigDecimal(new BigInteger("1845076"), 2)));
        params.add(new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new IntLiteral(1), new IntLiteral(1)));
        FunctionCallExpr node = new FunctionCallExpr(FunctionSet.TRUNCATE, params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction(FunctionSet.TRUNCATE, paramTypes.toArray(new Type[0]),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertNotNull(function);
        Function newFn = DecimalV3FunctionAnalyzer.getFunctionOfRound(node, function, paramTypes);
        Type returnType = newFn.getReturnType();
        Assert.assertTrue(returnType.isDouble());
    }

    /**
     * Required return type is double, but the function return type is decimal
     */
    @Test
    public void testDecimalStd() {
        List<Expr> params = Lists.newArrayList();
        params.add(new DecimalLiteral(new BigDecimal(new BigInteger("1845076"), 2)));

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.DECIMAL128);

        List<String> stdFunctions =
                Arrays.asList(FunctionSet.STD, FunctionSet.STDDEV, FunctionSet.VARIANCE, FunctionSet.VARIANCE_POP,
                        FunctionSet.VAR_POP, FunctionSet.VARIANCE_SAMP, FunctionSet.VAR_SAMP, FunctionSet.STDDEV_POP,
                        FunctionSet.STDDEV_SAMP);
        for (String funcName : stdFunctions) {
            AggregateFunction function =
                    (AggregateFunction) Expr.getBuiltinFunction(funcName, paramTypes.toArray(new Type[0]),
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Assert.assertNotNull(function);
            Type argType = ScalarType.createWildcardDecimalV3Type(PrimitiveType.DECIMAL128);
            Type retType = Type.DOUBLE;
            AggregateFunction aggFunc =
                    DecimalV3FunctionAnalyzer.rectifyAggregationFunction(function, argType, retType);
            Type returnType = aggFunc.getReturnType();
            Assert.assertTrue(returnType.isDecimalV3());
        }
    }
}
