// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/FEFunctions.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.rewrite;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * compute functions in FE.
 * <p>
 * when you add a new function, please ensure the name, argTypes , returnType and compute logic are consistent with BE's function
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
@Deprecated
public class FEFunctions {
    /**
     * Arithmetic function
     */

    @FEFunction(name = "add", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral addInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.addExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "add", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral addDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() + second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral addDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.add(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral addDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral addDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral addDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral addDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral addDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return addDecimalImpl(first, second);
    }

    @FEFunction(name = "add", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral addBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.add(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "subtract", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral subtractInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long result = Math.subtractExact(first.getLongValue(), second.getLongValue());
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "subtract", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral subtractDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() - second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral subtractDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.subtract(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral subtractDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral subtractDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral subtractDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral subtractDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral subtractDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return subtractDecimalImpl(first, second);
    }

    @FEFunction(name = "subtract", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral subtractBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.subtract(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "multiply", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral multiplyInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long left = first.getLongValue();
        long right = second.getLongValue();
        long result = Math.multiplyExact(left, right);
        return new IntLiteral(result, Type.BIGINT);
    }

    @FEFunction(name = "multiply", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral multiplyDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        double result = first.getDoubleValue() * second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral multiplyDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        BigDecimal result = left.multiply(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral multiplyDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral multiplyDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral multiplyDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral multiplyDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral multiplyDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return multiplyDecimalImpl(first, second);
    }

    @FEFunction(name = "multiply", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral multiplyBigInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        BigInteger result = left.multiply(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "divide", argTypes = {"DOUBLE", "DOUBLE"}, returnType = "DOUBLE")
    public static FloatLiteral divideDouble(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        if (second.getDoubleValue() == 0.0) {
            return null;
        }
        double result = first.getDoubleValue() / second.getDoubleValue();
        return new FloatLiteral(result, Type.DOUBLE);
    }

    private static DecimalLiteral divideDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        if (right.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = left.divide(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral divideDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral divideDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral divideDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral divideDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "divide", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral divideDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return divideDecimalImpl(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"BIGINT", "BIGINT"}, returnType = "BIGINT")
    public static IntLiteral modInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        long left = first.getLongValue();
        long right = second.getLongValue();
        if (right == 0) {
            return null;
        }
        long result = left % right;
        return new IntLiteral(result, Type.BIGINT);
    }

    private static DecimalLiteral modDecimalImpl(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigDecimal left = new BigDecimal(first.getStringValue());
        BigDecimal right = new BigDecimal(second.getStringValue());
        if (right.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }
        BigDecimal result = left.remainder(right);
        return new DecimalLiteral(result);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL", "DECIMAL"}, returnType = "DECIMAL")
    public static DecimalLiteral modDecimal(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimalImpl(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMALV2", "DECIMALV2"}, returnType = "DECIMALV2")
    public static DecimalLiteral modDecimalV2(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL32", "DECIMAL32"}, returnType = "DECIMAL32")
    public static DecimalLiteral modDecimal32(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL64", "DECIMAL64"}, returnType = "DECIMAL64")
    public static DecimalLiteral modDecimal64(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"DECIMAL128", "DECIMAL128"}, returnType = "DECIMAL128")
    public static DecimalLiteral modDecimal128(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        return modDecimal(first, second);
    }

    @FEFunction(name = "mod", argTypes = {"LARGEINT", "LARGEINT"}, returnType = "LARGEINT")
    public static LargeIntLiteral modLargeInt(LiteralExpr first, LiteralExpr second) throws AnalysisException {
        BigInteger left = new BigInteger(first.getStringValue());
        BigInteger right = new BigInteger(second.getStringValue());
        if (right.compareTo(BigInteger.ZERO) == 0) {
            return null;
        }
        BigInteger result = left.mod(right);
        return new LargeIntLiteral(result.toString());
    }

    @FEFunction(name = "concat", argTypes = {"VARCHAR"}, returnType = "VARCHAR")
    public static StringLiteral concat(StringLiteral... values) throws AnalysisException {
        Preconditions.checkArgument(values.length > 0);
        final StringBuilder resultBuilder = new StringBuilder();
        for (StringLiteral value : values) {
            resultBuilder.append(value.getStringValue());
        }
        return new StringLiteral(resultBuilder.toString());
    }
}
