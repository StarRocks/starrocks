package com.starrocks.sql.analyzer;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

public class DecimalV3FunctionAnalyzerTest {

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
                Arrays.asList("std", "stddev", "variance", "variance_pop", "var_pop", "variance_samp", "var_samp", "stddev_pop",
                        "stddev_samp");
        for (String funcName : stdFunctions) {
            AggregateFunction function = (AggregateFunction) Expr.getBuiltinFunction(funcName, paramTypes.toArray(new Type[0]),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            Assert.assertNotNull(function);
            Type argType = ScalarType.createWildcardDecimalV3Type(PrimitiveType.DECIMAL128);
            Type retType = Type.DOUBLE;
            AggregateFunction aggFunc = DecimalV3FunctionAnalyzer.rectifyAggregationFunction(function, argType, retType);
            Type returnType = aggFunc.getReturnType();
            Assert.assertTrue(returnType.isDecimalV3());
        }
    }
}
