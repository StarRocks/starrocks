package com.starrocks.sql.analyzer;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class DecimalV3FunctionAnalyzerTest {

    @Test
    public void testGetFnOfTruncateForDouble() {
        List<Expr> params = Lists.newArrayList();
        params.add(new FloatLiteral(18450.76));
        params.add(new IntLiteral(1));
        FunctionCallExpr node = new FunctionCallExpr("truncate", params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.DOUBLE);
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction("truncate", paramTypes.toArray(new Type[0]),
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
        FunctionCallExpr node = new FunctionCallExpr("truncate", params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction("truncate", paramTypes.toArray(new Type[0]),
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
        FunctionCallExpr node = new FunctionCallExpr("truncate", params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction("truncate", paramTypes.toArray(new Type[0]),
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
        FunctionCallExpr node = new FunctionCallExpr("truncate", params);

        List<Type> paramTypes = Lists.newArrayList();
        paramTypes.add(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 7, 2));
        paramTypes.add(Type.TINYINT);

        Function function = Expr.getBuiltinFunction("truncate", paramTypes.toArray(new Type[0]),
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assert.assertNotNull(function);
        Function newFn = DecimalV3FunctionAnalyzer.getFunctionOfRound(node, function, paramTypes);
        Type returnType = newFn.getReturnType();
        Assert.assertTrue(returnType.isDouble());
    }
}
