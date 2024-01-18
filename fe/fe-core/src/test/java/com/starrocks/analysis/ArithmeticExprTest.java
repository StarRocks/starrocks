package com.starrocks.analysis;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ArithmeticExprTest {
    @Test
    public void testDecimal32Add() throws IOException {
        UtFrameUtils.createDefaultCtx();
        Expr lhsExpr = new SlotRef(new TableName("foo_db", "bar_table"), "c0");
        Expr rhsExpr = new SlotRef(new TableName("foo_db", "bar_table"), "c1");
        ScalarType decimal32p9s2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2);
        lhsExpr.setType(decimal32p9s2);
        rhsExpr.setType(decimal32p9s2);
        ArithmeticExpr addExpr = new ArithmeticExpr(
                ArithmeticExpr.Operator.ADD, lhsExpr, rhsExpr);
        ScalarType decimal64p10s2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 2);
        ExpressionAnalyzer.analyzeExpressionIgnoreSlot(addExpr, ConnectContext.get());
        Assert.assertEquals(addExpr.type, decimal64p10s2);
        Assert.assertNotNull(addExpr.getFn());
        Assert.assertEquals(addExpr.getFn().getArgs()[0], decimal64p10s2);
        Assert.assertEquals(addExpr.getFn().getArgs()[1], decimal64p10s2);
    }

    private ScalarType dec(int bits, int precision, int scale) {

        PrimitiveType pType = PrimitiveType.INVALID_TYPE;
        switch (bits) {
            case 32:
                pType = PrimitiveType.DECIMAL32;
                break;
            case 64:
                pType = PrimitiveType.DECIMAL64;
                break;
            case 128:
                pType = PrimitiveType.DECIMAL128;
                break;
        }
        return ScalarType.createDecimalV3Type(pType, precision, scale);
    }

    @Test
    public void testDecimalMultiply() throws AnalysisException {
        Object[][] cases = new Object[][] {
                {
                        dec(32, 4, 3),
                        dec(32, 4, 3),
                        dec(32, 8, 6),
                        dec(32, 4, 3),
                        dec(32, 4, 3),
                },
                {
                        dec(64, 7, 2),
                        dec(64, 7, 2),
                        dec(64, 14, 4),
                        dec(64, 7, 2),
                        dec(64, 7, 2),
                },
                {
                        dec(32, 7, 2),
                        dec(32, 9, 4),
                        dec(64, 16, 6),
                        dec(64, 7, 2),
                        dec(64, 9, 4),
                },
                {
                        dec(64, 14, 4),
                        dec(32, 7, 2),
                        dec(128, 21, 6),
                        dec(128, 14, 4),
                        dec(128, 7, 2),
                },
                {
                        dec(64, 14, 4),
                        dec(64, 18, 14),
                        dec(128, 32, 18),
                        dec(128, 14, 4),
                        dec(128, 18, 14),
                },
                {
                        dec(128, 35, 18),
                        dec(128, 35, 20),
                        dec(128, 38, 38),
                        dec(128, 35, 18),
                        dec(128, 35, 20),
                },
                {
                        dec(128, 35, 30),
                        dec(32, 8, 7),
                        dec(128, 38, 37),
                        dec(128, 35, 30),
                        dec(128, 8, 7),
                },
                {
                        dec(128, 36, 31),
                        dec(64, 18, 7),
                        dec(128, 38, 38),
                        dec(128, 36, 31),
                        dec(128, 18, 7),
                }
        };
        for (Object[] c : cases) {
            ScalarType lhsType = (ScalarType) c[0];
            ScalarType rhsType = (ScalarType) c[1];
            ScalarType expectReturnType = (ScalarType) c[2];
            ScalarType expectLhsType = (ScalarType) c[3];
            ScalarType expectRhsType = (ScalarType) c[4];
            ArithmeticExpr.TypeTriple tr =
                    ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.MULTIPLY, lhsType, rhsType);
            Assert.assertEquals(tr.returnType, expectReturnType);
            Assert.assertEquals(tr.lhsTargetType, expectLhsType);
            Assert.assertEquals(tr.rhsTargetType, expectRhsType);
        }
    }

    @Test
    public void testDecimalMultiplyFail() {
        Object[][] cases = new Object[][] {
                {
                        dec(128, 38, 36),
                        dec(32, 4, 3),
                },
                {
                        dec(128, 37, 24),
                        dec(64, 18, 15),
                },
                {
                        dec(128, 30, 19),
                        dec(128, 30, 20),
                },
        };
        for (Object[] c : cases) {
            ScalarType lhsType = (ScalarType) c[0];
            ScalarType rhsType = (ScalarType) c[1];
            try {
                ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.MULTIPLY, lhsType, rhsType);
                Assert.fail("should throw exception");
            } catch (SemanticException ignored) {
            }
        }
    }
}
