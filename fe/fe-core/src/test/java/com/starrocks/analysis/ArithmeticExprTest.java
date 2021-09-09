package com.starrocks.analysis;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
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
        try {
            ScalarType decimal64p18s2 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 2);
            addExpr.analyzeImpl(null);
            Assert.assertEquals(addExpr.type, decimal64p18s2);
            Assert.assertTrue(addExpr.getChild(0) instanceof CastExpr);
            Assert.assertTrue(addExpr.getChild(1) instanceof CastExpr);
            Assert.assertEquals(addExpr.getChild(0).type, decimal64p18s2);
            Assert.assertEquals(addExpr.getChild(1).type, decimal64p18s2);
        } catch (AnalysisException e) {
            Assert.fail("Should not throw exception");
        }

    }
}
