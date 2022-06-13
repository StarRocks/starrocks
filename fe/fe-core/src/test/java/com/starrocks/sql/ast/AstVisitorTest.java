package com.starrocks.sql.ast;

import org.junit.Assert;
import org.junit.Test;



public class AstVisitorTest {
    
    @Test
    public void testVisitModifyFrontendHostClause() {
        AstVisitor<String, String> visitor = new AstVisitor<String, String>() {};
        Object ret = visitor.visitModifyFrontendHostClause(null, null);
        Assert.assertNull(ret);
    }

    @Test
    public void testVisitModifyBackendHostClause() {
        AstVisitor<String, String> visitor = new AstVisitor<String, String>() {};
        Object ret = visitor.visitModifyBackendHostClause(null, null);
        Assert.assertNull(ret);
    }
}
