// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;


import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class AlterSystemStmtAnalyzerTest {

    @Mocked
    InetAddress addr1;

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr1;
            }
        };
    }

    @Test
    public void testVisitModifyBackendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test", "fqdn");
        Void resutl = visitor.visitModifyBackendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test
    public void testVisitModifyFrontendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test", "fqdn");
        Void resutl = visitor.visitModifyFrontendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendHostClauseException() {
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyBackendHostClause(clause, null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyFrontendHostClauseException() {
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyFrontendHostClause(clause, null);
    }
}
