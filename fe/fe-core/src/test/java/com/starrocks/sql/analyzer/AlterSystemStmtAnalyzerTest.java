// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor;

import org.junit.Assert;
import org.junit.Test;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

public class AlterSystemStmtAnalyzerTest {

    @Mocked 
    InetAddress addr1;

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendHostClauseAnalysisException() {
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test","fqdn");
        visitor.visitModifyBackendHostClause(clause, null);
    }

    @Test
    public void testVisitModifyBackendHostClause() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr1;
            }
        };
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test:1000","fqdn");
        Void resutl = visitor.visitModifyBackendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyFrontendHostClauseAnalysisException() {
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test","fqdn");
        visitor.visitModifyFrontendHostClause(clause, null);
    }

    @Test
    public void testVisitModifyFrontendHostClause() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr1;
            }
        };
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test:1000","fqdn");
        Void resutl = visitor.visitModifyFrontendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }
}
