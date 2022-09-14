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
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test","fqdn");
        Void resutl = visitor.visitModifyBackendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test
    public void testVisitModifyFrontendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test","fqdn");
        Void resutl = visitor.visitModifyFrontendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendHostClauseException() {
<<<<<<< HEAD
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test","127.0.0.1");
=======
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.2", "127.0.0.1");
>>>>>>> f4a3fff8e ([Enhancement] Enables SR cluster can support IP and FQDN conversion (#11018))
        visitor.visitModifyBackendHostClause(clause, null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyFrontendHostClauseException() {
<<<<<<< HEAD
        AlterSystemStmtAnalyzerVisitor visitor = new AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test","127.0.0.1");
=======
        AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor visitor =
                new AlterSystemStmtAnalyzer.AlterSystemStmtAnalyzerVisitor();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.2", "127.0.0.1");
>>>>>>> f4a3fff8e ([Enhancement] Enables SR cluster can support IP and FQDN conversion (#11018))
        visitor.visitModifyFrontendHostClause(clause, null);
    }
}
