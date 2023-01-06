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


import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AlterSystemStmtAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

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

    @Test
    public void testAnalyzeCancelAlterSystem() {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) analyzeSuccess(
                "CANCEL DECOMMISSION BACKEND \"127.0.0.1:8080\", \"127.0.0.2:8080\"");
        Assert.assertEquals("[127.0.0.1:8080, 127.0.0.2:8080]", cancelAlterSystemStmt.getHostPortPairs().toString());
    }
}
