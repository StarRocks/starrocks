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

import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AlterSystemStmtAnalyzerTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        AnalyzeTestUtil.init();
    }

    @Mocked
    InetAddress addr1;

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) {
                return addr1;
            }
        };
    }

    @Test
    public void testVisitModifyBackendClause() {
        mockNet();
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendClause clause = new ModifyBackendClause("test", "fqdn");
        Void resutl = visitor.visitModifyBackendClause(clause, null);
    }

    @Test
    public void testVisitModifyFrontendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test", "fqdn");
        Void result = visitor.visitModifyFrontendHostClause(clause, null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendClauseException() {
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyBackendClause(clause, null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyFrontendHostClauseException() {
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyFrontendHostClause(clause, null);
    }

    @Test
    public void testAnalyzeCancelAlterSystem() {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) analyzeSuccess(
                "CANCEL DECOMMISSION BACKEND \"127.0.0.1:8080\", \"127.0.0.2:8080\"");
        Assert.assertEquals("[127.0.0.1:8080, 127.0.0.2:8080]", cancelAlterSystemStmt.getHostPortPairs().toString());
    }

    @Test
    public void testAnalyzeModifyBackendProp() {
        String[] testLocs = {"*", "a:*", "bcd_123:*", "123bcd_:val_123", "invalidFormat",
                ":", "aa_123:*", "*:123", "a:b,c:d", "a: b", "  a  :  b  ", "   ", "a:b*"};
        Boolean[] analyzeSuccess = {false, false, false, true, false, false,
                false, false, false, true, true, false, false};
        int i = 0;
        for (String loc : testLocs) {
            String stmtStr = "alter system modify backend '127.0.0.1:9091' set ('" +
                    AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = '" + loc + "')";
            System.out.println(stmtStr);
            try {
                UtFrameUtils.parseStmtWithNewParser(stmtStr, connectContext);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                Assert.assertFalse(analyzeSuccess[i++]);
                continue;
            }

            Assert.assertTrue(analyzeSuccess[i++]);
        }

        String stmtStr = "alter system modify backend '127.0.0.1:9091'" +
                " set ('invalid_prop_key' = 'val', '" + PropertyAnalyzer.PROPERTIES_LABELS_LOCATION +  "' = 'a:b')";
        System.out.println(stmtStr);
        try {
            UtFrameUtils.parseStmtWithNewParser(stmtStr, connectContext);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("unsupported property: invalid_prop_key"));
        }
    }

    @Test
    public void testShowBackendLocation() throws Exception {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = 'a:b')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);
        String showBackendLocationSqlStr = "show backends";
        ShowBackendsStmt showBackendsStmt = (ShowBackendsStmt) UtFrameUtils.parseStmtWithNewParser(showBackendLocationSqlStr,
                connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showBackendsStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getResultRows());
        Assert.assertTrue(showResultSet.getResultRows().get(0).toString().contains("a:b"));
    }
}
