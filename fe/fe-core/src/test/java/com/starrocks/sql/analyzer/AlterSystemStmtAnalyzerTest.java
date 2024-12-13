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

<<<<<<< HEAD

import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
=======
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
<<<<<<< HEAD
import java.net.UnknownHostException;
=======
import java.util.List;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AlterSystemStmtAnalyzerTest {
<<<<<<< HEAD
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
=======
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        AnalyzeTestUtil.init();

        UtFrameUtils.setUpForPersistTest();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Mocked
    InetAddress addr1;

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
<<<<<<< HEAD
            public InetAddress getByName(String host) throws UnknownHostException {
=======
            public InetAddress getByName(String host) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                return addr1;
            }
        };
    }

    @Test
<<<<<<< HEAD
    public void testVisitModifyBackendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("test", "fqdn");
        Void resutl = visitor.visitModifyBackendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
=======
    public void testVisitModifyBackendClause() {
        mockNet();
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendClause clause = new ModifyBackendClause("test", "fqdn");
        Void result = visitor.visitModifyBackendClause(clause, null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void testVisitModifyFrontendHostClause() {
        mockNet();
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyFrontendAddressClause clause = new ModifyFrontendAddressClause("test", "fqdn");
<<<<<<< HEAD
        Void resutl = visitor.visitModifyFrontendHostClause(clause, null);
        Assert.assertTrue(resutl == null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendHostClauseException() {
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyBackendHostClause(clause, null);
=======
        Void result = visitor.visitModifyFrontendHostClause(clause, null);
    }

    @Test(expected = SemanticException.class)
    public void testVisitModifyBackendClauseException() {
        AlterSystemStmtAnalyzer visitor = new AlterSystemStmtAnalyzer();
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.2", "127.0.0.1");
        visitor.visitModifyBackendClause(clause, null);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
=======

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

    private void modifyBackendLocation(String location) throws Exception {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        System.out.println(systemInfoService.getBackends());
        List<Long> backendIds = systemInfoService.getBackendIds();
        Backend backend = systemInfoService.getBackend(backendIds.get(0));
        String modifyBackendPropSqlStr = "alter system modify backend '" + backend.getHost() +
                ":" + backend.getHeartbeatPort() + "' set ('" +
                AlterSystemStmtAnalyzer.PROP_KEY_LOCATION + "' = '" + location + "')";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(modifyBackendPropSqlStr, connectContext),
                connectContext);
    }

    @Test
    public void testShowBackendLocation() throws Exception {
        modifyBackendLocation("a:b");
        String showBackendLocationSqlStr = "show backends";
        ShowBackendsStmt showBackendsStmt = (ShowBackendsStmt) UtFrameUtils.parseStmtWithNewParser(showBackendLocationSqlStr,
                connectContext);
        ShowResultSet showResultSet = ShowExecutor.execute(showBackendsStmt, connectContext);
        System.out.println(showResultSet.getResultRows());
        Assert.assertTrue(showResultSet.getResultRows().get(0).toString().contains("a:b"));
    }

    @Test
    public void testModifyBackendLocationPersistence() throws Exception {
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage initialImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getNodeMgr().save(initialImage.getImageWriter());

        modifyBackendLocation("c:d");

        // make final image
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getNodeMgr().save(finalImage.getImageWriter());

        // test replay
        NodeMgr nodeMgrFollower = new NodeMgr();
        nodeMgrFollower.load(initialImage.getMetaBlockReader());
        Backend persistentState =
                (Backend) UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_BACKEND_STATE_CHANGE_V2);
        nodeMgrFollower.getClusterInfo().updateInMemoryStateBackend(persistentState);
        Assert.assertEquals("{c=d}",
                nodeMgrFollower.getClusterInfo().getBackend(persistentState.getId()).getLocation().toString());

        // test restart
        NodeMgr nodeMgrLeader = new NodeMgr();
        nodeMgrLeader.load(finalImage.getMetaBlockReader());
        Assert.assertEquals("{c=d}",
                nodeMgrLeader.getClusterInfo().getBackend(persistentState.getId()).getLocation().toString());
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
