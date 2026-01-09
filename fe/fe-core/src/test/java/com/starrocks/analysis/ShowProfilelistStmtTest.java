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

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.ShowProfilelistStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TGetProfileListResponse;
import com.starrocks.thrift.TProfileInfo;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class ShowProfilelistStmtTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        testSuccess("SHOW PROFILELIST");
        testSuccess("SHOW PROFILELIST LIMIT 10");
    }

    private void testSuccess(String originStmt) throws Exception {
        ShowProfilelistStmt stmt = (ShowProfilelistStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertNotNull(metaData);
        Assertions.assertEquals(5, metaData.getColumnCount());
        Assertions.assertEquals("QueryId", metaData.getColumn(0).getName());
        Assertions.assertEquals("StartTime", metaData.getColumn(1).getName());
        Assertions.assertEquals("Time", metaData.getColumn(2).getName());
        Assertions.assertEquals("State", metaData.getColumn(3).getName());
        Assertions.assertEquals("Statement", metaData.getColumn(4).getName());
    }

    @Test
    public void testRemoteFeProfileList() {
        ShowProfilelistStmt showProfilelistStmt = new ShowProfilelistStmt(-1, NodePosition.ZERO);

        Frontend frontend1 = new Frontend(1, FrontendNodeType.LEADER, "127.0.0.1", "127.0.0.1", 9030);
        Frontend frontend2 = new Frontend(2, FrontendNodeType.FOLLOWER, "127.0.0.2", "127.0.0.2", 9030);
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return frontend1;
            }

            @Mock
            public List<Frontend> getFrontends(FrontendNodeType nodeType) {
                return Lists.newArrayList(frontend1, frontend2);
            }
        };

        // Create a profile on local FE
        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfile("test-query-id-1", "Query");
        manager.pushProfile(null, profile);

        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);

        TGetProfileListResponse tGetProfileListResponse = new TGetProfileListResponse();
        TProfileInfo tProfileInfo = new TProfileInfo();
        tProfileInfo.setQuery_id("test-query-id-2");
        tProfileInfo.setStart_time("2024-01-09 12:00:00");
        tProfileInfo.setTotal_time("100ms");
        tProfileInfo.setQuery_state("Finished");
        tProfileInfo.setStatement("SELECT * FROM test_table");
        tGetProfileListResponse.addToProfile_infos(tProfileInfo);

        try (MockedStatic<ThriftRPCRequestExecutor> thriftConnectionPoolMockedStatic =
                Mockito.mockStatic(ThriftRPCRequestExecutor.class)) {
            thriftConnectionPoolMockedStatic.when(()
                            -> ThriftRPCRequestExecutor.call(Mockito.any(), Mockito.any(), Mockito.any()))
                    .thenReturn(tGetProfileListResponse);
            ShowResultSet showResultSet = ShowExecutor.execute(showProfilelistStmt, context);
            
            // Should have 2 profiles: 1 from local FE + 1 from remote FE
            Assertions.assertEquals(2, showResultSet.getResultRows().size());

            List<List<String>> resultRows = showResultSet.getResultRows();
            
            // Check remote FE profile
            boolean foundRemoteProfile = false;
            for (List<String> row : resultRows) {
                if ("test-query-id-2".equals(row.get(0))) {
                    foundRemoteProfile = true;
                    Assertions.assertEquals("2024-01-09 12:00:00", row.get(1));
                    Assertions.assertEquals("100ms", row.get(2));
                    Assertions.assertEquals("Finished", row.get(3));
                    Assertions.assertEquals("SELECT * FROM test_table", row.get(4));
                }
            }
            Assertions.assertTrue(foundRemoteProfile, "Remote FE profile should be included in results");
        } finally {
            manager.clearProfiles();
        }
    }

    private RuntimeProfile buildRuntimeProfile(String queryId, String queryType) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.START_TIME, "2024-01-09 11:00:00");
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, "50ms");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, "Finished");
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "SELECT 1");
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);

        profile.addChild(summaryProfile);

        return profile;
    }
}
