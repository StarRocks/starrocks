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

package com.starrocks.qe;

import com.google.gson.Gson;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class QueryDetailTest {
    @Test
    public void testQueryDetail() {
        QueryDetail queryDetail = new QueryDetail("219a2d5443c542d4-8fc938db37c892e3", true, 1, "127.0.0.1",
                System.currentTimeMillis(), -1, -1, QueryDetail.QueryMemState.RUNNING,
                "testDb", "select * from table1 limit 1",
                "root", "", "default_catalog");
        queryDetail.setProfile("bbbbb");
        queryDetail.setErrorMessage("cancelled");

        QueryDetail copyOfQueryDetail = queryDetail.copy();
        Gson gson = new Gson();
        Assertions.assertEquals(gson.toJson(queryDetail), gson.toJson(copyOfQueryDetail));

        queryDetail.setLatency(10);
        Assertions.assertEquals(-1, copyOfQueryDetail.getLatency());
    }

    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        Config.enable_collect_query_detail_info = true;
    }

    @Test
    public void testExternalQuerySource() throws Exception {
        // Test external query (user-initiated)
        String sql = "SELECT 1";
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        StatementBase statement = statements.get(0);

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        // Execute using normal StmtExecutor (external query)
        StmtExecutor executor = new StmtExecutor(testContext, statement);
        testContext.setExecutor(executor);
        testContext.setThreadLocalInfo();

        // Add running query detail
        executor.addRunningQueryDetail(statement);

        // Verify query source is EXTERNAL
        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(QueryDetail.QuerySource.EXTERNAL, queryDetail.getQuerySource());
    }

    @Test
    public void testInternalQuerySource() throws Exception {
        // Test internal query (system query)
        String sql = "SELECT 1";
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        StatementBase statement = statements.get(0);

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();

        // Execute using internal StmtExecutor
        StmtExecutor executor = StmtExecutor.newInternalExecutor(testContext, statement);
        testContext.setExecutor(executor);
        testContext.setThreadLocalInfo();

        // Add running query detail
        executor.addRunningQueryDetail(statement);

        // Verify query source is INTERNAL
        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(QueryDetail.QuerySource.INTERNAL, queryDetail.getQuerySource());
    }

    @Test
    public void testTaskQuerySource() throws Exception {
        // Test task query using SqlTaskRunProcessor
        String sql = "SELECT 1";

        // Create a mock TaskRunContext
        com.starrocks.scheduler.TaskRunContext taskRunContext = new com.starrocks.scheduler.TaskRunContext();

        // Create a new context for this test
        ConnectContext testContext = new ConnectContext();
        testContext.setGlobalStateMgr(connectContext.getGlobalStateMgr());
        testContext.setQueryId(UUIDUtil.genUUID());
        testContext.setStartTime();
        testContext.setDatabase("testDb");
        testContext.setQualifiedUser("root");
        testContext.setCurrentCatalog("default_catalog");

        // Set the context in TaskRunContext
        taskRunContext.setCtx(testContext);
        taskRunContext.setDefinition(sql);
        taskRunContext.setRemoteIp("127.0.0.1");

        // Create and execute SqlTaskRunProcessor
        com.starrocks.scheduler.SqlTaskRunProcessor processor = new com.starrocks.scheduler.SqlTaskRunProcessor();

        // Mock the execution to avoid actual SQL execution
        try {
            processor.processTaskRun(taskRunContext);
        } catch (Exception e) {
            // Expected to fail due to mock setup, but querySource should be set
        }

        // Verify query source is TASK
        QueryDetail queryDetail = testContext.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(QueryDetail.QuerySource.TASK, queryDetail.getQuerySource());
    }

    @Test
    public void testMVQuerySource() throws Exception {
        // TODO
    }
}
