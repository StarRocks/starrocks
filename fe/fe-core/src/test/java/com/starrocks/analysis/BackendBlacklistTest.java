// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class BackendBlacklistTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        SimpleScheduler.disableUpdateBlocklistThread();
    }

    @AfterEach
    public void tearDown() {
        SimpleScheduler.getHostBlacklist().clear();
    }

    @Test
    public void testBackendBlacklist() {
        AddBackendBlackListStmt st = (AddBackendBlackListStmt) analyzeSuccess("ADD BACKEND BLACKLIST 1, 2, 3");
        Assertions.assertEquals(1L, st.getBackendIds().get(0).longValue());
        Assertions.assertEquals(3L, st.getBackendIds().get(2).longValue());

        DelBackendBlackListStmt del = (DelBackendBlackListStmt) analyzeSuccess("DELETE BACKEND BLACKLIST 1, 2, 3");
        Assertions.assertEquals(1L, del.getBackendIds().get(0).longValue());
        Assertions.assertEquals(3L, del.getBackendIds().get(2).longValue());
        analyzeSuccess("SHOW BACKEND BLACKLIST");

        analyzeFail("ADD BACKEND BLACKLIST aa");
        analyzeFail("ADD BACKEND BLACKLIST '1'");
        analyzeFail("ADD BACKEND BLACKLIST 1.0");
        analyzeFail("DELETE BACKEND BLACKLIST 'a',");
    }

    @Test
    public void testAddBackendBlacklistExecution() throws Exception {
        new MockUp<SystemInfoService>() {
            @Mock
            Backend getBackend(long backendId) {
                if (backendId >= 1 && backendId <= 3) {
                    Backend backend = new Backend();
                    backend.setId(backendId);
                    return backend;
                }
                return null;
            }
        };

        AddBackendBlackListStmt addStmt = (AddBackendBlackListStmt) analyzeSuccess("ADD BACKEND BLACKLIST 1, 2, 3");
        StmtExecutor addStmtExecutor = new StmtExecutor(AnalyzeTestUtil.getConnectContext(), addStmt);
        addStmtExecutor.execute();

        Assertions.assertTrue(SimpleScheduler.isInBlocklist(1));
        Assertions.assertTrue(SimpleScheduler.isInBlocklist(2));
        Assertions.assertTrue(SimpleScheduler.isInBlocklist(3));
    }

    @Test
    public void testAddBackendBlacklistWithNonExistentBackend() throws Exception {
        AddBackendBlackListStmt addStmt = (AddBackendBlackListStmt) analyzeSuccess("ADD BACKEND BLACKLIST 999999");
        com.starrocks.qe.ConnectContext ctx = AnalyzeTestUtil.getConnectContext();
        StmtExecutor addStmtExecutor = new StmtExecutor(ctx, addStmt);

        addStmtExecutor.execute();

        Assertions.assertTrue(ctx.getState().isError());
        String errMsg = ctx.getState().getErrorMessage();
        Assertions.assertTrue(errMsg.contains("Not found backend") || errMsg.contains("999999"));
    }
}
