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

package com.starrocks.http;

import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class HttpConnectProcessorTest {

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
        UtFrameUtils.createMinStarRocksCluster();
    }

    // QueryFEAllocatedMemory is measured against a per-statement baseline. Without one, an HTTP query reports
    // everything its worker thread has allocated across all the requests it has served.
    @Test
    public void testHandleQuerySetsAllocatedMemoryBaseline() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setThreadLocalInfo();
        ctx.setStatement(UtFrameUtils.parseStmtWithNewParser("show catalogs", ctx));
        ctx.setCurrentThreadAllocatedMemory(0);

        long allocatedBefore = ConnectProcessor.getThreadAllocatedBytes(Thread.currentThread().getId());
        Assumptions.assumeTrue(allocatedBefore > 0, "thread allocated memory measurement is not supported");

        AtomicLong baselineAtAudit = new AtomicLong(-1);
        new HttpConnectProcessor(ctx) {
            @Override
            public void auditAfterExec(String origStmt, StatementBase parsedStmt, PQueryStatistics statistics) {
                baselineAtAudit.set(ctx.getCurrentThreadAllocatedMemory());
            }
        }.processOnce();

        Assertions.assertTrue(baselineAtAudit.get() >= allocatedBefore,
                "baseline " + baselineAtAudit.get() + " was not set at the start of the query");
    }
}
