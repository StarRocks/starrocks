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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.scheduler.history.TaskRunHistoryTable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleExecutorTest {

    private static final String INSERT_SQL = "INSERT INTO db1.t1 VALUES(1)";

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
        FeConstants.runningUnitTest = false;
    }

    /**
     * StmtExecutor.execute() does not rethrow a StarRocksException, it only records the failure in
     * the ConnectContext state and returns normally. The internal executor must not report that as
     * a success, otherwise its callers silently lose the data they believe they have persisted.
     */
    @Test
    public void testExecuteDMLFailsWhenStatementSetsErrorState() {
        mockStatementFailure("Tablet lost replicas. Check if any backend is down or not. tablet_id: 10093");

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL);
        SemanticException e =
                assertThrows(SemanticException.class, () -> executor.executeDML(INSERT_SQL));
        assertTrue(e.getMessage().contains("Tablet lost replicas"), e.getMessage());
    }

    @Test
    public void testExecuteControlFailsWhenStatementSetsErrorState() {
        mockStatementFailure("Unknown thread id: 1024");

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.MYSQL_PROTOCAL);
        SemanticException e =
                assertThrows(SemanticException.class, () -> executor.executeControl("KILL 1024"));
        assertTrue(e.getMessage().contains("Unknown thread id"), e.getMessage());
    }

    @Test
    public void testExecuteDMLSucceedsWhenStatementIsOk() {
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
            }
        };

        SimpleExecutor executor = new SimpleExecutor("testExecutor", TResultSinkType.HTTP_PROTOCAL);
        executor.executeDML(INSERT_SQL);
    }

    /**
     * Serving-path internal queries (e.g. filling information_schema.materialized_views, lookup_string)
     * must be bounded by the outer user query's remaining query_timeout, not the 1h
     * statistic_collect_query_timeout. Background callers (no outer query) keep the old fallback.
     */
    @Test
    public void testOuterRemainingQueryTimeoutS() throws Exception {
        // No outer user-query context: fall back to statistic_collect_query_timeout (old behavior).
        ConnectContext.remove();
        assertEquals((int) Config.statistic_collect_query_timeout, SimpleExecutor.outerRemainingQueryTimeoutS());

        // Outer query with query_timeout=300s started ~100s ago -> remaining ~200s.
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setQueryTimeoutS(300);
        ctx.setStartTime(Instant.now().minusSeconds(100));
        int remaining = SimpleExecutor.outerRemainingQueryTimeoutS();
        assertTrue(remaining > 190 && remaining <= 200, "remaining=" + remaining);

        // Budget already exhausted (started ~400s ago, budget 300s) -> <= 0, so the caller can time out.
        ctx.setStartTime(Instant.now().minusSeconds(400));
        assertTrue(SimpleExecutor.outerRemainingQueryTimeoutS() <= 0);

        ConnectContext.remove();
    }

    /**
     * The internal task_run_history read (fired while filling information_schema.materialized_views /
     * SHOW MATERIALIZED VIEWS) must inherit the OUTER user query's remaining query_timeout, not the
     * default statistic_collect_query_timeout (1h).
     */
    @Test
    public void testInternalTaskRunHistoryReadUsesOuterQueryTimeout() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.getSessionVariable().setQueryTimeoutS(300);
        ctx.setStartTime(Instant.now());

        int[] capturedTimeout = {-1};
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql, int queryTimeoutSeconds) {
                capturedTimeout[0] = queryTimeoutSeconds;
                return Collections.emptyList();
            }
        };

        new TaskRunHistoryTable().lookupLastJobOfTasks("db", Collections.singleton("mvTask"));
        ConnectContext.remove();

        // Inherited the outer query_timeout (~300s), not statistic_collect_query_timeout (3600s).
        assertTrue(capturedTimeout[0] > 290 && capturedTimeout[0] <= 300, "timeout=" + capturedTimeout[0]);
    }

    private static void mockStatementFailure(String errorMessage) {
        new MockUp<StmtExecutor>() {
            @Mock
            public void execute() {
                ConnectContext.get().getState().setError(errorMessage);
            }
        };
    }
}
