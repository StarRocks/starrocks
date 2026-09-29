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

package com.starrocks.sql.plan;

import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataResultQueueScanReleaseTest extends ConnectorPlanTestBase {
    private static final String HIVE_QUERY = "select * from hive0.partitioned_db.t1";

    @BeforeEach
    public void resetQueryState() {
        connectContext.getState().reset();
    }

    @Test
    public void testSuccessfulExecutionReleasesScansThroughCoordinator() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() {
            }

            @Mock
            public RowBatch getNext() {
                return new RowBatch();
            }

            @Mock
            public void clearExternalResources() {
                cleared.incrementAndGet();
            }
        };

        execute(plan);

        assertEquals(1, cleared.get());
        assertFalse(connectContext.getState().isError());
    }

    @Test
    public void testExecutionFailureReleasesScansThroughCoordinator() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() throws StarRocksException {
                throw new StarRocksException("backend unavailable");
            }

            @Mock
            public void clearExternalResources() {
                cleared.incrementAndGet();
            }
        };

        execute(plan);

        assertEquals(1, cleared.get());
        assertTrue(connectContext.getState().isError());
        assertTrue(connectContext.getState().getErrorMessage().contains("backend unavailable"));
    }

    @Test
    public void testFailureBeforeSchedulingReleasesScansAndKeepsCause() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<DescriptorTable>() {
            @Mock
            public TDescriptorTable toThrift() {
                throw new StarRocksConnectorException("metastore unavailable");
            }
        };
        new MockUp<HdfsScanNode>() {
            @Mock
            public void clear() {
                cleared.incrementAndGet();
            }
        };

        StmtExecutor executor = execute(plan);

        assertNull(executor.getCoordinator());
        assertEquals(1, cleared.get());
        assertTrue(connectContext.getState().isError());
        assertTrue(connectContext.getState().getErrorMessage().contains("metastore unavailable"));
    }

    private StmtExecutor execute(ExecPlan plan) throws Exception {
        StmtExecutor executor = new StmtExecutor(connectContext,
                UtFrameUtils.parseStmtWithNewParser(HIVE_QUERY, connectContext));
        Queue<TResultBatch> result = new ConcurrentLinkedQueue<>();
        executor.executeStmtWithResultQueue(connectContext, plan, result);
        return executor;
    }
}
