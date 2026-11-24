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

package com.starrocks.load.streamload;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A transaction stream load with channels ({@code channel_id} / {@code channel_num}) is planned on the FE by
 * {@link StreamLoadTask}, which builds its own session. The {@code log_rejected_record_num} of the request must still
 * reach the query options the compute nodes run the load with, or they record no rejected rows.
 */
public class StreamLoadTaskLogRejectedRecordTest {
    private static final String DB_NAME = "test_txn_stream_load_rejected_record";
    private static final String STOPPED = "stopped before execution";

    // The query options of the last load planned, as the coordinator would send them to the compute nodes.
    private static final AtomicReference<TQueryOptions> QUERY_OPTIONS = new AtomicReference<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext).withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE t (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                        + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        // Build the job spec from the planned load the way DefaultCoordinator.Factory does, then stop the load before
        // it reaches a backend.
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createStreamLoadScheduler(LoadPlanner loadPlanner) throws StarRocksException {
                QUERY_OPTIONS.set(JobSpec.Factory.fromStreamLoadJobSpec(loadPlanner).getQueryOptions());
                throw new StarRocksException(STOPPED);
            }
        };
    }

    @Test
    public void testLogRejectedRecordNumReachesQueryOptions() throws Exception {
        // -1 records every rejected row, a positive value caps how many.
        Assertions.assertEquals(-1L, planAndGetQueryOptions("-1").getLog_rejected_record_num());
        Assertions.assertEquals(10L, planAndGetQueryOptions("10").getLog_rejected_record_num());
    }

    @Test
    public void testNoLogRejectedRecordNumLeavesQueryOptionsUnset() throws Exception {
        Assertions.assertFalse(planAndGetQueryOptions(null).isSetLog_rejected_record_num());
    }

    @Test
    public void testGlobalLogRejectedRecordNumOnlyWithoutHeader() throws Exception {
        // The task's session starts from the global variables: a request without the header keeps the global value,
        // and one with it, 0 included, uses its own.
        SessionVariable globalDefault = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable();
        long saved = globalDefault.getLogRejectedRecordNum();
        globalDefault.setLogRejectedRecordNum(-1);
        try {
            Assertions.assertEquals(-1L, planAndGetQueryOptions(null).getLog_rejected_record_num());
            Assertions.assertEquals(10L, planAndGetQueryOptions("10").getLog_rejected_record_num());
            Assertions.assertFalse(planAndGetQueryOptions("0").isSetLog_rejected_record_num());
        } finally {
            globalDefault.setLogRejectedRecordNum(saved);
        }
    }

    /**
     * Plans a transaction stream load whose request sets {@code log_rejected_record_num} to the given value (null: not
     * set) and returns the query options the compute nodes would execute it with.
     */
    private static TQueryOptions planAndGetQueryOptions(String logRejectedRecordNum) throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
        HttpHeaders headers = new DefaultHttpHeaders();
        if (logRejectedRecordNum != null) {
            headers.set(StreamLoadHttpHeader.HTTP_LOG_REJECTED_RECORD_NUM, logRejectedRecordNum);
        }
        long id = GlobalStateMgr.getCurrentState().getNextId();
        StreamLoadTask task = new StreamLoadTask(id, db, table, "label_rejected_record_" + id, "root", "127.0.0.1",
                60_000L, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);

        QUERY_OPTIONS.set(null);
        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> task.unprotectedExecute(headers));
        Assertions.assertTrue(e.getMessage().contains(STOPPED), e.getMessage());
        Assertions.assertNotNull(QUERY_OPTIONS.get());
        return QUERY_OPTIONS.get();
    }
}
