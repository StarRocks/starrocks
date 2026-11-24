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
import com.starrocks.qe.SetExecutor;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.thrift.TCompressionType;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A transaction stream load with channels ({@code channel_id} / {@code channel_num}), and each table of a
 * multi-statement transaction, is planned on the FE by {@link StreamLoadTask} with {@link LoadPlanner} and a session
 * of its own, from which {@link JobSpec} builds the query options. The {@code transmission_compression_type} and
 * {@code exec_mem_limit} of the request must still reach the query options the compute nodes run the load with.
 */
public class StreamLoadTaskPlanOptionsTest {
    private static final String DB_NAME = "test_txn_stream_load_plan_options";
    private static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    private static final String STOPPED = "stopped before execution";

    // The query options of the last load planned, as the coordinator would send them to the compute nodes.
    private static final AtomicReference<TQueryOptions> QUERY_OPTIONS = new AtomicReference<>();

    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        new StarRocksAssert(connectContext).withDatabase(DB_NAME).useDatabase(DB_NAME)
                .withTable("CREATE TABLE t (k INT NOT NULL, v1 INT, v2 INT) PRIMARY KEY(k) "
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
    public void testTransmissionCompressionTypeReachesQueryOptions() throws Exception {
        TQueryOptions queryOptions =
                planAndGetQueryOptions(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "LZ4"));
        Assertions.assertEquals(TCompressionType.LZ4, queryOptions.getLoad_transmission_compression_type());
    }

    @Test
    public void testGlobalTransmissionCompressionTypeKeptWithoutHeader() throws Exception {
        // The task's session starts from the global variables: a request without the header keeps the global value,
        // and one with it uses its own.
        setGlobalVariable("load_transmission_compression_type", "ZSTD");
        try {
            Assertions.assertEquals(TCompressionType.ZSTD,
                    planAndGetQueryOptions(Map.of()).getLoad_transmission_compression_type());
            Assertions.assertEquals(TCompressionType.LZ4,
                    planAndGetQueryOptions(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "LZ4"))
                            .getLoad_transmission_compression_type());
            Assertions.assertEquals(TCompressionType.NO_COMPRESSION,
                    planAndGetQueryOptions(
                            Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "NO_COMPRESSION"))
                            .getLoad_transmission_compression_type());
            // A compression that does not exist, or that the tablet sink has no codec for, does not replace the one the
            // cluster uses.
            for (String unusable : new String[] {"NO_SUCH_CODEC", "BZIP2", "DEFLATE", "AUTO"}) {
                Assertions.assertEquals(TCompressionType.ZSTD,
                        planAndGetQueryOptions(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, unusable))
                                .getLoad_transmission_compression_type(), unusable);
            }
        } finally {
            setGlobalVariable("load_transmission_compression_type", "NO_COMPRESSION");
        }
    }

    @Test
    public void testExecMemLimitReachesQueryOptions() throws Exception {
        // The compute nodes limit the memory of a pipeline load by query_mem_limit.
        TQueryOptions queryOptions = planAndGetQueryOptions(Map.of(EXEC_MEM_LIMIT, "123456789"));
        Assertions.assertEquals(123456789L, queryOptions.getMem_limit());
        Assertions.assertEquals(123456789L, queryOptions.getQuery_mem_limit());
    }

    @Test
    public void testNoExecMemLimitKeepsQueryMemLimit() throws Exception {
        // An empty header is one the request does not set, as the compute node reads it.
        for (Map<String, String> values : List.of(Map.<String, String>of(), Map.of(EXEC_MEM_LIMIT, ""))) {
            TQueryOptions queryOptions = planAndGetQueryOptions(values);
            Assertions.assertFalse(queryOptions.getQuery_mem_limit() > 0,
                    values + ": query_mem_limit " + queryOptions.getQuery_mem_limit());
        }
        // A request without exec_mem_limit, or with an empty one, keeps the session's query_mem_limit, which a new
        // session takes from the global one.
        setGlobalVariable("query_mem_limit", "987654321");
        try {
            Assertions.assertEquals(987654321L, planAndGetQueryOptions(Map.of()).getQuery_mem_limit());
            Assertions.assertEquals(987654321L, planAndGetQueryOptions(Map.of(EXEC_MEM_LIMIT, "")).getQuery_mem_limit());
            Assertions.assertEquals(123456789L,
                    planAndGetQueryOptions(Map.of(EXEC_MEM_LIMIT, "123456789")).getQuery_mem_limit());
        } finally {
            setGlobalVariable("query_mem_limit", "0");
        }
    }

    @Test
    public void testInvalidExecMemLimitRefused() {
        // As the compute node refuses it for the classic Stream Load.
        Map<String, String> expectedMessages = Map.of(
                "0", "exec_mem_limit must be greater than 0",
                "-1", "exec_mem_limit must be greater than 0",
                "abc", "Invalid parameter exec_mem_limit");
        expectedMessages.forEach((value, message) -> {
            StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                    () -> newTask().unprotectedExecute(headers(Map.of(EXEC_MEM_LIMIT, value))), value);
            Assertions.assertTrue(e.getMessage().contains(message), e.getMessage());
        });
    }

    private static HttpHeaders headers(Map<String, String> values) {
        HttpHeaders headers = new DefaultHttpHeaders();
        values.forEach(headers::set);
        return headers;
    }

    private static StreamLoadTask newTask() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t");
        long id = GlobalStateMgr.getCurrentState().getNextId();
        return new StreamLoadTask(id, db, table, "label_plan_options_" + id, "root", "127.0.0.1",
                60_000L, 1, 0, System.currentTimeMillis(), WarehouseManager.DEFAULT_RESOURCE);
    }

    /**
     * Plans a transaction stream load whose request carries the given headers, stops it when the coordinator is
     * created, and returns the query options the compute nodes would execute it with.
     */
    private static TQueryOptions planAndGetQueryOptions(Map<String, String> values) throws Exception {
        QUERY_OPTIONS.set(null);
        StarRocksException e = Assertions.assertThrows(StarRocksException.class,
                () -> newTask().unprotectedExecute(headers(values)));
        Assertions.assertTrue(e.getMessage().contains(STOPPED), e.getMessage());
        Assertions.assertNotNull(QUERY_OPTIONS.get());
        return QUERY_OPTIONS.get();
    }

    private static void setGlobalVariable(String name, String value) throws Exception {
        try (var scope = connectContext.bindScope()) {
            SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    "SET GLOBAL " + name + " = '" + value + "'", connectContext);
            new SetExecutor(connectContext, stmt).execute();
        }
    }
}
