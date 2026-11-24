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

package com.starrocks.load.batchwrite;

import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.load.streamload.StreamLoadHttpHeader;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.SetExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * {@link MergeCommitTask} plans its load with {@link LoadPlanner} and a session of its own, from which {@link JobSpec}
 * builds the query options. The {@code transmission_compression_type} and {@code exec_mem_limit} of the request must
 * still reach the query options the compute nodes run the load with.
 */
public class MergeCommitTaskPlanOptionsTest extends BatchWriteTestBase {
    private static final String PK_TABLE = "merge_commit_plan_options_pk";
    private static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    private static final String STOPPED = "stopped before execution";

    @BeforeAll
    public static void createPrimaryKeyTable() throws Exception {
        starRocksAssert.useDatabase(DB_NAME_1).withTable("CREATE TABLE " + PK_TABLE
                + " (k INT NOT NULL, v1 INT, v2 INT) PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1"
                + " PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void testTransmissionCompressionTypeReachesQueryOptions() throws Exception {
        TQueryOptions queryOptions = plan(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "LZ4"));
        assertEquals(TCompressionType.LZ4, queryOptions.getLoad_transmission_compression_type());
    }

    @Test
    public void testGlobalTransmissionCompressionTypeKeptWithoutHeader() throws Exception {
        // The task's session starts from the global variables: a request without the header keeps the global value,
        // and one with it uses its own.
        setGlobalVariable("load_transmission_compression_type", "ZSTD");
        try {
            assertEquals(TCompressionType.ZSTD, plan(Map.of()).getLoad_transmission_compression_type());
            assertEquals(TCompressionType.LZ4,
                    plan(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "LZ4"))
                            .getLoad_transmission_compression_type());
            assertEquals(TCompressionType.NO_COMPRESSION,
                    plan(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, "NO_COMPRESSION"))
                            .getLoad_transmission_compression_type());
            // A compression that does not exist, or that the tablet sink has no codec for, does not replace the one the
            // cluster uses.
            for (String unusable : new String[] {"NO_SUCH_CODEC", "BZIP2", "DEFLATE", "AUTO"}) {
                assertEquals(TCompressionType.ZSTD,
                        plan(Map.of(StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE, unusable))
                                .getLoad_transmission_compression_type(), unusable);
            }
        } finally {
            setGlobalVariable("load_transmission_compression_type", "NO_COMPRESSION");
        }
    }

    @Test
    public void testExecMemLimitReachesQueryOptions() throws Exception {
        // The compute nodes limit the memory of a pipeline load by query_mem_limit.
        TQueryOptions queryOptions = plan(Map.of(EXEC_MEM_LIMIT, "123456789"));
        assertEquals(123456789L, queryOptions.getMem_limit());
        assertEquals(123456789L, queryOptions.getQuery_mem_limit());
    }

    @Test
    public void testNoExecMemLimitKeepsQueryMemLimit() throws Exception {
        assertFalse(plan(Map.of()).getQuery_mem_limit() > 0);
        // A request without exec_mem_limit, or with an empty one, keeps the session's query_mem_limit, which a new
        // session takes from the global one.
        setGlobalVariable("query_mem_limit", "987654321");
        try {
            assertEquals(987654321L, plan(Map.of()).getQuery_mem_limit());
            assertEquals(987654321L, plan(Map.of(EXEC_MEM_LIMIT, "")).getQuery_mem_limit());
            assertEquals(123456789L, plan(Map.of(EXEC_MEM_LIMIT, "123456789")).getQuery_mem_limit());
        } finally {
            setGlobalVariable("query_mem_limit", "0");
        }
    }

    @Test
    public void testExecMemLimitParsed() throws Exception {
        assertEquals(123456789L, buildStreamLoadInfo(Map.of(EXEC_MEM_LIMIT, "123456789")).getExecMemLimit());
        // As the compute node's parser does.
        assertEquals(123456789L, buildStreamLoadInfo(Map.of(EXEC_MEM_LIMIT, " 123456789 ")).getExecMemLimit());
        assertEquals(0L, buildStreamLoadInfo(Map.of()).getExecMemLimit());
        // An empty header is one the request does not set, as the compute node reads it.
        assertEquals(0L, buildStreamLoadInfo(Map.of(EXEC_MEM_LIMIT, "")).getExecMemLimit());
    }

    @Test
    public void testInvalidExecMemLimitRefused() {
        // As the compute node refuses it for the classic Stream Load.
        Map<String, String> expectedMessages = Map.of(
                "0", "exec_mem_limit must be greater than 0",
                "-1", "exec_mem_limit must be greater than 0",
                "abc", "Invalid parameter exec_mem_limit");
        expectedMessages.forEach((value, message) -> {
            StarRocksException e = assertThrows(StarRocksException.class,
                    () -> buildStreamLoadInfo(Map.of(EXEC_MEM_LIMIT, value)), value);
            assertTrue(e.getMessage().contains(message), e.getMessage());
        });
    }

    private static StreamLoadKvParams buildParams(Map<String, String> headers) {
        Map<String, String> map = new HashMap<>();
        map.put(StreamLoadHttpHeader.HTTP_FORMAT, "csv");
        map.put(StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE, "true");
        map.put(StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC, "true");
        map.putAll(headers);
        return new StreamLoadKvParams(map);
    }

    private static StreamLoadInfo buildStreamLoadInfo(Map<String, String> headers) throws StarRocksException {
        return StreamLoadInfo.fromHttpStreamLoadRequest(null, -1, Optional.empty(), buildParams(headers),
                CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_NAME));
    }

    /**
     * Runs a merge commit task for a request with the given headers up to the point where the coordinator is created,
     * and returns the query options the compute nodes would execute its plan with.
     */
    private static TQueryOptions plan(Map<String, String> headers) throws Exception {
        StreamLoadKvParams params = buildParams(headers);
        StreamLoadInfo info = buildStreamLoadInfo(headers);

        // Build the job spec from the planned load the way DefaultCoordinator.Factory does, then stop the load before
        // it reaches a backend.
        AtomicReference<TQueryOptions> queryOptions = new AtomicReference<>();
        Coordinator.Factory coordinatorFactory = Mockito.mock(Coordinator.Factory.class);
        when(coordinatorFactory.createStreamLoadScheduler(any(LoadPlanner.class))).thenAnswer(invocation -> {
            LoadPlanner loadPlanner = invocation.getArgument(0);
            queryOptions.set(JobSpec.Factory.fromStreamLoadJobSpec(loadPlanner).getQueryOptions());
            throw new StarRocksException(STOPPED);
        });

        TUniqueId loadId = UUIDUtil.genTUniqueId();
        MergeCommitTask task = new MergeCommitTask(
                GlobalStateMgr.getCurrentState().getNextId(),
                DATABASE_1.getId(),
                new TableId(DB_NAME_1, PK_TABLE),
                "merge_commit_" + DebugUtil.printId(loadId),
                loadId,
                info,
                1000,
                params,
                "root",
                "default_warehouse",
                new HashSet<>(Arrays.asList(10002L, 10003L)),
                coordinatorFactory,
                finishedTask -> {});
        task.run();

        assertNotNull(queryOptions.get(), "the load was not planned: " + task.getTaskState());
        assertTrue(task.getTaskState().second.contains(STOPPED), String.valueOf(task.getTaskState()));
        return queryOptions.get();
    }

    private static void setGlobalVariable(String name, String value) throws Exception {
        try (var scope = connectContext.bindScope()) {
            SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    "SET GLOBAL " + name + " = '" + value + "'", connectContext);
            new SetExecutor(connectContext, stmt).execute();
        }
    }
}
