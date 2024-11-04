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

import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.thrift.TStatusCode;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_PARALLEL;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_FORMAT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_WAREHOUSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BatchWriteMgrTest extends BatchWriteTestBase {

    private BatchWriteMgr batchWriteMgr;

    private TableId tableId1;
    private TableId tableId2;
    private TableId tableId3;
    private TableId tableId4;

    @Before
    public void setup() {
        batchWriteMgr = new BatchWriteMgr();
        batchWriteMgr.start();

        tableId1 = new TableId(DB_NAME_1, TABLE_NAME_1_1);
        tableId2 = new TableId(DB_NAME_1, TABLE_NAME_1_2);
        tableId3 = new TableId(DB_NAME_2, TABLE_NAME_2_1);
        tableId4 = new TableId(DB_NAME_2, TABLE_NAME_2_2);
    }

    @Test
    public void testRequestBackends() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "1");
            }});
        RequestCoordinatorBackendResult result1 =
                batchWriteMgr.requestCoordinatorBackends(tableId1, params1);
        assertTrue(result1.isOk());
        assertEquals(1, result1.getValue().size());
        assertEquals(1, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_FORMAT, "json");
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "1");
            }});
        RequestCoordinatorBackendResult result2 =
                batchWriteMgr.requestCoordinatorBackends(tableId1, params2);
        assertTrue(result2.isOk());
        assertEquals(1, result2.getValue().size());
        assertEquals(2, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result3 =
                batchWriteMgr.requestCoordinatorBackends(tableId2, params3);
        assertTrue(result3.isOk());
        assertEquals(4, result3.getValue().size());
        assertEquals(3, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params4 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result4 =
                batchWriteMgr.requestCoordinatorBackends(tableId3, params4);
        assertTrue(result4.isOk());
        assertEquals(4, result4.getValue().size());
        assertEquals(4, batchWriteMgr.numBatchWrites());
    }

    @Test
    public void testRequestLoad() {
        StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "100000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestLoadResult result1 = batchWriteMgr.requestLoad(
                tableId4, params, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result1.isOk());
        assertNotNull(result1.getValue());
        assertEquals(1, batchWriteMgr.numBatchWrites());

        RequestLoadResult result2 = batchWriteMgr.requestLoad(
                tableId4, params, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(result1.getValue(), result2.getValue());
        assertEquals(1, batchWriteMgr.numBatchWrites());
    }

    @Test
    public void testCheckParameters() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>());
        RequestCoordinatorBackendResult result1 =
                batchWriteMgr.requestCoordinatorBackends(tableId1, params1);
        assertFalse(result1.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result1.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
            }});
        RequestCoordinatorBackendResult result2 =
                batchWriteMgr.requestCoordinatorBackends(tableId2, params2);
        assertFalse(result2.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result2.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
                put(HTTP_WAREHOUSE, "no_exist");
            }});
        RequestCoordinatorBackendResult result3 =
                batchWriteMgr.requestCoordinatorBackends(tableId3, params3);
        assertFalse(result3.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result3.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numBatchWrites());
    }

    @Test
    public void testCleanupInactiveBatchWrite() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result1 = batchWriteMgr.requestCoordinatorBackends(tableId1, params1);
        assertTrue(result1.isOk());
        assertEquals(1, batchWriteMgr.numBatchWrites());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "100000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestLoadResult result2 = batchWriteMgr.requestLoad(
                tableId4, params2, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(2, batchWriteMgr.numBatchWrites());

        new MockUp<IsomorphicBatchWrite>() {

            @Mock
            public boolean isActive() {
                return false;
            }
        };
        batchWriteMgr.cleanupInactiveBatchWrite();
        assertEquals(0, batchWriteMgr.numBatchWrites());
    }

    @Test
    public void testAssignerRegisterBatchWriteFail() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        CoordinatorBackendAssigner assigner = batchWriteMgr.getCoordinatorBackendAssigner();
        new Expectations(assigner) {
            {
                assigner.registerBatchWrite(anyLong, anyLong, (TableId) any, anyInt);
                result = new Exception("registerBatchWrite failed");
            }
        };
        RequestCoordinatorBackendResult result = batchWriteMgr.requestCoordinatorBackends(tableId1, params1);
        assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numBatchWrites());
    }
}
