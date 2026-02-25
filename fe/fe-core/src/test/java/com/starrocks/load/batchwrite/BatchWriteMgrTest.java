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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Utils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_PARALLEL;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_FORMAT;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_WAREHOUSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchWriteMgrTest extends BatchWriteTestBase {

    private BatchWriteMgr batchWriteMgr;

    private TableId tableId1;
    private TableId tableId2;
    private TableId tableId3;
    private TableId tableId4;

    @BeforeEach
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
                batchWriteMgr.requestCoordinatorBackends(tableId1, params1, UserIdentity.ROOT);
        assertTrue(result1.isOk());
        assertEquals(1, result1.getValue().size());
        assertEquals(1, batchWriteMgr.numJobs());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_FORMAT, "json");
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "1");
            }});
        RequestCoordinatorBackendResult result2 =
                batchWriteMgr.requestCoordinatorBackends(tableId1, params2, UserIdentity.ROOT);
        assertTrue(result2.isOk());
        assertEquals(1, result2.getValue().size());
        assertEquals(2, batchWriteMgr.numJobs());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result3 =
                batchWriteMgr.requestCoordinatorBackends(tableId2, params3, UserIdentity.ROOT);
        assertTrue(result3.isOk());
        assertEquals(4, result3.getValue().size());
        assertEquals(3, batchWriteMgr.numJobs());

        StreamLoadKvParams params4 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result4 =
                batchWriteMgr.requestCoordinatorBackends(tableId3, params4, UserIdentity.ROOT);
        assertTrue(result4.isOk());
        assertEquals(4, result4.getValue().size());
        assertEquals(4, batchWriteMgr.numJobs());
    }

    @Test
    public void testRequestLoad() {
        StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "100000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestLoadResult result1 = batchWriteMgr.requestLoad(
                tableId4, params, UserIdentity.ROOT, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result1.isOk());
        assertNotNull(result1.getValue());
        assertEquals(1, batchWriteMgr.numJobs());

        RequestLoadResult result2 = batchWriteMgr.requestLoad(
                tableId4, params, UserIdentity.ROOT, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(result1.getValue(), result2.getValue());
        assertEquals(1, batchWriteMgr.numJobs());
    }

    @Test
    public void testCheckParameters() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>());
        RequestCoordinatorBackendResult result1 =
                batchWriteMgr.requestCoordinatorBackends(tableId1, params1, UserIdentity.ROOT);
        assertFalse(result1.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result1.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numJobs());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
            }});
        RequestCoordinatorBackendResult result2 =
                batchWriteMgr.requestCoordinatorBackends(tableId2, params2, UserIdentity.ROOT);
        assertFalse(result2.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result2.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numJobs());

        StreamLoadKvParams params3 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
                put(HTTP_WAREHOUSE, "no_exist");
            }});
        RequestCoordinatorBackendResult result3 =
                batchWriteMgr.requestCoordinatorBackends(tableId3, params3, UserIdentity.ROOT);
        assertFalse(result3.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result3.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numJobs());
    }

    @Test
    public void testCleanupInactiveJobs() {
        StreamLoadKvParams params1 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "10000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestCoordinatorBackendResult result1 = batchWriteMgr.requestCoordinatorBackends(tableId1, params1, UserIdentity.ROOT);
        assertTrue(result1.isOk());
        assertEquals(1, batchWriteMgr.numJobs());

        StreamLoadKvParams params2 = new StreamLoadKvParams(new HashMap<>() {{
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "100000");
                put(HTTP_BATCH_WRITE_PARALLEL, "4");
            }});
        RequestLoadResult result2 = batchWriteMgr.requestLoad(
                tableId4, params2, UserIdentity.ROOT, allNodes.get(0).getId(), allNodes.get(0).getHost());
        assertTrue(result2.isOk());
        assertEquals(2, batchWriteMgr.numJobs());

        new MockUp<MergeCommitJob>() {

            @Mock
            public boolean isActive() {
                return false;
            }
        };
        batchWriteMgr.cleanupInactiveJobs();
        assertEquals(0, batchWriteMgr.numJobs());
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
                assigner.registerBatchWrite(anyLong, (ComputeResource) any, (TableId) any, anyInt);
                result = new Exception("registerBatchWrite failed");
            }
        };
        RequestCoordinatorBackendResult result = batchWriteMgr.requestCoordinatorBackends(tableId1, params1, UserIdentity.ROOT);
        assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
        assertEquals(0, batchWriteMgr.numJobs());
    }

    @Test
    public void testRequestMergeCommitWithUserWarehouse() {
        String user = "user1";
        String warehouse = "test_warehouse";

        new MockUp<Utils>() {
            @Mock
            public Optional<String> getUserDefaultWarehouse(UserIdentity userIdentity) {
                return Optional.of(warehouse);
            }
        };

        // Mock WarehouseMgr
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean warehouseExists(String name) {
                return warehouse.equals(name);
            }

            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new Warehouse(1L, warehouseName, "wyb") {
                    @Override
                    public long getResumeTime() {
                        return 0L;
                    }

                    @Override
                    public Long getAnyWorkerGroupId() {
                        return 0L;
                    }

                    @Override
                    public void addNodeToCNGroup(ComputeNode node, String cnGroupName) throws DdlException {

                    }

                    @Override
                    public void validateRemoveNodeFromCNGroup(ComputeNode node, String cnGroupName) throws DdlException {

                    }

                    @Override
                    public List<Long> getWorkerGroupIds() {
                        return List.of();
                    }

                    @Override
                    public List<String> getWarehouseInfo() {
                        return List.of();
                    }

                    @Override
                    public List<List<String>> getWarehouseNodesInfo() {
                        return List.of();
                    }

                    @Override
                    public ProcResult fetchResult() {
                        return null;
                    }

                    @Override
                    public void createCNGroup(CreateCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void dropCNGroup(DropCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void replayInternalOpLog(String payload) {

                    }

                    @Override
                    public boolean isAvailable() {
                        return false;
                    }
                };
            }
        };

        StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>() {
            {
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "1");
            }
        });

        RequestCoordinatorBackendResult result = batchWriteMgr.requestCoordinatorBackends(
                tableId1, params, new UserIdentity(user, "%"));
        assertTrue(result.isOk());
        ConcurrentHashMap<BatchWriteId, MergeCommitJob> jobs = Deencapsulation.getField(batchWriteMgr, "mergeCommitJobs");
        assertEquals(1, jobs.size());
        MergeCommitJob job = jobs.values().iterator().next();
        assertEquals(warehouse, Deencapsulation.getField(job, "warehouseName"));
    }

    @Test
    public void testRequestMergeCommitWithUserWarehouseMismatch() {
        String user = "user1";
        String warehouse1 = "warehouse_1";
        String warehouse2 = "warehouse_2";

        new MockUp<Utils>() {
            private int count = 0;

            @Mock
            public Optional<String> getUserDefaultWarehouse(UserIdentity userIdentity) {
                if (count == 0) {
                    count++;
                    return Optional.of(warehouse1);
                }
                return Optional.of(warehouse2);
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public boolean warehouseExists(String name) {
                return true;
            }

            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new Warehouse(1L, warehouseName, "wyb") {
                    @Override
                    public long getResumeTime() {
                        return 0L;
                    }

                    @Override
                    public Long getAnyWorkerGroupId() {
                        return 0L;
                    }

                    @Override
                    public void addNodeToCNGroup(ComputeNode node, String cnGroupName) throws DdlException {

                    }

                    @Override
                    public void validateRemoveNodeFromCNGroup(ComputeNode node, String cnGroupName) throws DdlException {

                    }

                    @Override
                    public List<Long> getWorkerGroupIds() {
                        return List.of();
                    }

                    @Override
                    public List<String> getWarehouseInfo() {
                        return List.of();
                    }

                    @Override
                    public List<List<String>> getWarehouseNodesInfo() {
                        return List.of();
                    }

                    @Override
                    public ProcResult fetchResult() {
                        return null;
                    }

                    @Override
                    public void createCNGroup(CreateCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void dropCNGroup(DropCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException {

                    }

                    @Override
                    public void replayInternalOpLog(String payload) {

                    }

                    @Override
                    public boolean isAvailable() {
                        return true;
                    }
                };
            }
        };

        StreamLoadKvParams params = new StreamLoadKvParams(new HashMap<>() {
            {
                put(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
                put(HTTP_BATCH_WRITE_PARALLEL, "1");
            }
        });

        RequestCoordinatorBackendResult result = batchWriteMgr.requestCoordinatorBackends(
                tableId1, params, new UserIdentity(user, "%"));
        assertTrue(result.isOk());

        RequestCoordinatorBackendResult result2 = batchWriteMgr.requestCoordinatorBackends(
                tableId1, params, new UserIdentity(user, "%"));
        assertFalse(result2.isOk());
        assertEquals(TStatusCode.INVALID_ARGUMENT, result2.getStatus().getStatus_code());
        assertTrue(result2.getStatus().getError_msgs().get(0).contains("does not match"));
    }
}
