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

package com.starrocks.warehouse.multi;

import com.google.common.collect.Maps;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.WALApplier;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MultiWarehouseTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private NodeMgr nodeMgr;
    @Mocked
    private SystemInfoService systemInfo;

    private TestableMultiWarehouseManager mgr;

    /**
     * A manager whose journal write applies in-memory immediately (as it does on a real leader), whose id
     * allocation does not need a running GlobalStateMgr, and which talks to a fake StarMgr client.
     */
    private static class TestableMultiWarehouseManager extends MultiWarehouseManager {
        private final AtomicLong nextId = new AtomicLong(10000L);
        private final FakeStarOSAgent agent = new FakeStarOSAgent();
        private List<ResourceGroup> resourceGroups = new ArrayList<>();

        @Override
        protected List<ResourceGroup> fetchAllResourceGroups() {
            return resourceGroups;
        }

        @Override
        protected void logEdit(short op, Object payload, WALApplier applier) {
            applier.apply(payload);
        }

        @Override
        protected long allocateId() {
            return nextId.incrementAndGet();
        }

        @Override
        protected StarOSAgent getStarOSAgent() {
            return agent;
        }
    }

    /**
     * Records the worker-group calls the manager makes.
     *
     * <p>Hand-written rather than a JMockit mock on purpose: a {@code @Mocked StarOSAgent} stubs out every
     * method of the class - including ones a {@code MockUp} defines - which silently turns
     * {@code createWorkerGroup} into "return 0" and makes every warehouse share worker group 0.
     */
    private static class FakeStarOSAgent extends StarOSAgent {
        @Override
        public long createWorkerGroup(String size, int replicaNumber) {
            CREATED_SIZES.add(size);
            return NEXT_WORKER_GROUP_ID.incrementAndGet();
        }

        @Override
        public void deleteWorkerGroup(long groupId) {
            DELETED_WORKER_GROUPS.add(groupId);
        }

        @Override
        public void updateWorkerGroup(long workerGroupId, int replicaNumber) {
            UPDATED_REPLICA_NUMBERS.add(replicaNumber);
        }
    }

    private static final AtomicLong NEXT_WORKER_GROUP_ID = new AtomicLong(1000L);
    private static final List<Long> DELETED_WORKER_GROUPS = new ArrayList<>();
    private static final List<String> CREATED_SIZES = new ArrayList<>();
    private static final List<Integer> UPDATED_REPLICA_NUMBERS = new ArrayList<>();

    @BeforeAll
    public static void setUp() {
        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
    }

    @BeforeEach
    public void before() {
        mgr = new TestableMultiWarehouseManager();
        mgr.initDefaultWarehouse();
        DELETED_WORKER_GROUPS.clear();
        CREATED_SIZES.clear();
        UPDATED_REPLICA_NUMBERS.clear();
    }

    private static CreateWarehouseStmt createStmt(String name, Map<String, String> properties, boolean ifNotExists) {
        return new CreateWarehouseStmt(ifNotExists, name, properties, "az-local warehouse");
    }

    @Test
    public void testCreateWarehouse() throws DdlException {
        mgr.createWarehouse(createStmt("warehouse_az_a", null, false));
        Assertions.assertTrue(mgr.warehouseExists("warehouse_az_a"));
        Warehouse warehouse = mgr.getWarehouse("warehouse_az_a");
        Assertions.assertInstanceOf(MultiWarehouse.class, warehouse);
        Assertions.assertEquals(1, warehouse.getWorkerGroupIds().size());
        Assertions.assertNotEquals(StarOSAgent.DEFAULT_WORKER_GROUP_ID,
                warehouse.getAnyWorkerGroupId().longValue());
        Assertions.assertEquals(MultiWarehouseManager.DEFAULT_SIZE, CREATED_SIZES.get(0));
        Assertions.assertTrue(mgr.getAllWarehouseNames().contains("warehouse_az_a"));
        Assertions.assertTrue(mgr.getAllWarehouseNames().contains(WarehouseManager.DEFAULT_WAREHOUSE_NAME));

        // Each warehouse gets its own worker group, which is what keeps AZs apart.
        mgr.createWarehouse(createStmt("warehouse_az_b", null, false));
        Assertions.assertNotEquals(mgr.getWarehouse("warehouse_az_a").getAnyWorkerGroupId(),
                mgr.getWarehouse("warehouse_az_b").getAnyWorkerGroupId());
    }

    @Test
    public void testCreateWarehouseWithProperties() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(MultiWarehouseManager.PROPERTY_SIZE, "x2");
        properties.put(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER, "2");
        mgr.createWarehouse(createStmt("wh", properties, false));
        Assertions.assertEquals("x2", CREATED_SIZES.get(0));
        MultiWarehouse warehouse = (MultiWarehouse) mgr.getWarehouse("wh");
        // Declared properties are kept verbatim so SHOW WAREHOUSES reflects what the user asked for.
        Assertions.assertEquals("x2", warehouse.getProperties().get(MultiWarehouseManager.PROPERTY_SIZE));
        Assertions.assertEquals("2", warehouse.getProperties().get(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER));
    }

    @Test
    public void testCreateWarehouseAlreadyExists() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse wh already exists.",
                () -> mgr.createWarehouse(createStmt("wh", null, false)));

        // IF NOT EXISTS is a no-op and must not allocate another worker group.
        long groupId = mgr.getWarehouse("wh").getAnyWorkerGroupId();
        mgr.createWarehouse(createStmt("wh", null, true));
        Assertions.assertEquals(groupId, mgr.getWarehouse("wh").getAnyWorkerGroupId().longValue());
        Assertions.assertEquals(1, CREATED_SIZES.size());
    }

    @Test
    public void testCreateWarehouseRejectsUnknownProperty() {
        Map<String, String> properties = new HashMap<>();
        properties.put("availability_zone", "eu-west-1a");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown warehouse properties",
                () -> mgr.createWarehouse(createStmt("wh", properties, false)));
        Assertions.assertFalse(mgr.warehouseExists("wh"));
        // Nothing was allocated in StarMgr, so nothing needs cleaning up.
        Assertions.assertTrue(CREATED_SIZES.isEmpty());
        Assertions.assertTrue(DELETED_WORKER_GROUPS.isEmpty());
    }

    @Test
    public void testCreateWarehouseRejectsBadReplicaNumber() {
        Map<String, String> notANumber = new HashMap<>();
        notANumber.put(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER, "many");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "must be an integer",
                () -> mgr.createWarehouse(createStmt("wh", notANumber, false)));

        Map<String, String> zero = new HashMap<>();
        zero.put(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER, "0");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "must be >= 1",
                () -> mgr.createWarehouse(createStmt("wh", zero, false)));
    }

    @Test
    public void testDropWarehouse() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        long groupId = mgr.getWarehouse("wh").getAnyWorkerGroupId();
        mgr.dropWarehouse(new DropWarehouseStmt(false, "wh"));
        Assertions.assertFalse(mgr.warehouseExists("wh"));
        Assertions.assertEquals(List.of(groupId), DELETED_WORKER_GROUPS);
    }

    @Test
    public void testDropWarehouseNotExists() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: wh not exist.",
                () -> mgr.dropWarehouse(new DropWarehouseStmt(false, "wh")));
        ExceptionChecker.expectThrowsNoException(() -> mgr.dropWarehouse(new DropWarehouseStmt(true, "wh")));
    }

    @Test
    public void testDropDefaultWarehouseIsRejected() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Can't drop the default_warehouse",
                () -> mgr.dropWarehouse(new DropWarehouseStmt(false, WarehouseManager.DEFAULT_WAREHOUSE_NAME)));
    }

    @Test
    public void testDropWarehouseWithNodesIsRejected() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        MultiWarehouse warehouse = (MultiWarehouse) mgr.getWarehouse("wh");
        ComputeNode node = new ComputeNode(1001L, "127.0.0.1", 9050);
        warehouse.addNodeToCNGroup(node, null);
        new MockUp<MultiWarehouse>() {
            @Mock
            public List<ComputeNode> getNodes() {
                return List.of(node);
            }
        };
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "still has 1 node(s)",
                () -> mgr.dropWarehouse(new DropWarehouseStmt(false, "wh")));
        Assertions.assertTrue(mgr.warehouseExists("wh"));
        Assertions.assertTrue(DELETED_WORKER_GROUPS.isEmpty());
    }

    @Test
    public void testAlterWarehouse() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        Map<String, String> changes = new HashMap<>();
        changes.put(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER, "3");
        mgr.alterWarehouse(new AlterWarehouseStmt("wh", changes));
        Assertions.assertEquals(List.of(3), UPDATED_REPLICA_NUMBERS);
        MultiWarehouse warehouse = (MultiWarehouse) mgr.getWarehouse("wh");
        Assertions.assertEquals("3", warehouse.getProperties().get(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER));
        Assertions.assertTrue(warehouse.getUpdatedTime() >= warehouse.getCreatedTime());
    }

    @Test
    public void testAlterWarehouseRejectsImmutableProperty() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        Map<String, String> changes = new HashMap<>();
        changes.put(MultiWarehouseManager.PROPERTY_SIZE, "x4");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "can not be altered",
                () -> mgr.alterWarehouse(new AlterWarehouseStmt("wh", changes)));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "No property to alter",
                () -> mgr.alterWarehouse(new AlterWarehouseStmt("wh", Maps.newHashMap())));
    }

    @Test
    public void testAlterUnknownWarehouse() {
        Map<String, String> changes = new HashMap<>();
        changes.put(MultiWarehouseManager.PROPERTY_REPLICA_NUMBER, "3");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Warehouse name: nope not exist.",
                () -> mgr.alterWarehouse(new AlterWarehouseStmt("nope", changes)));
    }

    @Test
    public void testSuspendResumeUnsupported() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "SUSPEND WAREHOUSE is not supported",
                () -> mgr.suspendWarehouse(new SuspendWarehouseStmt("wh")));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "RESUME WAREHOUSE is not supported",
                () -> mgr.resumeWarehouse(new ResumeWarehouseStmt("wh")));
    }

    @Test
    public void testNodeAssignment() throws DdlException {
        mgr.createWarehouse(createStmt("warehouse_az_a", null, false));
        MultiWarehouse warehouse = (MultiWarehouse) mgr.getWarehouse("warehouse_az_a");
        ComputeNode node = new ComputeNode(1001L, "10.0.1.10", 9050);
        warehouse.addNodeToCNGroup(node, null);

        // This is the whole point: the node lands in the warehouse's worker group, so the scheduler will only
        // pick it for sessions pinned to that warehouse.
        Assertions.assertEquals(warehouse.getAnyWorkerGroupId().longValue(), node.getWorkerGroupId());
        Assertions.assertEquals(warehouse.getId(), node.getWarehouseId());

        // CNGROUPs are not implemented, so naming one must fail rather than silently ignore it.
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "CNGroup feature not implemented",
                () -> warehouse.addNodeToCNGroup(node, "cngroup_1"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "CNGroup feature not implemented",
                () -> warehouse.validateRemoveNodeFromCNGroup(node, "cngroup_1"));
    }

    @Test
    public void testWarehouseInfoColumnCount() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        List<String> info = mgr.getWarehouse("wh").getWarehouseInfo();
        // Must match ShowResultMetaFactory#visitShowWarehousesStatement.
        Assertions.assertEquals(14, info.size());
        Assertions.assertEquals("wh", info.get(1));
        Assertions.assertEquals("az-local warehouse", info.get(13));
    }

    private static ResourceGroup resourceGroup(String name, String... warehouses) {
        ResourceGroup group = new ResourceGroup();
        group.setName(name);
        if (warehouses.length > 0) {
            group.setWarehouses(List.of(warehouses));
        }
        return group;
    }

    /**
     * A resource group binds warehouses by name and only validates them at CREATE/ALTER time, so dropping a
     * bound warehouse would leave a binding that silently stops matching.
     */
    @Test
    public void testDropWarehouseBoundByResourceGroupIsRejected() throws DdlException {
        mgr.createWarehouse(createStmt("warehouse_az_b", null, false));
        mgr.resourceGroups = List.of(
                resourceGroup("rg_global"), // bound to nothing: applies everywhere
                resourceGroup("rg_az_b", "warehouse_az_b"),
                resourceGroup("rg_both", "warehouse_az_a", "warehouse_az_b"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "is still bound by resource group(s) [rg_az_b, rg_both]",
                () -> mgr.dropWarehouse(new DropWarehouseStmt(false, "warehouse_az_b")));
        Assertions.assertTrue(mgr.warehouseExists("warehouse_az_b"));
        // The StarMgr worker group must survive a refused drop.
        Assertions.assertTrue(DELETED_WORKER_GROUPS.isEmpty());

        // Once the bindings are gone the drop succeeds.
        mgr.resourceGroups = List.of(resourceGroup("rg_global"));
        long groupId = mgr.getWarehouse("warehouse_az_b").getAnyWorkerGroupId();
        mgr.dropWarehouse(new DropWarehouseStmt(false, "warehouse_az_b"));
        Assertions.assertFalse(mgr.warehouseExists("warehouse_az_b"));
        Assertions.assertEquals(List.of(groupId), DELETED_WORKER_GROUPS);
    }

    @Test
    public void testBoundResourceGroupNames() {
        List<ResourceGroup> groups = List.of(
                resourceGroup("rg_none"),
                resourceGroup("rg_b", "warehouse_az_b"),
                resourceGroup("rg_a", "warehouse_az_a"),
                resourceGroup("rg_ab", "warehouse_az_a", "warehouse_az_b"));
        // Sorted, so the error message is deterministic.
        Assertions.assertEquals(List.of("rg_a", "rg_ab"),
                MultiWarehouseManager.boundResourceGroupNames("warehouse_az_a", groups));
        Assertions.assertEquals(List.of("rg_ab", "rg_b"),
                MultiWarehouseManager.boundResourceGroupNames("warehouse_az_b", groups));
        // A group bound to nothing applies to every warehouse and never blocks a drop.
        Assertions.assertEquals(List.of(),
                MultiWarehouseManager.boundResourceGroupNames("warehouse_az_c", groups));
        // Case-sensitive, mirroring ResourceGroupMgr#isResourceGroupMatchWarehouse (List#contains): a binding
        // that differs only in case never matched a query, so it is not a reference worth blocking a drop for.
        Assertions.assertEquals(List.of(),
                MultiWarehouseManager.boundResourceGroupNames("WAREHOUSE_AZ_A", groups));
    }

    @Test
    public void testBackgroundWarehouseFollowsConfig() throws DdlException {
        mgr.createWarehouse(createStmt("wh", null, false));
        // Default: background work stays in default_warehouse.
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME, mgr.getBackgroundWarehouse().getName());

        String saved = Config.lake_background_warehouse;
        try {
            Config.lake_background_warehouse = "wh";
            Assertions.assertEquals("wh", mgr.getBackgroundWarehouse().getName());
            // A config pointing at a warehouse that does not exist must not break background jobs.
            Config.lake_background_warehouse = "gone";
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_NAME, mgr.getBackgroundWarehouse().getName());
        } finally {
            Config.lake_background_warehouse = saved;
        }
    }

    @Test
    public void testCompactionComputeResourceFollowsConfig() throws DdlException {
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };
        mgr.createWarehouse(createStmt("wh", null, false));

        String saved = Config.lake_compaction_warehouse;
        try {
            Config.lake_compaction_warehouse = "wh";
            Assertions.assertEquals(mgr.getWarehouse("wh").getId(),
                    mgr.getCompactionComputeResource(1L).getWarehouseId());
            Config.lake_compaction_warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
            Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                    mgr.getCompactionComputeResource(1L).getWarehouseId());
        } finally {
            Config.lake_compaction_warehouse = saved;
        }
    }

    /**
     * The image and the edit log both round-trip warehouses through the abstract {@link Warehouse} type, which
     * only works if MultiWarehouse is registered as a gson subtype.
     */
    @Test
    public void testGsonRoundTrip() {
        Map<String, String> properties = new HashMap<>();
        properties.put(MultiWarehouseManager.PROPERTY_SIZE, "x1");
        MultiWarehouse original = new MultiWarehouse(123L, "wh", "comment", 456L, properties, 1000L);
        String json = GsonUtils.GSON.toJson(original);
        Assertions.assertTrue(json.contains("MultiWarehouse"), "type discriminator missing: " + json);
        Warehouse restored = GsonUtils.GSON.fromJson(json, Warehouse.class);
        Assertions.assertInstanceOf(MultiWarehouse.class, restored);
        Assertions.assertEquals(123L, restored.getId());
        Assertions.assertEquals("wh", restored.getName());
        Assertions.assertEquals("comment", restored.getComment());
        Assertions.assertEquals(456L, restored.getAnyWorkerGroupId().longValue());
        Assertions.assertEquals("x1", ((MultiWarehouse) restored).getProperties()
                .get(MultiWarehouseManager.PROPERTY_SIZE));
        Assertions.assertEquals(1000L, ((MultiWarehouse) restored).getCreatedTime());
    }
}
