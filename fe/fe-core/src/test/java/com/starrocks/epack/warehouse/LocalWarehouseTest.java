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

package com.starrocks.epack.warehouse;

import com.google.common.collect.ImmutableSet;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.epack.warehouse.cngroup.CNGroupMetricEntity;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.metric.PrometheusMetricVisitor;
import com.starrocks.persist.WarehouseInternalOpLog;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.warehouse.WarehouseMetricEntity;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalWarehouseTest extends LocalWarehouseTestBase {
    @BeforeClass
    public static void init() {
        setupBeforeClass();
    }

    @Test
    public void testCreateWarehouseWithBuiltinCNGroup() {
        String warehouseName = randomWarehouseName();
        // builtin cngroup will be created along with the warehouse creation.
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Cluster c = wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNotNull(c);
        Assert.assertEquals(1L, wh.getClusters().size());

        // builtin cngroup can be dropped with no problem.
        ensureCnGroupDropped(warehouseName, LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNull(wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME));
        Assert.assertEquals(0L, wh.getClusters().size());

        ensureWarehouseDropped(warehouseName);
    }

    @Test
    public void testGetWarehouseInfo() {
        String warehouseName = randomWarehouseName();
        // remove the milliseconds part
        long timeLowBound = System.currentTimeMillis() / 1000 * 1000;
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        // remove the milliseconds part
        long timeUpBound = System.currentTimeMillis() / 1000 * 1000;
        List<String> whInfo = wh.getWarehouseInfo();
        // ShowWarehousesStmt.META_DATA
        Assert.assertEquals(14L, whInfo.size());
        int index = 0;
        // 1. ID
        Assert.assertEquals(String.valueOf(wh.getId()), whInfo.get(index++));
        // 2. Name
        Assert.assertEquals(wh.getName(), whInfo.get(index++));
        // 3. State
        Assert.assertEquals("AVAILABLE", whInfo.get(index++));
        // 4. NodeCount
        Assert.assertEquals(String.valueOf(0), whInfo.get(index++));
        // 5. CurrentClusterCount, contains the `_builtin_cngroup_0_`
        Assert.assertEquals(String.valueOf(1), whInfo.get(index++));
        // 6. MaxClusterCount
        Assert.assertEquals(String.valueOf(-1), whInfo.get(index++));
        // 7. StartedClusters
        Assert.assertEquals(String.valueOf(1), whInfo.get(index++));
        // 8. RunningSql
        Assert.assertEquals(String.valueOf(0), whInfo.get(index++));
        // 9. QueuedSql
        Assert.assertEquals(String.valueOf(0), whInfo.get(index++));
        // 10. CreatedOn
        String createdStr = whInfo.get(index++);
        long createdTS = TimeUtils.timeStringToLong(createdStr);
        String msg = String.format(
                "CreatedTime between [%d, %d], actual ts: %d=%s", timeLowBound, timeUpBound, createdTS, createdStr);
        Assert.assertTrue(msg, createdTS >= timeLowBound && createdTS <= timeUpBound);
        // 11. ResumedOn
        Assert.assertEquals(FeConstants.NULL_STRING, whInfo.get(index++));
        // 12. UpdatedOn
        Assert.assertEquals(FeConstants.NULL_STRING, whInfo.get(index++));
        // 13. Property
        Assert.assertEquals(wh.getProperty().toString(), whInfo.get(index++));
        // 14. Comment
        Assert.assertEquals(13L, index);
        Assert.assertNull(whInfo.get(index));

        // create a new cngroup and then check the warehouse info again, two CNGroups there
        String cngroupName = randomCNGroupName();
        ensureCnGroupCreated(warehouseName, cngroupName);
        {
            List<String> whInfo2 = wh.getWarehouseInfo();
            // 4. NodeCount
            Assert.assertEquals(String.valueOf(0), whInfo2.get(3));
            // 5. CurrentClusterCount
            Assert.assertEquals(String.valueOf(2), whInfo2.get(4));
            // 6. MaxClusterCount
            Assert.assertEquals(String.valueOf(-1), whInfo2.get(5));
            // 7. StartedClusters
            Assert.assertEquals(String.valueOf(2), whInfo2.get(6));
        }
        ensureCnGroupDropped(warehouseName, cngroupName);
        ensureWarehouseDropped(warehouseName);
    }

    @Test
    public void testReplayCNGroupOpLogs() {
        WarehouseManager manager = GlobalStateMgr.getServingState().getWarehouseMgr();
        String warehouseName = randomWarehouseName();
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        String cngroupName = randomCNGroupName();

        { // Create a new CNGROUP
            Assert.assertEquals(1, wh.getClusters().size());
            Assert.assertNotNull(wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME));

            Cluster cluster = new Cluster(1024, cngroupName, 1025);
            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.createCNGroupOpLog(cluster);
            WarehouseInternalOpLog whOpLog = new WarehouseInternalOpLog(warehouseName, opLog.toJson());

            Assert.assertEquals(1, wh.getClusters().size());
            manager.replayInternalOpLog(whOpLog);
            // the cngroup should be created to the warehouse
            Assert.assertEquals(2, wh.getClusters().size());
            Cluster c = wh.getCluster(cngroupName);
            Assert.assertNotNull(c);
            Assert.assertEquals(cluster.getId(), c.getId());
            Assert.assertEquals(cluster.getName(), c.getName());
            Assert.assertEquals(cluster.getWorkerGroupId(), c.getWorkerGroupId());
        }
        { // Disable the cngroup
            Cluster c = getClusterByName(warehouseName, cngroupName);
            Assert.assertNotNull(c);
            Assert.assertTrue(c.isEnabled());

            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.disableCNGroupOpLog(cngroupName);
            WarehouseInternalOpLog whOpLog = new WarehouseInternalOpLog(warehouseName, opLog.toJson());
            manager.replayInternalOpLog(whOpLog);
            Assert.assertFalse(c.isEnabled());
        }
        { // Enable the cngroup
            Cluster c = getClusterByName(warehouseName, cngroupName);
            Assert.assertNotNull(c);
            Assert.assertFalse(c.isEnabled());

            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.enableCNGroupOpLog(cngroupName);
            WarehouseInternalOpLog whOpLog = new WarehouseInternalOpLog(warehouseName, opLog.toJson());
            manager.replayInternalOpLog(whOpLog);
            Assert.assertTrue(c.isEnabled());
        }
        { // Drop the cngroup
            Cluster c = getClusterByName(warehouseName, cngroupName);
            Assert.assertNotNull(c);

            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.dropCNGroupOpLog(cngroupName);
            WarehouseInternalOpLog whOpLog = new WarehouseInternalOpLog(warehouseName, opLog.toJson());
            manager.replayInternalOpLog(whOpLog);

            Assert.assertNull(getClusterByName(warehouseName, cngroupName));
            Assert.assertEquals(1, wh.getClusters().size());
        }
        ensureWarehouseDropped(warehouseName);
    }

    @Test
    public void testShowCnGroup() throws DdlException {
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        { // single cngroup, default warehouse
            ShowClustersStmt stmt = new ShowClustersStmt("default_warehouse");
            ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
            Assert.assertEquals(new ShowResultMetaFactory().getMetadata(stmt).getColumnCount(),
                    resultSet.getMetaData().getColumnCount());
            Assert.assertEquals(1L, resultSet.getResultRows().size());

            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(8L, resultSet.numColumns());
            // 0. CNGroupId
            Assert.assertEquals("CNGroupId", resultSet.getMetaData().getColumn(0).getName());
            Assert.assertEquals(String.valueOf(LocalWarehouse.DEFAULT_CLUSTER_ID), resultSet.getString(0));
            // 1. CNGroupName
            Assert.assertEquals("CNGroupName", resultSet.getMetaData().getColumn(1).getName());
            Assert.assertEquals(LocalWarehouse.DEFAULT_CLUSTER_NAME, resultSet.getString(1));
            // 2. WorkerGroupId
            Assert.assertEquals("WorkerGroupId", resultSet.getMetaData().getColumn(2).getName());
            Assert.assertEquals(String.valueOf(StarOSAgent.DEFAULT_WORKER_GROUP_ID), resultSet.getString(2));
            // 3. ComputeNodeIds
            // NOTE: there is a backend:10001 added when the miniCluster is created.
            // refer to UtFrameUtils.createMinStarRocksCluster
            Assert.assertEquals("ComputeNodeIds", resultSet.getMetaData().getColumn(3).getName());
            Assert.assertEquals("10001", resultSet.getString(3));
            // 4. Pending
            Assert.assertEquals("Pending", resultSet.getMetaData().getColumn(4).getName());
            Assert.assertEquals("-1", resultSet.getString(4));
            // 5. Running
            Assert.assertEquals("Running", resultSet.getMetaData().getColumn(5).getName());
            Assert.assertEquals("-1", resultSet.getString(5));
            // 6. Enabled
            Assert.assertEquals("Enabled", resultSet.getMetaData().getColumn(6).getName());
            Assert.assertEquals("true", resultSet.getString(6));
            // 7. Properties
            Assert.assertEquals("Properties", resultSet.getMetaData().getColumn(7).getName());
            Assert.assertEquals("{}", resultSet.getString(7));
        }
        { // add a second cngroup into default warehouse
            LocalWarehouse wh = (LocalWarehouse) warehouseManager.getWarehouseAllowNull("default_warehouse");
            Assert.assertNotNull(wh);
            String cngroupName = randomCNGroupName();
            Cluster c = ensureCnGroupCreated("default_warehouse", cngroupName);
            c.setDisabled();

            Map<String, String> props = new HashMap<>();
            props.put("a", "aa");
            props.put("b", "bb");
            c.updateProperties(props);

            Assert.assertEquals(2, wh.getClusters().size());

            ShowClustersStmt stmt = new ShowClustersStmt("default_warehouse");
            ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
            Assert.assertEquals(new ShowResultMetaFactory().getMetadata(stmt).getColumnCount(),
                    resultSet.getMetaData().getColumnCount());
            Assert.assertEquals(8L, resultSet.numColumns());
            Assert.assertEquals(2L, resultSet.getResultRows().size());

            // builtin cngroup
            Assert.assertTrue(resultSet.next());
            // cngroup_2
            Assert.assertTrue(resultSet.next());
            // 0. CNGroupId
            Assert.assertEquals(String.valueOf(c.getId()), resultSet.getString(0));
            // 1. CNGroupName
            Assert.assertEquals(c.getName(), resultSet.getString(1));
            // 2. WorkerGroupId
            Assert.assertEquals(String.valueOf(c.getWorkerGroupId()), resultSet.getString(2));
            // 3. ComputeNodeIds
            Assert.assertEquals("", resultSet.getString(3));
            // 4. Pending
            Assert.assertEquals("-1", resultSet.getString(4));
            // 5. Running
            Assert.assertEquals("-1", resultSet.getString(5));
            // 6. Enabled
            Assert.assertEquals("false", resultSet.getString(6));
            // 7. Properties
            Assert.assertEquals("{\"a\":\"aa\",\"b\":\"bb\"}", resultSet.getString(7));

            ensureCnGroupDropped("default_warehouse", cngroupName);
        }
    }

    private static class NodeAddressCnGroup {
        String nodeAddress;
        String cnGroupName;
        ComputeNode computeNode;
    }

    @Test
    public void testShowNodesStmt() {
        String warehouseName = randomWarehouseName();
        String cnGroupName = randomCNGroupName();
        String builtinCnGroupName = LocalWarehouse.DEFAULT_CLUSTER_NAME;
        ensureWarehouseCreated(warehouseName);
        ensureCnGroupCreated(warehouseName, cnGroupName);

        List<NodeAddressCnGroup> testNodes = new ArrayList<>();
        testNodes.add(new NodeAddressCnGroup());
        testNodes.add(new NodeAddressCnGroup());
        testNodes.add(new NodeAddressCnGroup());
        testNodes.get(0).cnGroupName = builtinCnGroupName;
        testNodes.get(1).cnGroupName = cnGroupName;
        testNodes.get(2).cnGroupName = cnGroupName;

        for (NodeAddressCnGroup testNode : testNodes) {
            testNode.nodeAddress = randomNodeAddress();
            String sql =
                    "ALTER SYSTEM ADD COMPUTE NODE '" + testNode.nodeAddress + "' INTO WAREHOUSE " + warehouseName
                            + " CNGROUP " + testNode.cnGroupName;
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));

            ComputeNode node = getComputeNode(testNode.nodeAddress);
            testNode.computeNode = node;
            Assert.assertNotNull(testNode.computeNode);

            node.setStarletPort(node.getHeartbeatPort());
            String workerAddress = NetUtils.getHostPortInAccessibleFormat(node.getHost(), node.getHeartbeatPort());
            GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .addWorker(node.getId(), workerAddress, node.getWorkerGroupId());
        }

        int warehouseNameIndex = 0;
        int cnGroupNameIndex = 20;
        int nodeAddressIndex = 5;
        {
            ShowNodesStmt stmt = new ShowNodesStmt(warehouseName, null, null, NodePosition.ZERO);
            ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
            Assert.assertEquals(3L, resultSet.getResultRows().size());
            for (int row = 0; row < 3; ++row) {
                Assert.assertEquals(warehouseName, resultSet.getResultRows().get(row).get(warehouseNameIndex));
                Assert.assertEquals(testNodes.get(row).computeNode.getHost(),
                        resultSet.getResultRows().get(row).get(nodeAddressIndex));
                Assert.assertEquals(testNodes.get(row).cnGroupName,
                        resultSet.getResultRows().get(row).get(cnGroupNameIndex));
            }
        }
        {
            ShowNodesStmt stmt = new ShowNodesStmt(warehouseName, builtinCnGroupName, null, NodePosition.ZERO);
            ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
            Assert.assertEquals(1L, resultSet.getResultRows().size());
            Assert.assertEquals(warehouseName, resultSet.getResultRows().get(0).get(warehouseNameIndex));
            Assert.assertEquals(testNodes.get(0).computeNode.getHost(),
                    resultSet.getResultRows().get(0).get(nodeAddressIndex));
            Assert.assertEquals(testNodes.get(0).cnGroupName, resultSet.getResultRows().get(0).get(cnGroupNameIndex));
        }
        {
            ShowNodesStmt stmt = new ShowNodesStmt(warehouseName, cnGroupName, null, NodePosition.ZERO);
            ShowResultSet resultSet = ShowExecutor.execute(stmt, connectContext);
            Assert.assertEquals(2L, resultSet.getResultRows().size());
            for (int row = 0; row < 2; ++row) {
                // testNodes[1] and testNodes[2] are in the same cngroup
                Assert.assertEquals(warehouseName, resultSet.getResultRows().get(row).get(warehouseNameIndex));
                Assert.assertEquals(testNodes.get(row + 1).computeNode.getHost(),
                        resultSet.getResultRows().get(row).get(nodeAddressIndex));
                Assert.assertEquals(testNodes.get(row + 1).cnGroupName,
                        resultSet.getResultRows().get(row).get(cnGroupNameIndex));
            }
        }

        ensureCnGroupDropped(warehouseName, cnGroupName);
        ensureWarehouseDropped(warehouseName);
    }

    @Test
    public void testGetClusterByWorkGroupId() {
        String warehouseName = randomWarehouseName();
        // builtin cngroup will be created along with the warehouse creation.
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Cluster c1 = wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNotNull(c1);
        Assert.assertEquals(1L, wh.getClusters().size());
        Cluster c2 = wh.getClusterByWorkGroupId(c1.getWorkerGroupId());
        Assert.assertNotNull(c2);
        Assert.assertEquals(1L, wh.getClusters().size());
    }

    @Test
    public void testOnWarehouseCreateDrop() {
        String warehouseName = randomWarehouseName();
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Cluster c = wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNotNull(c);
        Assert.assertEquals(1L, wh.getClusters().size());

        BaseSlotManager baseSlotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        Assert.assertNotNull(baseSlotManager);
        WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) baseSlotManager;

        // check warehouse metrics
        Map<Long, WarehouseMetricEntity> warehouseMetricEntityMap =
                warehouseSlotManager.getWarehouseMetrics();
        Assert.assertFalse(warehouseMetricEntityMap.containsKey(wh.getId()));

        // enable query queue
        WarehouseProperty property = wh.getProperty();
        property.setEnableQueryQueue(true);

        warehouseMetricEntityMap =
                warehouseSlotManager.getWarehouseMetrics();
        Assert.assertTrue(warehouseMetricEntityMap.containsKey(wh.getId()));

        ensureWarehouseDropped(warehouseName);
        warehouseMetricEntityMap =
                warehouseSlotManager.getWarehouseMetrics();
        Assert.assertFalse(warehouseMetricEntityMap.containsKey(wh.getId()));
    }

    @Test
    public void testOnCNGroupCreateAndDrop() {
        String warehouseName = randomWarehouseName();
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Cluster c = wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNotNull(c);
        Assert.assertEquals(1L, wh.getClusters().size());

        String cnGroupName = randomCNGroupName();
        Cluster cnGroup = ensureCnGroupCreated(warehouseName, cnGroupName);
        Assert.assertNotNull(cnGroup);

        BaseSlotManager baseSlotManager =
                GlobalStateMgr.getCurrentState().getSlotManager();
        WarehouseSlotManager warehouseSlotManager =
                (WarehouseSlotManager) baseSlotManager;
        Map<ComputeResource, CNGroupMetricEntity> cnGroupMetricEntityMap = warehouseSlotManager.getCNGroupMetrics();
        ComputeResource computeResource = CNGroupResource.of(wh.getId(), cnGroup.getWorkerGroupId());
        Assert.assertTrue(cnGroupMetricEntityMap.containsKey(computeResource));

        ensureCnGroupDropped(warehouseName, cnGroupName);
        cnGroupMetricEntityMap = warehouseSlotManager.getCNGroupMetrics();
        Assert.assertFalse(cnGroupMetricEntityMap.containsKey(computeResource));
    }

    @Test
    public void testOnCNGroupMetrics() {
        String warehouseName = randomWarehouseName();
        LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Cluster c = wh.getCluster(LocalWarehouse.DEFAULT_CLUSTER_NAME);
        Assert.assertNotNull(c);
        Assert.assertEquals(1L, wh.getClusters().size());

        String cnGroupName = randomCNGroupName();
        Cluster cnGroup = ensureCnGroupCreated(warehouseName, cnGroupName);
        Assert.assertNotNull(cnGroup);

        BaseSlotManager baseSlotManager =
                GlobalStateMgr.getCurrentState().getSlotManager();
        WarehouseSlotManager warehouseSlotManager =
                (WarehouseSlotManager) baseSlotManager;
        Map<ComputeResource, CNGroupMetricEntity> cnGroupMetricEntityMap = warehouseSlotManager.getCNGroupMetrics();
        ComputeResource computeResource = CNGroupResource.of(wh.getId(), cnGroup.getWorkerGroupId());
        Assert.assertTrue(cnGroupMetricEntityMap.containsKey(computeResource));

        MetricVisitor visitor = new PrometheusMetricVisitor("fe_ut");
        warehouseSlotManager.collectWarehouseMetrics(visitor);
        Set<String> cnGroupMetrics = ImmutableSet.of(
                "cngroup_nodes_count",
                "cngroup_alive_nodes_count",
                "running_queries_count",
                "cngroup_status",
                "scheduled_queries_count",
                "success_queries_count",
                "failed_queries_count",
                "query_max_latency_ms",
                "query_avg_latency_ms",
                "avg_cpu_used_permille",
                "max_compute_node_running_queries_count"
        );
        String result = visitor.build();
        System.out.println("MetricVisitor produces: " + result);
        for (String metric : cnGroupMetrics) {
            Assert.assertTrue("Metric " + metric + " not found in result: " + result,
                    result.contains(metric));
        }

        ensureCnGroupDropped(warehouseName, cnGroupName);
        cnGroupMetricEntityMap = warehouseSlotManager.getCNGroupMetrics();
        Assert.assertFalse(cnGroupMetricEntityMap.containsKey(computeResource));
    }
}
