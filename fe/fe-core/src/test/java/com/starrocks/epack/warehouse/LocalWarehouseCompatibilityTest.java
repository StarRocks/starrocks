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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class LocalWarehouseCompatibilityTest {
    private static final Logger LOG = LogManager.getLogger(LocalWarehouseCompatibilityTest.class);

    // simulate the Cluster before cngroup implementation
    public static class ClusterV1 {
        @SerializedName(value = "id")
        public long id;
        @SerializedName(value = "wgid")
        public long workerGroupId;

        public ClusterV1() {
        }
    }

    // simulate the LocalWarehouse before cngroup implementation
    public static class LocalWarehouseV1 extends Warehouse {
        @SerializedName(value = "cluster")
        public ClusterV1 cluster;
        @SerializedName(value = "state")
        public LocalWarehouse.WarehouseState state = LocalWarehouse.WarehouseState.AVAILABLE;
        @SerializedName(value = "ctime")
        public volatile long createdTime;
        @SerializedName(value = "rtime")
        public volatile long resumedTime;
        @SerializedName(value = "mtime")
        public volatile long updatedTime;
        @SerializedName(value = "property")
        public WarehouseProperty property;

        public LocalWarehouseV1() {
            super(0, "", "");
        }

        public LocalWarehouseV1(long id, String name, String comment) {
            super(id, name, comment);
        }

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
            return null;
        }

        @Override
        public List<String> getWarehouseInfo() {
            return null;
        }

        @Override
        public List<List<String>> getWarehouseNodesInfo() {
            return null;
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
    }

    @Test
    public void testCNGroupUpgradeCompatibility() {
        { // default warehouse upgrade, single cngroup
            LocalWarehouseV1 defaultWarehouse =
                    new LocalWarehouseV1(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                            "");
            ClusterV1 c1 = new ClusterV1();
            c1.id = LocalWarehouse.DEFAULT_CLUSTER_ID;
            c1.workerGroupId = StarOSAgent.DEFAULT_WORKER_GROUP_ID;
            defaultWarehouse.cluster = c1;

            String jsonString = GsonUtils.GSON.toJson(defaultWarehouse);
            // {"cluster":{"id":0,"wgid":0},"state":"AVAILABLE","ctime":0,"rtime":0,"mtime":0,"name":"default_warehouse","id":0,"comment":""}
            LOG.warn("Simulating default warehouse JSONString before upgrade: {}", jsonString);

            LocalWarehouse wh = GsonUtils.GSON.fromJson(jsonString, LocalWarehouse.class);
            // check clusters set correctly
            Object obj = Deencapsulation.getField(wh, "clusters");
            Assert.assertNotNull(obj);
            Assert.assertTrue(obj instanceof List<?>);
            List<Cluster> clusters = (List<Cluster>) obj;
            Assert.assertEquals(1, clusters.size());
            Cluster cluster = clusters.get(0);
            Assert.assertEquals(c1.id, cluster.getId());
            Assert.assertEquals(c1.workerGroupId, cluster.getWorkerGroupId());
            // name set to the default one
            Assert.assertEquals(LocalWarehouse.DEFAULT_CLUSTER_NAME, cluster.getName());
            Assert.assertTrue(cluster.isEnabled());
        }
        { // non-default warehouse upgrade, single cngroup
            LocalWarehouseV1 warehouse = new LocalWarehouseV1(3456, "ingestion_wh", "ingestion_wh");
            ClusterV1 c1 = new ClusterV1();
            c1.id = 1035;
            c1.workerGroupId = 1036;
            warehouse.cluster = c1;

            String jsonString = GsonUtils.GSON.toJson(warehouse);
            // {"cluster":{"id":1035,"wgid":1036},"state":"AVAILABLE","ctime":0,"rtime":0,"mtime":0,"name":"ingestion_wh","id":3456,"comment":"ingestion_wh"}
            LOG.warn("Simulating non-default warehouse JSONString before upgrade: {}", jsonString);

            LocalWarehouse wh = GsonUtils.GSON.fromJson(jsonString, LocalWarehouse.class);
            // check clusters set correctly
            Object obj = Deencapsulation.getField(wh, "clusters");
            Assert.assertNotNull(obj);
            Assert.assertTrue(obj instanceof List<?>);
            List<Cluster> clusters = (List<Cluster>) obj;
            Assert.assertEquals(1, clusters.size());
            Cluster cluster = clusters.get(0);
            Assert.assertEquals(c1.id, cluster.getId());
            Assert.assertEquals(c1.workerGroupId, cluster.getWorkerGroupId());
            // name set to the default one
            Assert.assertEquals(LocalWarehouse.DEFAULT_CLUSTER_NAME, cluster.getName());
            Assert.assertTrue(cluster.isEnabled());
        }
    }

    String removeClazzFromJsonString(String jsonStr) {
        JSONObject json = new JSONObject(jsonStr);
        json.remove("clazz");
        return json.toString();
    }

    @Test
    public void testCNGroupDowngradeCompatibility() {
        { // default warehouse, single cngroup
            LocalWarehouse defaultWarehouse = LocalWarehouse.createDefaultLocalWarehouse("default");
            String jsonString = GsonUtils.GSON.toJson(defaultWarehouse);
            jsonString = removeClazzFromJsonString(jsonString);
            LOG.warn("Simulating default warehouse JSONString before downgrade: {}", jsonString);

            LocalWarehouseV1 whV1 = GsonUtils.GSON.fromJson(jsonString, LocalWarehouseV1.class);
            Assert.assertEquals(defaultWarehouse.getId(), whV1.getId());
            Assert.assertEquals(defaultWarehouse.getName(), whV1.getName());
            Assert.assertEquals(defaultWarehouse.getAnyWorkerGroupId(), (Long) whV1.cluster.workerGroupId);
            Cluster c = defaultWarehouse.getAnyAvailableCluster();
            Assert.assertNotNull(c);
            Assert.assertEquals(c.getId(), whV1.cluster.id);
            Assert.assertEquals(c.getWorkerGroupId(), whV1.cluster.workerGroupId);
        }

        long idGen = 12000;
        { // non-default warehouse, single cngroup
            LocalWarehouse warehouse =
                    new LocalWarehouse(++idGen, "query_warehouse", new WarehouseProperty(), "query_only");
            Cluster c1 = new Cluster(++idGen, "cngroup_1", ++idGen);
            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.createCNGroupOpLog(c1);
            warehouse.replayInternalOpLog(opLog.toJson());

            String jsonString = GsonUtils.GSON.toJson(warehouse);
            // remove the 'clazz' key so the json string can be parsed by LocalWarehouseV1
            jsonString = removeClazzFromJsonString(jsonString);
            LOG.warn("Simulating non-default warehouse JSONString before downgrade: {}", jsonString);

            LocalWarehouseV1 whV1 = GsonUtils.GSON.fromJson(jsonString, LocalWarehouseV1.class);
            Assert.assertEquals(warehouse.getId(), whV1.getId());
            Assert.assertEquals(warehouse.getName(), whV1.getName());
            Assert.assertEquals(warehouse.getAnyWorkerGroupId(), (Long) whV1.cluster.workerGroupId);
            Assert.assertEquals(c1.getId(), whV1.cluster.id);
            Assert.assertEquals(c1.getWorkerGroupId(), whV1.cluster.workerGroupId);
        }

        idGen = 13000;
        { // multiple CNGroups, all the other CNGroups are lost except the first one
            LocalWarehouse warehouse =
                    new LocalWarehouse(++idGen, "api_wh", new WarehouseProperty(), "api_wh");
            Cluster c1 = new Cluster(++idGen, "cngroup_1", ++idGen);
            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.createCNGroupOpLog(c1);
            warehouse.replayInternalOpLog(opLog.toJson());

            Cluster c2 = new Cluster(++idGen, "cngroup_2", ++idGen);
            LocalWarehouseOpLog opLog2 = LocalWarehouseOpLog.createCNGroupOpLog(c2);
            warehouse.replayInternalOpLog(opLog2.toJson());

            String jsonString = GsonUtils.GSON.toJson(warehouse);
            // remove the 'clazz' key so the json string can be parsed by LocalWarehouseV1
            jsonString = removeClazzFromJsonString(jsonString);
            LOG.warn("Simulating non-default warehouse multiple CNGroups before downgrade: {}", jsonString);

            LocalWarehouseV1 whV1 = GsonUtils.GSON.fromJson(jsonString, LocalWarehouseV1.class);
            Assert.assertEquals(warehouse.getId(), whV1.getId());
            Assert.assertEquals(warehouse.getName(), whV1.getName());
            Assert.assertEquals(warehouse.getAnyWorkerGroupId(), (Long) whV1.cluster.workerGroupId);
            Assert.assertEquals(c1.getId(), whV1.cluster.id);
            Assert.assertEquals(c1.getWorkerGroupId(), whV1.cluster.workerGroupId);
            // c2 get lost
        }
    }
}
