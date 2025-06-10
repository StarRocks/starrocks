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

import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

// JUNIT-5 Style
public class CNGroupAddDropNodeTest extends LocalWarehouseTestBase {

    String addBackendNodeSql(String nodeIpAddr, String warehouseName, String cngroupName) {
        return addDropNodeSql(nodeIpAddr, warehouseName, cngroupName, false, true);
    }

    String addComputeNodeSql(String nodeIpAddr, String warehouseName, String cngroupName) {
        return addDropNodeSql(nodeIpAddr, warehouseName, cngroupName, true, true);
    }

    String addNodeSql(String nodeIpAddr, String warehouseName, String cngroupName, boolean isCnNode) {
        if (isCnNode) {
            return addComputeNodeSql(nodeIpAddr, warehouseName, cngroupName);
        } else {
            return addBackendNodeSql(nodeIpAddr, warehouseName, cngroupName);
        }
    }

    String dropBackendNodeSql(String nodeIpAddr, String warehouseName, String cngroupName) {
        return addDropNodeSql(nodeIpAddr, warehouseName, cngroupName, false, false);
    }

    String dropComputeNodeSql(String nodeIpAddr, String warehouseName, String cngroupName) {
        return addDropNodeSql(nodeIpAddr, warehouseName, cngroupName, true, false);
    }

    String dropNodeSql(String nodeIpAddr, String warehouseName, String cngroupName, boolean isCnNode) {
        if (isCnNode) {
            return dropComputeNodeSql(nodeIpAddr, warehouseName, cngroupName);
        } else {
            return dropBackendNodeSql(nodeIpAddr, warehouseName, cngroupName);
        }
    }

    String addDropNodeSql(String nodeIpAddr, String warehouseName, String cngroupName, boolean isCnNode,
                          boolean isAdd) {
        String sql = String.format("ALTER SYSTEM %s %s '%s' %s WAREHOUSE %s", isAdd ? "ADD" : "DROP",
                isCnNode ? "COMPUTE NODE" : "BACKEND", nodeIpAddr, isAdd ? "INTO" : "FROM", warehouseName);
        if (cngroupName != null) {
            sql += " CNGROUP '" + cngroupName + "'";
        }
        return sql;
    }

    ComputeNode getNode(String nodeAddress, boolean isCnNode) {
        if (isCnNode) {
            return getComputeNode(nodeAddress);
        } else {
            return getBackendNode(nodeAddress);
        }
    }

    @BeforeAll
    public static void init() {
        setupBeforeClass();
    }

    @ParameterizedTest
    @MethodSource("nodeTypes")
    public void addNodeToCnGroupTest(String nodeType) {
        boolean isCnNode = Objects.equals(nodeType, "CN");
        { // warehouse not exist
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            String cngroupName = randomCNGroupName();
            String sql = addNodeSql(nodeAddress, warehouseName, cngroupName, isCnNode);
            Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
            Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_WAREHOUSE, connectContext.getState().getErrorCode(), sql);
        }
        { // cngroup name validation
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            { // Invalid cngroup name.
                // Invalid cngroup name will be only checked during cngroup creation, so fallback to non-exist error.
                String sql = addNodeSql(nodeAddress, warehouseName, "addnode_cgroup%1", isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_CNGROUP, connectContext.getState().getErrorCode(), sql);
            }
            { // empty cngroup name
                // Empty cngroup name will be taken as no cngroup provided, and name check will be skipped.
                // Fall back to the non-exist warehouse error.
                String sql = addNodeSql(nodeAddress, warehouseName, "", isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode(),
                        sql);
            }
            { // non-exist cngroup name
                String cnGroupName = randomCNGroupName();
                String sql = addNodeSql(nodeAddress, warehouseName, cnGroupName, isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_CNGROUP, connectContext.getState().getErrorCode(), sql);
            }
            ensureWarehouseDropped(warehouseName);
        }
        { // sunny case
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            String cnGroupName = randomCNGroupName();
            Warehouse wh = ensureWarehouseCreated(warehouseName);
            Cluster cluster = ensureCnGroupCreated(warehouseName, cnGroupName);
            String sql = addNodeSql(nodeAddress, warehouseName, cnGroupName, isCnNode);
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));

            ComputeNode node = getNode(nodeAddress, isCnNode);
            Assertions.assertNotNull(node);
            Assertions.assertEquals(wh.getId(), node.getWarehouseId());
            Assertions.assertEquals(cluster.getWorkerGroupId(), node.getWorkerGroupId());

            ensureCnGroupDropped(warehouseName, cnGroupName);
            Assertions.assertNull(getNode(nodeAddress, isCnNode));
            ensureWarehouseDropped(warehouseName);
        }
    }

    @ParameterizedTest
    @MethodSource("nodeTypes")
    public void dropNodeFromCnGroupTest(String nodeType) {
        boolean isCnNode = Objects.equals(nodeType, "CN");
        { // warehouse not exist
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            String cngroupName = randomCNGroupName();
            String sql = dropNodeSql(nodeAddress, warehouseName, cngroupName, isCnNode);
            Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
            Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_WAREHOUSE, connectContext.getState().getErrorCode(), sql);
        }
        { // cngroup name validation
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            { // Invalid cngroup name.
                // Invalid cngroup name will be only checked during cngroup creation, so fallback to non-exist error.
                String sql = addNodeSql(nodeAddress, warehouseName, "dropnode_cgroup%1", isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_CNGROUP, connectContext.getState().getErrorCode(), sql);
            }
            { // empty cngroup name
                // Empty cngroup name will be taken as no cngroup provided, and cngroup name check will be skipped.
                // Fall back to the backend/computenode does not exist
                String sql = dropNodeSql(nodeAddress, warehouseName, "", isCnNode);
                DdlException exception = Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertTrue(
                        exception.getMessage().contains((isCnNode ? "compute node" : "backend") + " does not exist"),
                        exception.getMessage());
            }
            { // non-exist cngroup name
                String cnGroupName = randomCNGroupName();
                String sql = dropNodeSql(nodeAddress, warehouseName, cnGroupName, isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_UNKNOWN_CNGROUP, connectContext.getState().getErrorCode(), sql);
            }
            ensureWarehouseDropped(warehouseName);
        }
        { // sunny case
            String nodeAddress = randomNodeAddress();
            String warehouseName = randomWarehouseName();
            String cnGroupName = randomCNGroupName();
            String cnGroupName2 = randomCNGroupName();
            Warehouse wh = ensureWarehouseCreated(warehouseName);
            Cluster cluster = ensureCnGroupCreated(warehouseName, cnGroupName);
            Cluster cluster2 = ensureCnGroupCreated(warehouseName, cnGroupName2);
            { // add a node
                String sql = addNodeSql(nodeAddress, warehouseName, cnGroupName, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);
                Assertions.assertEquals(wh.getId(), node.getWarehouseId());
                Assertions.assertEquals(cluster.getWorkerGroupId(), node.getWorkerGroupId());
                // The node will be added into starmgr only when the response of the heartbeat is back.
            }
            { // drop the node with incorrect cngroup name
                String sql = dropNodeSql(nodeAddress, warehouseName, cnGroupName2, isCnNode);
                Assert.assertThrows(sql, DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_NODE_CNGROUP_MISMATCH, connectContext.getState().getErrorCode(),
                        sql);
                // drop failed, the node is still there
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);
            }
            { // drop the node with correct cngroup name
                String sql = dropNodeSql(nodeAddress, warehouseName, cnGroupName, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNull(node);
            }
            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureCnGroupDropped(warehouseName, cnGroupName2);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @ParameterizedTest
    @MethodSource("nodeTypes")
    public void addNodeCompatibilityTest(String nodeType) {
        // CNGROUP name can be omitted only when the warehouse has one cnGroup and
        // the cngroup name is LocalWarehouse.DEFAULT_CLUSTER_NAME
        boolean isCnNode = Objects.equals(nodeType, "CN");
        { // default warehouse
            { // 1 cngroup, DEFAULT_CLUSTER_NAME, single warehouse, UPGRADE FROM community version. OK!
                String nodeAddress = randomNodeAddress();
                String warehouseName = "default_warehouse";
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);
                Assertions.assertEquals(0L, node.getWorkerGroupId(), sql);
                Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
                Assertions.assertEquals(wh.getId(), node.getWarehouseId(), sql);
            }
            { // 2 cngroups. FAIL
                String nodeAddress = randomNodeAddress();
                String warehouseName = "default_warehouse";
                String cngroupName2 = randomCNGroupName();
                ensureCnGroupCreated(warehouseName, cngroupName2);
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                Assertions.assertThrows(DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode(),
                        sql);
                // node not exist
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
                ensureCnGroupDropped(warehouseName, cngroupName2);
            }
        }
        { // non-default warehouse
            List<String> cngroupNames = new ArrayList<>();
            String warehouseName = randomWarehouseName();

            LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
            { // 0 cngroup, no CNGroup. FAIL
                String nodeAddress = randomNodeAddress();
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                Assertions.assertThrows(DdlException.class, () -> starRocksAssert.ddl(sql));
                Assertions.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode(),
                        sql);
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
                Assertions.assertEquals(0L, wh.getClusters().size());
            }
            { // 1 cngroup == DEFAULT_CLUSTER_NAME, UPGRADE FROM enterprise version
                // NOTE: create an cngroup with name DEFAULT_CLUSTER_NAME should not be allowed.
                String nodeAddress = randomNodeAddress();
                Cluster cluster = ensureCnGroupCreated(warehouseName, LocalWarehouse.DEFAULT_CLUSTER_NAME);
                Assertions.assertEquals(1L, wh.getClusters().size());
                cngroupNames.add(LocalWarehouse.DEFAULT_CLUSTER_NAME);

                // Add node to the warehouse without cngroup name, OK!
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);
                Assertions.assertEquals(wh.getId(), node.getWarehouseId(), sql);
                Assertions.assertEquals(cluster.getWorkerGroupId(), node.getWorkerGroupId(), sql);
            }
            { // 2 cngroups, cngroup[0] == DEFAULT_CLUSTER_NAME
                String nodeAddress = randomNodeAddress();
                String cngroupName = randomCNGroupName();
                Cluster cluster = ensureCnGroupCreated(warehouseName, cngroupName);
                Assertions.assertEquals(2L, wh.getClusters().size());
                cngroupNames.add(cngroupName);

                // Add node to the warehouse without cngroup name, FAIL!
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                Assertions.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode(),
                        sql);
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
            }
            for (String cngroupName : cngroupNames) {
                ensureCnGroupDropped(warehouseName, cngroupName);
            }
            ensureWarehouseDropped(warehouseName);
        }
        { // non-default warehouse
            // 1 cngroup != DEFAULT_CLUSTER_NAME
            String warehouseName = randomWarehouseName();
            String nodeAddress = randomNodeAddress();
            String cngroupName = randomCNGroupName();

            LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
            Cluster cluster = ensureCnGroupCreated(warehouseName, cngroupName);
            Assertions.assertEquals(1L, wh.getClusters().size());

            // Add node to the warehouse without cngroup name, FAIL!
            String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
            Assertions.assertEquals(ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode(), sql);
            Assertions.assertNull(getNode(nodeAddress, isCnNode));

            ensureCnGroupDropped(warehouseName, cngroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @ParameterizedTest
    @MethodSource("nodeTypes")
    public void dropNodeCompatibilityTest(String nodeType) {
        // CNGROUP name can be omitted as always because the node contains the warehouseId and cngroupId.
        // the node will be removed in background with the cngroupId correctly
        boolean isCnNode = Objects.equals(nodeType, "CN");
        { // default warehouse
            { // 1 cngroup, DEFAULT_CLUSTER_NAME, single warehouse, UPGRADE FROM community version. OK!
                String nodeAddress = randomNodeAddress();
                String warehouseName = "default_warehouse";
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assertions.assertNotNull(getNode(nodeAddress, isCnNode));

                String sql2 = dropNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
            }
            { // 2 cngroups. OK
                String nodeAddress = randomNodeAddress();
                String warehouseName = "default_warehouse";
                String cngroupName2 = randomCNGroupName();
                ensureCnGroupCreated(warehouseName, cngroupName2);
                String sql = addNodeSql(nodeAddress, warehouseName, cngroupName2, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assertions.assertNotNull(getNode(nodeAddress, isCnNode));

                String sql2 = dropNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
                Assertions.assertNull(getNode(nodeAddress, isCnNode));

                ensureCnGroupDropped(warehouseName, cngroupName2);
            }
        }
        { // non-default warehouse
            List<String> cngroupNames = new ArrayList<>();
            String warehouseName = randomWarehouseName();
            LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
            { // 1 cngroup == DEFAULT_CLUSTER_NAME, UPGRADE FROM enterprise version
                // NOTE: create an cngroup with name DEFAULT_CLUSTER_NAME should not be allowed.
                String nodeAddress = randomNodeAddress();
                ensureCnGroupCreated(warehouseName, LocalWarehouse.DEFAULT_CLUSTER_NAME);
                Assertions.assertEquals(1L, wh.getClusters().size());
                cngroupNames.add(LocalWarehouse.DEFAULT_CLUSTER_NAME);

                // Add node to the warehouse without cngroup name, OK!
                String sql = addNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);

                // drop the node without cngroup name. OK!
                String sql2 = dropNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
            }
            { // 2 cngroups, cngroup[0] == DEFAULT_CLUSTER_NAME
                String nodeAddress = randomNodeAddress();
                String cngroupName = randomCNGroupName();
                ensureCnGroupCreated(warehouseName, cngroupName);
                Assertions.assertEquals(2L, wh.getClusters().size());
                cngroupNames.add(cngroupName);

                // Add node to the warehouse, cngroup= cngroupName
                String sql = addNodeSql(nodeAddress, warehouseName, cngroupName, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                ComputeNode node = getNode(nodeAddress, isCnNode);
                Assertions.assertNotNull(node);

                // drop the node without cngroup name. OK!
                String sql2 = dropNodeSql(nodeAddress, warehouseName, null, isCnNode);
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
                Assertions.assertNull(getNode(nodeAddress, isCnNode));
            }
            for (String cngroupName : cngroupNames) {
                ensureCnGroupDropped(warehouseName, cngroupName);
            }
            ensureWarehouseDropped(warehouseName);
        }
        { // non-default warehouse
            // 1 cngroup != DEFAULT_CLUSTER_NAME
            String warehouseName = randomWarehouseName();
            String nodeAddress = randomNodeAddress();
            String cngroupName = randomCNGroupName();

            LocalWarehouse wh = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
            ensureCnGroupCreated(warehouseName, cngroupName);
            Assertions.assertEquals(1L, wh.getClusters().size());

            // Add node to the warehouse
            String sql = addNodeSql(nodeAddress, warehouseName, cngroupName, isCnNode);
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
            ComputeNode node = getNode(nodeAddress, isCnNode);
            Assertions.assertNotNull(node);

            // drop the node without cngroup name. OK!
            String sql2 = dropNodeSql(nodeAddress, warehouseName, null, isCnNode);
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
            Assertions.assertNull(getNode(nodeAddress, isCnNode));

            ensureCnGroupDropped(warehouseName, cngroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @Test
    public void testAnyCluster() {
        String cnGroupName = randomCNGroupName();
        String warehouseName = randomWarehouseName();
        LocalWarehouse warehouse = (LocalWarehouse) ensureWarehouseCreated(warehouseName);
        Assertions.assertNull(warehouse.getAnyWorkerGroupId());
        Assertions.assertNull(warehouse.getAnyAvailableCluster());

        Cluster cluster = ensureCnGroupCreated(warehouseName, cnGroupName);
        Assertions.assertEquals((Long) cluster.getWorkerGroupId(), warehouse.getAnyWorkerGroupId());
        Assertions.assertEquals(cluster, warehouse.getAnyAvailableCluster());

        ensureCnGroupDropped(warehouseName, cnGroupName);
        ensureWarehouseDropped(warehouseName);
    }

    static Stream<String> nodeTypes() {
        return Stream.of("BE", "CN");
    }
}
