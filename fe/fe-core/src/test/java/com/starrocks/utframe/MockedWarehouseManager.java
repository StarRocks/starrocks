// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.utframe;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MockedWarehouseManager is used to mock WarehouseManager for unit test.
 */
public class MockedWarehouseManager extends WarehouseManager {
    private final Map<Long, List<Long>> warehouseIdToComputeNodeIds = new HashMap<>();
    private final List<Long> computeNodeIdSetAssignedToTablet = new ArrayList<>();
    private final Set<ComputeNode> computeNodeSetAssignedToTablet = new HashSet<>();

    private Long computeNodeId = 1000L;
    private boolean throwUnknownWarehouseException = false;

    public MockedWarehouseManager() {
        super();
        warehouseIdToComputeNodeIds.put(DEFAULT_WAREHOUSE_ID, List.of(1000L));
        computeNodeIdSetAssignedToTablet.addAll(Lists.newArrayList(1000L));
        computeNodeSetAssignedToTablet.addAll(Sets.newHashSet(new ComputeNode(1000L, "127.0.0.1", 9030)));
    }
    @Override
    public Warehouse getWarehouse(String warehouseName) {
        Warehouse warehouse = nameToWh.get(warehouseName);
        if (warehouse != null) {
            return warehouse;
        }
        return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
    }

    @Override
    public Warehouse getWarehouse(long warehouseId) {
        Warehouse warehouse = idToWh.get(warehouseId);
        if (warehouse != null) {
            return warehouse;
        }
        return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                WarehouseManager.DEFAULT_WAREHOUSE_NAME);
    }

    @Override
    public List<Long> getAllComputeNodeIds(long warehouseId) {
        if (throwUnknownWarehouseException) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("id: %d", 1L));
        }
        if (warehouseIdToComputeNodeIds.containsKey(warehouseId)) {
            return warehouseIdToComputeNodeIds.get(warehouseId);
        }
        return warehouseIdToComputeNodeIds.get(DEFAULT_WAREHOUSE_ID);
    }

    public void setAllComputeNodeIds(List<Long> computeNodeIds) {
        if (computeNodeIds == null) {
            warehouseIdToComputeNodeIds.remove(DEFAULT_WAREHOUSE_ID);
            return;
        }
        warehouseIdToComputeNodeIds.put(DEFAULT_WAREHOUSE_ID, computeNodeIds);
    }

    public void setComputeNodeId(Long computeNodeId) {
        this.computeNodeId = computeNodeId;
    }

    @Override
    public Long getComputeNodeId(Long warehouseId, LakeTablet tablet) {
        return computeNodeId;
    }

    @Override
    public List<Long> getAllComputeNodeIdsAssignToTablet(Long warehouseId, LakeTablet tablet) {
        return computeNodeIdSetAssignedToTablet;
    }

    public void setComputeNodeIdsAssignToTablet(Set<Long> computeNodeIds) {
        computeNodeIdSetAssignedToTablet.addAll(computeNodeIds);
    }

    @Override
    public ComputeNode getComputeNodeAssignedToTablet(String warehouseName, LakeTablet tablet) {
        return computeNodeSetAssignedToTablet.iterator().next();
    }

    public void setComputeNodesAssignedToTablet(Set<ComputeNode> computeNodeSet) {
        computeNodeSetAssignedToTablet.clear();
        if (computeNodeSet != null) {
            computeNodeSetAssignedToTablet.addAll(computeNodeSet);
        }
    }

    public void setThrowUnknownWarehouseException() {
        this.throwUnknownWarehouseException = true;
    }

    @Override
    public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
        return new ArrayList<>(computeNodeSetAssignedToTablet);
    }
}
