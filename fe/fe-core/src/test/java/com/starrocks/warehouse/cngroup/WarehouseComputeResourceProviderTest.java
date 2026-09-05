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

package com.starrocks.warehouse.cngroup;

import com.starrocks.common.ErrorReportException;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class WarehouseComputeResourceProviderTest extends WarehouseTestBase {
    private final WarehouseComputeResourceProvider provider = new WarehouseComputeResourceProvider();

    @Test
    public void testProviderAcquireComputeResourceGood() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<ComputeResource> result = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
        assertThat(result.isPresent()).isTrue();
    }

    @Test
    public void testProviderAcquireComputeResourceBad() {
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(1);
        try {
            provider.acquireComputeResource(null, cnAcquireContext);
            Assertions.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }

    private ComputeResource acquireDefaultWarehouseResource() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CRAcquireContext cnAcquireContext = CRAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<ComputeResource> result = provider.acquireComputeResource(defaultWarehouse, cnAcquireContext);
        assertThat(result.isPresent()).isTrue();
        return result.get();
    }

    @Test
    public void testProviderIsResourceAvailableGood() {
        ComputeResource computeResource = acquireDefaultWarehouseResource();
        assertThat(provider.isResourceAvailable(computeResource)).isTrue();
    }

    @Test
    public void testProviderIsResourceAvailableBad() {
        ComputeResource computeResource = WarehouseComputeResource.of(1);
        assertThat(provider.isResourceAvailable(computeResource)).isFalse();
    }

    @Test
    public void testProviderGetAllComputeNodeIds() {
        ComputeResource computeResource = acquireDefaultWarehouseResource();
        List<Long> result = provider.getAllComputeNodeIds(computeResource);
        assertThat(result).isEqualTo(Lists.newArrayList(10001L));
    }

    @Test
    public void testProviderGetAllComputeNodeIdsBad() {
        ComputeResource computeResource = WarehouseComputeResource.of(1);
        try {
            provider.getAllComputeNodeIds(computeResource);
            Assertions.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }

    @Test
    public void testProviderGetAliveComputeNodes() {
        ComputeResource computeResource = acquireDefaultWarehouseResource();
        List<ComputeNode> result = provider.getAliveComputeNodes(computeResource);
        assertThat(result.isEmpty()).isFalse();
    }

    @Test
    public void testProviderGetAliveComputeNodesBad() {
        ComputeResource computeResource = WarehouseComputeResource.of(1);
        try {
            provider.getAliveComputeNodes(computeResource);
            Assertions.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }

    @Test
    public void testProviderGetAliveComputeNodesSkipsMissingNodeIds() {
        ComputeNode aliveNode = new ComputeNode(10001L, "192.168.0.1", 9050);
        aliveNode.setAlive(true);

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                return Lists.newArrayList(10001L, 99999L);
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                if (nodeId == 10001L) {
                    return aliveNode;
                }
                return null;
            }
        };

        ComputeResource computeResource = acquireDefaultWarehouseResource();
        List<ComputeNode> result = provider.getAliveComputeNodes(computeResource);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(10001L, result.get(0).getId());
    }

    @Test
    public void ofComputeResource_returnsValidComputeResource() {
        ComputeResource computeResource = provider.ofComputeResource(1L, 100L);
        assertThat(computeResource).isNotNull();
        assertThat(computeResource.getWarehouseId()).isEqualTo(1L);
    }

    @Test
    public void ofComputeResource_handlesInvalidWarehouseId() {
        ComputeResource computeResource = provider.ofComputeResource(-1L, 100L);
        assertThat(computeResource).isNotNull();
        assertThat(computeResource.getWarehouseId()).isEqualTo(-1L);
    }
}
