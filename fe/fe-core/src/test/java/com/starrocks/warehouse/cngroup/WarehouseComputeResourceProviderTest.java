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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseTestBase;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

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
            Optional<ComputeResource> result = provider.acquireComputeResource(null, cnAcquireContext);
            Assert.fail();
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
            Assert.fail();
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
            Assert.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }
}
