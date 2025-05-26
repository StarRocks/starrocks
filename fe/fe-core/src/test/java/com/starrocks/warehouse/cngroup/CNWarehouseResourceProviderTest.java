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

public class CNWarehouseResourceProviderTest extends WarehouseTestBase {
    private final CNWarehouseResourceProvider provider = new CNWarehouseResourceProvider();

    @Test
    public void testProviderAcquireCNResourceGood() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CNAcquireContext cnAcquireContext = CNAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<CNResource> result = provider.acquireCNResource(defaultWarehouse, cnAcquireContext);
        assertThat(result.isPresent()).isTrue();
    }

    @Test
    public void testProviderAcquireCNResourceBad() {
        CNAcquireContext cnAcquireContext = CNAcquireContext.of(1);
        try {
            Optional<CNResource> result = provider.acquireCNResource(null, cnAcquireContext);
            Assert.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }

    private CNResource acquireDefaultWarehouseResource() {
        WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
        Warehouse defaultWarehouse = warehouseManager.getWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        CNAcquireContext cnAcquireContext = CNAcquireContext.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Optional<CNResource> result = provider.acquireCNResource(defaultWarehouse, cnAcquireContext);
        assertThat(result.isPresent()).isTrue();
        return result.get();
    }

    @Test
    public void testProviderIsResourceAvailableGood() {
        CNResource cnResource = acquireDefaultWarehouseResource();
        assertThat(provider.isResourceAvailable(cnResource)).isTrue();
    }

    @Test
    public void testProviderIsResourceAvailableBad() {
        CNResource cnResource = CNWarehouseResource.of(1);
        assertThat(provider.isResourceAvailable(cnResource)).isFalse();
    }

    @Test
    public void testProviderGetAllComputeNodeIds() {
        CNResource cnResource = acquireDefaultWarehouseResource();
        List<Long> result = provider.getAllComputeNodeIds(cnResource);
        assertThat(result).isEqualTo(Lists.newArrayList(10001L));
    }

    @Test
    public void testProviderGetAllComputeNodeIdsBad() {
        CNResource cnResource = CNWarehouseResource.of(1);
        try {
            provider.getAllComputeNodeIds(cnResource);
            Assert.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }

    @Test
    public void testProviderGetAliveComputeNodes() {
        CNResource cnResource = acquireDefaultWarehouseResource();
        List<ComputeNode> result = provider.getAliveComputeNodes(cnResource);
        assertThat(result.isEmpty()).isFalse();
    }

    @Test
    public void testProviderGetAliveComputeNodesBad() {
        CNResource cnResource = CNWarehouseResource.of(1);
        try {
            provider.getAliveComputeNodes(cnResource);
            Assert.fail();
        } catch (ErrorReportException e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }
}
