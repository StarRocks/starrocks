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

import com.starrocks.lake.StarOSAgent;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.WarehouseTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class WarehouseComputeResourceTest extends WarehouseTestBase {
    @Test
    public void testCNWarehouseResourceEquals() {
        WarehouseComputeResource resource1 = new WarehouseComputeResource(1);
        WarehouseComputeResource resource2 = new WarehouseComputeResource(1);
        WarehouseComputeResource resource3 = new WarehouseComputeResource(2);

        assertThat(resource1.equals(resource2)).isTrue();
        assertThat(resource1.equals(resource3)).isFalse();
        assertThat(resource1.equals(null)).isFalse();
    }

    @Test
    public void testCNWarehouseResourceToString() {
        ComputeResource computeResource = WarehouseComputeResource.of(1);
        assertThat(computeResource.toString()).isEqualTo("{warehouseId=1}");
        assertThat(computeResource.getWarehouseId()).isEqualTo(1);
    }

    @Test
    public void testCNWarehouseResourceWorkerGroupIdGood() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        ComputeResource computeResource = WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(computeResource.getWarehouseId()).isEqualTo(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(computeResource.getWorkerGroupId()).isEqualTo(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    @Test
    public void testCNWarehouseResourceWorkerGroupIdBad() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        ComputeResource computeResource = WarehouseComputeResource.of(1);
        assertThat(computeResource.getWarehouseId()).isEqualTo(1);
        try {
            computeResource.getWorkerGroupId();
            Assertions.fail();
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }
}
