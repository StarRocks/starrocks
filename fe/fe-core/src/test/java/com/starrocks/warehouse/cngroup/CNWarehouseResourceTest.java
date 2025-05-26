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
import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class CNWarehouseResourceTest extends WarehouseTestBase {
    @Test
    public void testCNWarehouseResourceEquals() {
        CNWarehouseResource resource1 = new CNWarehouseResource(1);
        CNWarehouseResource resource2 = new CNWarehouseResource(1);
        CNWarehouseResource resource3 = new CNWarehouseResource(2);

        assertThat(resource1.equals(resource2)).isTrue();
        assertThat(resource1.equals(resource3)).isFalse();
        assertThat(resource1.equals(null)).isFalse();
    }

    @Test
    public void testCNWarehouseResourceToString() {
        CNResource cnResource = CNWarehouseResource.of(1);
        assertThat(cnResource.toString()).isEqualTo("{warehouseId=1}");
        assertThat(cnResource.getWarehouseId()).isEqualTo(1);
    }

    @Test
    public void testCNWarehouseResourceWorkerGroupIdGood() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        CNResource cnResource = CNWarehouseResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(cnResource.getWarehouseId()).isEqualTo(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(cnResource.getWorkerGroupId()).isEqualTo(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    @Test
    public void testCNWarehouseResourceWorkerGroupIdBad() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        CNResource cnResource = CNWarehouseResource.of(1);
        assertThat(cnResource.getWarehouseId()).isEqualTo(1);
        try {
            cnResource.getWorkerGroupId();
            Assert.fail();
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("Warehouse id: 1 not exist");
        }
    }
}
