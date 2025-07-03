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

package com.starrocks.epack.warehouse.cngroup;

import com.starrocks.epack.warehouse.Cluster;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseSlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CNGroupMetricEntityTest {
    private LocalWarehouse mockWarehouse;
    private Cluster mockCluster;
    private WarehouseSlotManager mockSlotManager;
    private WarehouseManager mockWarehouseManager;
    private CNGroupMetricEntity entity;

    @Before
    public void setup() {
        mockWarehouse = mock(LocalWarehouse.class);
        mockCluster = mock(Cluster.class);
        mockSlotManager = mock(WarehouseSlotManager.class);
        mockWarehouseManager = mock(WarehouseManager.class);

        when(mockWarehouse.getId()).thenReturn(1L);
        when(mockWarehouse.getName()).thenReturn("TestWarehouse");
        when(mockCluster.getWorkerGroupId()).thenReturn(100L);
        when(mockCluster.isEnabled()).thenReturn(true);

        GlobalStateMgr mockStateMgr = mock(GlobalStateMgr.class);
        when(mockStateMgr.getWarehouseMgr()).thenReturn(mockWarehouseManager);

        entity = new CNGroupMetricEntity(mockWarehouse, mockCluster, mockSlotManager);
    }

    @Test
    public void returnsCorrectWarehouseId() {
        assertEquals(1L, entity.getWarehouseId());
    }

    @Test
    public void returnsCorrectWarehouseName() {
        assertEquals("TestWarehouse", entity.getWarehouseName());
    }

    @Test
    public void returnsCorrectCNGroupName() {
        when(mockCluster.getName()).thenReturn("TestCluster");
        assertEquals("TestCluster", entity.getCNGroupName());
    }

    @Test
    public void incrementsQuerySuccessCount() {
        entity.incrSuccessQueryLatencyMs(1);
        assertEquals(1L, entity.cnGroupQuerySuccessCount.getValue().longValue());
    }

    @Test
    public void incrementsQueryFailedCount() {
        entity.incrFailedQueryLatencyMs(2);
        assertEquals(1L, entity.cnGroupQueryFailedCount.getValue().longValue());
    }

    @Test
    public void updatesQueryLatencyCorrectly() {
        entity.incrSuccessQueryLatencyMs(100);
        entity.incrFailedQueryLatencyMs(200);

        assertEquals(200L, entity.cnGroupQueryMaxLatencyMs.getValue().longValue());
        assertEquals(150.0, entity.cnGroupQueryAvgLatencyMs.getValue(), 0.01);
    }

    @Test
    public void handlesNoRunningQueriesGracefully() {
        when(mockSlotManager.getCurrentConnectionsByComputeResource()).thenReturn(Map.of());
        assertEquals(0L, entity.cnGroupQueryRunningCount.getValue().longValue());
    }

    @Test
    public void handlesDisabledClusterStatus() {
        when(mockCluster.isEnabled()).thenReturn(false);
        assertEquals(0, entity.cnGroupQueryStatus.getValue().intValue());
    }
}
