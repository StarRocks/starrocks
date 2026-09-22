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

import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseEventListener;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.stream.Collectors;

public class WarehouseEPEventListener implements WarehouseEventListener {
    private static final Logger LOG = LogManager.getLogger(WarehouseEPEventListener.class);

    @Override
    public void onCreateWarehouse(Warehouse wh) {
        if (GlobalStateMgr.isCheckpointThread() || wh == null) {
            return;
        }
        // register warehouse to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.registerWarehouse(wh.getId());
        } catch (Exception e) {
            LOG.warn("register warehouse {} to slot manager failed", wh.getName(), e);
        }
    }

    @Override
    public void onDropWarehouse(Warehouse wh) {
        if (GlobalStateMgr.isCheckpointThread() || wh == null) {
            return;
        }
        // drop the per-warehouse metrics so no stale series survive the warehouse
        try {
            WarehouseMetricMgr.onDropWarehouse(wh.getId());
        } catch (Exception e) {
            LOG.warn("remove metrics of warehouse {} failed", wh.getName(), e);
        }
        // unregister warehouse to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.unregisterWarehouse(wh.getId());
        } catch (Exception e) {
            LOG.warn("unregister warehouse {} to slot manager failed", wh.getName(), e);
        }
    }

    @Override
    public void onCreateCNGroup(Warehouse wh, long workerGroupId) {
        if (GlobalStateMgr.isCheckpointThread() || wh == null) {
            return;
        }
        // register cngroup to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(wh instanceof LocalWarehouse) || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            LocalWarehouse localWarehouse = (LocalWarehouse) wh;
            Cluster cluster = localWarehouse.getClusterByWorkGroupId(workerGroupId);
            if (cluster == null) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.registerCNGroupResource(wh, cluster);
        } catch (Exception e) {
            LOG.warn("register warehouse {} cngroup {} to slot manager failed", wh.getName(), workerGroupId, e);
        }
    }

    @Override
    public void onDropCNGroup(Warehouse wh, long workerGroupId) {
        if (GlobalStateMgr.isCheckpointThread() || wh == null) {
            return;
        }
        // the dropped CN group is already gone from the warehouse; evict the series of every CN group that no longer exists
        try {
            if (wh instanceof LocalWarehouse) {
                Set<String> remaining = ((LocalWarehouse) wh).getClusters().values().stream()
                        .map(Cluster::getName).collect(Collectors.toSet());
                WarehouseMetricMgr.onDropCNGroup(wh.getId(), remaining);
            }
        } catch (Exception e) {
            LOG.warn("remove cngroup metrics of warehouse {} failed", wh.getName(), e);
        }
        // unregister cngroup to slot manager
        try {
            BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
            if (slotManager == null || !(wh instanceof LocalWarehouse) || !(slotManager instanceof WarehouseSlotManager)) {
                return;
            }
            WarehouseSlotManager warehouseSlotManager = (WarehouseSlotManager) slotManager;
            warehouseSlotManager.unregisterCNGroupResource(wh, workerGroupId);
        } catch (Exception e) {
            LOG.warn("unregister warehouse {} cngroup {} to slot manager failed", wh.getName(), workerGroupId, e);
        }
    }
}
