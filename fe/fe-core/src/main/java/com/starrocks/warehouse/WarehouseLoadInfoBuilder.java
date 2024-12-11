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

package com.starrocks.warehouse;

import com.google.common.collect.Maps;
<<<<<<< HEAD
import com.starrocks.load.LoadJobWithWarehouse;
=======
import com.starrocks.server.GlobalStateMgr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

import java.util.Collection;
import java.util.Map;

public class WarehouseLoadInfoBuilder {
<<<<<<< HEAD
    private final Map<String, Long> warehouseToRemovedJobLastFinishedTimeMs = Maps.newConcurrentMap();

    public <T extends LoadJobWithWarehouse> Map<String, WarehouseLoadStatusInfo> buildFromJobs(Collection<T> jobs) {
        Map<String, WarehouseLoadStatusInfo> warehouseToInfo = Maps.newHashMap();
=======
    private final Map<Long, Long> warehouseToRemovedJobLastFinishedTimeMs = Maps.newConcurrentMap();

    public <T extends LoadJobWithWarehouse> Map<Long, WarehouseLoadStatusInfo> buildFromJobs(Collection<T> jobs) {
        Map<Long, WarehouseLoadStatusInfo> warehouseToInfo = Maps.newHashMap();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        for (T job : jobs) {
            if (job.isInternalJob()) {
                continue;
            }

<<<<<<< HEAD
            WarehouseLoadStatusInfo info =
                    warehouseToInfo.computeIfAbsent(job.getCurrentWarehouse(), k -> new WarehouseLoadStatusInfo());
=======
            long warehouseId = job.getCurrentWarehouseId();
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
            WarehouseLoadStatusInfo info =
                    warehouseToInfo.computeIfAbsent(warehouse.getId(), k -> new WarehouseLoadStatusInfo());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (job.isFinal()) {
                info.updateLastFinishedJobTimeMs(job.getFinishTimestampMs());
            } else {
                info.increaseUnfinishedJobs();
            }
        }

        // Merge warehouseToRemovedTaskLastFinishedTimeMs to warehouseToInfo.
<<<<<<< HEAD
        warehouseToRemovedJobLastFinishedTimeMs.forEach((warehouse, removedTaskLastFinishedTimeMs) -> {
            if (warehouseToInfo.containsKey(warehouse)) {
                WarehouseLoadStatusInfo info = warehouseToInfo.get(warehouse);
                info.updateLastFinishedJobTimeMs(removedTaskLastFinishedTimeMs);
            } else {
                warehouseToInfo.put(warehouse, new WarehouseLoadStatusInfo(0L, removedTaskLastFinishedTimeMs));
=======
        warehouseToRemovedJobLastFinishedTimeMs.forEach((warehouseId, removedTaskLastFinishedTimeMs) -> {
            if (warehouseToInfo.containsKey(warehouseId)) {
                WarehouseLoadStatusInfo info = warehouseToInfo.get(warehouseId);
                info.updateLastFinishedJobTimeMs(removedTaskLastFinishedTimeMs);
            } else {
                warehouseToInfo.put(warehouseId, new WarehouseLoadStatusInfo(0L, removedTaskLastFinishedTimeMs));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        });

        return warehouseToInfo;
    }

    public void withRemovedJob(LoadJobWithWarehouse job) {
        if (!job.isFinal() || job.isInternalJob()) {
            return;
        }
<<<<<<< HEAD
        warehouseToRemovedJobLastFinishedTimeMs.compute(job.getCurrentWarehouse(), (k, lastFinishedTimeMs) -> {
=======

        long warehouseId = job.getCurrentWarehouseId();
        warehouseToRemovedJobLastFinishedTimeMs.compute(warehouseId, (k, lastFinishedTimeMs) -> {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (lastFinishedTimeMs == null) {
                return job.getFinishTimestampMs();
            } else {
                return Math.max(lastFinishedTimeMs, job.getFinishTimestampMs());
            }
        });
    }
}
