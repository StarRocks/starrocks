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
import com.starrocks.load.LoadJobWithWarehouse;

import java.util.Collection;
import java.util.Map;

public class WarehouseLoadInfoBuilder {
    private final Map<String, Long> warehouseToRemovedJobLastFinishedTimeMs = Maps.newConcurrentMap();

    public <T extends LoadJobWithWarehouse> Map<String, WarehouseLoadStatusInfo> buildFromJobs(Collection<T> jobs) {
        Map<String, WarehouseLoadStatusInfo> warehouseToInfo = Maps.newHashMap();
        for (T job : jobs) {
            if (job.isInternalJob()) {
                continue;
            }

            WarehouseLoadStatusInfo info =
                    warehouseToInfo.computeIfAbsent(job.getCurrentWarehouse(), k -> new WarehouseLoadStatusInfo());
            if (job.isFinal()) {
                info.updateLastFinishedJobTimeMs(job.getFinishTimestampMs());
            } else {
                info.increaseUnfinishedJobs();
            }
        }

        // Merge warehouseToRemovedTaskLastFinishedTimeMs to warehouseToInfo.
        warehouseToRemovedJobLastFinishedTimeMs.forEach((warehouse, removedTaskLastFinishedTimeMs) -> {
            if (warehouseToInfo.containsKey(warehouse)) {
                WarehouseLoadStatusInfo info = warehouseToInfo.get(warehouse);
                info.updateLastFinishedJobTimeMs(removedTaskLastFinishedTimeMs);
            } else {
                warehouseToInfo.put(warehouse, new WarehouseLoadStatusInfo(0L, removedTaskLastFinishedTimeMs));
            }
        });

        return warehouseToInfo;
    }

    public void withRemovedJob(LoadJobWithWarehouse job) {
        if (!job.isFinal()) {
            return;
        }
        warehouseToRemovedJobLastFinishedTimeMs.compute(job.getCurrentWarehouse(), (k, lastFinishedTimeMs) -> {
            if (lastFinishedTimeMs == null) {
                return job.getFinishTimestampMs();
            } else {
                return Math.max(lastFinishedTimeMs, job.getFinishTimestampMs());
            }
        });
    }
}
