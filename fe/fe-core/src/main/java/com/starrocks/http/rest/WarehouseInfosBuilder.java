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

package com.starrocks.http.rest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseInfo;
import com.starrocks.warehouse.WarehouseLoadStatusInfo;

import java.util.Map;

public class WarehouseInfosBuilder {
    private final Map<Long, WarehouseInfo> warehouseToInfo = Maps.newHashMap();

    public static WarehouseInfosBuilder makeBuilderFromMetricAndMgrs() {
        WarehouseInfosBuilder builder = new WarehouseInfosBuilder();

        for (Warehouse warehouse : GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouses()) {
            builder.withWarehouseInfo(new WarehouseInfo(warehouse.getId(), warehouse.getName()));
        }

        return builder
                .withNumUnfinishedQueries(WarehouseMetricMgr.getUnfinishedQueries())
                .withNumUnfinishedBackupJobs(WarehouseMetricMgr.getUnfinishedBackupJobs())
                .withNumUnfinishedRestoreJobs(WarehouseMetricMgr.getUnfinishedRestoreJobs())
                .withLastFinishedJobTimestampMs(WarehouseMetricMgr.getLastFinishedJobTimestampMs())
                .withLoadStatusInfo(GlobalStateMgr.getCurrentState().getLoadMgr().getWarehouseLoadInfo())
                .withLoadStatusInfo(GlobalStateMgr.getCurrentState().getRoutineLoadMgr().getWarehouseLoadInfo())
                .withLoadStatusInfo(GlobalStateMgr.getCurrentState().getStreamLoadMgr().getWarehouseLoadInfo());
    }

    @VisibleForTesting
    WarehouseInfosBuilder() {
    }

    public Map<Long, WarehouseInfo> build() {
        return warehouseToInfo;
    }

    public WarehouseInfosBuilder withNumUnfinishedQueries(Map<Long, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedQueryJobs);
    }

    public WarehouseInfosBuilder withNumUnfinishedBackupJobs(Map<Long, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedBackupJobs);
    }

    public WarehouseInfosBuilder withNumUnfinishedRestoreJobs(Map<Long, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedRestoreJobs);
    }

    public WarehouseInfosBuilder withLastFinishedJobTimestampMs(Map<Long, Long> warehouseToTimestampMs) {
        return withUpdater(warehouseToTimestampMs, WarehouseInfo::updateLastFinishedJobTimeMs);
    }

    public WarehouseInfosBuilder withLoadStatusInfo(Map<Long, WarehouseLoadStatusInfo> warehouseToLoadInfo) {
        warehouseToLoadInfo.forEach((warehouseId, loadInfo) -> {
            WarehouseInfo info = warehouseToInfo.computeIfAbsent(warehouseId, WarehouseInfo::new);
            info.increaseNumUnfinishedLoadJobs(loadInfo.getNumUnfinishedJobs());
            info.updateLastFinishedJobTimeMs(loadInfo.getLastFinishedJobTimeMs());
        });
        return this;
    }

    public WarehouseInfosBuilder withWarehouseInfo(WarehouseInfo info) {
        if (!warehouseToInfo.containsKey(info.getId())) {
            warehouseToInfo.put(info.getId(), info);
        } else {
            WarehouseInfo destInfo = warehouseToInfo.get(info.getId());
            destInfo.increaseNumUnfinishedQueryJobs(info.getNumUnfinishedQueryJobs());
            destInfo.increaseNumUnfinishedLoadJobs(info.getNumUnfinishedLoadJobs());
            destInfo.increaseNumUnfinishedBackupJobs(info.getNumUnfinishedBackupJobs());
            destInfo.increaseNumUnfinishedRestoreJobs(info.getNumUnfinishedRestoreJobs());
            destInfo.updateLastFinishedJobTimeMs(info.getLastFinishedJobTimestampMs());
        }
        return this;
    }

    @FunctionalInterface
    interface BiConsumer<T1, T2> {
        void accept(T1 t1, T2 t2);
    }

    private WarehouseInfosBuilder withUpdater(Map<Long, Long> warehouseIdToValue,
                                              BiConsumer<WarehouseInfo, Long> updater) {
        warehouseIdToValue.forEach((warehouseId, delta) -> {
            WarehouseInfo info = warehouseToInfo.computeIfAbsent(warehouseId, WarehouseInfo::new);
            updater.accept(info, delta);
        });

        return this;
    }

}
