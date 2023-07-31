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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

public class WarehouseInfosBuilder {
    private final Map<String, WarehouseInfo> warehouseToInfo = Maps.newHashMap();

    public static WarehouseInfosBuilder makeBuilderFromMetricAndMgrs() {
        WarehouseInfosBuilder builder = new WarehouseInfosBuilder();

        GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getWarehouseInfos()
                .forEach(builder::withWarehouseInfo);

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

    public Map<String, WarehouseInfo> build() {
        return warehouseToInfo;
    }

    public WarehouseInfosBuilder withNumUnfinishedQueries(Map<String, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedQueryJobs);
    }

    public WarehouseInfosBuilder withNumUnfinishedBackupJobs(Map<String, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedBackupJobs);
    }

    public WarehouseInfosBuilder withNumUnfinishedRestoreJobs(Map<String, Long> warehouseToDelta) {
        return withUpdater(warehouseToDelta, WarehouseInfo::increaseNumUnfinishedRestoreJobs);
    }

    public WarehouseInfosBuilder withLastFinishedJobTimestampMs(Map<String, Long> warehouseToTimestampMs) {
        return withUpdater(warehouseToTimestampMs, WarehouseInfo::updateLastFinishedJobTimeMs);
    }

    public WarehouseInfosBuilder withLoadStatusInfo(Map<String, WarehouseLoadStatusInfo> warehouseToLoadInfo) {
        warehouseToLoadInfo.forEach((warehouse, loadInfo) -> {
            WarehouseInfo info = warehouseToInfo.computeIfAbsent(warehouse, WarehouseInfo::new);
            info.increaseNumUnfinishedLoadJobs(loadInfo.getNumUnfinishedJobs());
            info.updateLastFinishedJobTimeMs(loadInfo.getLastFinishedJobTimeMs());
        });
        return this;
    }

    public WarehouseInfosBuilder withWarehouseInfo(WarehouseInfo info) {
        if (!warehouseToInfo.containsKey(info.getWarehouse())) {
            warehouseToInfo.put(info.getWarehouse(), info);
        } else {
            WarehouseInfo destInfo = warehouseToInfo.get(info.getWarehouse());
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

    private WarehouseInfosBuilder withUpdater(Map<String, Long> warehouseToValue,
                                              BiConsumer<WarehouseInfo, Long> updater) {
        warehouseToValue.forEach((warehouse, delta) -> {
            WarehouseInfo info = warehouseToInfo.computeIfAbsent(warehouse, WarehouseInfo::new);
            updater.accept(info, delta);
        });

        return this;
    }

}
