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

import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class WarehouseIdleChecker extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(WarehouseIdleChecker.class);

    private static final Map<Long, AtomicLong> RUNNING_SQL_COUNT = new ConcurrentHashMap<>();

    private static final Map<Long, Long> LAST_FINISHED_JOB_TIME = new ConcurrentHashMap<>();

    private final Map<Long, Long> warehouseIdleTime = new ConcurrentHashMap<>();

    public WarehouseIdleChecker() {
        super("WarehouseIdleChecker", Config.warehouse_idle_check_interval_seconds * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!Config.warehouse_idle_check_enable) {
            return;
        }

        if (getInterval() != Config.warehouse_idle_check_interval_seconds * 1000) {
            setInterval(Config.warehouse_idle_check_interval_seconds * 1000);
        }

        List<Long> warehouseIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouseIds();
        Map<Long, Long> runningStreamLoadCnt = GlobalStateMgr.getCurrentState().getStreamLoadMgr().getRunningTaskCount();
        Map<Long, Long> runningLoadCnt = GlobalStateMgr.getCurrentState().getLoadMgr()
                .getRunningLoadCount();
        Map<Long, Long> runningRoutineLoadCnt = GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                .getRunningRoutingLoadCount();
        Map<Long, Long> runningBackupRestoreCnt = GlobalStateMgr.getCurrentState().getBackupHandler()
                .getRunningBackupRestoreCount();
        Map<Long, Long> runningAlterJobCnt = GlobalStateMgr.getCurrentState().getAlterJobMgr().getRunningAlterJobCount();
        Map<Long, Long> runningTaskCnt = GlobalStateMgr.getCurrentState().getTaskManager().getTaskRunScheduler()
                .getAllRunnableTaskCount();

        for (long wId : warehouseIds) {
            long lastFinishedJobTime = getLastFinishedJobTime(wId);
            long runningJobCnt = 0;
            runningJobCnt += getRunningSQLCount(wId).get();
            runningJobCnt += runningStreamLoadCnt.getOrDefault(wId, 0L);
            runningJobCnt += runningLoadCnt.getOrDefault(wId, 0L);
            runningJobCnt += runningRoutineLoadCnt.getOrDefault(wId, 0L);
            runningJobCnt += runningBackupRestoreCnt.getOrDefault(wId, 0L);
            runningJobCnt += runningAlterJobCnt.getOrDefault(wId, 0L);
            runningJobCnt += runningTaskCnt.getOrDefault(wId, 0L);

            if (runningJobCnt == 0
                    && lastFinishedJobTime <
                        System.currentTimeMillis() - Config.warehouse_idle_check_interval_seconds * 2000) {
                long resumeTime = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseResumeTime(wId);
                warehouseIdleTime.compute(wId, (k, v) -> {
                    // If this is the first time to become idle, set idleTime to now.
                    // If resumed during an idle period, change idleTime to resumeTime.
                    if (v == null) {
                        return System.currentTimeMillis();
                    } else {
                        return v < resumeTime ? resumeTime : v;
                    }
                });
                LOG.info("warehouse: {} is idle, idle start time: {}",
                        wId, TimeUtils.longToTimeString(warehouseIdleTime.get(wId)));
            } else {
                warehouseIdleTime.remove(wId);
            }
        }
    }

    private static AtomicLong getRunningSQLCount(long wId) {
        return RUNNING_SQL_COUNT.computeIfAbsent(wId, key -> new AtomicLong(0));
    }

    public static void increaseRunningSQL(long wId) {
        AtomicLong runningSQL = getRunningSQLCount(wId);
        runningSQL.incrementAndGet();
    }

    public static void decreaseRunningSQL(long wId) {
        AtomicLong runningSQL = getRunningSQLCount(wId);
        runningSQL.decrementAndGet();
        updateJobLastFinishTime(wId, System.currentTimeMillis());
    }

    public static void updateJobLastFinishTime(long wId) {
        updateJobLastFinishTime(wId, System.currentTimeMillis());
    }

    public static void updateJobLastFinishTime(long wId, long ts) {
        LAST_FINISHED_JOB_TIME.compute(wId, (key, value) -> {
            if (value == null) {
                return ts;
            } else {
                return ts > value ? ts : value;
            }
        });
    }

    public static long getLastFinishedJobTime(long wId) {
        return LAST_FINISHED_JOB_TIME.getOrDefault(wId, -1L);
    }

    public IdleStatus getIdleStatus() {
        runAfterCatalogReady();

        List<Warehouse> warehouses = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouses();

        boolean isClusterIdle = true;
        List<IdleStatus.WarehouseStatus> statusList = new ArrayList<>(warehouses.size());
        long latestWarehouseIdleTime = -1L;
        for (Warehouse warehouse : warehouses) {
            Long wIdleTime = warehouseIdleTime.getOrDefault(warehouse.getId(), -1L);
            if (wIdleTime == -1L) {
                isClusterIdle = false;
            } else {
                latestWarehouseIdleTime = Math.max(latestWarehouseIdleTime, wIdleTime);
            }

            statusList.add(new IdleStatus.WarehouseStatus(
                    warehouse.getId(), warehouse.getName(), wIdleTime != -1L, wIdleTime));
        }

        return new IdleStatus(isClusterIdle, isClusterIdle ? latestWarehouseIdleTime : -1L, statusList);
    }
}
