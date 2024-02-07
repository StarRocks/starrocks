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

package com.starrocks.memory;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.QeProcessor;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryUsageTracker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(MemoryUsageTracker.class);

    // Used to save references to metadata submodules which need to be tracked memory on.
    // If the object needs to be counted, it first needs to be added to this collection.
    public static final Map<String, Map<String, MemoryTrackable>> REFERENCE =
            new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

    public static final Map<String, Map<String, MemoryStat>> MEMORY_USAGE = Maps.newConcurrentMap();

    private boolean initialize;
    public MemoryUsageTracker() {
        super("MemoryUsageTracker", Config.memory_tracker_interval_seconds * 1000L);
    }

    private void initMemoryTracker() {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();

        registerMemoryTracker("Load", currentState.getLoadMgr());
        registerMemoryTracker("Load", currentState.getRoutineLoadMgr());
        registerMemoryTracker("Load", currentState.getStreamLoadMgr());

        registerMemoryTracker("Export", currentState.getExportMgr());
        registerMemoryTracker("Delete", currentState.getDeleteMgr());
        registerMemoryTracker("Transaction", currentState.getGlobalTransactionMgr());
        registerMemoryTracker("Backup", currentState.getBackupHandler());
        registerMemoryTracker("Task", currentState.getTaskManager());
        registerMemoryTracker("Task", currentState.getTaskManager().getTaskRunManager());
        registerMemoryTracker("Tablet", currentState.getTabletInvertedIndex());
        registerMemoryTracker("Profile", ProfileManager.getInstance());
        registerMemoryTracker("LocalCatalog", new InternalCatalogMemoryTracker());

        QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;
        if (qeProcessor instanceof QeProcessorImpl) {
            registerMemoryTracker("Coordinator", (QeProcessorImpl) qeProcessor);
        }

        IDictManager dictManager = IDictManager.getInstance();
        if (dictManager instanceof CacheDictManager) {
            registerMemoryTracker("Dict", (CacheDictManager) dictManager);
        }

        LOG.info("Memory usage tracker init success");

        initialize = true;
    }

    public static void registerMemoryTracker(String moduleName, MemoryTrackable object) {
        REFERENCE.computeIfAbsent(moduleName, k -> new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER));
        REFERENCE.get(moduleName).put(object.getClass().getSimpleName(), object);
    }

    public static void trackMemory() {
        long startTime;
        long endTime;
        for (Map.Entry<String, Map<String, MemoryTrackable>> entry : REFERENCE.entrySet()) {
            String moduleName = entry.getKey();
            Map<String, MemoryTrackable> statMap = entry.getValue();
            for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
                String className = statEntry.getKey();
                MemoryTrackable tracker = statEntry.getValue();
                startTime = System.currentTimeMillis();
                long currentEstimateSize = tracker.estimateSize();
                Map<String, Long> counterMap = tracker.estimateCount();
                endTime = System.currentTimeMillis();

                StringBuilder sb  = new StringBuilder();
                for (Map.Entry<String, Long> subEntry : counterMap.entrySet()) {
                    sb.append(subEntry.getKey()).append(" with ").append(subEntry.getValue())
                            .append(" object(s). ");
                }
                MemoryStat memoryStat = new MemoryStat();
                MEMORY_USAGE.computeIfAbsent(moduleName, k -> new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER));
                memoryStat.setCurrentConsumption(currentEstimateSize);
                Map<String, MemoryStat> usageMap = MEMORY_USAGE.get(moduleName);
                MemoryStat oldMemoryStat = usageMap.get(className);
                if (oldMemoryStat != null) {
                    memoryStat.setPeakConsumption(Math.max(oldMemoryStat.getPeakConsumption(), currentEstimateSize));
                } else {
                    memoryStat.setPeakConsumption(currentEstimateSize);
                }
                memoryStat.setCounterInfo(GsonUtils.GSON.toJson(counterMap));
                usageMap.put(className, memoryStat);

                LOG.info("({}ms) Module {} - {} estimated {} of memory. Contains {}",
                        endTime - startTime, moduleName, className,
                        new ByteSizeValue(currentEstimateSize), sb.toString());
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!initialize) {
            initMemoryTracker();
        }
        setInterval(Config.memory_tracker_interval_seconds * 1000L);
        if (Config.memory_tracker_enable) {
            trackMemory();
        }
    }
}
