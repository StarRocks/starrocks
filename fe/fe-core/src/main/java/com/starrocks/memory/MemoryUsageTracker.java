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
import com.starrocks.qe.QeProcessor;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class MemoryUsageTracker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(MemoryUsageTracker.class);

    // Used to save references to metadata submodules which need to be tracked memory on.
    // If the object needs to be counted, it first needs to be added to this collection.
    private static final Map<String, Map<String, MemoryTrackable>> REFERENCE = Maps.newConcurrentMap();

    private static final Map<String, MemoryStat> MEMORY_USAGE = Maps.newConcurrentMap();

    private boolean initialize;
    public MemoryUsageTracker() {
        super("MemoryUsageTracker", Config.memory_tracker_interval_seconds * 1000L);
    }

    private void initMemoryTracker() {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();

        registerMemoryReference("Load", currentState.getLoadMgr());
        registerMemoryReference("Load", currentState.getRoutineLoadMgr());
        registerMemoryReference("Load", currentState.getStreamLoadMgr());

        registerMemoryReference("Export", currentState.getExportMgr());
        registerMemoryReference("Delete", currentState.getDeleteMgr());
        registerMemoryReference("Transaction", currentState.getGlobalTransactionMgr());
        registerMemoryReference("Backup", currentState.getBackupHandler());
        registerMemoryReference("Task", currentState.getTaskManager());
        registerMemoryReference("Task", currentState.getTaskManager().getTaskRunManager());
        registerMemoryReference("Tablet", currentState.getTabletInvertedIndex());
        registerMemoryReference("Profile", ProfileManager.getInstance());
        registerMemoryReference("Partition", new PartitionMemoryTracker());

        QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;
        if (qeProcessor instanceof QeProcessorImpl) {
            registerMemoryReference("Coordinator", (QeProcessorImpl) qeProcessor);
        }

        IDictManager dictManager = IDictManager.getInstance();
        if (dictManager instanceof CacheDictManager) {
            registerMemoryReference("Dict", (CacheDictManager) dictManager);
        }

        LOG.info("Memory usage tracker init success");

        initialize = true;
    }

    public static void registerMemoryReference(String moduleName, MemoryTrackable object) {
        REFERENCE.computeIfAbsent(moduleName, k -> new HashMap<>());
        REFERENCE.get(moduleName).put(object.getClass().getSimpleName(), object);
    }

    public static void trackMemory() {
        long startTime;
        long endTime;
        for (Map.Entry<String, Map<String, MemoryTrackable>> entry : REFERENCE.entrySet()) {
            String moduleName = entry.getKey();
            Map<String, MemoryTrackable> statMap = entry.getValue();
            MemoryStat memoryStat = new MemoryStat();
            long estimateSize = 0L;
            long estimateCount = 0L;
            for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
                String className = statEntry.getKey();
                MemoryTrackable tracker = statEntry.getValue();
                startTime = System.currentTimeMillis();
                long currentEstimateSize = tracker.estimateSize();
                long currentEstimateCount = tracker.estimateCount();
                endTime = System.currentTimeMillis();
                estimateSize += currentEstimateSize;
                estimateCount += currentEstimateCount;
                LOG.info("({}ms) Module {} - {} estimated {} of memory and {} object used",
                        endTime - startTime, moduleName, className,
                        new ByteSizeValue(currentEstimateSize), currentEstimateCount);
            }
            memoryStat.setCurrentConsumption(estimateSize);
            memoryStat.setObjectCount(estimateCount);
            MemoryStat oldMemoryStat = MEMORY_USAGE.get(moduleName);
            if (oldMemoryStat != null) {
                memoryStat.setPeakConsumption(Math.max(oldMemoryStat.getPeakConsumption(), estimateSize));
            } else {
                memoryStat.setPeakConsumption(estimateSize);
            }
            MEMORY_USAGE.put(moduleName, memoryStat);
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
