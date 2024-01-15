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

    // Used to save references to memory objects that need to be counted.
    // If the object needs to be counted, it first needs to be added to this collection.
    private static final Map<String, Map<String, MemoryTrackable>> REFERENCE = Maps.newConcurrentMap();

    private static final Map<String, MemoryStat> MEMORY_USAGE = Maps.newConcurrentMap();

    private boolean initialize;
    public MemoryUsageTracker() {
        super("MemoryUsageTracker", Config.memory_tracker_interval_seconds * 1000L);
    }

    private void initMemoryTracker() {
        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();

        addReference("Load", currentState.getLoadMgr());
        addReference("Load", currentState.getRoutineLoadMgr());
        addReference("Load", currentState.getStreamLoadMgr());

        addReference("Export", currentState.getExportMgr());
        addReference("Delete", currentState.getDeleteMgr());
        addReference("Transaction", currentState.getGlobalTransactionMgr());
        addReference("Backup", currentState.getBackupHandler());
        addReference("Task", currentState.getTaskManager());
        addReference("Task", currentState.getTaskManager().getTaskRunManager());
        addReference("Tablet", currentState.getTabletInvertedIndex());
        addReference("Profile", ProfileManager.getInstance());
        addReference("Partition", new PartitionMemoryTracker());
        addReference("ExternalCache", new PartitionMemoryTracker());

        QeProcessor qeProcessor = QeProcessorImpl.INSTANCE;
        if (qeProcessor instanceof QeProcessorImpl) {
            addReference("Coordinator", (QeProcessorImpl) qeProcessor);
        }

        IDictManager dictManager = IDictManager.getInstance();
        if (dictManager instanceof CacheDictManager) {
            addReference("Dict", (CacheDictManager) dictManager);
        }


        LOG.info("Memory usage tracker init success");

        initialize = true;
    }

    public static void registerMemoryReference(String moduleName, MemoryTrackable object) {
        REFERENCE.computeIfAbsent(moduleName, k -> new HashMap<>());
        REFERENCE.get(moduleName).put(object.getClass().getSimpleName(), object);
    }

    private void addReference(String moduleName, MemoryTrackable object) {
        Map<String, MemoryTrackable> statMap = REFERENCE.computeIfAbsent(moduleName, k -> new HashMap<>());
        statMap.put(object.getClass().getSimpleName(), object);
    }

    public static void trackerMemory() {
        for (Map.Entry<String, Map<String, MemoryTrackable>> entry : REFERENCE.entrySet()) {
            String moduleName = entry.getKey();
            Map<String, MemoryTrackable> statMap = entry.getValue();
            MemoryStat memoryStat = new MemoryStat();
            long estimateSize = 0L;
            long estimateCount = 0L;
            for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
                String className = statEntry.getKey();
                MemoryTrackable tracker = statEntry.getValue();
                long currentEstimateSize = tracker.estimateSize();
                long currentEstimateCount = tracker.estimateCount();
                estimateSize += currentEstimateSize;
                estimateCount += currentEstimateCount;
                LOG.info("Module {} - {} estimated {} of memory and {} object used",
                        moduleName, className, new ByteSizeValue(currentEstimateSize), currentEstimateCount);
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
        if (Config.enable_memory_tracker) {
            trackerMemory();
        }
    }
}
