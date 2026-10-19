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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.memory.estimate.ByteBufferEstimator;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.memory.estimate.StringEstimator;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.CacheRelaxDictManager;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryUsageTracker extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(MemoryUsageTracker.class);

    // Used to save references to metadata submodules which need to be tracked memory on.
    // If the object needs to be counted, it first needs to be added to this collection.
    public static final Map<String, Map<String, MemoryTrackable>> REFERENCE =
            new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

    public static final Map<String, Map<String, MemoryStat>> MEMORY_USAGE = Maps.newConcurrentMap();

    private static final long COLLECT_INTERVAL_MS = 10_000;

    private static final Object COLLECT_LOCK = new Object();

    private static final AtomicLong LAST_COLLECT_TIME = new AtomicLong(0);

    private boolean initialize;
    public MemoryUsageTracker() {
        super("MemoryUsageTracker", Config.memory_tracker_interval_seconds * 1000L);
    }

    private void initMemoryTracker() {
        registerCustomEstimators();
        registerShallowMemoryClasses();

        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();

        registerMemoryTracker("Load", currentState.getLoadMgr());
        registerMemoryTracker("Load", currentState.getRoutineLoadMgr());
        registerMemoryTracker("Load", currentState.getStreamLoadMgr());
        registerMemoryTracker("Load", currentState.getInsertOverwriteJobMgr());

        registerMemoryTracker("Compaction", currentState.getCompactionMgr());
        registerMemoryTracker("Export", currentState.getExportMgr());
        registerMemoryTracker("Delete", currentState.getDeleteMgr());
        registerMemoryTracker("Transaction", currentState.getGlobalTransactionMgr());
        registerMemoryTracker("Backup", currentState.getBackupHandler());
        registerMemoryTracker("Task", currentState.getTaskManager());
        registerMemoryTracker("Task", currentState.getTaskManager().getTaskRunManager());
        registerMemoryTracker("TabletInvertedIndex", currentState.getTabletInvertedIndex());
        registerMemoryTracker("LocalMetastore", currentState.getLocalMetastore());
        registerMemoryTracker("RecycleBin", currentState.getRecycleBin());
        registerMemoryTracker("Report", currentState.getReportHandler());

        // MV
        registerMemoryTracker("MV", currentState.getMaterializedViewMgr().getMvTimelinessMgr());

        registerMemoryTracker("Query", new QueryTracker());
        registerMemoryTracker("Profile", ProfileManager.getInstance());
        registerMemoryTracker("Agent", new AgentTaskTracker());
        if (currentState.getStatisticStorage() instanceof CachedStatisticStorage) {
            registerMemoryTracker("Statistics", (CachedStatisticStorage) currentState.getStatisticStorage());
        }
        registerMemoryTracker("Statistics", (CacheRelaxDictManager) IRelaxDictManager.getInstance());
        registerMemoryTracker("Statistics", (ColumnMinMaxMgr) IMinMaxStatsMgr.internalInstance());

        QeProcessorImpl qeProcessor = QeProcessorImpl.INSTANCE;
        if (qeProcessor != null) {
            registerMemoryTracker("Coordinator", qeProcessor);
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

    // Periodic call - only collects counts, no estimateSize()
    public static void trackMemory() {
        trackCount(REFERENCE);
        trackCount(ImmutableMap.of("Connector",
                GlobalStateMgr.getCurrentState().getConnectorMgr().getMemTrackers()));

        LOG.info("jvm: {}", getJVMMemory());
    }

    /**
     * Return the memory stats, refreshing them first if the snapshot has expired.
     *
     * MEMORY_USAGE has three readers and two of them come through here: the /api/memory_usage
     * endpoint and sys.fe_memory_usage. The periodic task deliberately does not call
     * estimateSize(), so neither of those can read the map and expect to find anything in it.
     *
     * The third, MetricRepo#updateMemoryUsageMetrics, still reads the map directly, and still
     * finds it empty except when one of the two above has just filled it. Its gauges are therefore
     * not refreshed on any schedule of their own, whatever memory_tracker_enable suggests. That is
     * a second consumer of the same regression and is left for its own change, along with the
     * count-equality guard in that method, which returns early once the gauge list and the map
     * agree on size and so pins the gauges to the values of the first collection.
     *
     * A collection runs synchronously on the calling thread -- a query waits for it -- and what it
     * costs is decided by the registered trackers, not here. Each one implements estimateSize() as
     * it likes, and nothing in the interface bounds what that may do or which locks it may take:
     *
     *   LocalMetastore        walks every database and holds that database's READ lock while it
     *                         estimates the tables inside it, which is the lock DDL writes under
     *   CachingIcebergCatalog walks every entry of its data-file and delete-file caches, passing
     *                         the entry count as the sample size, which is to say not sampling
     *   ReportHandler         loops over pendingTaskMap, one sampled estimate per report type
     *
     * So the cost grows with catalog and cache state, and a collection can stand between DDL and
     * the lock it needs. Hence a shared snapshot rather than a collection per caller: a caller
     * inside the window reads the last one, and callers that arrive together produce one
     * collection between them rather than one each.
     *
     * Without this, ten sessions running `select * from sys.fe_memory_usage` would take ten
     * passes over every database lock. The endpoint has never been throttled either, and is
     * covered here for the same reason -- it runs this same code; only its reachability
     * (127.0.0.1 alone) has kept it from mattering.
     *
     * A stale-by-{@value #COLLECT_INTERVAL_MS}ms answer is the right trade for a diagnostic table.
     */
    public static Map<String, Map<String, MemoryStat>> collectMemoryUsage() {
        if (isSnapshotFresh()) {
            return MEMORY_USAGE;
        }
        synchronized (COLLECT_LOCK) {
            // Re-check: whoever held the lock before us may have just refreshed it.
            if (isSnapshotFresh()) {
                return MEMORY_USAGE;
            }
            collectFullMemory(REFERENCE);
            collectFullMemory(ImmutableMap.of("Connector",
                    GlobalStateMgr.getCurrentState().getConnectorMgr().getMemTrackers()));
            LAST_COLLECT_TIME.set(System.currentTimeMillis());
        }
        return MEMORY_USAGE;
    }

    /**
     * An empty map is never fresh: answering nothing because someone else asked a moment ago is
     * the failure this whole path exists to prevent.
     */
    private static boolean isSnapshotFresh() {
        return !MEMORY_USAGE.isEmpty()
                && System.currentTimeMillis() - LAST_COLLECT_TIME.get() < COLLECT_INTERVAL_MS;
    }

    private static String getJVMMemory() {
        JvmStats jvmStats = JvmStats.jvmStats();
        long directBufferUsed = 0;
        for (JvmStats.BufferPool pool : jvmStats.getBufferPools()) {
            if (pool.getName().equalsIgnoreCase("direct")) {
                directBufferUsed = pool.getUsed();
            }
        }
        return String.format("heap committed: %s, heap used: %s, non heap used: %s, direct buffer used: %s",
                new ByteSizeValue(Runtime.getRuntime().totalMemory()),
                new ByteSizeValue(jvmStats.getMem().getHeapUsed()),
                new ByteSizeValue(jvmStats.getMem().getNonHeapUsed()),
                new ByteSizeValue(directBufferUsed));
    }

    public static Map<String, Object> getJVMMemoryMap() {
        JvmStats jvmStats = JvmStats.jvmStats();
        long directBufferUsed = 0;
        for (JvmStats.BufferPool pool : jvmStats.getBufferPools()) {
            if (pool.getName().equalsIgnoreCase("direct")) {
                directBufferUsed = pool.getUsed();
            }
        }
        Map<String, Object> jvmMap = Maps.newLinkedHashMap();
        jvmMap.put("heap_committed", new ByteSizeValue(Runtime.getRuntime().totalMemory()).toString());
        jvmMap.put("heap_used", new ByteSizeValue(jvmStats.getMem().getHeapUsed()).toString());
        jvmMap.put("non_heap_used", new ByteSizeValue(jvmStats.getMem().getNonHeapUsed()).toString());
        jvmMap.put("direct_buffer_used", new ByteSizeValue(directBufferUsed).toString());
        return jvmMap;
    }

    private static void trackCount(Map<String, Map<String, MemoryTrackable>> trackers) {
        for (Map.Entry<String, Map<String, MemoryTrackable>> entry : trackers.entrySet()) {
            String moduleName = entry.getKey();
            Map<String, MemoryTrackable> statMap = entry.getValue();

            for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
                String className = statEntry.getKey();
                MemoryTrackable tracker = statEntry.getValue();
                Map<String, Long> counterMap = tracker.estimateCount();

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Long> subEntry : counterMap.entrySet()) {
                    sb.append(subEntry.getKey()).append(" with ").append(subEntry.getValue())
                            .append(" object(s). ");
                }
                LOG.info("Module {} - {}. Contains {}", moduleName, className, sb.toString());
            }
        }
    }

    private static void collectFullMemory(Map<String, Map<String, MemoryTrackable>> trackers) {
        for (Map.Entry<String, Map<String, MemoryTrackable>> entry : trackers.entrySet()) {
            String moduleName = entry.getKey();
            Map<String, MemoryTrackable> statMap = entry.getValue();

            for (Map.Entry<String, MemoryTrackable> statEntry : statMap.entrySet()) {
                String className = statEntry.getKey();
                MemoryTrackable tracker = statEntry.getValue();
                long startTime = System.currentTimeMillis();
                long currentEstimateSize = tracker.estimateSize();
                Map<String, Long> counterMap = tracker.estimateCount();
                long endTime = System.currentTimeMillis();

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
                memoryStat.setCounterMap(counterMap);
                usageMap.put(className, memoryStat);

                if (currentEstimateSize > 0) {
                    LOG.info("({}ms) Module {} - {} estimated {} of memory. Contains {}",
                            endTime - startTime, moduleName, className,
                            new ByteSizeValue(currentEstimateSize), sb.toString());
                } else {
                    LOG.info("({}ms) Module {} - {}. Contains {}",
                            endTime - startTime, moduleName, className, sb.toString());
                }
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

    private void registerCustomEstimators() {
        Estimator.registerCustomEstimator(String.class, new StringEstimator());
        Estimator.registerCustomEstimator(ByteBuffer.class, new ByteBufferEstimator());
        Estimator.registerCustomEstimator("java.nio.HeapByteBuffer", new ByteBufferEstimator());
    }

    private void registerShallowMemoryClasses() {
        Estimator.registerShallowMemoryClass(Boolean.class);
        Estimator.registerShallowMemoryClass(Byte.class);
        Estimator.registerShallowMemoryClass(Character.class);
        Estimator.registerShallowMemoryClass(Short.class);
        Estimator.registerShallowMemoryClass(Integer.class);
        Estimator.registerShallowMemoryClass(Long.class);
        Estimator.registerShallowMemoryClass(Float.class);
        Estimator.registerShallowMemoryClass(Double.class);
    }
}
