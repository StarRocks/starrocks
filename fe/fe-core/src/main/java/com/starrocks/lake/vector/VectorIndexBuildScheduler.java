// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.lake.vector;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.BuildVectorIndexResponse;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

/**
 * Queue-driven scheduler for async vector index building in shared-data mode.
 * <p>
 * Normal operation: commit callbacks enqueue dirty tablets via {@link #addPendingTablet}.
 * Leader switch: one-time recovery scan finds tablets where builtVersion &lt; visibleVersion.
 * <p>
 * builtVersion is stored on {@link LakeTablet} (with @SerializedName) and persisted via checkpoint.
 * Publish path reads builtVersion directly from the tablet object.
 */
public class VectorIndexBuildScheduler extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(VectorIndexBuildScheduler.class);

    static final int MAX_CONCURRENT_TASKS = 64;
    private static final long DEFAULT_INTERVAL_MS = 5000;
    static final long BUILD_TIMEOUT_MS = 2 * 60 * 60 * 1000L; // 2 hours

    // Pending queue: tabletId -> target visibleVersion
    private final ConcurrentHashMap<Long, Long> pendingTablets = new ConcurrentHashMap<>();

    // Running tasks: tabletId -> task
    private final Map<Long, VectorIndexBuildTask> runningTasks = new ConcurrentHashMap<>();

    // Preferred CN for re-enqueued tablets: tabletId -> last CN that was building it.
    // On re-enqueue, scheduling prefers the same CN to avoid duplicate work
    // (the CN already has partial .vi files + StarCache warm).
    private final Map<Long, ComputeNode> preferredNodes = new ConcurrentHashMap<>();

    // Cooldown for tablets whose CN reported "already in progress" (dedup rejection).
    // Avoids tight retry loops — wait at least DEDUP_COOLDOWN_MS before re-dispatching.
    private static final long DEDUP_COOLDOWN_MS = 5 * 60 * 1000L; // 5 minutes
    private final Map<Long, Long> cooldownUntil = new ConcurrentHashMap<>();

    private volatile boolean recoveryScanDone = false;

    public VectorIndexBuildScheduler() {
        super("vector-index-build-scheduler", DEFAULT_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            recoveryScanDone = false;
            return;
        }

        if (!recoveryScanDone) {
            recoveryScan();
            recoveryScanDone = true;
        }

        checkRunningTasks();
        checkRunningTaskTimeout();
        cleanupStaleEntries();
        scheduleFromPending();
    }

    // ========== Public API ==========

    /**
     * Enqueue a tablet for async vector index building.
     * Called from {@link com.starrocks.transaction.LakeTableTxnStateListener#preWriteCommitLog}.
     */
    public void addPendingTablet(long tabletId, long version) {
        pendingTablets.merge(tabletId, version, Math::max);
    }

    /**
     * Returns the builtVersion for a tablet, used by publish path.
     * Reads directly from the LakeTablet object (checkpoint-persisted).
     */
    public Long getBuiltVersion(long tabletId) {
        LakeTablet tablet = findLakeTablet(tabletId);
        if (tablet != null) {
            long bv = tablet.getVectorIndexBuiltVersion();
            return bv > 0 ? bv : null;
        }
        return null;
    }

    /**
     * Convenience entry point for publish callers: enqueue all build infos
     * returned by BE in the publish response. No-op if scheduler is not initialized
     * or input is empty.
     */
    public static void onPublishComplete(List<VectorIndexBuildInfoPB> infos) {
        if (infos == null || infos.isEmpty()) {
            return;
        }
        VectorIndexBuildScheduler scheduler = GlobalStateMgr.getCurrentState().getVectorIndexBuildScheduler();
        if (scheduler == null) {
            return;
        }
        for (VectorIndexBuildInfoPB info : infos) {
            if (info.tabletId != null && info.version != null) {
                scheduler.addPendingTablet(info.tabletId, info.version);
            }
        }
    }

    // ========== Recovery scan after leader switch ==========

    /**
     * One-time scan after becoming leader.
     * Finds all tablets in async vector index tables where builtVersion &lt; visibleVersion.
     *
     * TODO: this scan is currently synchronous and runs on the daemon thread. On large
     * clusters (many DBs/tables/partitions) it can block checkRunningTasks /
     * checkRunningTaskTimeout / scheduleFromPending for the duration of the scan and
     * compete with DDL for catalog readlocks. Make it incremental (paged) and/or move
     * it off the daemon thread so the scheduler stays responsive after a leader switch.
     */
    void recoveryScan() {
        int count = 0;
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        for (Long dbId : metastore.getDbIds()) {
            Database db = metastore.getDb(dbId);
            if (db == null) {
                continue;
            }

            for (Table table : db.getTables()) {
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                if (!olapTable.isCloudNativeTableOrMaterializedView()) {
                    continue;
                }
                if (!hasAsyncVectorIndex(olapTable)) {
                    continue;
                }

                for (PhysicalPartition partition : olapTable.getPhysicalPartitions()) {
                    long visibleVersion = partition.getVisibleVersion();
                    if (visibleVersion <= 1) {
                        continue;
                    }
                    for (MaterializedIndex index :
                            partition.getLatestMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        for (Tablet tablet : index.getTablets()) {
                            long builtVersion = 0;
                            if (tablet instanceof LakeTablet) {
                                builtVersion = ((LakeTablet) tablet).getVectorIndexBuiltVersion();
                            }
                            if (builtVersion < visibleVersion) {
                                // Use merge(Math::max) so a concurrent onPublishComplete
                                // enqueue with a newer target version is not overwritten
                                // by recoveryScan's snapshot of visibleVersion.
                                pendingTablets.merge(tablet.getId(), visibleVersion, Math::max);
                                count++;
                            }
                        }
                    }
                }
            }
        }
        LOG.info("Vector index build recovery scan: {} tablets enqueued", count);
    }

    // ========== Check running tasks ==========

    void checkRunningTasks() {
        Iterator<Map.Entry<Long, VectorIndexBuildTask>> it = runningTasks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, VectorIndexBuildTask> entry = it.next();
            VectorIndexBuildTask task = entry.getValue();
            if (!task.isDone()) {
                continue;
            }

            long tabletId = task.getTabletId();
            long targetVersion = task.getVersion();

            // Check dedup rejection (RESOURCE_BUSY) before getResponse()
            // which throws for non-zero status codes.
            if (task.isAlreadyBuilding()) {
                // CN is still building this tablet. Re-enqueue with cooldown
                // to avoid tight retry loop. Prefer same CN.
                pendingTablets.merge(tabletId, targetVersion, Math::max);
                preferredNodes.put(tabletId, task.getNode());
                cooldownUntil.put(tabletId, System.currentTimeMillis() + DEDUP_COOLDOWN_MS);
                LOG.info("Vector index build in progress on CN (dedup), "
                        + "re-enqueued with {}s cooldown: tablet={}", DEDUP_COOLDOWN_MS / 1000, tabletId);
                it.remove();
                continue;
            }

            try {
                BuildVectorIndexResponse response = task.getResponse();
                long newBuiltVersion = response.newBuiltVersion != null ? response.newBuiltVersion : 0;

                // Update builtVersion directly on the LakeTablet object
                LakeTablet tablet = findLakeTablet(tabletId);
                if (tablet != null) {
                    tablet.setVectorIndexBuiltVersion(newBuiltVersion);
                }

                if (newBuiltVersion < targetVersion) {
                    // Not all rowsets built yet (batch_limit), re-enqueue for next round.
                    // Keep CN affinity so the next round reuses the same CN's cache warmup.
                    pendingTablets.merge(tabletId, targetVersion, Math::max);
                    preferredNodes.put(tabletId, task.getNode());
                    LOG.info("Async vector index build partial: tablet={}, newBuiltVersion={}, "
                                    + "targetVersion={}, re-enqueued",
                            tabletId, newBuiltVersion, targetVersion);
                } else {
                    LOG.info("Async vector index build completed: tablet={}, newBuiltVersion={}",
                            tabletId, newBuiltVersion);
                    preferredNodes.remove(tabletId);
                }
            } catch (Exception e) {
                // Real failure: re-enqueue, let scheduler freely pick any CN.
                pendingTablets.merge(tabletId, targetVersion, Math::max);
                LOG.warn("Vector index build failed, re-enqueued: tablet={}", tabletId, e);
            }
            it.remove();
        }
    }

    void checkRunningTaskTimeout() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<Long, VectorIndexBuildTask>> it = runningTasks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, VectorIndexBuildTask> entry = it.next();
            VectorIndexBuildTask task = entry.getValue();
            if (!task.isDone() && now - task.getStartTimeMs() > BUILD_TIMEOUT_MS) {
                LOG.warn("Vector index build timeout: tablet={}", task.getTabletId());
                pendingTablets.merge(task.getTabletId(), task.getVersion(), Math::max);
                preferredNodes.put(task.getTabletId(), task.getNode());
                it.remove();
            }
        }
    }

    /**
     * Remove stale entries from preferredNodes and cooldownUntil for tablets
     * that are no longer pending, running, or tracked anywhere.
     */
    void cleanupStaleEntries() {
        long now = System.currentTimeMillis();
        // Remove expired cooldowns
        cooldownUntil.entrySet().removeIf(e -> now >= e.getValue());
        // Remove preferredNodes for tablets not in pending or running
        preferredNodes.keySet().removeIf(
                tabletId -> !pendingTablets.containsKey(tabletId) && !runningTasks.containsKey(tabletId));
    }

    // ========== Schedule from pending queue ==========

    void scheduleFromPending() {
        Iterator<Map.Entry<Long, Long>> it = pendingTablets.entrySet().iterator();
        while (it.hasNext()) {
            if (runningTasks.size() >= MAX_CONCURRENT_TASKS) {
                break;
            }

            Map.Entry<Long, Long> entry = it.next();
            long tabletId = entry.getKey();
            long version = entry.getValue();

            if (runningTasks.containsKey(tabletId)) {
                continue;
            }

            // Skip tablets in dedup cooldown (CN reported "already in progress")
            Long cooldown = cooldownUntil.get(tabletId);
            if (cooldown != null) {
                if (System.currentTimeMillis() < cooldown) {
                    continue;
                }
                cooldownUntil.remove(tabletId);
            }

            // Tablet dropped or metadata gone, discard orphan entry
            LakeTablet tablet = findLakeTablet(tabletId);
            if (tablet == null) {
                it.remove();
                continue;
            }

            // Fast check: already built, skip
            if (tablet.getVectorIndexBuiltVersion() >= version) {
                it.remove();
                continue;
            }

            // Prefer the CN that was previously building this tablet (it has
            // partial .vi files + warm StarCache). Fall back to any alive CN.
            ComputeNode node = null;
            ComputeNode preferred = preferredNodes.get(tabletId);
            if (preferred != null && preferred.isAlive()) {
                node = preferred;
            }
            if (node == null) {
                node = pickComputeNode(tabletId);
            }
            if (node == null) {
                continue;
            }
            preferredNodes.remove(tabletId);

            long builtVersion = (tablet != null) ? tablet.getVectorIndexBuiltVersion() : 0;
            VectorIndexBuildTask task = new VectorIndexBuildTask(node, tabletId, version, builtVersion);
            try {
                task.sendRequest();
                runningTasks.put(tabletId, task);
                it.remove();
                LOG.info("Scheduled async vector index build: tablet={}, version={}, node={}",
                        tabletId, version, node.getId());
            } catch (Exception e) {
                LOG.warn("Failed to send build vector index request: tablet={}", tabletId, e);
            }
        }
    }

    // ========== Helpers ==========

    /**
     * Find a LakeTablet object by tabletId via TabletInvertedIndex -> table -> partition -> index -> tablet.
     */
    @Nullable
    private static LakeTablet findLakeTablet(long tabletId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        if (invertedIndex == null) {
            return null;
        }
        TabletMeta meta = invertedIndex.getTabletMeta(tabletId);
        if (meta == null) {
            return null;
        }
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = metastore.getDb(meta.getDbId());
        if (db == null) {
            return null;
        }
        Table table = db.getTable(meta.getTableId());
        if (!(table instanceof OlapTable)) {
            return null;
        }
        PhysicalPartition partition = ((OlapTable) table).getPhysicalPartition(meta.getPhysicalPartitionId());
        if (partition == null) {
            return null;
        }
        MaterializedIndex index = partition.getIndex(meta.getIndexId());
        if (index == null) {
            return null;
        }
        Tablet tablet = index.getTablet(tabletId);
        if (tablet instanceof LakeTablet) {
            return (LakeTablet) tablet;
        }
        return null;
    }

    private ComputeNode pickComputeNode(long tabletId) {
        try {
            WarehouseManager whMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            long tableId = getTableIdByTabletId(tabletId);
            ComputeResource computeResource = whMgr.getVectorIndexBuildComputeResource(tableId);
            return whMgr.getComputeNodeAssignedToTablet(computeResource, tabletId);
        } catch (Exception e) {
            LOG.warn("Failed to pick compute node for tablet {}", tabletId, e);
            return null;
        }
    }

    private static long getTableIdByTabletId(long tabletId) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        if (invertedIndex != null) {
            TabletMeta meta = invertedIndex.getTabletMeta(tabletId);
            if (meta != null) {
                return meta.getTableId();
            }
        }
        return -1;
    }

    static boolean hasAsyncVectorIndex(OlapTable table) {
        if (table.getIndexes() == null) {
            return false;
        }
        for (Index index : table.getIndexes()) {
            if (index.getIndexType() == IndexDef.IndexType.VECTOR) {
                Map<String, String> props = index.getProperties();
                if (props != null && "async".equalsIgnoreCase(props.get("index_build_mode"))) {
                    return true;
                }
            }
        }
        return false;
    }

    // ========== Test helpers ==========

    Map<Long, VectorIndexBuildTask> getRunningTasksForTest() {
        return runningTasks;
    }

    ConcurrentHashMap<Long, Long> getPendingTabletsForTest() {
        return pendingTablets;
    }
}
