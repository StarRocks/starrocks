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


package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 * for LakeTablet
 */
public class LakeTabletsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(LakeTabletsProcDir.class);

    /**
     * Columns returned by both {@code SHOW TABLETS FROM table} and
     * {@code SHOW PROC /dbs/.../partitions/indexId[/tabletId]}.
     * <p>
     * The last two columns (StorageVolume, StoragePath) are populated by a
     * best-effort StarOS RPC call in {@link #fetchComparableResult()}.
     */
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("BackendId").add("DataSize").add("RowCount")
            .add("MinVersion").add("Range").add("StorageVolume").add("StoragePath")
            .build();

    private final Database db;
    // lake table or lake materialized view
    private final OlapTable table;
    private final MaterializedIndex index;

    public LakeTabletsProcDir(Database db, OlapTable table, MaterializedIndex index) {
        this.db = db;
        this.table = table;
        this.index = index;
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    /**
     * Returns one 8-element row per tablet:
     * [TabletId, BackendId, DataSize, RowCount, MinVersion, Range, StorageVolume, StoragePath].
     * <p>
     * Phase 1 (under DB read lock): collect basic tablet info and tablet IDs.
     * Phase 2 (outside DB lock): batch-query ShardInfo from StarOS and enrich
     * StorageVolume / StoragePath.  StarOS errors are logged and silently swallowed
     * so that the command always returns something even when StarOS is unavailable.
     */
    public List<List<Comparable>> fetchComparableResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());

        List<List<Comparable>> tabletInfos = Lists.newArrayList();
        List<Long> tabletIds = new ArrayList<>();

        // Phase 1: collect basic info under DB read lock
        Locker locker = new Locker();
        long tableId = table.getId();
        locker.lockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        try {
            for (Tablet tablet : index.getTablets()) {
                LakeTablet lakeTablet = (LakeTablet) tablet;
                List<Comparable> row = Lists.newArrayList();
                row.add(lakeTablet.getId());
                row.add(new Gson().toJson(lakeTablet.getBackendIds(ConnectContext.get().getCurrentComputeResource())));
                row.add(new ByteSizeValue(lakeTablet.getDataSize(true)));
                row.add(lakeTablet.getRowCount(0L));
                row.add(lakeTablet.getMinVersion());
                row.add(String.valueOf(lakeTablet.getRange()));
                row.add("");  // StorageVolume placeholder
                row.add("");  // StoragePath  placeholder
                tabletInfos.add(row);
                tabletIds.add(lakeTablet.getId());
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        // Phase 2: enrich with StarOS shard info (best-effort, outside DB lock)
        if (!tabletIds.isEmpty()) {
            try {
                ComputeResource computeResource = ConnectContext.get().getCurrentComputeResource();
                long workerGroupId = computeResource.getWorkerGroupId();
                StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                List<ShardInfo> shardInfos = starOSAgent.getShardInfoBatch(tabletIds, workerGroupId);

                Map<Long, ShardInfo> shardInfoMap = new HashMap<>();
                for (ShardInfo si : shardInfos) {
                    shardInfoMap.put(si.getShardId(), si);
                }

                StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                for (List<Comparable> row : tabletInfos) {
                    long tabletId = (Long) row.get(0);
                    ShardInfo si = shardInfoMap.get(tabletId);
                    if (si != null && si.hasFilePath()) {
                        String fullPath = si.getFilePath().getFullPath();
                        String fsKey = si.getFilePath().getFsInfo().getFsKey();
                        StorageVolume sv = svm.getStorageVolume(fsKey);
                        row.set(6, sv != null ? sv.getName() : fsKey);
                        row.set(7, fullPath);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to fetch shard info for LakeTabletsProcDir", e);
            }
        }

        return tabletInfos;
    }

    @Override
    public ProcResult fetchResult() {
        List<List<Comparable>> tabletInfos = fetchComparableResult();

        // Sort by tabletId
        ListComparator<List<Comparable>> comparator = new ListComparator<>(0);
        Collections.sort(tabletInfos, comparator);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (List<Comparable> info : tabletInfos) {
            List<String> row = new ArrayList<>(info.size());
            for (Comparable c : info) {
                row.add(c.toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());

        long tabletId = -1L;
        try {
            tabletId = Long.parseLong(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        // Take per-table READ: index.getTablet reads MaterializedIndex.idToTablets,
        // which is a plain HashMap. A concurrent schema change or rebalance
        // (IX + table WRITE) can race with this get, so the lock pins the table while
        // we resolve the tablet reference.
        Locker locker = new Locker();
        long lockTableId = table.getId();
        locker.lockTableWithIntensiveDbLock(db.getId(), lockTableId, LockType.READ);
        try {
            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                throw new AnalysisException("Can't find tablet id: " + tabletIdStr);
            }
            Preconditions.checkState(tablet instanceof LakeTablet);
            return new LakeTabletProcNode((LakeTablet) tablet);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), lockTableId, LockType.READ);
        }
    }

    // Handle showing single tablet info
    public static class LakeTabletProcNode implements ProcNodeInterface {
        private static final Logger LOG = LogManager.getLogger(LakeTabletProcNode.class);

        private final LakeTablet tablet;

        public LakeTabletProcNode(LakeTablet tablet) {
            this.tablet = tablet;
        }

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);

            // get current warehouse and compute resource
            ComputeResource computeResource = ConnectContext.get().getCurrentComputeResource();
            String svName = "";
            String storagePath = "";
            try {
                long workerGroupId = computeResource.getWorkerGroupId();
                StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
                ShardInfo si = starOSAgent.getShardInfo(tablet.getId(), workerGroupId);
                if (si != null && si.hasFilePath()) {
                    storagePath = si.getFilePath().getFullPath();
                    String fsKey = si.getFilePath().getFsInfo().getFsKey();
                    StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                    StorageVolume sv = svm.getStorageVolume(fsKey);
                    svName = (sv != null) ? sv.getName() : fsKey;
                }
            } catch (Exception e) {
                LOG.warn("Failed to fetch shard info for tablet {}", tablet.getId(), e);
            }


            List<String> row = Arrays.asList(
                    String.valueOf(tablet.getId()),
                    new Gson().toJson(tablet.getBackendIds(computeResource)),
                    new ByteSizeValue(tablet.getDataSize(true)).toString(),
                    String.valueOf(tablet.getRowCount(0L)),
                    String.valueOf(tablet.getMinVersion()),
                    String.valueOf(tablet.getRange()),
                    svName,
                    storagePath
            );
            result.addRow(row);

            return result;
        }
    }
}
