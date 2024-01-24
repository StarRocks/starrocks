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

package com.starrocks.lake.backup;

import autovalue.shaded.com.google.common.common.collect.Maps;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.staros.proto.FilePathInfo;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupJobInfo.BackupIndexInfo;
import com.starrocks.backup.BackupJobInfo.BackupTabletInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.RestoreFileMapping.IdChain;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.backup.Status;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StorageInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.RestoreInfo;
import com.starrocks.proto.RestoreSnapshotsRequest;
import com.starrocks.proto.RestoreSnapshotsResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LakeRestoreJob extends RestoreJob {
    private static final Logger LOG = LogManager.getLogger(LakeRestoreJob.class);

    private Map<Long, RestoreSnapshotsRequest> requests = Maps.newHashMap();

    private Map<Long, Future<RestoreSnapshotsResponse>> responses = Maps.newHashMap();

    public LakeRestoreJob() {
    }

    public LakeRestoreJob(String label, String backupTs, long dbId, String dbName, BackupJobInfo jobInfo,
                          boolean allowLoad, int restoreReplicationNum, long timeoutMs,
                          GlobalStateMgr globalStateMgr, long repoId, BackupMeta backupMeta,
                          MvRestoreContext mvRestoreContext) {
        super(label, backupTs, dbId, dbName, jobInfo, allowLoad, restoreReplicationNum, timeoutMs,
                globalStateMgr, repoId, backupMeta, mvRestoreContext);
        this.type = JobType.LAKE_RESTORE;
    }

    @Override
    protected void createReplicas(OlapTable localTbl, Partition restorePart) {
        modifyInvertedIndex(localTbl, restorePart);
    }

    @Override
    protected void genFileMapping(OlapTable localTbl, Partition localPartition, Long remoteTblId,
                                  BackupJobInfo.BackupPartitionInfo backupPartInfo, boolean overwrite) {
        for (MaterializedIndex localIdx : localPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            BackupIndexInfo backupIdxInfo = backupPartInfo.getIdx(localTbl.getIndexNameById(localIdx.getId()));
            Preconditions.checkState(backupIdxInfo.tablets.size() == localIdx.getTablets().size());
            for (int i = 0; i < localIdx.getTablets().size(); i++) {
                BackupTabletInfo backupTabletInfo = backupIdxInfo.tablets.get(i);
                LakeTablet localTablet = (LakeTablet) localIdx.getTablets().get(i);
                IdChain src = new IdChain(remoteTblId, backupPartInfo.id, backupIdxInfo.id, backupTabletInfo.id,
                        -1L /* no replica id */);
                IdChain dest = new IdChain(localTbl.getId(), localPartition.getId(),
                        localIdx.getId(), localTablet.getId(), -1L /* no replica id */);
                fileMapping.putMapping(dest, src, overwrite);
            }
        }
    }

    @Override
    protected void sendCreateReplicaTasks() {
        // It is no need to send create replica tasks for lake table.
    }

    @Override
    protected void prepareAndSendSnapshotTasks(Database db) {
        for (IdChain idChain : fileMapping.getMapping().keySet()) {
            LakeTablet tablet = null;
            try {
                OlapTable tbl = (OlapTable) db.getTable(idChain.getTblId());
                Partition part = tbl.getPartition(idChain.getPartId());
                MaterializedIndex index = part.getIndex(idChain.getIdxId());
                tablet = (LakeTablet) index.getTablet(idChain.getTabletId());
                Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .getBackend(tablet.getPrimaryComputeNodeId());
                LakeTableSnapshotInfo info = new LakeTableSnapshotInfo(db.getId(), idChain.getTblId(),
                        idChain.getPartId(), idChain.getIdxId(), idChain.getTabletId(),
                        backend.getId(), tbl.getSchemaHashByIndexId(index.getId()), -1);
                snapshotInfos.put(idChain.getTabletId(), backend.getId(), info);
            } catch (UserException e) {
                LOG.error(e.getMessage());
                status = new Status(Status.ErrCode.COMMON_ERROR,
                        "failed to choose replica to make snapshot for tablet " + tablet.getId());
            }
        }
    }

    @Override
    protected void prepareDownloadTasks(List<SnapshotInfo> beSnapshotInfos, Database db, long beId,
                                        List<FsBroker> brokerAddrs, THdfsProperties hdfsProperties) {
        RestoreSnapshotsRequest request = new RestoreSnapshotsRequest();
        if (repo.getStorage().hasBroker()) {
            request.broker = brokerAddrs.get(0).toString();
            request.brokerProperties = Maps.newHashMap();
            request.brokerProperties.putAll(repo.getStorage().getProperties());
        }
        request.restoreInfos = Lists.newArrayList();
        for (SnapshotInfo info : beSnapshotInfos) {
            OlapTable tbl = (OlapTable) db.getTable(info.getTblId());
            if (tbl == null) {
                status = new Status(Status.ErrCode.NOT_FOUND, "restored table "
                        + info.getTblId() + " does not exist");
                return;
            }

            Partition part = tbl.getPartition(info.getPartitionId());
            if (part == null) {
                status = new Status(Status.ErrCode.NOT_FOUND, "partition "
                        + info.getPartitionId() + " does not exist in restored table: "
                        + tbl.getName());
                return;
            }

            MaterializedIndex idx = part.getIndex(info.getIndexId());
            if (idx == null) {
                status = new Status(Status.ErrCode.NOT_FOUND,
                        "index " + info.getIndexId() + " does not exist in partion " + part.getName()
                                + "of restored table " + tbl.getName());
                return;
            }

            LakeTablet tablet = (LakeTablet) idx.getTablet(info.getTabletId());
            if (tablet == null) {
                status = new Status(Status.ErrCode.NOT_FOUND,
                        "tablet " + info.getTabletId() + " does not exist in restored table "
                                + tbl.getName());
                return;
            }

            IdChain catalogIds = new IdChain(tbl.getId(), part.getId(), idx.getId(),
                    info.getTabletId(), -1L);
            IdChain repoIds = fileMapping.get(catalogIds);
            if (repoIds == null) {
                status = new Status(Status.ErrCode.NOT_FOUND,
                        "failed to get id mapping of globalStateMgr ids: " + catalogIds.toString());
                LOG.info("current file mapping: {}", fileMapping);
                return;
            }

            String repoTabletPath = jobInfo.getFilePath(repoIds);
            String src = repo.getRepoPath(label, repoTabletPath);
            RestoreInfo restoreInfo = new RestoreInfo();
            restoreInfo.snapshotPath = src;
            restoreInfo.tabletId = info.getTabletId();
            request.restoreInfos.add(restoreInfo);
        }
        requests.put(beId, request);
        unfinishedSignatureToId.put(beId, 1L);
    }

    @Override
    protected void sendDownloadTasks() {
        for (Map.Entry<Long, RestoreSnapshotsRequest> entry : requests.entrySet()) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(entry.getKey());
            LakeService lakeService = null;
            try {
                lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            } catch (RpcException e) {
                throw new RuntimeException(e);
            }
            Future<RestoreSnapshotsResponse> response = lakeService.restoreSnapshots(entry.getValue());
            responses.put(entry.getKey(), response);
        }
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    protected void waitingAllDownloadFinished() {
        try {
            Iterator<Map.Entry<Long, Future<RestoreSnapshotsResponse>>> entries = responses.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<Long, Future<RestoreSnapshotsResponse>> entry = entries.next();
                getRestoreSnapshotsResponse(entry);
                entries.remove();
            }
            super.waitingAllDownloadFinished();
        } catch (InterruptedException | ExecutionException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    private void getRestoreSnapshotsResponse(Map.Entry<Long, Future<RestoreSnapshotsResponse>> entry)
            throws ExecutionException, InterruptedException {
        try {
            entry.getValue().get(1000, TimeUnit.MILLISECONDS);
            unfinishedSignatureToId.remove(entry.getKey());
        } catch (TimeoutException e) {
            // Maybe there are many download tasks, so we ignore timeout exception.
        }
    }

    @Override
    protected void prepareAndSendDirMoveTasks() {
        // It is no need to move dir for lake table.
    }

    @Override
    protected void updateTablets(MaterializedIndex idx, PhysicalPartition part) {
        // It is no need to update tablets for lake table.
    }

    @Override
    protected void releaseSnapshots() {
        // It is no need to release snapshots for lake table.
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static LakeRestoreJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeRestoreJob.class);
    }

    @Override
    protected void modifyInvertedIndex(OlapTable restoreTbl, Partition restorePart) {
        for (MaterializedIndex restoredIdx : restorePart.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            MaterializedIndexMeta indexMeta = restoreTbl.getIndexMetaByIndexId(restoredIdx.getId());
            TStorageMedium medium = restoreTbl.getPartitionInfo().getDataProperty(restorePart.getId()).getStorageMedium();
            TabletMeta tabletMeta = new TabletMeta(dbId, restoreTbl.getId(), restorePart.getId(),
                    restoredIdx.getId(), indexMeta.getSchemaHash(), medium, true);
            for (Tablet restoreTablet : restoredIdx.getTablets()) {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().addTablet(restoreTablet.getId(), tabletMeta);
            }
        }
    }

    @Override
    protected void addRestoredPartitions(Database db, boolean modify) {
        for (Pair<String, Partition> entry : restoredPartitions) {
            OlapTable localTbl = (OlapTable) db.getTable(entry.first);
            Partition restorePart = entry.second;
            OlapTable remoteTbl = (OlapTable) backupMeta.getTable(entry.first);
            RangePartitionInfo localPartitionInfo = (RangePartitionInfo) localTbl.getPartitionInfo();
            RangePartitionInfo remotePartitionInfo = (RangePartitionInfo) remoteTbl.getPartitionInfo();
            BackupJobInfo.BackupPartitionInfo backupPartitionInfo =
                    jobInfo.getTableInfo(entry.first).getPartInfo(restorePart.getName());
            long remotePartId = backupPartitionInfo.id;
            Range<PartitionKey> remoteRange = remotePartitionInfo.getRange(remotePartId);
            DataProperty remoteDataProperty = remotePartitionInfo.getDataProperty(remotePartId);
            localPartitionInfo.addPartition(restorePart.getId(), false, remoteRange,
                    remoteDataProperty, (short) restoreReplicationNum,
                    remotePartitionInfo.getIsInMemory(remotePartId),
                    remotePartitionInfo.getDataCacheInfo(remotePartId));
            localTbl.addPartition(restorePart);
            if (modify) {
                // modify tablet inverted index
                modifyInvertedIndex(localTbl, restorePart);
            }
        }
    }

    @Override
    protected Status resetTableForRestore(OlapTable remoteOlapTbl, Database db) {
        try {
            FilePathInfo pathInfo = globalStateMgr.getStarOSAgent().allocateFilePath(db.getId(), remoteOlapTbl.getId());
            LakeTable remoteLakeTbl = (LakeTable) remoteOlapTbl;
            StorageInfo storageInfo = remoteLakeTbl.getTableProperty().getStorageInfo();
            remoteLakeTbl.setStorageInfo(pathInfo, storageInfo.getDataCacheInfo());
            remoteLakeTbl.resetIdsForRestore(globalStateMgr, db, restoreReplicationNum, new MvRestoreContext());
        } catch (DdlException e) {
            return new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
        return Status.OK;
    }
}
