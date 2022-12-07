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

import com.google.common.collect.Maps;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.backup.Status;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.proto.LockTabletMetadataRequest;
import com.starrocks.proto.LockTabletMetadataResponse;
import com.starrocks.proto.Snapshot;
import com.starrocks.proto.UnlockTabletMetadataRequest;
import com.starrocks.proto.UploadSnapshotsRequest;
import com.starrocks.proto.UploadSnapshotsResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.THdfsProperties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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

public class LakeBackupJob extends BackupJob {
    private static final Logger LOG = LogManager.getLogger(LakeBackupJob.class);

    private Map<SnapshotInfo, LockTabletMetadataRequest> lockRequests = Maps.newHashMap();

    private Map<Backend, UploadSnapshotsRequest> uploadRequests = Maps.newHashMap();

    private Map<SnapshotInfo, Future<LockTabletMetadataResponse>> lockResponses = Maps.newHashMap();

    private Map<Long, Future<UploadSnapshotsResponse>> uploadResponses = Maps.newHashMap();

    public LakeBackupJob() {
    }

    public LakeBackupJob(String label, long dbId, String dbName, List<TableRef> tableRefs, long timeoutMs,
                         GlobalStateMgr globalStateMgr, long repoId) {
        super(label, dbId, dbName, tableRefs, timeoutMs, globalStateMgr, repoId);
        this.type = JobType.LAKE_BACKUP;
    }

    @Override
    protected void checkBackupTables(Database db) {
        for (TableRef tableRef : tableRefs) {
            String tblName = tableRef.getName().getTbl();
            Table tbl = db.getTable(tblName);
            if (tbl == null) {
                status = new Status(Status.ErrCode.NOT_FOUND, "table " + tblName + " does not exist");
                return;
            }
            if (!tbl.isLakeTable()) {
                status = new Status(Status.ErrCode.COMMON_ERROR, "table " + tblName
                        + " is not LAKE table");
                return;
            }

            LakeTable lakeTbl = (LakeTable) tbl;
            if (tableRef.getPartitionNames() != null) {
                for (String partName : tableRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = lakeTbl.getPartition(partName);
                    if (partition == null) {
                        status = new Status(Status.ErrCode.NOT_FOUND, "partition " + partName
                                + " does not exist  in table" + tblName);
                        return;
                    }
                }
            }
        }
    }

    @Override
    protected void prepareSnapshotTask(Partition partition, Table tbl, Tablet tablet, MaterializedIndex index,
                                       long visibleVersion, int schemaHash) {
        try {
            Backend backend = GlobalStateMgr.getCurrentSystemInfo()
                    .getBackend(((LakeTablet) tablet).getPrimaryBackendId());
            LakeTableSnapshotInfo snapshotInfo = new LakeTableSnapshotInfo(dbId,
                    tbl.getId(), partition.getId(), index.getId(), tablet.getId(),
                    backend.getId(), schemaHash, visibleVersion);

            LockTabletMetadataRequest request = new LockTabletMetadataRequest();
            request.tabletId = tablet.getId();
            request.version = visibleVersion;
            request.expireTime = (createTime + timeoutMs) / 1000;
            lockRequests.put(snapshotInfo, request);
            unfinishedTaskIds.put(tablet.getId(), 1L);
        } catch (UserException e) {
            LOG.error(e.getMessage());
            status = new Status(Status.ErrCode.COMMON_ERROR,
                    "failed to choose replica to make snapshot for tablet " + tablet.getId()
                            + ". visible version: " + visibleVersion);
        }
    }

    @Override
    protected void sendSnapshotRequests() {
        for (Map.Entry<SnapshotInfo, LockTabletMetadataRequest> entry : lockRequests.entrySet()) {
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(entry.getKey().getBeId());
            LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
            Future<LockTabletMetadataResponse> response = lakeService.lockTabletMetadata(entry.getValue());
            lockResponses.put(entry.getKey(), response);
        }
    }

    @Override
    protected void releaseSnapshots() {
        if (snapshotInfos.isEmpty()) {
            return;
        }

        for (SnapshotInfo info : snapshotInfos.values()) {
            UnlockTabletMetadataRequest request = new UnlockTabletMetadataRequest();
            request.tabletId = info.getTabletId();
            request.version = ((LakeTableSnapshotInfo) info).getVersion();
            request.expireTime = (createTime + timeoutMs) / 1000;
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(info.getBeId());
            LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(),
                    backend.getBrpcPort());
            lakeService.unlockTabletMetadata(request);
        }
    }

    @Override
    protected void prepareUploadTasks(List<SnapshotInfo> infos, List<FsBroker> brokers,
                                      THdfsProperties hdfsProperties, Long beId) {
        UploadSnapshotsRequest request = new UploadSnapshotsRequest();
        if (repo.getStorage().hasBroker()) {
            request.broker = brokers.get(0).toString();
            request.brokerProperties = Maps.newHashMap();
            request.brokerProperties.putAll(repo.getStorage().getProperties());
        }
        request.snapshots = Maps.newHashMap();
        for (SnapshotInfo info : infos) {
            LakeTableSnapshotInfo lakeInfo = (LakeTableSnapshotInfo) info;
            Snapshot snapshot = new Snapshot();
            snapshot.version = ((LakeTableSnapshotInfo) info).getVersion();
            snapshot.destPath = repo.getRepoTabletPathBySnapshotInfo(label, info);
            request.snapshots.put(lakeInfo.getTabletId(), snapshot);
        }
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
        unfinishedTaskIds.put(beId, 1L);
        uploadRequests.put(backend, request);
    }

    @Override
    protected void sendUploadTasks() {
        for (Map.Entry<Backend, UploadSnapshotsRequest> entry : uploadRequests.entrySet()) {
            LakeService lakeService = BrpcProxy.getLakeService(entry.getKey().getHost(), entry.getKey().getBrpcPort());
            Future<UploadSnapshotsResponse> response = lakeService.uploadSnapshots(entry.getValue());
            uploadResponses.put(entry.getKey().getId(), response);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static LakeBackupJob read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, LakeBackupJob.class);
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    protected void waitingAllSnapshotsFinished() {
        try {
            Iterator<Map.Entry<SnapshotInfo, Future<LockTabletMetadataResponse>>> entries = lockResponses.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<SnapshotInfo, Future<LockTabletMetadataResponse>> entry = entries.next();
                getLockTabletMetadataResponse(entry);
                entries.remove();
            }
            super.waitingAllSnapshotsFinished();
        } catch (InterruptedException | ExecutionException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    private void getLockTabletMetadataResponse(Map.Entry<SnapshotInfo, Future<LockTabletMetadataResponse>> entry)
            throws ExecutionException, InterruptedException {
        try {
            entry.getValue().get(1000, TimeUnit.MILLISECONDS);
            unfinishedTaskIds.remove(entry.getKey().getTabletId());
            taskProgress.remove(entry.getKey().getTabletId());
            snapshotInfos.put(entry.getKey().getTabletId(), entry.getKey());
        } catch (TimeoutException e) {
            // Maybe there are many snapshot tasks, so we ignore timeout exception.
        }
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    protected void waitingAllUploadingFinished() {
        try {
            Iterator<Map.Entry<Long, Future<UploadSnapshotsResponse>>> entries = uploadResponses.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry<Long, Future<UploadSnapshotsResponse>> entry = entries.next();
                getUploadSnapshotsResponse(entry);
                entries.remove();
            }
            super.waitingAllUploadingFinished();
        } catch (InterruptedException | ExecutionException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    private void getUploadSnapshotsResponse(Map.Entry<Long, Future<UploadSnapshotsResponse>> entry)
            throws ExecutionException, InterruptedException {
        try {
            entry.getValue().get(1000, TimeUnit.MILLISECONDS);
            unfinishedTaskIds.remove(entry.getKey());
        } catch (TimeoutException e) {
            // Maybe there are many upload tasks, so we ignore timeout exception.
        }
    }
}
