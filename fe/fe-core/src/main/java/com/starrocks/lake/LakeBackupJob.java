// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

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
import com.starrocks.lake.proto.LockTabletMetadataRequest;
import com.starrocks.lake.proto.LockTabletMetadataResponse;
import com.starrocks.lake.proto.Snapshot;
import com.starrocks.lake.proto.UnlockTabletMetadataRequest;
import com.starrocks.lake.proto.UploadSnapshotsRequest;
import com.starrocks.lake.proto.UploadSnapshotsResponse;
import com.starrocks.persist.gson.GsonUtils;
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

    private Map<Long, Future<LockTabletMetadataResponse>> lockResponses = Maps.newHashMap();

    private Map<Long, Future<UploadSnapshotsResponse>> uploadResponses = Maps.newHashMap();

    public LakeBackupJob() {}

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
            lockResponses.put(entry.getKey().getTabletId(), response);
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
            Iterator<Map.Entry<Long, Future<LockTabletMetadataResponse>>> entries = lockResponses.entrySet().iterator();
            while (entries.hasNext()) {
                try {
                    Map.Entry<Long, Future<LockTabletMetadataResponse>> entry = entries.next();
                    entry.getValue().get(1000, TimeUnit.MILLISECONDS);
                    unfinishedTaskIds.remove(entry.getKey());
                    entries.remove();
                } catch (TimeoutException e) {
                    // Maybe there are many snapshot tasks, so we ignore timeout exception.
                }
            }
            super.waitingAllSnapshotsFinished();
        } catch (InterruptedException | ExecutionException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }

    @Override
    @java.lang.SuppressWarnings("squid:S2142")  // allow catch InterruptedException
    protected void waitingAllUploadingFinished() {
        try {
            Iterator<Map.Entry<Long, Future<UploadSnapshotsResponse>>> entries = uploadResponses.entrySet().iterator();
            while (entries.hasNext()) {
                try {
                    Map.Entry<Long, Future<UploadSnapshotsResponse>> entry = entries.next();
                    entry.getValue().get(1000, TimeUnit.MILLISECONDS);
                    unfinishedTaskIds.remove(entry.getKey());
                    entries.remove();
                } catch (TimeoutException e) {
                    // Maybe there are many upload tasks, so we ignore timeout exception.
                }
            }
            super.waitingAllUploadingFinished();
        } catch (InterruptedException | ExecutionException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }
    }
}
