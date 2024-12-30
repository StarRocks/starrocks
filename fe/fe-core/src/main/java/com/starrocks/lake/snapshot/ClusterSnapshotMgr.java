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

package com.starrocks.lake.snapshot;

import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.Status;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.Storage;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

// only used for AUTOMATED snapshot for now
public class ClusterSnapshotMgr implements GsonPostProcessable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotMgr.class);
    public static final String AUTOMATED_NAME_PREFIX = "automated_cluster_snapshot";
    private static final String UPLOAD_SUB_PATH = "/meta/image/";

    @SerializedName(value = "automatedSnapshotSvName")
    private String automatedSnapshotSvName = "";
    @SerializedName(value = "automatedSnapshot")
    private ClusterSnapshot automatedSnapshot = null;
    @SerializedName(value = "historyAutomatedSnapshotJobs")
    private TreeMap<Long, ClusterSnapshotJob> historyAutomatedSnapshotJobs = new TreeMap<>();

    private BlobStorage remoteStorage;
    private String locationWithServiceId;

    public ClusterSnapshotMgr() {}

    // Turn on automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOn(AdminSetAutomatedSnapshotOnStmt stmt) {
        String storageVolumeName = stmt.getStorageVolumeName();
        setAutomatedSnapshotOn(storageVolumeName);

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setCreateSnapshotNamePrefix(AUTOMATED_NAME_PREFIX, storageVolumeName);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    protected void setAutomatedSnapshotOn(String storageVolumeName) {
        automatedSnapshotSvName = storageVolumeName;
    }

    public void createAutomatedSnaphot(ClusterSnapshotJob job) {
        ClusterSnapshot newAutomatedSnapshot = new ClusterSnapshot(
                    GlobalStateMgr.getCurrentState().getNextId(), job.getSnapshotName(), job.getStorageVolumeName(),
                        job.getCreateTime(), job.getSuccessTime(), job.getFeJournalId(), job.getStarMgrJournalId());

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setCreateSnapshot(newAutomatedSnapshot);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);

        if (automatedSnapshot != null && automatedSnapshot.getSnapshotName().startsWith(AUTOMATED_NAME_PREFIX)) {
            deleteSnapshotFromRemote(automatedSnapshot.getSnapshotName());
        }

        automatedSnapshot = newAutomatedSnapshot;

        LOG.info("Finish automated cluster snapshot job successfully, job id: {}, snapshot name: {}", job.getJobId(),
                 job.getSnapshotName());
    }

    public ClusterSnapshotJob createNewAutomatedSnapshotJob() {
        long createTime = System.currentTimeMillis();
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        String snapshotNamePrefix = ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX;
        String snapshotName = snapshotNamePrefix + '_' + String.valueOf(createTime);
        String storageVolumeName = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshotSvName();
        ClusterSnapshotJob job = new ClusterSnapshotJob(jobId, snapshotNamePrefix, snapshotName, storageVolumeName);
        job.setState(ClusterSnapshotJobState.INITIALIZING, false);

        addJob(job);
    
        LOG.info("Create automated cluster snapshot job successfully, job id: {}, snapshot name: {}", jobId, snapshotName);

        return job;
    }

    public String getAutomatedSnapshotSvName() {
        return automatedSnapshotSvName;
    }

    public StorageVolume getAutomatedSnapshotSv() {
        if (automatedSnapshotSvName.isEmpty()) {
            return null;
        }

        return GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeByName(automatedSnapshotSvName);
    }

    public ClusterSnapshot getAutomatedSnapshot() {
        return automatedSnapshot;
    }

    public boolean containsAutomatedSnapshot() {
        return getAutomatedSnapshot() != null;
    }

    public boolean isAutomatedSnapshotOn() {
        return automatedSnapshotSvName != null && !automatedSnapshotSvName.isEmpty();
    }

    public synchronized void addJob(ClusterSnapshotJob job) {
        if (historyAutomatedSnapshotJobs.size() == Config.max_historical_automated_cluster_snapshot_jobs) {
            historyAutomatedSnapshotJobs.pollFirstEntry();
        }
        historyAutomatedSnapshotJobs.put(job.getJobId(), job);
    }

    public String getLastFinishedAutomatedSnapshotJobName() {
        ClusterSnapshot snapshot = getAutomatedSnapshot();
        if (snapshot == null) {
            return "";
        }
        return snapshot.getSnapshotName();
    }

    // Turn off automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOff(AdminSetAutomatedSnapshotOffStmt stmt) {
        setAutomatedSnapshotOff();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setDropSnapshot(AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);

        // avoid network communication when replay log
        if (automatedSnapshot != null) {
            deleteSnapshotFromRemote(automatedSnapshot.getSnapshotName());
        }
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        automatedSnapshotSvName = "";
        automatedSnapshot = null;
        historyAutomatedSnapshotJobs.clear();
    }

    public TClusterSnapshotJobsResponse getAllJobsInfo() {
        TClusterSnapshotJobsResponse response = new TClusterSnapshotJobsResponse();
        for (Map.Entry<Long, ClusterSnapshotJob> entry : historyAutomatedSnapshotJobs.entrySet()) {
            response.addToItems(entry.getValue().getInfo());
        }
        return response;
    }

    public TClusterSnapshotsResponse getAllInfo() {
        TClusterSnapshotsResponse response = new TClusterSnapshotsResponse();
        if (automatedSnapshot != null) {
            response.addToItems(automatedSnapshot.getInfo());
        }
        return response;
    }

    public Status actualUploadImageForSnapshot(boolean belongToGlobalStateMgr, String snapshotName, String localMetaDir) {
        initRemoteStorageContext();
        String curRemoteSnapshotPath = locationWithServiceId + UPLOAD_SUB_PATH + snapshotName;

        Status status = Status.OK;
        try {
            if (belongToGlobalStateMgr) {
                do {
                    Storage localStorage = new Storage(localMetaDir);
                    Storage localStorageV2 = new Storage(localMetaDir + "/v2");
                    long imageJournalId = localStorage.getImageJournalId();
                    File curFile = null;

                    curFile = localStorageV2.getCurrentImageFile();
                    status = remoteStorage.upload(curFile.getAbsolutePath(), curRemoteSnapshotPath + "/v2/" + curFile.getName());
                    if (!status.ok()) {
                        break;
                    }
    
                    curFile = localStorageV2.getCurrentChecksumFile();
                    status = remoteStorage.upload(curFile.getAbsolutePath(), curRemoteSnapshotPath + "/v2/" + curFile.getName());
                    if (!status.ok()) {
                        break;
                    }

                    curFile = localStorage.getRoleFile();
                    status = remoteStorage.upload(curFile.getAbsolutePath(), curRemoteSnapshotPath + "/" + curFile.getName());
                    if (!status.ok()) {
                        break;
                    }
    
                    curFile = localStorage.getVersionFile();
                    status = remoteStorage.upload(curFile.getAbsolutePath(), curRemoteSnapshotPath + "/" + curFile.getName());
                    if (!status.ok()) {
                        break;
                    }
                } while (false);
            } else {
                Storage localStorage = new Storage(localMetaDir);
                File curFile = localStorage.getCurrentImageFile();

                status = remoteStorage.upload(curFile.getAbsolutePath(), curRemoteSnapshotPath + "/starmgr/" + curFile.getName());
            }
        } catch (IOException e) {
            status = new Status(Status.ErrCode.COMMON_ERROR, e.getMessage());
        }

        return status;
    }

    public void deleteSnapshotFromRemote(String snapshotName) {
        if (snapshotName == null || snapshotName.isEmpty()) {
            return;
        }

        initRemoteStorageContext();
        String curRemoteSnapshotPath = locationWithServiceId + UPLOAD_SUB_PATH + snapshotName + '/';
        remoteStorage.delete(curRemoteSnapshotPath);
    }

    private void initRemoteStorageContext() {
        if (this.remoteStorage == null || this.locationWithServiceId == null) {
            StorageVolume sv = getAutomatedSnapshotSv();
            this.remoteStorage = new BlobStorage(null, sv.getProperties(), false);
            this.locationWithServiceId = sv.getLocations().get(0) + "/" +
                                         GlobalStateMgr.getCurrentState().getStarOSAgent().getRawServiceId();
        }
    }

    public void replayLog(ClusterSnapshotLog log) {
        ClusterSnapshotLog.ClusterSnapshotLogType logType = log.getType();
        switch (logType) {
            case CREATE_SNAPSHOT_PREFIX: {
                String createSnapshotNamePrefix = log.getCreateSnapshotNamePrefix();
                String storageVolumeName = log.getStorageVolumeName();
                if (createSnapshotNamePrefix.equals(AUTOMATED_NAME_PREFIX)) {
                    setAutomatedSnapshotOn(storageVolumeName);
                }
                break;
            }
            case CREATE_SNAPSHOT: {
                ClusterSnapshot snapshot = log.getSnapshot();
                automatedSnapshot = snapshot;
                break;
            }
            case DROP_SNAPSHOT: {
                String dropSnapshotName = log.getDropSnapshotName();
                if (dropSnapshotName.equals(AUTOMATED_NAME_PREFIX)) {
                    setAutomatedSnapshotOff();
                }
                break;
            }
            case UPDATE_SNAPSHOT_JOB: {
                FrontendNodeType feType = GlobalStateMgr.getCurrentState().getFeType();
                ClusterSnapshotJob job = log.getSnapshotJob();
                ClusterSnapshotJobState state = job.getState();

                if ((state == ClusterSnapshotJobState.INITIALIZING || state == ClusterSnapshotJobState.SNAPSHOTING) &&
                        (feType == FrontendNodeType.INIT)) {
                    job.setState(ClusterSnapshotJobState.ERROR, true);
                    job.setErrMsg("Snapshot job has been failed");
                }

                switch (state) {
                    case INITIALIZING: {
                        addJob(job);
                        break;
                    }
                    case SNAPSHOTING:
                    case FINISHED:
                    case ERROR: {
                        if (historyAutomatedSnapshotJobs.containsKey(job.getJobId())) {
                            historyAutomatedSnapshotJobs.remove(job.getJobId());
                            historyAutomatedSnapshotJobs.put(job.getJobId(), job);
                        }
                        break;
                    }
                    default: {
                        LOG.warn("Invalid Cluster Snapshot Job state {}", state);
                    }
                }
                break;
            }
            default: {
                LOG.warn("Invalid Cluster Snapshot Log Type {}", logType);
            }
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.CLUSTER_SNAPSHOT_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        ClusterSnapshotMgr data = reader.readJson(ClusterSnapshotMgr.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }
}
