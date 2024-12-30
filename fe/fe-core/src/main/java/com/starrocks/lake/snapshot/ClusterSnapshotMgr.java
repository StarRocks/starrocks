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
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

// only used for AUTOMATED snapshot for now
public class ClusterSnapshotMgr implements GsonPostProcessable {
    public static final Logger LOG = LogManager.getLogger(ClusterSnapshotMgr.class);
    public static final String AUTOMATED_NAME_PREFIX = "automated_cluster_snapshot";

    @SerializedName(value = "automatedSnapshotSvName")
    private String automatedSnapshotSvName = "";

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

    public String getAutomatedSnapshotSvName() {
        return automatedSnapshotSvName;
    }

    public boolean isAutomatedSnapshotOn() {
        return automatedSnapshotSvName != null && !automatedSnapshotSvName.isEmpty();
    }

    // Turn off automated snapshot, use stmt for extension in future
    public void setAutomatedSnapshotOff(AdminSetAutomatedSnapshotOffStmt stmt) {
        setAutomatedSnapshotOff();

        ClusterSnapshotLog log = new ClusterSnapshotLog();
        log.setDropSnapshot(AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getEditLog().logClusterSnapshotLog(log);
    }

    protected void setAutomatedSnapshotOff() {
        // drop AUTOMATED snapshot
        automatedSnapshotSvName = "";
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
            case DROP_SNAPSHOT: {
                String dropSnapshotName = log.getDropSnapshotName();
                if (dropSnapshotName.equals(AUTOMATED_NAME_PREFIX)) {
                    setAutomatedSnapshotOff();
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
