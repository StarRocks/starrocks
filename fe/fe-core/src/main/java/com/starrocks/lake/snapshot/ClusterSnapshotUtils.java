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

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.storagevolume.StorageVolume;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class ClusterSnapshotUtils {
    private static final Logger LOG = LogManager.getLogger(ClusterSnapshotUtils.class);

    public static final String SNAPSHOT_META_FILE_NAME = "snapshot_meta.json";

    public static String getSnapshotImagePath(StorageVolume sv, String snapshotName) {
        return String.join("/", sv.getLocations().get(0),
                GlobalStateMgr.getCurrentState().getStarOSAgent().getRawServiceId(), "meta/image", snapshotName);
    }

    public static void uploadClusterSnapshotToRemote(ClusterSnapshotJob job) throws StarRocksException {
        String snapshotName = job.getSnapshotName();
        if (snapshotName == null || snapshotName.isEmpty()) {
            return;
        }

        StorageVolume sv = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getStorageVolumeBySnapshotJob(job);
        String snapshotImagePath = getSnapshotImagePath(sv, snapshotName);
        String localImagePath = GlobalStateMgr.getImageDirPath();

        HdfsUtil.copyFromLocal(localImagePath, snapshotImagePath, sv.getProperties());

        writeSnapshotMetaFile(job, snapshotImagePath, new BrokerDesc(sv.getProperties()));
    }

    public static void clearClusterSnapshotFromRemote(ClusterSnapshotJob job) throws StarRocksException {
        String snapshotName = job.getSnapshotName();
        if (snapshotName == null || snapshotName.isEmpty()) {
            return;
        }

        StorageVolume sv = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getStorageVolumeBySnapshotJob(job);
        BrokerDesc brokerDesc = new BrokerDesc(sv.getProperties());
        String snapshotImagePath = getSnapshotImagePath(sv, snapshotName);

        // Delete meta file first to mark snapshot as incomplete immediately
        deleteSnapshotMetaFile(snapshotImagePath, brokerDesc);

        HdfsUtil.deletePath(snapshotImagePath, brokerDesc);
    }

    public static String getSnapshotMetaFilePath(String snapshotImagePath) {
        return snapshotImagePath + "/" + SNAPSHOT_META_FILE_NAME;
    }

    public static boolean checkSnapshotMetaFileExist(String snapshotImagePath,
            BrokerDesc brokerDesc) throws StarRocksException {
        return HdfsUtil.checkPathExist(getSnapshotMetaFilePath(snapshotImagePath), brokerDesc);
    }

    public static void deleteSnapshotMetaFile(String snapshotImagePath, BrokerDesc brokerDesc) {
        String metaFilePath = getSnapshotMetaFilePath(snapshotImagePath);
        try {
            HdfsUtil.deletePath(metaFilePath, brokerDesc);
        } catch (Exception e) {
            LOG.warn("Failed to delete snapshot meta file: {}", metaFilePath, e);
        }
    }

    public static ClusterSnapshot readLocalSnapshotMetaFile(String localImagePath) {
        File localMetaFile = new File(localImagePath, SNAPSHOT_META_FILE_NAME);
        if (!localMetaFile.exists()) {
            return null;
        }
        try {
            String json = new String(Files.readAllBytes(localMetaFile.toPath()), StandardCharsets.UTF_8);
            ClusterSnapshot snapshot = GsonUtils.GSON.fromJson(json, ClusterSnapshot.class);
            if (snapshot == null || snapshot.getSnapshotName() == null || snapshot.getSnapshotName().isEmpty()
                    || snapshot.getFeJournalId() <= 0 || snapshot.getStarMgrJournalId() <= 0) {
                LOG.warn("Invalid local snapshot meta file: {}", localMetaFile.getAbsolutePath());
                return null;
            }
            return snapshot;
        } catch (Exception e) {
            LOG.warn("Failed to read local snapshot meta file: {}", localMetaFile.getAbsolutePath(), e);
            return null;
        }
    }

    public static void deleteLocalSnapshotMetaFile(String localImagePath) {
        File localMetaFile = new File(localImagePath, SNAPSHOT_META_FILE_NAME);
        if (localMetaFile.exists() && !localMetaFile.delete()) {
            LOG.warn("Failed to delete local snapshot meta file: {}", localMetaFile.getAbsolutePath());
        }
    }

    private static void writeSnapshotMetaFile(ClusterSnapshotJob job, String snapshotImagePath,
            BrokerDesc brokerDesc) throws StarRocksException {
        String metaFilePath = getSnapshotMetaFilePath(snapshotImagePath);
        HdfsUtil.writeFile(GsonUtils.GSON.toJson(job.getSnapshot()).getBytes(StandardCharsets.UTF_8),
                metaFilePath, brokerDesc);
    }
}
