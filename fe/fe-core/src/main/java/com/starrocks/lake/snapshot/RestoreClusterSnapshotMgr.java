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
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class RestoreClusterSnapshotMgr {
    private static final Logger LOG = LogManager.getLogger(RestoreClusterSnapshotMgr.class);

    private static RestoreClusterSnapshotMgr instance;

    private ClusterSnapshotConfig config;
    private boolean oldStartWithIncompleteMeta;
    private boolean oldResetElectionGroup;
    private RestoredSnapshotInfo restoredSnapshotInfo;

    private RestoreClusterSnapshotMgr(String clusterSnapshotYamlFile) throws StarRocksException {
        config = ClusterSnapshotConfig.load(clusterSnapshotYamlFile);
        downloadSnapshot();
        updateConfig();
    }

    public static void init(String clusterSnapshotYamlFile, String[] args) throws StarRocksException {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-cluster_snapshot")) {
                LOG.info("FE start to restore from a cluster snapshot (-cluster_snapshot)");
                instance = new RestoreClusterSnapshotMgr(clusterSnapshotYamlFile);
                return;
            }
        }

        String restoreClusterSnapshotEnv = System.getenv("RESTORE_CLUSTER_SNAPSHOT");
        if (restoreClusterSnapshotEnv != null && restoreClusterSnapshotEnv.equalsIgnoreCase("true")) {
            LOG.info("FE start to restore from a cluster snapshot (RESTORE_CLUSTER_SNAPSHOT=true)");
            instance = new RestoreClusterSnapshotMgr(clusterSnapshotYamlFile);
        }
    }

    public static boolean isRestoring() {
        return instance != null;
    }

    public static ClusterSnapshotConfig getConfig() {
        RestoreClusterSnapshotMgr self = instance;
        if (self == null) {
            return null;
        }
        return self.config;
    }

    public static void finishRestoring() throws StarRocksException {
        RestoreClusterSnapshotMgr self = instance;
        if (self == null) {
            return;
        }

        try {
            self.updateFrontends();
            self.updateComputeNodes();
            self.updateStorageVolumes();
        } finally {
            self.rollbackConfig();
            instance = null;
            LOG.info("FE finished to restore from a cluster snapshot");
        }
    }

    public static RestoredSnapshotInfo getRestoredSnapshotInfo() {
        RestoreClusterSnapshotMgr self = instance;
        if (self == null) {
            return null;
        }
        return self.restoredSnapshotInfo;
    }

    private void updateConfig() {
        // Save the old config
        oldStartWithIncompleteMeta = Config.start_with_incomplete_meta;
        // Allow starting with only image no bdb log
        Config.start_with_incomplete_meta = true;
        // Save the old config
        oldResetElectionGroup = Config.bdbje_reset_election_group;
        // Reset election group
        Config.bdbje_reset_election_group = true;
    }

    private void rollbackConfig() {
        Config.start_with_incomplete_meta = oldStartWithIncompleteMeta;
        Config.bdbje_reset_election_group = oldResetElectionGroup;
    }

    private void downloadSnapshot() throws StarRocksException {
        ClusterSnapshotConfig.ClusterSnapshot clusterSnapshot = config.getClusterSnapshot();
        if (clusterSnapshot == null) {
            return;
        }

        String localImagePath = GlobalStateMgr.getImageDirPath();
        String localBdbPath = BDBEnvironment.getBdbDir();

        if (FileUtils.deleteQuietly(new File(localImagePath))) {
            LOG.info("Deleted image dir {}", localImagePath);
        }
        if (FileUtils.deleteQuietly(new File(localBdbPath))) {
            LOG.info("Deleted bdb dir {}", localBdbPath);
        }

        String snapshotImagePath = clusterSnapshot.getClusterSnapshotPath();
        snapshotImagePath = snapshotImagePath.replaceAll("/+$", "");

        if (snapshotImagePath.endsWith("/meta")) {
            String pathPattern = snapshotImagePath + "/image/" + ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX + '*';
            List<FileStatus> fileStatusList = HdfsUtil.listFileMeta(pathPattern,
                    new BrokerDesc(clusterSnapshot.getStorageVolume().getProperties()), false);
            if (fileStatusList.isEmpty() || fileStatusList.get(0).isFile()) {
                throw new StarRocksException("No cluster snapshot found in path " + pathPattern);
            }
            snapshotImagePath = fileStatusList.get(0).getPath().toString();
        }

        LOG.info("Download cluster snapshot {} to local dir {}", snapshotImagePath, localImagePath);
        HdfsUtil.copyToLocal(snapshotImagePath, localImagePath, clusterSnapshot.getStorageVolume().getProperties());

        collectSnapshotInfoAfterDownloaded(snapshotImagePath, localImagePath);
    }

    private void collectSnapshotInfoAfterDownloaded(String snapshotImagePath, String localImagePath)
            throws StarRocksException {
        long feImageJournalId = 0L;
        long starMgrImageJournalId = 0L;

        try {
            Storage storageFe = new Storage(localImagePath);
            Storage storageStarMgr = new Storage(localImagePath + StarMgrServer.IMAGE_SUBDIR);
            // get image version
            feImageJournalId = storageFe.getImageJournalId();
            starMgrImageJournalId = storageStarMgr.getImageJournalId();
        } catch (Exception e) {
            throw new StarRocksException("Failed to get local image version", e);
        }

        int lastSlashIndex = snapshotImagePath.lastIndexOf('/');
        if (lastSlashIndex < 0) {
            throw new StarRocksException("Failed to get snapshot name from snapshot path " + snapshotImagePath);
        }

        String restoredSnapshotName = snapshotImagePath.substring(lastSlashIndex + 1);

        restoredSnapshotInfo = new RestoredSnapshotInfo(restoredSnapshotName,
                feImageJournalId, starMgrImageJournalId);

        LOG.info("Downloaded cluster snapshot {} successfully, FE image version: {}, StarMgr image version: {}",
                restoredSnapshotName, feImageJournalId, starMgrImageJournalId);
    }

    private void updateFrontends() throws StarRocksException {
        List<ClusterSnapshotConfig.Frontend> frontends = config.getFrontends();
        if (frontends == null) {
            return;
        }

        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        // Drop old frontends
        for (Frontend frontend : nodeMgr.getOtherFrontends()) {
            LOG.info("Drop old frontend {}", frontend);
            nodeMgr.dropFrontend(frontend.getRole(), frontend.getHost(), frontend.getEditLogPort());
        }

        // Add new frontends
        for (ClusterSnapshotConfig.Frontend frontend : frontends) {
            LOG.info("Add new frontend {}", frontend);
            nodeMgr.addFrontend(frontend.isFollower() ? FrontendNodeType.FOLLOWER : FrontendNodeType.OBSERVER,
                    frontend.getHost(), frontend.getEditLogPort());
        }
    }

    private void updateComputeNodes() throws StarRocksException {
        List<ClusterSnapshotConfig.ComputeNode> computeNodes = config.getComputeNodes();
        if (computeNodes == null) {
            return;
        }

        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (Backend be : systemInfoService.getIdToBackend().values()) {
            LOG.info("Drop old backend {}", be);
            systemInfoService.dropBackend(be.getHost(), be.getHeartbeatPort(),
                    WarehouseManager.DEFAULT_WAREHOUSE_NAME, false);
        }

        // Drop old compute nodes
        for (ComputeNode cn : systemInfoService.getIdComputeNode().values()) {
            LOG.info("Drop old compute node {}", cn);
            systemInfoService.dropComputeNode(cn.getHost(), cn.getHeartbeatPort(),
                    WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        }

        // Add new compute nodes
        for (ClusterSnapshotConfig.ComputeNode cn : computeNodes) {
            LOG.info("Add new compute node {}", cn);
            systemInfoService.addComputeNode(cn.getHost(), cn.getHeartbeatServicePort(),
                    WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        }
    }

    private void updateStorageVolumes() throws StarRocksException {
        List<ClusterSnapshotConfig.StorageVolume> storageVolumes = config.getStorageVolumes();
        if (storageVolumes == null) {
            return;
        }

        boolean oldValue = com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED;
        com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED = true;
        try {
            StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
            for (ClusterSnapshotConfig.StorageVolume storageVolume : storageVolumes) {
                LOG.info("Update storage volume {}", storageVolume.getName());
                List<String> locations = storageVolume.getLocation() == null ? null
                        : Collections.singletonList(storageVolume.getLocation());
                storageVolumeMgr.replaceStorageVolume(storageVolume.getName(), storageVolume.getType(),
                        locations, storageVolume.getProperties(), storageVolume.getComment());
            }
        } finally {
            com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED = oldValue;
        }
    }
}
