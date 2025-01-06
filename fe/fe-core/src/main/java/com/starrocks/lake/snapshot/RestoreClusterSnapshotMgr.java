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

import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestoreClusterSnapshotMgr {
    private static final Logger LOG = LogManager.getLogger(RestoreClusterSnapshotMgr.class);

    private static RestoreClusterSnapshotMgr instance;

    private ClusterSnapshotConfig config;
    private boolean oldStartWithIncompleteMeta;
    private boolean oldResetElectionGroup;

    private RestoreClusterSnapshotMgr(String clusterSnapshotYamlFile) throws UserException {
        config = ClusterSnapshotConfig.load(clusterSnapshotYamlFile);
        downloadSnapshot();
        updateConfig();
    }

    public static void init(String clusterSnapshotYamlFile, String[] args) throws UserException {
        for (String arg : args) {
            if (arg.equalsIgnoreCase("-cluster_snapshot")) {
                LOG.info("FE start to restore from a cluster snapshot");
                instance = new RestoreClusterSnapshotMgr(clusterSnapshotYamlFile);
                return;
            }
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

    public static void finishRestoring() throws UserException {
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
        }
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

    private void downloadSnapshot() throws UserException {
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
            LOG.info("Deleted bdb {}", localBdbPath);
        }

        ClusterSnapshotConfig.StorageVolume storageVolume = clusterSnapshot.getStorageVolume();
        // TODO: use constant and support no snapshot name
        String snapshotImagePath = String.join("/", storageVolume.getLocation(), clusterSnapshot.getClusterServiceId(),
                "meta/image", clusterSnapshot.getClusterSnapshotName());
        Map<String, String> properties = storageVolume.getProperties();

        LOG.info("Copy snapshot image {} to local dir {}", snapshotImagePath, localImagePath);
        HdfsUtil.copyToLocal(snapshotImagePath, localImagePath, properties);
    }

    private void updateFrontends() throws UserException {
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

    private void updateComputeNodes() throws UserException {
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

    private void updateStorageVolumes() throws UserException {
        List<ClusterSnapshotConfig.StorageVolume> storageVolumes = config.getStorageVolumes();
        if (storageVolumes == null) {
            return;
        }

        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        for (ClusterSnapshotConfig.StorageVolume storageVolume : storageVolumes) {
            LOG.info("Update storage volume {}", storageVolume.getName());
            storageVolumeMgr.updateStorageVolume(storageVolume.getName(), storageVolume.getType(),
                    Collections.singletonList(storageVolume.getLocation()), storageVolume.getProperties(),
                    storageVolume.getComment());
        }
    }
}
