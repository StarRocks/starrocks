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

import com.starrocks.catalog.BrokerMgr;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static void init(String clusterSnapshotYamlFile, boolean startFromSnapshot) throws StarRocksException {
        if (startFromSnapshot) {
            LOG.info("FE start to restore from a cluster snapshot (--cluster_snapshot)");
            instance = new RestoreClusterSnapshotMgr(clusterSnapshotYamlFile);
            return;
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

    public static boolean isExternalSnapshot() {
        RestoreClusterSnapshotMgr self = instance;
        if (self == null || self.config.getClusterSnapshot() == null) {
            return false;
        }
        return self.config.getClusterSnapshot().isExternalSnapshot();
    }

    public static void finishRestoring() throws StarRocksException {
        RestoreClusterSnapshotMgr self = instance;
        if (self == null) {
            return;
        }

        try {
            self.updateFrontends();
            self.updateComputeNodes();
            if (isExternalSnapshot()) {
                // Source-image brokers/snapshot-jobs only need scrubbing when source != target.
                // For in-place restore (source == target) these entries are still valid and
                // dropping them would erase legitimate brokers and pending automated snapshot jobs.
                self.dropImageBrokers();
                self.dropImageSnapshotJobs();
            }
            self.updateStorageVolumes();
            self.disableAutomatedSnapshot();
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
            collectSnapshotInfoFromLocalImage();
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
                    clusterSnapshot.getStorageVolume().getProperties(), false);
            if (fileStatusList.isEmpty()) {
                throw new StarRocksException("No cluster snapshot found in path " + pathPattern);
            }

            snapshotImagePath = null;

            // Sort by name descending (name contains timestamp, larger = newer)
            fileStatusList.sort((a, b) -> b.getPath().getName().compareTo(a.getPath().getName()));

            // Find the newest snapshot that has a snapshot_meta.json (complete)
            for (FileStatus fs : fileStatusList) {
                String candidatePath = fs.getPath().toString();
                if (ClusterSnapshotUtils.checkSnapshotMetaFileExist(candidatePath,
                        clusterSnapshot.getStorageVolume().getProperties())) {
                    snapshotImagePath = candidatePath;
                    break;
                }
            }

            // Fallback: no snapshot has meta file (old format), pick the newest
            if (snapshotImagePath == null) {
                LOG.warn("No snapshot with meta file found, fallback to first snapshot directory");
                snapshotImagePath = fileStatusList.get(0).getPath().toString();
            }
        }

        LOG.info("Download cluster snapshot {} to local dir {}", snapshotImagePath, localImagePath);
        HdfsUtil.copyToLocal(snapshotImagePath, localImagePath, clusterSnapshot.getStorageVolume().getProperties());

        collectSnapshotInfoAfterDownloaded(snapshotImagePath);
    }

    private void collectSnapshotInfoFromLocalImage() throws StarRocksException {
        restoredSnapshotInfo = buildRestoredSnapshotInfo(null);
        LOG.info("Use local image for cluster snapshot restore, FE image version: {}, StarMgr image version: {}",
                restoredSnapshotInfo.getFeJournalId(), restoredSnapshotInfo.getStarMgrJournalId());
    }

    private void collectSnapshotInfoAfterDownloaded(String snapshotImagePath) throws StarRocksException {
        int lastSlashIndex = snapshotImagePath.lastIndexOf('/');
        if (lastSlashIndex < 0) {
            throw new StarRocksException("Failed to get snapshot name from snapshot path " + snapshotImagePath);
        }

        String restoredSnapshotName = snapshotImagePath.substring(lastSlashIndex + 1);

        restoredSnapshotInfo = buildRestoredSnapshotInfo(restoredSnapshotName);

        LOG.info("Downloaded cluster snapshot {} successfully, FE image version: {}, StarMgr image version: {}",
                restoredSnapshotName, restoredSnapshotInfo.getFeJournalId(), restoredSnapshotInfo.getStarMgrJournalId());
    }

    private RestoredSnapshotInfo buildRestoredSnapshotInfo(String snapshotName) throws StarRocksException {
        try {
            String localImagePath = GlobalStateMgr.getImageDirPath();

            // Try to read snapshot info from snapshot_meta.json first, then delete it regardless
            ClusterSnapshot snapshotMeta = ClusterSnapshotUtils.readLocalSnapshotMetaFile(localImagePath);
            ClusterSnapshotUtils.deleteLocalSnapshotMetaFile(localImagePath);
            if (snapshotMeta != null) {
                return new RestoredSnapshotInfo(snapshotMeta.getSnapshotName(),
                        snapshotMeta.getFeJournalId(), snapshotMeta.getStarMgrJournalId());
            }

            // Fallback: read image version from local image files (old format without meta file)
            long feImageJournalId = new ImageLoader(localImagePath).getImageJournalId();
            long starMgrImageJournalId = new Storage(localImagePath + StarMgrServer.IMAGE_SUBDIR).getImageJournalId();
            return new RestoredSnapshotInfo(snapshotName, feImageJournalId, starMgrImageJournalId);
        } catch (Exception e) {
            throw new StarRocksException("Failed to get local image version for restore", e);
        }
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
        // Drop old backend nodes
        for (Backend be : systemInfoService.getIdToBackend().values()) {
            LOG.info("Drop old backend {}", be);
            systemInfoService.dropBackend(be.getHost(), be.getHeartbeatPort(), null, null, false);
        }

        // Drop old compute nodes
        for (ComputeNode cn : systemInfoService.getIdComputeNode().values()) {
            LOG.info("Drop old compute node {}", cn);
            systemInfoService.dropComputeNode(cn.getHost(), cn.getHeartbeatPort(), null, null);
        }

        // Add new compute nodes
        for (ClusterSnapshotConfig.ComputeNode cn : computeNodes) {
            LOG.info("Add new compute node {}", cn);
            systemInfoService.addComputeNode(cn.getHost(), cn.getHeartbeatServicePort(), cn.getWarehouse(), cn.getCNGroup());
        }
    }

    /**
     * Drop every broker inherited from the source cluster's image. Source brokers point at hosts
     * that target cannot reach, so HeartbeatMgr would otherwise spam Connection-refused warnings
     * forever. yaml does not currently declare brokers; operators add them post-restore via
     * {@code ALTER SYSTEM ADD BROKER} if needed.
     */
    private void dropImageBrokers() {
        BrokerMgr brokerMgr = GlobalStateMgr.getCurrentState().getBrokerMgr();
        for (String name : new ArrayList<>(brokerMgr.getBrokerListMap().keySet())) {
            try {
                LOG.info("Drop broker {} inherited from source cluster image", name);
                brokerMgr.dropAllBroker(name);
            } catch (DdlException e) {
                LOG.warn("Failed to drop inherited broker {}", name, e);
            }
        }
    }

    private void dropImageSnapshotJobs() {
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().dropAllInheritedSnapshotJobs();
    }

    private void updateStorageVolumes() throws StarRocksException {
        StorageVolumeMgr storageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        boolean external = isExternalSnapshot();

        if (external) {
            // Refuse to leave any image SV pointing at the source bucket; tables bound to such an
            // SV would silently route writes back into the source cluster's storage.
            verifyAllImageStorageVolumesAreDeclaredInYaml(storageVolumeMgr);

            ClusterSnapshotConfig.StorageVolume snapshotStorageVolume = config.getSnapshotStorageVolume();
            String snapshotStorageVolumeName = StorageVolumeMgr.BASE_STORAGE_VOLUME;
            if (storageVolumeMgr.getStorageVolumeByName(snapshotStorageVolumeName) == null) {
                LOG.info("Create snapshot storage volume {}", snapshotStorageVolumeName);
                storageVolumeMgr.createStorageVolume(snapshotStorageVolumeName, snapshotStorageVolume.getType(),
                        Collections.singletonList(snapshotStorageVolume.getLocation()), snapshotStorageVolume.getProperties(),
                        Optional.of(true), snapshotStorageVolume.getComment());
            } else {
                LOG.info("Replace snapshot storage volume {}", snapshotStorageVolumeName);
                storageVolumeMgr.replaceStorageVolume(snapshotStorageVolumeName, snapshotStorageVolume.getType(),
                        Collections.singletonList(snapshotStorageVolume.getLocation()), snapshotStorageVolume.getProperties(),
                        snapshotStorageVolume.getComment(), "");
            }
        }

        boolean oldValue = com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED;
        com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED = true;
        try {
            List<ClusterSnapshotConfig.StorageVolume> storageVolumes =
                    config.getStorageVolumes() != null ? config.getStorageVolumes() : Collections.emptyList();
            String baseStorageVolumeName = external ? StorageVolumeMgr.BASE_STORAGE_VOLUME : "";
            for (ClusterSnapshotConfig.StorageVolume storageVolume : storageVolumes) {
                if (storageVolume.getName().equalsIgnoreCase(StorageVolumeMgr.BASE_STORAGE_VOLUME)) {
                    continue;
                }
                List<String> locations = storageVolume.getLocation() == null ? null
                        : Collections.singletonList(storageVolume.getLocation());
                if (storageVolumeMgr.getStorageVolumeByName(storageVolume.getName()) == null) {
                    LOG.info("Create storage volume {} declared in cluster_snapshot.yaml but absent from "
                            + "source image", storageVolume.getName());
                    storageVolumeMgr.createStorageVolume(storageVolume.getName(), storageVolume.getType(),
                            locations, storageVolume.getProperties(),
                            Optional.of(true), storageVolume.getComment());
                } else {
                    LOG.info("Update storage volume {}", storageVolume.getName());
                    storageVolumeMgr.replaceStorageVolume(storageVolume.getName(), storageVolume.getType(),
                            locations, storageVolume.getProperties(), storageVolume.getComment(),
                            baseStorageVolumeName);
                }
            }

            if (external) {
                // Idempotent: covers create-path SVs above plus any SV the loop did not visit.
                storageVolumeMgr.updateBaseStorageVolumeName(StorageVolumeMgr.BASE_STORAGE_VOLUME);
            }
        } finally {
            com.staros.util.Config.STARMGR_REPLACE_FILESTORE_ENABLED = oldValue;
        }
    }

    private void verifyAllImageStorageVolumesAreDeclaredInYaml(StorageVolumeMgr storageVolumeMgr)
            throws StarRocksException {
        List<ClusterSnapshotConfig.StorageVolume> yamlSvs =
                config.getStorageVolumes() != null ? config.getStorageVolumes() : Collections.emptyList();
        Set<String> declared = yamlSvs.stream()
                .map(ClusterSnapshotConfig.StorageVolume::getName)
                .collect(Collectors.toCollection(HashSet::new));
        declared.add(StorageVolumeMgr.BASE_STORAGE_VOLUME);

        List<String> existing;
        try {
            existing = storageVolumeMgr.listStorageVolumeNames();
        } catch (DdlException e) {
            throw new StarRocksException("Failed to list storage volumes during BCDR restore", e);
        }
        List<String> undeclared = existing.stream()
                .filter(name -> !declared.contains(name))
                .sorted()
                .collect(Collectors.toList());
        if (undeclared.isEmpty()) {
            return;
        }
        StringBuilder details = new StringBuilder();
        for (String name : undeclared) {
            StorageVolume sv = storageVolumeMgr.getStorageVolumeByName(name);
            details.append("\n  - name: ").append(name);
            if (sv == null) {
                details.append(" (no longer present in starmgr)");
                continue;
            }
            details.append("\n    type: ").append(sv.getType());
            details.append("\n    source_locations: ").append(sv.getLocations());
            Map<String, String> props = sv.getProperties();
            if (props != null && !props.isEmpty()) {
                Map<String, String> masked = new HashMap<>(props);
                StorageVolume.addMaskForCredential(masked);
                details.append("\n    source_properties: ").append(masked);
            }
        }
        throw new StarRocksException(String.format(
                "Cluster snapshot restore aborted: source-cluster image contains storage volumes that "
                        + "are not declared in cluster_snapshot.yaml.storage_volumes. Each source SV must be "
                        + "redirected to a target-side location, otherwise tables bound to it would route "
                        + "writes back into the source bucket and corrupt source data. Add each of the "
                        + "following SVs to storage_volumes with a target-side location, then re-run "
                        + "--cluster_snapshot:%s", details));
    }

    private void disableAutomatedSnapshot() {
        ClusterSnapshotMgr clusterSnapshotMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        clusterSnapshotMgr.setAutomatedSnapshotOff();
    }
}
