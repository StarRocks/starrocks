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

package com.starrocks.lake;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadataRepairStatus;
import com.starrocks.proto.TabletMetadatas;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TabletRepairHelper {
    private static final Logger LOG = LogManager.getLogger(TabletRepairHelper.class);

    private static final long BATCH_VERSION_NUM = 5L;

    record PhysicalPartitionInfo(
            long physicalPartitionId,
            List<Long> allTablets, // all tablets in one physical partition
            Set<Long> leftTablets, // tablets whose valid metadata still need to be found
            Map<ComputeNode, Set<Long>> nodeToTablets,
            long maxVersion,
            long minVersion
    ) {
    }

    private static Map<Long, Map<Long, TabletMetadataPB>> getTabletMetadatas(PhysicalPartitionInfo info, long maxVersion,
                                                                             long minVersion) throws Exception {
        long physicalPartitionId = info.physicalPartitionId;
        Set<Long> leftTablets = info.leftTablets;
        Map<ComputeNode, Set<Long>> nodeToTablets = info.nodeToTablets;

        List<Future<GetTabletMetadatasResponse>> responses = Lists.newArrayList();
        List<ComputeNode> nodes = Lists.newArrayList();
        for (Map.Entry<ComputeNode, Set<Long>> entry : nodeToTablets.entrySet()) {
            ComputeNode node = entry.getKey();
            Set<Long> tabletIds = Sets.newHashSet(entry.getValue());

            tabletIds.retainAll(leftTablets);
            if (tabletIds.isEmpty()) {
                continue;
            }

            GetTabletMetadatasRequest request = new GetTabletMetadatasRequest();
            request.tabletIds = Lists.newArrayList(tabletIds);
            request.maxVersion = maxVersion;
            request.minVersion = minVersion;

            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                Future<GetTabletMetadatasResponse> future = lakeService.getTabletMetadatas(request);
                responses.add(future);
                nodes.add(node);
            } catch (RpcException e) {
                LOG.warn("Fail to send get tablet metadatas request to node {}, partition: {}, error: {}", node.getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
        for (int i = 0; i < responses.size(); ++i) {
            try {
                GetTabletMetadatasResponse response = responses.get(i).get(LakeService.TIMEOUT_GET_TABLET_STATS,
                        TimeUnit.MILLISECONDS);

                if (response == null) {
                    throw new StarRocksException("response is null");
                }

                TStatusCode statusCode = TStatusCode.findByValue(response.status.statusCode);
                if (statusCode != TStatusCode.OK) {
                    List<String> errMsgs = response.status.errorMsgs;
                    throw new StarRocksException(errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                }

                if (response.tabletMetadatas != null) {
                    for (TabletMetadatas tm : response.tabletMetadatas) {
                        long tabletId = tm.tabletId;
                        TStatusCode tabletStatusCode = TStatusCode.findByValue(tm.status.statusCode);
                        if (tabletStatusCode == TStatusCode.OK) {
                            Map<Long, TabletMetadataPB> versionMetadatas = tm.versionMetadatas;
                            tabletVersionMetadatas.put(tabletId, versionMetadatas);
                        } else if (tabletStatusCode != TStatusCode.NOT_FOUND) {
                            List<String> errMsgs = tm.status.errorMsgs;
                            throw new StarRocksException(
                                    errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    Map<Long, List<Long>> tabletVersions = Maps.newHashMap();
                    if (response.tabletMetadatas != null) {
                        for (TabletMetadatas tm : response.tabletMetadatas) {
                            TStatusCode tabletStatusCode = TStatusCode.findByValue(tm.status.statusCode);
                            if (tabletStatusCode == TStatusCode.OK) {
                                tabletVersions.put(tm.tabletId, Lists.newArrayList(tm.versionMetadatas.keySet()));
                            }
                        }
                    }
                    LOG.debug("Get {} tablet metadatas from node {}, partition: {}, version range: [{}, {}], tablet versions: {}",
                            tabletVersions.size(), nodes.get(i).getId(), physicalPartitionId, minVersion, maxVersion,
                            tabletVersions);
                }
            } catch (Exception e) {
                LOG.warn("Fail to get tablet metadatas from node {}, partition: {}, error: {}", nodes.get(i).getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        return tabletVersionMetadatas;
    }

    /**
     * Finds valid tablet metadata for a physical partition based on version consistency requirements.
     * If `enforceConsistentVersion` is true, it attempts to find a single version for which all tablets in the
     * physical partition have metadata. This ensures all tablets are repaired to a consistent state.
     * If `enforceConsistentVersion` is false, it finds the latest available valid metadata for each individual tablet
     * within the specified version range.
     * The identified valid metadata entries are stored in the `validMetadatas` map.
     */
    private static void findValidTabletMetadata(PhysicalPartitionInfo info,
                                                Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas, long maxVersion,
                                                long minVersion, boolean enforceConsistentVersion,
                                                Map<Long, TabletMetadataPB> validMetadatas) {
        List<Long> allTablets = info.allTablets;

        // try to find the valid tablet metadata
        if (enforceConsistentVersion) {
            // find consistent valid metadata version
            Preconditions.checkState(validMetadatas.isEmpty());

            long validVersion = 0;
            for (long version = maxVersion; version >= minVersion; --version) {
                boolean allTabletsMetadataExist = true;
                for (long tabletId : allTablets) {
                    Map<Long, TabletMetadataPB> versionMetadatas = tabletVersionMetadatas.get(tabletId);
                    if (versionMetadatas == null || !versionMetadatas.containsKey(version)) {
                        allTabletsMetadataExist = false;
                        break;
                    }
                }

                if (allTabletsMetadataExist) {
                    validVersion = version;
                    break;
                }
            }

            if (validVersion != 0) {
                for (long tabletId : allTablets) {
                    validMetadatas.put(tabletId, tabletVersionMetadatas.get(tabletId).get(validVersion));
                }
            }
        } else {
            // find the latest valid metadata for each tablet
            Preconditions.checkState(!validMetadatas.keySet().containsAll(allTablets));

            for (long tabletId : allTablets) {
                if (validMetadatas.containsKey(tabletId)) {
                    continue;
                }

                Map<Long, TabletMetadataPB> versionMetadatas = tabletVersionMetadatas.get(tabletId);
                if (versionMetadatas == null) {
                    continue;
                }

                for (long version = maxVersion; version >= minVersion; --version) {
                    TabletMetadataPB metadata = versionMetadatas.get(version);
                    if (metadata != null) {
                        validMetadatas.put(tabletId, metadata);
                        break;
                    }
                }
            }
        }
    }

    private static TabletMetadataPB createEmptyTabletMetadata(long tabletId, TabletMetadataPB otherValidMetadata) {
        TabletMetadataPB metadata = new TabletMetadataPB();
        metadata.id = tabletId;
        // version will be overwritten to visible version later
        metadata.version = 0L;
        metadata.schema = otherValidMetadata.schema;
        metadata.rowsets = Lists.newArrayList();
        metadata.nextRowsetId = 1;
        metadata.cumulativePoint = 0;
        metadata.enablePersistentIndex = otherValidMetadata.enablePersistentIndex;
        metadata.persistentIndexType = otherValidMetadata.persistentIndexType;
        metadata.gtid = GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid();
        metadata.compactionStrategy = otherValidMetadata.compactionStrategy;
        metadata.flatJsonConfig = otherValidMetadata.flatJsonConfig;
        return metadata;
    }

    private static void checkOrCreateEmptyTabletMetadata(PhysicalPartitionInfo info, Map<Long, TabletMetadataPB> validMetadatas,
                                                         boolean enforceConsistentVersion, boolean allowEmptyTabletRecovery)
            throws StarRocksException {
        long maxVersion = info.maxVersion;
        List<Long> allTablets = info.allTablets;
        if (validMetadatas.keySet().containsAll(allTablets)) {
            // check all tablets have consistent valid metadata, and the version is visible version
            boolean allHaveVisibleVersionMetadata = true;
            for (TabletMetadataPB metadata : validMetadatas.values()) {
                if (metadata.version != maxVersion) {
                    allHaveVisibleVersionMetadata = false;
                    break;
                }
            }
            if (allHaveVisibleVersionMetadata) {
                throw new StarRocksException(
                        String.format("all tablets have valid metadata with version %d, no need for repair", maxVersion));
            } else {
                return;
            }
        }

        if (enforceConsistentVersion) {
            throw new StarRocksException("no consistent valid tablet metadata version was found, " +
                    "you can set enforce_consistent_version=false");
        } else {
            if (validMetadatas.isEmpty()) {
                throw new StarRocksException(
                        "no latest valid tablet metadatas were found for all allTablets, you should recreate the partition");
            }

            Set<Long> missingTablets = Sets.newHashSet(allTablets);
            missingTablets.removeAll(validMetadatas.keySet());
            Preconditions.checkState(!missingTablets.isEmpty());
            if (!allowEmptyTabletRecovery) {
                throw new StarRocksException(String.format(
                        "tablet %d has no valid tablet metadatas, " +
                                "you can set enforce_consistent_version=false and allow_empty_tablet_recovery=true",
                        missingTablets.iterator().next()));
            }

            TabletMetadataPB validMetadata = validMetadatas.values().iterator().next();
            for (long tabletId : missingTablets) {
                validMetadatas.put(tabletId, createEmptyTabletMetadata(tabletId, validMetadata));
            }
        }
    }

    private static Map<Long, String> repairTabletMetadata(PhysicalPartitionInfo info, Map<Long, TabletMetadataPB> validMetadatas,
                                                          boolean isFileBundling) throws Exception {
        long physicalPartitionId = info.physicalPartitionId;
        Map<ComputeNode, Set<Long>> nodeToTablets = info.nodeToTablets;

        boolean writeBundlingFile = true;
        List<Future<RepairTabletMetadataResponse>> responses = Lists.newArrayList();
        List<ComputeNode> nodes = Lists.newArrayList();
        for (Map.Entry<ComputeNode, Set<Long>> entry : nodeToTablets.entrySet()) {
            RepairTabletMetadataRequest request = new RepairTabletMetadataRequest();
            request.enableFileBundling = isFileBundling;
            request.writeBundlingFile = isFileBundling && writeBundlingFile;

            // if enable file bundling, we only need to send the write bundling metadata request to one node.
            // other node requests are used for some cleanup work.
            Set<Long> tabletIds = request.writeBundlingFile ? Sets.newHashSet(info.allTablets) : entry.getValue();
            Preconditions.checkState(!tabletIds.isEmpty());
            if (request.writeBundlingFile) {
                writeBundlingFile = false;
            }

            List<TabletMetadataPB> newMetadatas = Lists.newArrayList();
            for (long tabletId : tabletIds) {
                TabletMetadataPB metadata = validMetadatas.get(tabletId);
                Preconditions.checkState(metadata != null);
                // set version to physical partition visible version
                metadata.version = info.maxVersion;
                newMetadatas.add(metadata);
            }

            request.tabletMetadatas = newMetadatas;

            ComputeNode node = entry.getKey();
            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                Future<RepairTabletMetadataResponse> future = lakeService.repairTabletMetadata(request);
                responses.add(future);
                nodes.add(node);
            } catch (RpcException e) {
                LOG.warn("Fail to send repair tablet metadata request to node {}, partition: {}, error: {}", node.getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        Map<Long, String> tabletErrors = Maps.newHashMap();
        for (int i = 0; i < responses.size(); i++) {
            try {
                RepairTabletMetadataResponse response = responses.get(i).get(LakeService.TIMEOUT_REPAIR_METADATA,
                        TimeUnit.MILLISECONDS);

                if (response == null) {
                    throw new StarRocksException("response is null");
                }

                TStatusCode statusCode = TStatusCode.findByValue(response.status.statusCode);
                if (statusCode != TStatusCode.OK) {
                    List<String> errMsgs = response.status.errorMsgs;
                    throw new StarRocksException(errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                }

                if (response.tabletRepairStatuses != null) {
                    for (TabletMetadataRepairStatus repairStatus : response.tabletRepairStatuses) {
                        TStatusCode tabletStatusCode = TStatusCode.findByValue(repairStatus.status.statusCode);
                        if (tabletStatusCode != TStatusCode.OK) {
                            List<String> errMsgs = repairStatus.status.errorMsgs;
                            tabletErrors.put(repairStatus.tabletId,
                                    errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Fail to repair tablet metadata from node {}, partition: {}, error: {}", nodes.get(i).getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        if (!tabletErrors.isEmpty()) {
            LOG.warn("Fail to repair tablet metadata for partition {}, failed tablets: {}", physicalPartitionId,
                    tabletErrors);
        } else {
            LOG.info("Repair tablet metadata for partition {} success", physicalPartitionId);
        }

        return tabletErrors;
    }

    /**
     * Repairs the tablet metadata for a single physical partition.
     * This function attempts to find valid tablet metadata for all tablets within the given physical partition.
     * It iterates through versions to retrieve tablet metadatas from compute nodes by batches.
     * If valid metadata is found for all tablets, or
     * if `allowEmptyTabletRecovery` is true and empty metadata can be created for missing tablets,
     * it then sends these valid metadatas to the compute nodes to perform the repair operation.
     *
     * @param info                     Information about the physical partition.
     * @param enforceConsistentVersion Whether to enforce consistent metadata version across all tablets.
     * @param allowEmptyTabletRecovery Whether to allow creating empty metadata for tablets without valid metadata.
     * @param isFileBundling           Whether file bundling is enabled for the table.
     * @return A map of tablet IDs to error messages for any tablets that failed to repair.
     * @throws Exception If an error occurs during the repair process.
     */
    private static Map<Long, String> repairPhysicalPartition(PhysicalPartitionInfo info, boolean enforceConsistentVersion,
                                                             boolean allowEmptyTabletRecovery, boolean isFileBundling)
            throws Exception {
        List<Long> allTablets = info.allTablets;
        Set<Long> leftTablets = info.leftTablets;
        long partitionMaxVersion = info.maxVersion;
        long partitionMinVersion = info.minVersion;
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();

        for (long maxVersion = partitionMaxVersion; maxVersion >= partitionMinVersion; maxVersion -= BATCH_VERSION_NUM) {
            long minVersion = Math.max(maxVersion - BATCH_VERSION_NUM + 1, partitionMinVersion);

            // get tablet metadatas from backends
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = getTabletMetadatas(info, maxVersion, minVersion);

            // find the valid tablet metadata
            findValidTabletMetadata(info, tabletVersionMetadatas, maxVersion, minVersion, enforceConsistentVersion,
                    validMetadatas);

            if (validMetadatas.keySet().containsAll(allTablets)) {
                break;
            } else {
                leftTablets.removeAll(validMetadatas.keySet());
            }
        }

        // check the valid tablet metadata, and create empty tablet metadata if no valid metadata is found
        checkOrCreateEmptyTabletMetadata(info, validMetadatas, enforceConsistentVersion, allowEmptyTabletRecovery);
        LOG.info("Found valid tablet metadatas for partition {}, tablet versions: {}", info.physicalPartitionId,
                validMetadatas.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().version)));

        // repair the valid tablet metadata through backends
        return repairTabletMetadata(info, validMetadatas, isFileBundling);
    }
}
