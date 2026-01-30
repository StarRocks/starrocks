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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.TabletMetadataEntry;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadataRepairStatus;
import com.starrocks.proto.TabletResult;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.type.TypeFactory;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class TabletRepairHelper {
    private static final Logger LOG = LogManager.getLogger(TabletRepairHelper.class);

    private static final long BATCH_VERSION_NUM = 5L;
    private static final String SST_FILE_SUFFIX = ".sst";

    private static ShowResultSetMetaData DRY_RUN_REPAIR_RESULT_META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("PartitionId", TypeFactory.createVarcharType(20)));
        builder.addColumn(new Column("VisibleVersion", TypeFactory.createVarcharType(20)));
        builder.addColumn(new Column("RepairStatus", TypeFactory.createVarcharType(60)));
        builder.addColumn(new Column("TabletRecoverInfo", TypeFactory.createVarcharType(65535)));
        builder.addColumn(new Column("ErrorMsg", TypeFactory.createVarcharType(65535)));
        DRY_RUN_REPAIR_RESULT_META_DATA = builder.build();
    }

    // the version range [minVersion, maxVersion] is used to find valid tablet metadatas, both are included
    record PhysicalPartitionInfo(
            long physicalPartitionId,
            List<Long> allTablets, // all tablets in one physical partition
            Set<Long> unverifiedTablets, // tablets whose valid metadata still need to be found
            Map<ComputeNode, Set<Long>> nodeToTablets,
            long maxVersion, // physical partition visible version
            long minVersion  // the min version that has not been vacuumed
    ) {
    }

    private static List<Long> getPhysicalPartitionIds(Database db, OlapTable table, @NotNull List<String> partitionNames)
            throws StarRocksException {
        List<Long> physicalPartitionIds = Lists.newArrayList();

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            // ensure the table still exists under the lock
            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTableIncludeRecycleBin(db, table.getId()) == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, table.getName());
            }

            // table state should be NORMAL
            if (table.getState() != OlapTableState.NORMAL) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_STATE, table.getName());
            }

            if (partitionNames.isEmpty()) {
                // if no partition specified, repair all partitions
                for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                    physicalPartitionIds.add(physicalPartition.getId());
                }
            } else {
                for (String partitionName : partitionNames) {
                    Partition partition = table.getPartition(partitionName);
                    if (partition == null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_PARTITION, partitionName);
                    }

                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        physicalPartitionIds.add(physicalPartition.getId());
                    }
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }

        return physicalPartitionIds;
    }

    private static PhysicalPartitionInfo getPhysicalPartitionInfo(Database db, OlapTable table, long physicalPartitionId,
                                                                  boolean enforceConsistentVersion,
                                                                  ComputeResource computeResource) throws StarRocksException {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            // ensure the table still exists under the lock
            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTableIncludeRecycleBin(db, table.getId()) == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, table.getName());
            }

            // skip if physical partition does not exist
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                throw new MetaNotFoundException(String.format("physical partition %d does not exist", physicalPartitionId));
            }

            long maxVersion = physicalPartition.getVisibleVersion();
            long minVersion = enforceConsistentVersion ? 1L : Long.MAX_VALUE;

            List<MaterializedIndex> indexes = physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE);
            if (indexes.size() > 1 && !enforceConsistentVersion) {
                throw new StarRocksException(
                        "table with multiple materialized indexes should be repaired with consistent version");
            }

            List<Long> allTablets = Lists.newArrayList();
            Set<Long> unverifiedTablets = Sets.newHashSet();
            Map<ComputeNode, Set<Long>> nodeToTablets = Maps.newHashMap();
            for (MaterializedIndex index : indexes) {
                for (Tablet tablet : index.getTablets()) {
                    LakeTablet lakeTablet = (LakeTablet) tablet;

                    long tabletId = lakeTablet.getId();
                    allTablets.add(tabletId);
                    unverifiedTablets.add(tabletId);

                    ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr().getComputeNodeAssignedToTablet(
                            computeResource, tabletId);
                    if (computeNode == null) {
                        throw new NoAliveBackendException("no alive backend");
                    }
                    nodeToTablets.computeIfAbsent(computeNode, k -> Sets.newHashSet()).add(tabletId);

                    if (enforceConsistentVersion) {
                        minVersion = Math.max(minVersion, lakeTablet.getMinVersion());
                    } else {
                        minVersion = Math.min(minVersion, lakeTablet.getMinVersion());
                    }
                }
            }

            return new PhysicalPartitionInfo(physicalPartition.getId(), allTablets, unverifiedTablets, nodeToTablets, maxVersion,
                    minVersion);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
    }

    private static void printTabletMetadatas(GetTabletMetadatasResponse response, long nodeId, long physicalPartitionId,
                                             long maxVersion, long minVersion) {
        Map<Long, Map<Long, Integer>> tabletToVersionMissingFileNum = Maps.newHashMap();
        if (response.tabletResults != null) {
            for (TabletResult tr : response.tabletResults) {
                TStatusCode tabletStatusCode = TStatusCode.findByValue(tr.status.statusCode);
                if (tabletStatusCode == TStatusCode.OK) {
                    Map<Long, Integer> versionToMissingFilesNum = Maps.newHashMap();
                    if (tr.metadataEntries != null) {
                        for (TabletMetadataEntry entry : tr.metadataEntries) {
                            TabletMetadataPB metadata = entry.metadata;
                            List<String> missingFiles = entry.missingFiles;
                            if (metadata != null) {
                                int missingFilesNum = missingFiles != null ? missingFiles.size() : 0;
                                versionToMissingFilesNum.put(metadata.version, missingFilesNum);
                            }
                        }
                    }
                    tabletToVersionMissingFileNum.put(tr.tabletId, versionToMissingFilesNum);
                }
            }
        }
        LOG.debug("Get {} tablet metadatas from node {}, partition: {}, version range: [{}, {}], " +
                        "tablet->(version->missing files num): {}",
                tabletToVersionMissingFileNum.size(), nodeId, physicalPartitionId, minVersion, maxVersion,
                tabletToVersionMissingFileNum);
    }

    // returns a map of tablet IDs to valid tablet metadatas within the version range [minVersion, maxVersion]
    private static Map<Long, Map<Long, TabletMetadataEntry>> getTabletMetadatas(PhysicalPartitionInfo info, long maxVersion,
                                                                                long minVersion) throws Exception {
        long physicalPartitionId = info.physicalPartitionId;
        Set<Long> unverifiedTablets = info.unverifiedTablets;
        Map<ComputeNode, Set<Long>> nodeToTablets = info.nodeToTablets;

        List<Future<GetTabletMetadatasResponse>> responses = Lists.newArrayList();
        List<ComputeNode> nodes = Lists.newArrayList();
        for (Map.Entry<ComputeNode, Set<Long>> entry : nodeToTablets.entrySet()) {
            ComputeNode node = entry.getKey();
            Set<Long> tabletIds = Sets.newHashSet(entry.getValue());

            tabletIds.retainAll(unverifiedTablets);
            if (tabletIds.isEmpty()) {
                continue;
            }

            GetTabletMetadatasRequest request = new GetTabletMetadatasRequest();
            request.tabletIds = Lists.newArrayList(tabletIds);
            request.maxVersion = maxVersion;
            request.minVersion = minVersion;
            request.checkMissingFiles = true;

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

        // map<tablet id, map<version, TabletMetadataEntry>>
        Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
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

                if (response.tabletResults != null) {
                    for (TabletResult tr : response.tabletResults) {
                        long tabletId = tr.tabletId;
                        TStatusCode tabletStatusCode = TStatusCode.findByValue(tr.status.statusCode);
                        if (tabletStatusCode == TStatusCode.OK) {
                            Map<Long, TabletMetadataEntry> versionToMetadataEntry = Maps.newHashMap();
                            if (tr.metadataEntries != null) {
                                for (TabletMetadataEntry entry : tr.metadataEntries) {
                                    if (entry.metadata != null) {
                                        versionToMetadataEntry.put(entry.metadata.version, entry);
                                    }
                                }
                            }
                            tabletToVersionMetadataEntry.put(tabletId, versionToMetadataEntry);
                        } else if (tabletStatusCode != TStatusCode.NOT_FOUND) {
                            List<String> errMsgs = tr.status.errorMsgs;
                            throw new StarRocksException(
                                    errMsgs != null && !errMsgs.isEmpty() ? errMsgs.get(0) : "unknown error");
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    printTabletMetadatas(response, nodes.get(i).getId(), physicalPartitionId, maxVersion, minVersion);
                }
            } catch (Exception e) {
                LOG.warn("Fail to get tablet metadatas from node {}, partition: {}, error: {}", nodes.get(i).getId(),
                        physicalPartitionId, e.getMessage());
                throw e;
            }
        }

        return tabletToVersionMetadataEntry;
    }

    private static boolean checkOnlySstFilesMissing(List<String> missingFiles) {
        Preconditions.checkState(missingFiles != null && !missingFiles.isEmpty());
        for (String file : missingFiles) {
            if (!file.endsWith(SST_FILE_SUFFIX)) {
                return false;
            }
        }
        return true;
    }

    /**
     * valid metadata:
     * 1. no missing files
     * 2. only missing pk index sst files, BE can rebuild pk index
     */
    private static boolean checkTabletMetadataValid(TabletMetadataEntry metadataEntry) {
        List<String> missingFiles = metadataEntry.missingFiles;
        if (missingFiles == null || missingFiles.isEmpty()) {
            // no missing files
            return true;
        }

        // only missing pk index sst files
        if (checkOnlySstFilesMissing(missingFiles)) {
            return true;
        }

        return false;
    }

    private static TabletMetadataPB getValidTabletMetadata(TabletMetadataEntry metadataEntry) {
        TabletMetadataPB metadata = metadataEntry.metadata;
        List<String> missingFiles = metadataEntry.missingFiles;
        if (missingFiles == null || missingFiles.isEmpty()) {
            // no missing files, metadata is valid
            return metadata;
        }

        // only missing pk index sst files, clear sstableMeta
        if (checkOnlySstFilesMissing(missingFiles)) {
            metadata.sstableMeta = null;
            return metadata;
        }

        Preconditions.checkState(false, "should not reach here");
        return null;
    }

    /**
     * Finds valid tablet metadata for a physical partition based on version consistency requirements.
     * The identified valid metadata entries are stored in the `tabletToValidMetadata` map.
     */
    private static void findValidTabletMetadata(PhysicalPartitionInfo info,
                                                Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry,
                                                long maxVersion, long minVersion, boolean enforceConsistentVersion,
                                                Map<Long, TabletMetadataPB> tabletToValidMetadata) {
        if (enforceConsistentVersion) {
            findConsistentVersionTabletMetadata(info, tabletToVersionMetadataEntry, maxVersion, minVersion,
                    tabletToValidMetadata);
        } else {
            findLatestValidTabletMetadata(info, tabletToVersionMetadataEntry, maxVersion, minVersion, tabletToValidMetadata);
        }
    }

    /**
     * Attempts to find a single version for which all tablets in the physical partition have metadata.
     * This ensures all tablets are repaired to a consistent state.
     */
    private static void findConsistentVersionTabletMetadata(
            PhysicalPartitionInfo info, Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry,
            long maxVersion, long minVersion, Map<Long, TabletMetadataPB> tabletToValidMetadata) {
        Preconditions.checkState(tabletToValidMetadata.isEmpty());
        List<Long> allTablets = info.allTablets;

        long validVersion = 0;
        for (long version = maxVersion; version >= minVersion; --version) {
            boolean allTabletsMetadataValid = true;
            for (long tabletId : allTablets) {
                Map<Long, TabletMetadataEntry> versionToMetadataEntry = tabletToVersionMetadataEntry.get(tabletId);
                if (versionToMetadataEntry == null || !versionToMetadataEntry.containsKey(version)) {
                    allTabletsMetadataValid = false;
                    break;
                }

                if (!checkTabletMetadataValid(versionToMetadataEntry.get(version))) {
                    allTabletsMetadataValid = false;
                    break;
                }
            }

            if (allTabletsMetadataValid) {
                validVersion = version;
                break;
            }
        }

        if (validVersion != 0) {
            for (long tabletId : allTablets) {
                TabletMetadataEntry metadataEntry = tabletToVersionMetadataEntry.get(tabletId).get(validVersion);
                tabletToValidMetadata.put(tabletId, getValidTabletMetadata(metadataEntry));
            }
        }
    }

    /**
     * Attempts to find the latest available valid metadata for each individual tablet within the specified version range.
     */
    private static void findLatestValidTabletMetadata(PhysicalPartitionInfo info,
                                                      Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry,
                                                      long maxVersion, long minVersion,
                                                      Map<Long, TabletMetadataPB> tabletToValidMetadata) {
        List<Long> allTablets = info.allTablets;
        Preconditions.checkState(!tabletToValidMetadata.keySet().containsAll(allTablets));

        for (long tabletId : allTablets) {
            if (tabletToValidMetadata.containsKey(tabletId)) {
                continue;
            }

            Map<Long, TabletMetadataEntry> versionToMetadataEntry = tabletToVersionMetadataEntry.get(tabletId);
            if (versionToMetadataEntry == null || versionToMetadataEntry.isEmpty()) {
                continue;
            }

            for (long version = maxVersion; version >= minVersion; --version) {
                TabletMetadataEntry metadataEntry = versionToMetadataEntry.get(version);
                if (metadataEntry != null && checkTabletMetadataValid(metadataEntry)) {
                    tabletToValidMetadata.put(tabletId, getValidTabletMetadata(metadataEntry));
                    break;
                }
            }
        }
    }

    private static TabletMetadataPB createEmptyTabletMetadata(long tabletId, TabletMetadataPB otherValidMetadata) {
        TabletMetadataPB metadata = new TabletMetadataPB();
        metadata.id = tabletId;
        // version will be overwritten to the visible version in repairTabletMetadata()
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

    private static void checkOrCreateEmptyTabletMetadata(PhysicalPartitionInfo info,
                                                         Map<Long, TabletMetadataPB> tabletToValidMetadata,
                                                         boolean enforceConsistentVersion, boolean allowEmptyTabletRecovery)
            throws StarRocksException {
        long maxVersion = info.maxVersion;
        List<Long> allTablets = info.allTablets;
        if (tabletToValidMetadata.keySet().containsAll(allTablets)) {
            // check all tablets have consistent valid metadata, and the version is visible version
            boolean allHaveVisibleVersionMetadata = true;
            for (TabletMetadataPB metadata : tabletToValidMetadata.values()) {
                if (metadata.version != maxVersion) {
                    allHaveVisibleVersionMetadata = false;
                    break;
                }
            }
            if (allHaveVisibleVersionMetadata) {
                throw new AlreadyExistsException(
                        String.format("all tablets have valid tablet metadata with version %d, no need for repair", maxVersion));
            } else {
                return;
            }
        }

        if (enforceConsistentVersion) {
            throw new StarRocksException("no consistent valid tablet metadata version was found, " +
                    "you can set enforce_consistent_version=false");
        } else {
            if (tabletToValidMetadata.isEmpty()) {
                throw new StarRocksException(
                        "no valid tablet metadata was found for any tablet, you should recreate the partition");
            }

            Set<Long> missingTablets = Sets.newHashSet(allTablets);
            missingTablets.removeAll(tabletToValidMetadata.keySet());
            Preconditions.checkState(!missingTablets.isEmpty());
            if (!allowEmptyTabletRecovery) {
                throw new StarRocksException(String.format(
                        "no tablet metadatas were found for tablets [%s], " +
                                "you can set enforce_consistent_version=false and allow_empty_tablet_recovery=true",
                        Joiner.on(", ").join(missingTablets)));
            }

            TabletMetadataPB validMetadata = tabletToValidMetadata.values().iterator().next();
            for (long tabletId : missingTablets) {
                tabletToValidMetadata.put(tabletId, createEmptyTabletMetadata(tabletId, validMetadata));
            }
        }
    }

    private static Map<Long, String> repairTabletMetadata(PhysicalPartitionInfo info,
                                                          Map<Long, TabletMetadataPB> tabletToValidMetadata,
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
                TabletMetadataPB metadata = tabletToValidMetadata.get(tabletId);
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

    private static Map<Long, TabletMetadataPB> getValidTabletMetadatas(PhysicalPartitionInfo info,
                                                                       boolean enforceConsistentVersion) throws Exception {
        List<Long> allTablets = info.allTablets;
        Set<Long> unverifiedTablets = info.unverifiedTablets;
        long partitionMaxVersion = info.maxVersion;
        long partitionMinVersion = info.minVersion;
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();

        for (long maxVersion = partitionMaxVersion; maxVersion >= partitionMinVersion; maxVersion -= BATCH_VERSION_NUM) {
            long minVersion = Math.max(maxVersion - BATCH_VERSION_NUM + 1, partitionMinVersion);

            // get tablet metadatas from backends
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry =
                    getTabletMetadatas(info, maxVersion, minVersion);

            // find the valid tablet metadata
            findValidTabletMetadata(info, tabletToVersionMetadataEntry, maxVersion, minVersion, enforceConsistentVersion,
                    tabletToValidMetadata);

            unverifiedTablets.removeAll(tabletToValidMetadata.keySet());
            if (tabletToValidMetadata.keySet().containsAll(allTablets)) {
                Preconditions.checkState(unverifiedTablets.isEmpty());
                break;
            }
        }
        return tabletToValidMetadata;
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
        Map<Long, TabletMetadataPB> tabletToValidMetadata = getValidTabletMetadatas(info, enforceConsistentVersion);

        // check the valid tablet metadata, and create empty tablet metadata if no valid metadata is found
        checkOrCreateEmptyTabletMetadata(info, tabletToValidMetadata, enforceConsistentVersion, allowEmptyTabletRecovery);
        LOG.info("Found valid tablet metadatas for partition {}, tablet versions: {}", info.physicalPartitionId,
                tabletToValidMetadata.entrySet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, e -> e.getValue().version)));

        // repair the valid tablet metadata through backends
        return repairTabletMetadata(info, tabletToValidMetadata, isFileBundling);
    }

    /**
     * Repairs the tablet metadata for specified partitions of a table.
     * This function orchestrates the repair process by first obtaining physical partition IDs,
     * then iterating through each physical partition to find valid tablet metadata and
     * subsequently sending repair requests to compute nodes.
     * It handles different repair strategies based on `enforceConsistentVersion` and `allowEmptyTabletRecovery`
     * and aggregates errors from failed partition repairs.
     *
     * @param stmt The AdminRepairTableStmt containing repair parameters.
     * @param db The Database containing the table to be repaired.
     * @param table The OlapTable whose tablets are to be repaired.
     * @param partitionNames A list of partition names to repair. If empty, all partitions are repaired.
     * @param computeResource The compute resource used for assigning compute nodes.
     * @throws StarRocksException If any tablet repair fails or if there are issues during the process.
     */
    public static void repair(AdminRepairTableStmt stmt, Database db, OlapTable table, @NotNull List<String> partitionNames,
                              ComputeResource computeResource) throws StarRocksException {
        boolean enforceConsistentVersion = stmt.isEnforceConsistentVersion();
        boolean allowEmptyTabletRecovery = stmt.isAllowEmptyTabletRecovery();
        boolean isFileBundling = table.isFileBundling();

        // get physical partition ids in db table read lock
        List<Long> physicalPartitionIds = getPhysicalPartitionIds(db, table, partitionNames);

        // repair each physical partition
        Map<Long, Map<Long, String>> partitionErrors = Maps.newHashMap();
        for (Long physicalPartitionId : physicalPartitionIds) {
            try {
                PhysicalPartitionInfo info =
                        getPhysicalPartitionInfo(db, table, physicalPartitionId, enforceConsistentVersion, computeResource);

                Map<Long, String> tabletErrors =
                        repairPhysicalPartition(info, enforceConsistentVersion, allowEmptyTabletRecovery, isFileBundling);
                if (!tabletErrors.isEmpty()) {
                    partitionErrors.put(physicalPartitionId, tabletErrors);
                }
            } catch (AlreadyExistsException | MetaNotFoundException e) {
                // 1. all tablets have valid tablet metadata with visible version
                // 2. physical partition does not exist
                LOG.info("Skip repairing tablet metadata for partition {}, {}", physicalPartitionId, e.getMessage());
            } catch (Exception e) {
                LOG.warn("Fail to repair tablet metadata for partition {}", physicalPartitionId, e);
                partitionErrors.put(physicalPartitionId, Collections.singletonMap(0L, e.getMessage()));
            }
        }

        // check if any partitions fail
        if (!partitionErrors.isEmpty()) {
            LOG.warn("Fail to repair tablet metadata for {} partitions. db: {}, table: {}, partitions: {}",
                    partitionErrors.size(), db.getId(), table.getId(), partitionErrors.keySet());

            // throw exception with at most 3 failed partitions
            List<String> errorMsgs = Lists.newArrayList();
            for (Map.Entry<Long, Map<Long, String>> entry : partitionErrors.entrySet()) {
                errorMsgs.add(
                        String.format("{partition: %d, error: %s}", entry.getKey(), entry.getValue().values().iterator().next()));
                if (errorMsgs.size() >= 3) {
                    break;
                }
            }

            int partitionErrorsSize = partitionErrors.size();
            int errorMsgsSize = errorMsgs.size();
            throw new StarRocksException(
                    String.format("Fail to repair tablet metadata for %d partition%s, the first %d partition%s: [%s]",
                            partitionErrorsSize, partitionErrorsSize > 1 ? "s" : "",
                            errorMsgsSize, errorMsgsSize > 1 ? "s" : "", Joiner.on(", ").join(errorMsgs)));
        }
    }

    public static ShowResultSetMetaData getDryRunRepairResultMetaData() {
        return DRY_RUN_REPAIR_RESULT_META_DATA;
    }

    private static class TabletRecoverInfo {
        @SerializedName("tabletId")
        public long tabletId;
        @SerializedName("recoverVersion")
        public long recoverVersion;

        public TabletRecoverInfo(long tabletId, long recoverVersion) {
            this.tabletId = tabletId;
            this.recoverVersion = recoverVersion;
        }
    }

    enum RepairStatus {
        NORMAL,         // All tablets meta and data files are normal
        UNKNOWN,        // Dry run repair exception
        RECOVERABLE,    // Tablet has missing meta or data files, and is recoverable
        UNRECOVERABLE   // Tablet has missing meta or data files, and is unrecoverable
    }

    /**
     * Return the repair plan without executing it
     */
    public static List<List<String>> dryRunRepair(AdminRepairTableStmt stmt, Database db, OlapTable table,
                                                  List<String> partitionNames, ComputeResource computeResource)
            throws StarRocksException {
        boolean enforceConsistentVersion = stmt.isEnforceConsistentVersion();
        boolean allowEmptyTabletRecovery = stmt.isAllowEmptyTabletRecovery();
        Gson gson = new Gson();

        // get physical partition ids in db table read lock
        List<Long> physicalPartitionIds = getPhysicalPartitionIds(db, table, partitionNames);

        List<List<String>> result = Lists.newArrayList();
        for (Long physicalPartitionId : physicalPartitionIds) {
            long visibleVersion = -1;
            RepairStatus repairStatus = RepairStatus.UNKNOWN;
            List<TabletRecoverInfo> tabletRecoverInfos = Lists.newArrayList();
            String errorMsg = "";

            try {
                PhysicalPartitionInfo info =
                        getPhysicalPartitionInfo(db, table, physicalPartitionId, enforceConsistentVersion, computeResource);
                visibleVersion = info.maxVersion;

                Map<Long, TabletMetadataPB> validMetadatas = getValidTabletMetadatas(info, enforceConsistentVersion);

                // check the valid tablet metadata, and create empty tablet metadata if no valid metadata is found
                checkOrCreateEmptyTabletMetadata(info, validMetadatas, enforceConsistentVersion, allowEmptyTabletRecovery);

                // physical partition is recoverable
                repairStatus = RepairStatus.RECOVERABLE;
                errorMsg = "";
                for (Long tabletId : info.allTablets) {
                    TabletMetadataPB metadata = validMetadatas.get(tabletId);
                    Preconditions.checkNotNull(metadata);
                    long recoverVersion = metadata.version;
                    tabletRecoverInfos.add(new TabletRecoverInfo(tabletId, recoverVersion));
                }
            } catch (AlreadyExistsException e) {
                repairStatus = RepairStatus.NORMAL;
                tabletRecoverInfos.clear();
                errorMsg = "";
            } catch (StarRocksException e) {
                repairStatus = RepairStatus.UNRECOVERABLE;
                tabletRecoverInfos.clear();
                errorMsg = e.getMessage();
            } catch (Exception e) {
                LOG.warn("Fail to dry run repair tablet metadata for partition {}", physicalPartitionId, e);
                repairStatus = RepairStatus.UNKNOWN;
                tabletRecoverInfos.clear();
                errorMsg = String.format("ERROR: %s", e.getMessage());
            }

            result.add(Lists.newArrayList(String.valueOf(physicalPartitionId), String.valueOf(visibleVersion),
                    repairStatus.name(), gson.toJson(tabletRecoverInfos), errorMsg));
        }

        return result;
    }
}
