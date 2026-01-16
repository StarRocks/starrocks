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

package com.starrocks.lake.restore;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ClusterSnapshotUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.RestoreTableFromSnapshotStmt;
import com.starrocks.storagevolume.StorageVolume;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Planner that prepares and submits table-level restore jobs based on cluster snapshots.
 */
public class RestoreHandler {
    private static final Logger LOG = LogManager.getLogger(RestoreHandler.class);

    public RestoreHandler() {
    }

    /**
     * Submit table snapshot restore job - main workflow
     * <p>
     * Main steps:
     * 1. Build restore context: download snapshot image, load snapshot metadata, resolve source table
     * 2. Resolve and validate target database
     * 3. Reset all IDs: table ID, partition IDs, index IDs, tablet IDs, shard group IDs, and update target metadata
     * 4. Build tablet restore tasks using ID mappings and submit tablet-level restore jobs
     */
    public synchronized void submitTableSnapshotRestore(RestoreTableFromSnapshotStmt stmt, ConnectContext context)
            throws StarRocksException {
        Long jobId = GlobalStateMgr.getCurrentState().getNextId();

        // Step 1: Resolve and validate target database
        String fullDbName = stmt.getTargetTable().getDbName();
        if (fullDbName == null) {
            fullDbName = context.getDatabase();
        }
        Database targetDatabase = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(fullDbName);
        if (targetDatabase == null) {
            throw new StarRocksException(String.format("Database '%s' does not exist", fullDbName));
        }

        // Step 2: Build restore context (download snapshot, load metadata, resolve source table)
        ClusterSnapshotJob job = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .getClusterSnapshotJobByName(stmt.getSnapshotName());
        if (job == null) {
            throw new StarRocksException("Cluster snapshot '" + stmt.getSnapshotName() + "' not found");
        }
        if (!job.isFinished()) {
            throw new StarRocksException("Cluster snapshot '" + stmt.getSnapshotName() + "' is not finished yet");
        }

        Path localSnapshotDir = prepareSnapshotImage(job);
        String dbName = stmt.getSourceTable().getDbName();
        if (dbName == null) {
            dbName = context.getDatabase();
        }
        OlapTable tableForRestore = loadSnapshotSource(localSnapshotDir, dbName, stmt.getSourceTable().getTableName());
        LOG.info("Prepared snapshot {} at {} for table {}", stmt.getSnapshotName(), localSnapshotDir, tableForRestore.getName());
        tableForRestore.maySetDatabaseId(targetDatabase.getId());
        tableForRestore.setName(stmt.getTargetTable().getTableName());

        // Step 3: Reset all IDs (table, partition, index, tablet, shard group) and update metadata
        RestoreIdMappings mappings = resetIdsForRestore(targetDatabase, (LakeTable) tableForRestore);

        // Step 4: Build tablet restore tasks and submit tablet-level data restore jobs
        buildTabletRestoreTasksAndSchedule(jobId, targetDatabase.getId(), tableForRestore, mappings);

        LOG.info("Success submit table snapshot restore for table {}", tableForRestore.getName());
    }

    /**
     * Load table metadata from snapshot image.
     * <p>
     * Validates snapshot job state, downloads snapshot image, loads metadata,
     * and resolves source table definition.
     */
    private OlapTable loadSnapshotSource(Path localSnapshotDir, String dbName, String tableName)
            throws StarRocksException {
        GlobalStateMgr.setRestoreThreadId();
        GlobalStateMgr snapshotState = GlobalStateMgr.getCurrentState();
        try {
            snapshotState.loadImage(localSnapshotDir.toString());
            LocalMetastore snapshotMetastore = snapshotState.getLocalMetastore();
            Database sourceDatabase = snapshotMetastore.getDb(dbName);
            if (sourceDatabase == null) {
                throw new StarRocksException("Database '" + dbName + "' not found in snapshot");
            }
            return resolveSourceTable(snapshotState, dbName, tableName);
        } catch (IOException e) {
            throw new StarRocksException(e);
        } finally {
            GlobalStateMgr.resetRestoreThreadId();
            GlobalStateMgr.destroyRestoreState();
        }
    }

    /**
     * Prepare snapshot image: download from remote storage to local
     * <p>
     * Steps:
     * 1. Get storage volume for the snapshot
     * 2. Build remote snapshot path
     * 3. Create local restore directory (<image_dir>/table_restore/<snapshot_name>)
     * 4. Delete existing local snapshot directory if present
     * 5. Copy remote snapshot to local using HdfsUtil
     *
     * @return Local snapshot directory path
     */
    private Path prepareSnapshotImage(ClusterSnapshotJob job) throws StarRocksException {
        StorageVolume storageVolume = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                .getStorageVolumeBySnapshotJob(job);
        if (storageVolume == null) {
            throw new StarRocksException("Storage volume for snapshot '" + job.getSnapshotName() + "' not found");
        }

        String remoteSnapshotPath = ClusterSnapshotUtils.getSnapshotImagePath(storageVolume, job.getSnapshotName());
        remoteSnapshotPath = remoteSnapshotPath.replaceAll("/+$", "");

        Path restoreRoot = Paths.get(GlobalStateMgr.getImageDirPath(), "table_restore");
        try {
            Files.createDirectories(restoreRoot);
            Path localSnapshotDir = restoreRoot.resolve(job.getSnapshotName());
            if (Files.exists(localSnapshotDir)) {
                FileUtils.deleteDirectory(localSnapshotDir.toFile());
            }

            HdfsUtil.copyToLocal(remoteSnapshotPath, restoreRoot.toString(), storageVolume.getProperties());
            return localSnapshotDir;
        } catch (StarRocksException e) {
            throw e;
        } catch (IOException e) {
            throw new StarRocksException("Failed to prepare local directory for snapshot '"
                    + job.getSnapshotName() + "'", e);
        } catch (Exception e) {
            throw new StarRocksException("Failed to download snapshot image '" + job.getSnapshotName() + "'", e);
        }
    }

    /**
     * Resolve source table from snapshot metadata
     * <p>
     * Validates that:
     * - Database exists in snapshot
     * - Table exists in the database
     * - Table is an OlapTable
     *
     * @param snapshotState GlobalStateMgr state loaded from snapshot
     * @param dbName        Database name
     * @param tableName     Table name
     * @return OlapTable instance from snapshot
     */
    private OlapTable resolveSourceTable(GlobalStateMgr snapshotState, String dbName, String tableName)
            throws StarRocksException {
        if (dbName == null || tableName == null) {
            throw new StarRocksException("Source table name must include database: " + dbName + "." + tableName);
        }

        LocalMetastore localMetastore = snapshotState.getLocalMetastore();
        Database database = localMetastore.getDb(dbName);
        if (database == null) {
            throw new StarRocksException("Database '" + dbName + "' not found in snapshot");
        }
        Table table = localMetastore.getTable(dbName, tableName);
        if (table == null) {
            throw new StarRocksException("Table '" + dbName + "." + tableName + "' not found in snapshot");
        }
        if (!(table instanceof OlapTable)) {
            throw new StarRocksException("Table '" + dbName + "." + tableName + "' is not an OLAP table");
        }
        return (OlapTable) table;
    }

    /**
     * Build tablet restore tasks and schedule tablet-level restore jobs.
     */
    private void buildTabletRestoreTasksAndSchedule(Long jobId, long targetDatabaseId,
                                                    OlapTable tableForRestore,
                                                    RestoreIdMappings mappings)
            throws StarRocksException {
        List<RestoreTask> tasks = new ArrayList<>();

        for (Partition targetPartition : tableForRestore.getPartitions()) {
            for (PhysicalPartition targetPhysicalPartition : targetPartition.getSubPartitions()) {
                long targetPhysicalPartitionId = targetPhysicalPartition.getId();
                long sourceVisibleVersion = targetPhysicalPartition.getVisibleVersion();

                List<RestoreTask.TabletRestoreEntry> tabletEntries = new ArrayList<>();

                for (MaterializedIndex targetIndex :
                        targetPhysicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    long targetIndexMetaId = targetIndex.getMetaId();
                    MaterializedIndexMeta targetIndexMeta = tableForRestore.getIndexMetaByMetaId(targetIndexMetaId);
                    if (targetIndexMeta == null) {
                        throw new StarRocksException(String.format(
                                "Metadata for index meta '%d' missing in restored table '%s'",
                                targetIndexMetaId, tableForRestore.getName()));
                    }
                    long targetSchemaId = targetIndexMeta.getSchemaId();

                    for (Tablet targetTablet : targetIndex.getTablets()) {
                        long targetTabletId = targetTablet.getId();
                        Long sourceTabletId = mappings.tabletIdMapping.get(targetTabletId);
                        if (sourceTabletId == null) {
                            throw new StarRocksException(String.format(
                                    "Tablet id %d missing in source table for restored table '%s'",
                                    targetTabletId, tableForRestore.getName()));
                        }
                        tabletEntries.add(new RestoreTask.TabletRestoreEntry(
                                targetSchemaId, sourceTabletId, targetTabletId));
                    }
                }

                if (!tabletEntries.isEmpty()) {
                    tasks.add(new RestoreTask(targetPhysicalPartitionId, sourceVisibleVersion, tabletEntries));
                }
            }
        }

        if (tasks.isEmpty()) {
            LOG.info("No tablet restore tasks generated for {}; skip restore job", tableForRestore.getName());
            return;
        }

        String label = String.format("TABLE_SNAPSHOT_RESTORE_%d_%d_%d", targetDatabaseId,
                tableForRestore.getId(), System.currentTimeMillis());

        SnapshotRestoreJob job = new SnapshotRestoreJob(jobId, label, targetDatabaseId, tableForRestore.getId(),
                tasks, tableForRestore);
        try {
            GlobalStateMgr.getCurrentState().getBackupHandler().submitTableSnapshotRestoreJob(job);
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logJsonObject(OperationTypeEPack.OP_RESTORE_FROM_SNAPSHOT, job);
        } catch (AlreadyExistsException e) {
            throw new StarRocksException("Snapshot restore job already running for table "
                    + tableForRestore.getName(), e);
        }

        LOG.info("Submitted table snapshot restore job {} for restoring table {} ({} physical partitions)",
                jobId, tableForRestore.getName(), tasks.size());
    }

    /**
     * Reset all IDs for a LakeTable being restored from snapshot
     * <p>
     * This is the core ID reset logic that ensures no ID conflicts with existing objects.
     * <p>
     * Steps:
     * 1. Reset table ID and allocate new file path via StarOS
     * 2. Reset all index IDs (base index and rollup indexes) and schema IDs
     * 3. Reset partition IDs and update partition info
     * 4. Reset physical partition IDs
     * 5. For each physical partition and index:
     * - Reset index ID in physical partition
     * - Create new shard group via StarOS
     * - Clear old tablets and create new tablets via StarOS
     * 6. Update replication number
     *
     * @param tableForRestore LakeTable to reset IDs for
     * @param targetDatabase  Target database
     * @return RestoreIdMappings containing tablet ID mappings
     */
    public RestoreIdMappings resetIdsForRestore(Database targetDatabase,
                                                LakeTable tableForRestore)
            throws StarRocksException {
        // Copy an origin index id to name map
        Map<Long, String> origIdxIdToName = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : tableForRestore.getIndexNameToMetaId().entrySet()) {
            origIdxIdToName.put(entry.getValue(), entry.getKey());
        }

        RestoreIdMappings mappings = new RestoreIdMappings();

        // Reset table id
        tableForRestore.setId(GlobalStateMgr.getCurrentState().getNextId());

        try {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            FilePathInfo pathInfo = starOSAgent.allocateFilePath(targetDatabase.getId(), tableForRestore.getId());
            tableForRestore.setStorageInfo(pathInfo, tableForRestore.getTableProperty().getStorageInfo().getDataCacheInfo());
        } catch (DdlException e) {
            throw new StarRocksException("Failed to allocate file path: " + e.getMessage(), e);
        }

        // Reset all 'indexIdToXXX' map
        Map<Long, MaterializedIndexMeta> origIndexIdToMeta = Maps.newHashMap(tableForRestore.getIndexMetaIdToMeta());
        tableForRestore.getIndexMetaIdToMeta().clear();
        for (Map.Entry<Long, String> entry : origIdxIdToName.entrySet()) {
            long newIdxId = GlobalStateMgr.getCurrentState().getNextId();
            if (entry.getValue().equals(tableForRestore.getName())) {
                // Base index
                tableForRestore.setBaseIndexMetaId(newIdxId);
            }
            MaterializedIndexMeta indexMeta = origIndexIdToMeta.get(entry.getKey());
            indexMeta.setIndexMetaIdForRestore(newIdxId);
            indexMeta.setSchemaId(newIdxId);
            tableForRestore.getIndexMetaIdToMeta().put(newIdxId, indexMeta);
            tableForRestore.getIndexNameToMetaId().put(entry.getValue(), newIdxId);
        }

        // Generate a partition old id to new id map
        Map<Long, Long> partitionOldIdToNewId = Maps.newHashMap();
        for (Long id : tableForRestore.getIdToPartition().keySet()) {
            partitionOldIdToNewId.put(id, GlobalStateMgr.getCurrentState().getNextId());
        }

        // Reset partition info
        tableForRestore.getPartitionInfo().setPartitionIdsForRestore(partitionOldIdToNewId);

        // Reset partitions
        List<Partition> partitions = Lists.newArrayList(tableForRestore.getIdToPartition().values());
        tableForRestore.getIdToPartition().clear();
        tableForRestore.getPhysicalPartitionIdToPartitionId().clear();
        for (Partition partition : partitions) {
            long oldPartitionId = partition.getId();
            long newPartitionId = partitionOldIdToNewId.get(oldPartitionId);
            partition.setIdForRestore(newPartitionId);
            tableForRestore.getIdToPartition().put(newPartitionId, partition);
            List<PhysicalPartition> origPhysicalPartitions = Lists.newArrayList(partition.getSubPartitions());
            origPhysicalPartitions.forEach(physicalPartition -> {
                // after refactor, the first physicalPartition id is different from logical partition id
                if (physicalPartition.getParentId() != newPartitionId) {
                    partition.removeSubPartition(physicalPartition.getId());
                }
            });

            origPhysicalPartitions.forEach(physicalPartition -> {
                // After refactor, the first physicalPartition id is different from logical partition id
                if (physicalPartition.getParentId() != newPartitionId) {
                    physicalPartition.setIdForRestore(GlobalStateMgr.getCurrentState().getNextId());
                    physicalPartition.setParentId(newPartitionId);
                    partition.addSubPartition(physicalPartition);
                }
                tableForRestore.getPhysicalPartitionIdToPartitionId().put(physicalPartition.getId(), newPartitionId);
            });
        }

        // For each partition, reset rollup index map
        for (Map.Entry<Long, Partition> entry : tableForRestore.getIdToPartition().entrySet()) {
            Partition partition = entry.getValue();
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                Map<Long, MaterializedIndex> origIdToIndex = Maps.newHashMapWithExpectedSize(origIdxIdToName.size());
                for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                    MaterializedIndex idx = physicalPartition.getIndex(entry2.getKey());
                    origIdToIndex.put(entry2.getKey(), idx);
                    long newIdxId = tableForRestore.getIndexNameToMetaId().get(entry2.getValue());
                    if (newIdxId != tableForRestore.getBaseIndexMetaId()) {
                        // Not base table, delete old index
                        physicalPartition.deleteMaterializedIndexByMetaId(entry2.getKey());
                    }
                }
                for (Map.Entry<Long, String> entry2 : origIdxIdToName.entrySet()) {
                    MaterializedIndex idx = origIdToIndex.get(entry2.getKey());
                    long newIdxId = tableForRestore.getIndexNameToMetaId().get(entry2.getValue());
                    idx.setIdForRestore(newIdxId);
                    long newShardGroupId;
                    try {
                        newShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent()
                                .createShardGroup(targetDatabase.getId(), tableForRestore.getId(), physicalPartition.getId(),
                                        newIdxId);
                    } catch (DdlException e) {
                        throw new StarRocksException("Failed to create shard group: " + e.getMessage(), e);
                    }
                    idx.setShardGroupId(newShardGroupId);
                    if (newIdxId == tableForRestore.getBaseIndexMetaId()) {
                        physicalPartition.setShardGroupId(newShardGroupId);
                    }
                    if (newIdxId != tableForRestore.getBaseIndexMetaId()) {
                        // Not base table, reset
                        physicalPartition.createRollupIndex(idx);
                    }

                    // Generate new tablets in origin tablet order
                    List<Long> originalTabletIds = idx.getTabletIdsInOrder();
                    int tabletNum = originalTabletIds.size();
                    idx.clearTabletsForRestore();
                    createTabletsForRestore(
                            tableForRestore,
                            tabletNum,
                            idx,
                            physicalPartition.getId());
                    List<Long> newTabletIds = idx.getTabletIdsInOrder();
                    if (originalTabletIds.size() == newTabletIds.size()) {
                        for (int i = 0; i < originalTabletIds.size(); ++i) {
                            mappings.tabletIdMapping.put(newTabletIds.get(i), originalTabletIds.get(i));
                        }
                    } else {
                        LOG.warn("Tablet count changed during restore for index {}: {} -> {}", entry2.getKey(),
                                originalTabletIds.size(), newTabletIds.size());
                    }
                }
            }
        }

        return mappings;
    }

    /**
     * Create tablets for a restored index
     * <p>
     * Creates new tablets (shards) via StarOS for the specified index.
     * Each tablet is associated with the index's shard group and physical partition.
     * <p>
     * Steps:
     * 1. Get file path info and cache info for the physical partition
     * 2. Prepare properties (partition ID and index ID) for shard creation
     * 3. Create shards via StarOS agent
     * 4. Create LakeTablet instances and add them to the index
     *
     * @param table               LakeTable being restored
     * @param tabletNum           Number of tablets to create
     * @param index               MaterializedIndex to add tablets to
     * @param physicalPartitionId Physical partition ID
     */
    private void createTabletsForRestore(LakeTable table, int tabletNum, MaterializedIndex index, long physicalPartitionId)
            throws StarRocksException {
        // Use physical partition id as path id when creating a new physical partition
        FilePathInfo fsInfo = table.getPartitionFilePathInfo(physicalPartitionId);
        FileCacheInfo cacheInfo = table.getPartitionFileCacheInfo(physicalPartitionId);
        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        List<Long> shardIds;
        try {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            shardIds = starOSAgent.createShards(tabletNum, fsInfo, cacheInfo, index.getShardGroupId(), null, properties,
                    com.starrocks.server.WarehouseManager.DEFAULT_RESOURCE);
        } catch (DdlException e) {
            LOG.error(e.getMessage(), e);
            throw new StarRocksException("Failed to create shards: " + e.getMessage(), e);
        }
        for (long shardId : shardIds) {
            LakeTablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, null /* tablet meta */, false/* update inverted index */);
        }
    }

    /**
     * Static class to hold tablet ID mappings during restore operations.
     * Mapping uses target tablet ID as key and points back to the original snapshot tablet ID.
     */
    public static class RestoreIdMappings {
        public final Map<Long, Long> tabletIdMapping = Maps.newHashMap();
    }
}
