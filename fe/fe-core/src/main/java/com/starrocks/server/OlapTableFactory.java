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

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.Util;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.StorageInfo;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class OlapTableFactory implements AbstractTableFactory {
    private static final Logger LOG = LogManager.getLogger(OlapTableFactory.class);
    public static final OlapTableFactory INSTANCE = new OlapTableFactory();

    private OlapTableFactory() {
    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database db, CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr stateMgr = metastore.getStateMgr();
        ColocateTableIndex colocateTableIndex = metastore.getColocateTableIndex();
        String tableName = stmt.getTableName();

        LOG.debug("begin create olap table: {}", tableName);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        metastore.validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            // gen partition id first
            if (partitionDesc instanceof RangePartitionDesc) {
                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : rangePartitionDesc.getSingleRangePartitionDescs()) {
                    long partitionId = metastore.getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }
            } else if (partitionDesc instanceof ListPartitionDesc) {
                ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
                listPartitionDesc.findAllPartitionNames()
                        .forEach(partitionName -> partitionNameToId.put(partitionName, metastore.getNextId()));
            } else if (partitionDesc instanceof ExpressionPartitionDesc) {
                ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : expressionPartitionDesc.getRangePartitionDesc()
                        .getSingleRangePartitionDescs()) {
                    long partitionId = metastore.getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }

                DynamicPartitionUtil.checkIfAutomaticPartitionAllowed(stmt.getProperties());

            } else {
                throw new DdlException("Currently only support range or list partition with engine type olap");
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);

            // Automatic partitioning needs to ensure that at least one tablet is opened.
            if (partitionInfo.isAutomaticPartition()) {
                long partitionId = metastore.getNextId();
                String replicateNum = String.valueOf(RunMode.defaultReplicationNum());
                if (stmt.getProperties() != null) {
                    replicateNum = stmt.getProperties().getOrDefault("replication_num",
                            String.valueOf(RunMode.defaultReplicationNum()));
                }
                partitionInfo.createAutomaticShadowPartition(partitionId, replicateNum);
                partitionNameToId.put(ExpressionRangePartitionInfo.AUTOMATIC_SHADOW_PARTITION_NAME, partitionId);
            }

        } else {
            if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(stmt.getProperties())) {
                throw new DdlException("Only support dynamic partition properties on range partition table");
            }
            long partitionId = metastore.getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        short shortKeyColumnCount = 0;
        List<Integer> sortKeyIdxes = new ArrayList<>();
        if (stmt.getSortKeys() != null) {
            List<String> baseSchemaNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            for (String column : stmt.getSortKeys()) {
                int idx = baseSchemaNames.indexOf(column);
                if (idx == -1) {
                    throw new DdlException("Invalid column '" + column + "': not exists in all columns.");
                }
                sortKeyIdxes.add(idx);
            }
            shortKeyColumnCount =
                    GlobalStateMgr.calcShortKeyColumnCount(baseSchema, stmt.getProperties(), sortKeyIdxes);
        } else {
            shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        }
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);
        // indexes
        TableIndexes indexes = new TableIndexes(stmt.getIndexes());

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        // create table
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        OlapTable table;
        if (stmt.isExternal()) {
            table = new ExternalOlapTable(db.getId(), tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes, properties);
            if (GlobalStateMgr.getCurrentState().getNodeMgr()
                    .checkFeExistByRPCPort(((ExternalOlapTable) table).getSourceTableHost(),
                            ((ExternalOlapTable) table).getSourceTablePort())) {
                throw new DdlException("can not create OLAP external table of self cluster");
            }
        } else if (stmt.isOlapEngine()) {
            RunMode runMode = RunMode.getCurrentRunMode();
            String volume = "";
            if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
                volume = properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
            }
            if (runMode == RunMode.SHARED_DATA) {
                if (volume.equals(StorageVolumeMgr.LOCAL)) {
                    throw new DdlException("Cannot create table " +
                            "without persistent volume in current run mode \"" + runMode + "\"");
                }
                table = new LakeTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
                StorageVolumeMgr svm = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
                if (table.isCloudNativeTable() && !svm.bindTableToStorageVolume(volume, db.getId(), tableId)) {
                    throw new DdlException(String.format("Storage volume %s not exists", volume));
                }
                String storageVolumeId = svm.getStorageVolumeIdOfTable(tableId);
                metastore.setLakeStorageInfo(table, storageVolumeId, properties);
            } else {
                table = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
            }
        } else {
            throw new DdlException("Unrecognized engine \"" + stmt.getEngineName() + "\"");
        }

        try {
            table.setComment(stmt.getComment());

            // set base index id
            long baseIndexId = metastore.getNextId();
            table.setBaseIndexId(baseIndexId);

            // analyze bloom filter columns
            Set<String> bfColumns = null;
            double bfFpp = 0;
            try {
                bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                        table.getKeysType() == KeysType.PRIMARY_KEYS);
                if (bfColumns != null && bfColumns.isEmpty()) {
                    bfColumns = null;
                }

                bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
                if (bfColumns != null && bfFpp == 0) {
                    bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
                } else if (bfColumns == null) {
                    bfFpp = 0;
                }

                table.setBloomFilterInfo(bfColumns, bfFpp);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }

            // analyze replication_num
            short replicationNum = RunMode.defaultReplicationNum();
            try {
                boolean isReplicationNumSet =
                        properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
                replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
                if (isReplicationNumSet) {
                    table.setReplicationNum(replicationNum);
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }

            // set in memory
            boolean isInMemory =
                    PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
            table.setIsInMemory(isInMemory);

            boolean enablePersistentIndex =
                    PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                            false);
            if (enablePersistentIndex && table.isCloudNativeTable()) {
                throw new DdlException("Cannot create cloud native table with persistent index yet");
            }

            table.setEnablePersistentIndex(enablePersistentIndex);

            if (properties != null && (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                    properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE) ||
                    properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL))) {
                try {
                    boolean enableBinlog = PropertyAnalyzer.analyzeBooleanProp(properties,
                            PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, false);
                    long binlogTtl = PropertyAnalyzer.analyzeLongProp(properties,
                            PropertyAnalyzer.PROPERTIES_BINLOG_TTL, Config.binlog_ttl_second);
                    long binlogMaxSize = PropertyAnalyzer.analyzeLongProp(properties,
                            PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, Config.binlog_max_size);
                    BinlogConfig binlogConfig = new BinlogConfig(0, enableBinlog,
                            binlogTtl, binlogMaxSize);
                    table.setCurBinlogConfig(binlogConfig);
                    LOG.info("create table {} set binlog config, enable_binlog = {}, binlogTtl = {}, binlog_max_size = {}",
                            tableName, enableBinlog, binlogTtl, binlogMaxSize);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }

            // write quorum
            try {
                table.setWriteQuorum(PropertyAnalyzer.analyzeWriteQuorum(properties));
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }

            // replicated storage
            table.setEnableReplicatedStorage(
                    PropertyAnalyzer.analyzeBooleanProp(
                            properties, PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                            Config.enable_replicated_storage_as_default_engine));

            if (table.enableReplicatedStorage().equals(false)) {
                for (Column col : baseSchema) {
                    if (col.isAutoIncrement()) {
                        throw new DdlException("Table with AUTO_INCREMENT column must use Replicated Storage");
                    }
                }
            }

            TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
            try {
                tabletType = PropertyAnalyzer.analyzeTabletType(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }

            if (table.isCloudNativeTable()) {
                if (properties != null) {
                    try {
                        PeriodDuration duration = PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
                        if (duration != null) {
                            table.setDataCachePartitionDuration(duration);
                        }
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }
                }
                if (partitionInfo.getType() == PartitionType.LIST) {
                    throw new DdlException("Do not support create list partition Cloud Native table");
                }
            }

            if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                // if this is an unpartitioned table, we should analyze data property and replication num here.
                // if this is a partitioned table, there properties are already analyzed in RangePartitionDesc analyze phase.

                // use table name as this single partition name
                long partitionId = partitionNameToId.get(tableName);
                DataProperty dataProperty = null;
                try {
                    boolean hasMedium = false;
                    if (properties != null) {
                        hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                    }
                    dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                            DataProperty.getInferredDefaultDataProperty(), false);
                    if (hasMedium) {
                        table.setStorageMedium(dataProperty.getStorageMedium());
                    }
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                Preconditions.checkNotNull(dataProperty);
                partitionInfo.setDataProperty(partitionId, dataProperty);
                partitionInfo.setReplicationNum(partitionId, replicationNum);
                partitionInfo.setIsInMemory(partitionId, isInMemory);
                partitionInfo.setTabletType(partitionId, tabletType);
                StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
                DataCacheInfo dataCacheInfo = storageInfo == null ? null : storageInfo.getDataCacheInfo();
                partitionInfo.setDataCacheInfo(partitionId, dataCacheInfo);
            }

            // check colocation properties
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (StringUtils.isNotEmpty(colocateGroup)) {
                if (!distributionInfo.supportColocate()) {
                    throw new DdlException("random distribution does not support 'colocate_with'");
                }

                boolean addedToColocateGroup = colocateTableIndex.addTableToGroup(db, table,
                        colocateGroup, false /* expectLakeTable */);
                if (!(table instanceof ExternalOlapTable) && addedToColocateGroup) {
                    // Colocate table should keep the same bucket number across the partitions
                    DistributionInfo defaultDistributionInfo = table.getDefaultDistributionInfo();
                    table.inferDistribution(defaultDistributionInfo);
                }
            }

            // get base index storage type. default is COLUMN
            TStorageType baseIndexStorageType = null;
            try {
                baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(baseIndexStorageType);
            // set base index meta
            int schemaVersion = 0;
            try {
                schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            int schemaHash = Util.schemaHash(schemaVersion, baseSchema, bfColumns, bfFpp);

            if (stmt.getSortKeys() != null) {
                table.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                        shortKeyColumnCount, baseIndexStorageType, keysType, null, sortKeyIdxes);
            } else {
                table.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                        shortKeyColumnCount, baseIndexStorageType, keysType, null);
            }

            for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
                AddRollupClause addRollupClause = (AddRollupClause) alterClause;

                Long baseRollupIndex = table.getIndexIdByName(tableName);

                // get storage type for rollup index
                TStorageType rollupIndexStorageType = null;
                try {
                    rollupIndexStorageType = PropertyAnalyzer.analyzeStorageType(addRollupClause.getProperties());
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                Preconditions.checkNotNull(rollupIndexStorageType);
                // set rollup index meta to olap table
                List<Column> rollupColumns = stateMgr.getRollupHandler().checkAndPrepareMaterializedView(addRollupClause,
                        table, baseRollupIndex);
                short rollupShortKeyColumnCount =
                        GlobalStateMgr.calcShortKeyColumnCount(rollupColumns, alterClause.getProperties());
                int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupColumns, bfColumns, bfFpp);
                long rollupIndexId = metastore.getNextId();
                table.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
                        rollupSchemaHash, rollupShortKeyColumnCount, rollupIndexStorageType, keysType);
            }

            // analyze version info
            Long version = null;
            try {
                version = PropertyAnalyzer.analyzeVersionInfo(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(version);

            // storage_format is not necessary, remove storage_format if exist.
            if (properties != null) {
                properties.remove("storage_format");
            }

            // get compression type
            TCompressionType compressionType = TCompressionType.LZ4_FRAME;
            try {
                compressionType = PropertyAnalyzer.analyzeCompressionType(properties);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            table.setCompressionType(compressionType);

            // partition live number
            int partitionLiveNumber;
            if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
                try {
                    partitionLiveNumber = PropertyAnalyzer.analyzePartitionLiveNumber(properties, true);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
                table.setPartitionLiveNumber(partitionLiveNumber);
            }

            try {
                processConstraint(db, table, properties);
            } catch (AnalysisException e) {
                throw new DdlException(
                        String.format("processing constraint failed when creating table:%s. exception msg:%s",
                                table.getName(), e.getMessage()), e);
            }

            // a set to record every new tablet created when create table
            // if failed in any step, use this set to do clear things
            Set<Long> tabletIdSet = new HashSet<Long>();

            // do not create partition for external table
            if (table.isOlapOrCloudNativeTable()) {
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    if (properties != null && !properties.isEmpty()) {
                        // here, all properties should be checked
                        throw new DdlException("Unknown properties: " + properties);
                    }

                    // this is a 1-level partitioned table, use table name as partition name
                    long partitionId = partitionNameToId.get(tableName);
                    Partition partition = metastore.createPartition(db, table, partitionId, tableName, version, tabletIdSet);
                    metastore.buildPartitions(db, table, Collections.singletonList(partition));
                    table.addPartition(partition);
                } else if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                    try {
                        // just for remove entries in stmt.getProperties(),
                        // and then check if there still has unknown properties
                        boolean hasMedium = false;
                        if (properties != null) {
                            hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                        }
                        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                DataProperty.getInferredDefaultDataProperty(), false);
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(table, properties);
                        if (table.dynamicPartitionExists() && table.getColocateGroup() != null) {
                            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                            if (info.getBucketNum() !=
                                    table.getTableProperty().getDynamicPartitionProperty().getBuckets()) {
                                throw new DdlException("dynamic_partition.buckets should equal the distribution buckets"
                                        + " if creating a colocate table");
                            }
                        }
                        if (hasMedium) {
                            table.setStorageMedium(dataProperty.getStorageMedium());
                        }
                        if (properties != null && !properties.isEmpty()) {
                            // here, all properties should be checked
                            throw new DdlException("Unknown properties: " + properties);
                        }
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    // this is a 2-level partitioned tables
                    List<Partition> partitions = new ArrayList<>(partitionNameToId.size());
                    for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                        Partition partition = metastore.createPartition(db, table, entry.getValue(), entry.getKey(), version,
                                tabletIdSet);
                        partitions.add(partition);
                    }
                    // It's ok if partitions is empty.
                    metastore.buildPartitions(db, table, partitions);
                    for (Partition partition : partitions) {
                        table.addPartition(partition);
                    }
                } else {
                    throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
                }
                // if binlog_enable is true when creating table,
                // then set binlogAvailableVersion without statistics through reportHandler
                if (table.isBinlogEnabled()) {
                    Map<String, String> binlogAvailableVersion = table.buildBinlogAvailableVersion();
                    table.setBinlogAvailableVersion(binlogAvailableVersion);
                    LOG.info("set binlog available version when create table, tableName : {}, partitions : {}",
                            tableName, binlogAvailableVersion.toString());
                }
            }

            // process lake table colocation properties, after partition and tablet creation
            colocateTableIndex.addTableToGroup(db, table, colocateGroup, true /* expectLakeTable */);
        } catch (DdlException e) {
            GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(tableId);
            throw e;
        }

        // NOTE: The table has been added to the database, and the following procedure cannot throw exception.
        LOG.info("Successfully create table[{};{}]", tableName, tableId);
        return table;
    }

    private void processConstraint(
            Database db, OlapTable olapTable, Map<String, String> properties) throws AnalysisException {
        List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, olapTable);
        if (uniqueConstraints != null) {
            olapTable.setUniqueConstraints(uniqueConstraints);
        }

        List<ForeignKeyConstraint> foreignKeyConstraints =
                PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, olapTable);
        if (foreignKeyConstraints != null) {
            olapTable.setForeignKeyConstraints(foreignKeyConstraints);
        }
    }

}
