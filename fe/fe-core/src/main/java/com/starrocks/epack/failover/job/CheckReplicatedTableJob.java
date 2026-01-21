// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.replication.ReplicationJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CheckReplicatedTableJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CheckReplicatedTableJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Database localDatabase;
    private final boolean isIncludeObject;

    public CheckReplicatedTableJob(FailoverGroup failoverGroup, Database remoteDatabase,
            OlapTable remoteTable, Database localDatabase, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.localDatabase = localDatabase;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        Locker locker = new Locker();
        locker.lockDatabase(localDatabase.getId(), LockType.READ);

        try {
            OlapTable localTable = checkTable();
            if (localTable == null) {
                return;
            }

            failoverGroup.getObjectMap().putTableMap(remoteTable.getId(), localTable.getId());

            if (isIncludeObject) {
                failoverGroup.getIncludeMgr().addIncludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                        localDatabase.getId(), localTable.getId());
            }

            boolean needReplication = false;
            for (Partition remotePartition : remoteTable.getPartitions()) {
                if (remotePartition.getName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                    continue;
                }
                Partition localPartition = checkPartition(localTable, remotePartition);
                if (localPartition == null) {
                    return;
                }

                Map<PhysicalPartition, PhysicalPartition> localToRemotePhysicalPartitions = checkPhysicalPartitions(
                        localTable, localPartition, remotePartition);
                if (localToRemotePhysicalPartitions == null) {
                    return;
                }

                for (Map.Entry<PhysicalPartition, PhysicalPartition> entry : localToRemotePhysicalPartitions
                        .entrySet()) {
                    PhysicalPartition localPhysicalPartition = entry.getKey();
                    PhysicalPartition remotePhysicalPartition = entry.getValue();
                    if (localPhysicalPartition.getCommittedVersion() < remotePhysicalPartition.getVisibleVersion()) {
                        needReplication = true;
                        break;
                    }
                }
            }

            if (needReplication) {
                String jobId = String.format("FAILOVER_GROUP_%s-%d-%d-%d", failoverGroup.getName(),
                        localDatabase.getId(), localTable.getId(), System.currentTimeMillis());
                ReplicationJob job = new ReplicationJob(jobId,
                        failoverGroup.getObjectMeta().getClusterToken(),
                        localDatabase.getId(), localTable, remoteTable,
                        failoverGroup.getObjectMeta().getSystemInfoService());
                if (failoverGroup.getJobExecutor().addReplicationJob(job)) {
                    LOG.info("Succeed to create replication job for {}.{} in failover group {}",
                            localDatabase.getFullName(), localTable.getName(), failoverGroup.getName());
                }
            }

            if (!Config.failover_group_allow_drop_extra_partition) {
                return;
            }

            // Drop extra partitions
            for (Partition localPartition : localTable.getPartitions()) {
                if (remoteTable.getPartition(localPartition.getName(), false) == null) {
                    DropReplicatedPartitionJob job = new DropReplicatedPartitionJob(failoverGroup, null,
                            null, localDatabase, localTable, localPartition.getName(), false, true);
                    job.start();
                }
            }
        } finally {
            locker.unLockDatabase(localDatabase.getId(), LockType.READ);
        }
    }

    private OlapTable checkTable() {
        Table localTable = localDatabase.getTable(remoteTable.getName());
        if (localTable == null) {
            CreateReplicatedTableJob job = new CreateReplicatedTableJob(failoverGroup,
                    remoteDatabase, remoteTable, localDatabase, isIncludeObject);
            job.start();
            return null;
        }

        if (!localTable.isOlapTable()) {
            LOG.warn("Local table {}.{} with type {} is not olap table in failover group {}",
                    localDatabase.getFullName(), localTable.getName(), localTable.getType(),
                    failoverGroup.getName());
            if (Config.failover_group_allow_drop_inconsistent_table) {
                DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, remoteDatabase,
                        remoteTable, localDatabase, localTable, isIncludeObject, true);
                job.start();
            } else {
                LOG.warn("Ignore table {}.{} due to failover_group_allow_drop_inconsistent_table = false",
                        localDatabase.getFullName(), localTable.getName());
            }
            return null;
        }

        OlapTable localOlapTable = (OlapTable) localTable;
        if (!checkTableConsistency(localOlapTable)) {
            if (Config.failover_group_allow_drop_inconsistent_table) {
                if (localTable.getCreateTime() < failoverGroup.getSchedule().getRoundScheduledTimeMs() / 1000) {
                    // If local table is created in previous replication round, drop and create it
                    DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, remoteDatabase,
                            remoteTable, localDatabase, localTable, isIncludeObject, true);
                    job.start();
                } else {
                    LOG.error("Ignore inconsistent table {}.{} to avoid an infinite loop of drop and create",
                            localDatabase.getFullName(), localTable.getName());
                }
            } else {
                LOG.warn("Ignore table {}.{} due to failover_group_allow_drop_inconsistent_table = false",
                        localDatabase.getFullName(), localTable.getName());
            }
            return null;
        }

        return localOlapTable;
    }

    private Partition checkPartition(OlapTable localTable, Partition remotePartition) {
        Partition localPartition = localTable.getPartition(remotePartition.getName(), false);
        if (localPartition == null) {
            CreateReplicatedPartitionJob job = new CreateReplicatedPartitionJob(failoverGroup, remoteDatabase,
                    remoteTable, remotePartition, localDatabase, localTable, isIncludeObject);
            job.start();
            return null;
        }

        if (!checkPartitionConsistency(localTable, localPartition, remotePartition)) {
            if (Config.failover_group_allow_drop_inconsistent_partition) {
                if (localTable.getPartitionInfo().isPartitioned()) {
                    if (localPartition.getDefaultPhysicalPartition().getVisibleVersionTime() < failoverGroup
                            .getSchedule().getRoundScheduledTimeMs()) {
                        // If local partition is created in previous replication round, drop and create
                        DropReplicatedPartitionJob job = new DropReplicatedPartitionJob(failoverGroup,
                                remoteDatabase, remoteTable, localDatabase, localTable, localPartition.getName(),
                                isIncludeObject, true);
                        job.start();
                    } else {
                        LOG.error("Ignore inconsistent table {}.{} to avoid an infinite loop of drop and create",
                                localDatabase.getFullName(), localTable.getName());
                    }
                } else {
                    if (localTable.getCreateTime() < failoverGroup.getSchedule().getRoundScheduledTimeMs() / 1000) {
                        // If local table is created in previous replication round, drop and create it
                        DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, remoteDatabase,
                                remoteTable, localDatabase, localTable, isIncludeObject, true);
                        job.start();
                    } else {
                        LOG.error("Ignore inconsistent table {}.{} to avoid an infinite loop of drop and create",
                                localDatabase.getFullName(), localTable.getName());
                    }
                }
            } else {
                LOG.warn("Ignore table {}.{} due to failover_group_allow_drop_inconsistent_partition = false",
                        localDatabase.getFullName(), localTable.getName());
            }
            return null;
        }

        return localPartition;
    }

    // Return local to remote physical partitions
    private Map<PhysicalPartition, PhysicalPartition> checkPhysicalPartitions(OlapTable localTable,
            Partition localPartition, Partition remotePartition) {
        List<PhysicalPartition> localPhysicalPartitions = getOrderedPhysicalPartitions(localPartition);
        List<PhysicalPartition> remotePhysicalPartitions = getOrderedPhysicalPartitions(remotePartition);
        Preconditions.checkState(localPhysicalPartitions.size() <= remotePhysicalPartitions.size());
        Map<PhysicalPartition, PhysicalPartition> localToRemotePhysicalPartitions = new HashMap<>(
                remotePhysicalPartitions.size());
        for (int i = 0; i < remotePhysicalPartitions.size(); i++) {
            PhysicalPartition remotePhysicalPartition = remotePhysicalPartitions.get(i);
            if (i >= localPhysicalPartitions.size()) {
                CreateReplicatedPhysicalPartitionJob job = new CreateReplicatedPhysicalPartitionJob(failoverGroup,
                        remoteDatabase, remoteTable, remotePhysicalPartition, localDatabase, localTable,
                        localPartition, isIncludeObject);
                job.start();
                return null;
            }

            PhysicalPartition localPhysicalPartition = localPhysicalPartitions.get(i);
            if (!checkPhysicalPartitionConsistency(localTable, localPartition, remotePhysicalPartition,
                    localPhysicalPartition)) {
                if (Config.failover_group_allow_drop_inconsistent_partition) {
                    if (localTable.getPartitionInfo().isPartitioned()) {
                        if (localPhysicalPartition.getVisibleVersionTime() < failoverGroup
                                .getSchedule().getRoundScheduledTimeMs()) {
                            // If local partition is created in previous replication round, drop and create
                            DropReplicatedPartitionJob job = new DropReplicatedPartitionJob(failoverGroup,
                                    remoteDatabase, remoteTable, localDatabase, localTable, localPartition.getName(),
                                    isIncludeObject, true);
                            job.start();
                        } else {
                            LOG.error("Ignore inconsistent table {}.{} to avoid an infinite loop of drop and create",
                                    localDatabase.getFullName(), localTable.getName());
                        }
                    } else {
                        if (localTable.getCreateTime() < failoverGroup.getSchedule().getRoundScheduledTimeMs() / 1000) {
                            // If local table is created in previous replication round, drop and create it
                            DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, remoteDatabase,
                                    remoteTable, localDatabase, localTable, isIncludeObject, true);
                            job.start();
                        } else {
                            LOG.error("Ignore inconsistent table {}.{} to avoid an infinite loop of drop and create",
                                    localDatabase.getFullName(), localTable.getName());
                        }
                    }
                } else {
                    LOG.warn("Ignore table {}.{} due to failover_group_allow_drop_inconsistent_partition = false",
                            localDatabase.getFullName(), localTable.getName());
                }
                return null;
            }
            localToRemotePhysicalPartitions.put(localPhysicalPartition, remotePhysicalPartition);
        }
        return localToRemotePhysicalPartitions;
    }

    private boolean checkTableConsistency(OlapTable localTable) {
        if (localTable.getType() != remoteTable.getType()) {
            LOG.warn("Local table {}.{} has different type {} with remote table type {}",
                    localDatabase.getFullName(), localTable.getName(), localTable.getType(), remoteTable.getType());
            return false;
        }

        if (localTable.getKeysType() != remoteTable.getKeysType()) {
            LOG.warn("Local table {}.{} has different keys type {} with remote table type {}",
                    localDatabase.getFullName(), localTable.getName(), localTable.getKeysType(),
                    remoteTable.getKeysType());
            return false;
        }

        List<Column> localColumns = localTable.getBaseSchema();
        List<Column> remoteColumns = remoteTable.getBaseSchema();
        if (localColumns.size() != remoteColumns.size()) {
            LOG.warn("Local table {}.{} has different columns {} with remote table columns {}",
                    localDatabase.getFullName(), localTable.getName(), localColumns.size(), remoteColumns.size());
            return false;
        }

        for (int i = 0; i < localColumns.size(); ++i) {
            Column localColumn = localColumns.get(i);
            Column remoteColumn = remoteColumns.get(i);
            if (!localColumn.equals(remoteColumn)) {
                LOG.warn("Local table {}.{} has different column {} with remote table column {}",
                        localDatabase.getFullName(), localTable.getName(), localColumn, remoteColumn);
                return false;
            }
        }

        if (localTable.getPartitionInfo().getType() != remoteTable.getPartitionInfo().getType()) {
            LOG.warn("Local table {}.{} has different partition type {} with remote table partition type {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localTable.getPartitionInfo().getType(), remoteTable.getPartitionInfo().getType());
            return false;
        }

        DistributionInfo localDistributionInfo = localTable.getDefaultDistributionInfo();
        DistributionInfo remoteDistributionInfo = remoteTable.getDefaultDistributionInfo();
        if (localDistributionInfo.getType() != remoteDistributionInfo.getType()) {
            LOG.warn("Local table {}.{} has different distribution type {} with remote table distribution type {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localDistributionInfo.getType(), remoteDistributionInfo.getType());
            return false;
        }

        if (localTable.getPartitionInfo().isPartitioned()
                && localDistributionInfo.getBucketNum() != remoteDistributionInfo.getBucketNum()) {
            LOG.warn(
                    "Local table {}.{} has different default bucket number {} with remote table default bucket number {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localTable.getDefaultDistributionInfo().getBucketNum(),
                    remoteTable.getDefaultDistributionInfo().getBucketNum());
            return false;
        }

        if (!localTable.getPartitionColumnNames().equals(remoteTable.getPartitionColumnNames())) {
            LOG.warn("Local table {}.{} has different partition column {} with remote table partition column {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localTable.getPartitionColumnNames(), remoteTable.getPartitionColumnNames());
            return false;
        }

        if (!localTable.getDistributionColumnNames().equals(remoteTable.getDistributionColumnNames())) {
            LOG.warn("Local table {}.{} has different distribution column {} with remote table distribution column {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localTable.getDistributionColumnNames(), remoteTable.getDistributionColumnNames());
            return false;
        }

        MaterializedIndexMeta localBaseIndexMeta = localTable.getIndexMetaByMetaId(localTable.getBaseIndexMetaId());
        MaterializedIndexMeta remoteBaseIndexMeta = remoteTable.getIndexMetaByMetaId(remoteTable.getBaseIndexMetaId());
        if (localBaseIndexMeta.getSchemaVersion() != remoteBaseIndexMeta.getSchemaVersion()) {
            LOG.warn("Local table {}.{} has different schema version {} with remote table schema version {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localBaseIndexMeta.getSchemaVersion(), remoteBaseIndexMeta.getSchemaVersion());
            return false;
        }

        return true;
    }

    private boolean checkPartitionConsistency(OlapTable localTable, Partition localPartition,
            Partition remotePartition) {
        DistributionInfo localDistributionInfo = localPartition.getDistributionInfo();
        DistributionInfo remoteDistributionInfo = remotePartition.getDistributionInfo();
        if (localDistributionInfo.getType() != remoteDistributionInfo.getType()) {
            LOG.warn(
                    "Local partition {}.{}.{} has different distribution type {} with remote partition distribution type {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localDistributionInfo.getType(), remoteDistributionInfo.getType());
            return false;
        }

        if (localDistributionInfo.getBucketNum() != remoteDistributionInfo.getBucketNum()) {
            LOG.warn("Local partition {}.{}.{} has different bucket number {} with remote partition bucket number {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localDistributionInfo.getBucketNum(), remoteDistributionInfo.getBucketNum());
            return false;
        }

        if (!localDistributionInfo.getDistributionKey(localTable.getIdToColumn())
                .equals(remoteDistributionInfo.getDistributionKey(remoteTable.getIdToColumn()))) {
            LOG.warn(
                    "Local partition {}.{}.{} has different distribution key {} with remote partition distribution key {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localDistributionInfo.getDistributionKey(localTable.getIdToColumn()),
                    remoteDistributionInfo.getDistributionKey(remoteTable.getIdToColumn()));
            return false;
        }

        PartitionInfo localPartitionInfo = localTable.getPartitionInfo();
        PartitionInfo remotePartitionInfo = remoteTable.getPartitionInfo();
        if (localPartitionInfo.getType() != remotePartitionInfo.getType()) {
            LOG.warn("Local table {}.{} has different partition type {} with remote table partition type {}",
                    localDatabase.getFullName(), localTable.getName(),
                    localPartitionInfo.getType(), remotePartitionInfo.getType());
            return false;
        }

        if (localPartitionInfo.isRangePartition()) {
            RangePartitionInfo localRangePartitionInfo = (RangePartitionInfo) localPartitionInfo;
            RangePartitionInfo remoteRangePartitionInfo = (RangePartitionInfo) remotePartitionInfo;
            Range<PartitionKey> localRange = localRangePartitionInfo.getRange(localPartition.getId());
            Range<PartitionKey> remoteRange = remoteRangePartitionInfo.getRange(remotePartition.getId());
            if (!localRange.equals(remoteRange)) {
                LOG.warn("Local partition {}.{}.{} has different range {} with remote partition range {}",
                        localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                        localRange, remoteRange);
                return false;
            }
        } else if (localPartitionInfo.isListPartition()) {
            ListPartitionInfo localListPartitionInfo = (ListPartitionInfo) localPartitionInfo;
            ListPartitionInfo remoteListPartitionInfo = (ListPartitionInfo) remotePartitionInfo;
            if (localListPartitionInfo.isAutomaticPartition() != remoteListPartitionInfo.isAutomaticPartition()) {
                LOG.warn("Local partition {}.{}.{} has automatic value {} with remote automatic value {}",
                        localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                        localListPartitionInfo.isAutomaticPartition(), remoteListPartitionInfo.isAutomaticPartition());
                return false;
            }

            String localValuesString = localListPartitionInfo.getValuesFormat(localPartition.getId());
            String remoteValuesString = remoteListPartitionInfo.getValuesFormat(remotePartition.getId());
            if (!localValuesString.equals(remoteValuesString)) {
                LOG.warn("Local partition {}.{}.{} has different list value {} with remote list value {}",
                        localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                        localValuesString, remoteValuesString);
                return false;
            }
        }

        if (localPartition.getSubPartitions().size() > remotePartition.getSubPartitions().size()) {
            LOG.warn("Local partition {}.{}.{} has more sub partition num {} than remote sub partition num {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localPartition.getSubPartitions().size(), remotePartition.getSubPartitions().size());
            return false;
        }

        return true;
    }

    private boolean checkPhysicalPartitionConsistency(OlapTable localTable, Partition localPartition,
            PhysicalPartition remotePhysicalPartition, PhysicalPartition localPhysicalPartition) {
        // Do not support rollup index now, so only check base index
        if (localPhysicalPartition.getLatestBaseIndex().getTablets().size() != remotePhysicalPartition
                .getLatestBaseIndex().getTablets().size()) {
            LOG.warn("Local physical partition {}.{}.{}.{} has different tablet num {} than remote tablet num {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localPhysicalPartition.getId(),
                    localPhysicalPartition.getLatestBaseIndex().getTablets().size(),
                    remotePhysicalPartition.getLatestBaseIndex().getTablets().size());
            return false;
        }

        if (localPhysicalPartition.getCommittedVersion() > remotePhysicalPartition.getVisibleVersion()) {
            LOG.warn("Local physical partition {}.{}.{}.{} "
                    + "has greater committed version {} than remote visible version {}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localPhysicalPartition.getId(),
                    localPhysicalPartition.getCommittedVersion(), remotePhysicalPartition.getVisibleVersion());
            return false;
        }

        if (localPhysicalPartition.hasStorageData()
                && localPhysicalPartition.getVersionEpoch() != remotePhysicalPartition.getVersionEpoch()) {
            LOG.warn("Local physical partition {}.{}.{}.{} "
                    + "has different version epoch {}:{} with remote version epoch {}:{}",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    localPhysicalPartition.getId(),
                    localPhysicalPartition.getVisibleVersion(), localPhysicalPartition.getVersionEpoch(),
                    remotePhysicalPartition.getVisibleVersion(), remotePhysicalPartition.getVersionEpoch());
            return false;
        }

        return true;
    }

    private static List<PhysicalPartition> getOrderedPhysicalPartitions(Partition partition) {
        List<PhysicalPartition> physicalPartitions = partition.getSubPartitions().stream()
                .sorted((left, right) -> Long.compare(left.getId(), right.getId()))
                .collect(Collectors.toList());
        Preconditions.checkState(!physicalPartitions.isEmpty()
                && physicalPartitions.get(0).getId() == partition.getDefaultPhysicalPartition().getId());
        return physicalPartitions;
    }
}
