// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.ReplicatedObjectMeta;
import com.starrocks.replication.ReplicationJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CheckReplicatedTableJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CheckReplicatedTableJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Database localDatabase;
    private final boolean isReplicatedObject;

    public CheckReplicatedTableJob(FailoverGroup failoverGroup, ReplicatedObjectMeta objectMeta,
            Database remoteDatabase, OlapTable remoteTable, Database localDatabase, boolean isReplicatedObject) {
        super(failoverGroup, objectMeta);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.localDatabase = localDatabase;
        this.isReplicatedObject = isReplicatedObject;
    }

    @Override
    public void execute() {
        Locker locker = new Locker();
        locker.lockDatabase(localDatabase, LockType.READ);

        try {
            Table localTable = localDatabase.getTable(remoteTable.getName());
            if (localTable == null) {
                CreateReplicatedTableJob job = new CreateReplicatedTableJob(failoverGroup, objectMeta,
                        remoteDatabase, remoteTable, localDatabase, isReplicatedObject);
                job.start();
                return;
            }

            if (!localTable.isOlapTable()) {
                DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, objectMeta, remoteDatabase,
                        remoteTable, localDatabase, localTable, isReplicatedObject, true);
                job.start();
                return;
            }

            OlapTable localOlapTable = (OlapTable) localTable;
            OlapTable remoteOlapTable = remoteTable;
            if (!checkTableConsistency(localDatabase, localOlapTable, remoteOlapTable)) {
                DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, objectMeta, remoteDatabase,
                        remoteTable, localDatabase, localTable, isReplicatedObject, true);
                job.start();
                return;
            }

            if (isReplicatedObject) {
                failoverGroup.addReplicatedTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, localDatabase.getId(),
                        localTable.getId());
            }

            boolean needReplication = false;
            for (Partition remotePartition : remoteTable.getPartitions()) {
                if (remotePartition.getName().startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
                    continue;
                }
                Partition localPartition = checkPartition(localDatabase, localOlapTable, remoteOlapTable,
                        remotePartition);
                if (localPartition == null) {
                    return;
                }
                if (localPartition.getCommittedVersion() < remotePartition.getVisibleVersion()) {
                    needReplication = true;
                }
            }

            if (needReplication) {
                try {
                    ReplicationJob job = new ReplicationJob(null, objectMeta.getSystemMeta().getToken(),
                            localDatabase.getId(), localOlapTable, remoteOlapTable,
                            objectMeta.getSystemMeta().getSystemInfoService());
                    failoverGroup.addReplicationJob(job);
                } catch (Exception e) {
                    LOG.warn("Failed to create replication job for {}.{} in failover group {}, ",
                            localDatabase.getFullName(), localTable.getName(), failoverGroup.getName(), e);
                }
            }

            // Drop deleted partitions
            for (Partition localPartition : localOlapTable.getPartitions()) {
                if (remoteOlapTable.getPartition(localPartition.getName(), false) == null) {
                    DropReplicatedPartitionJob job = new DropReplicatedPartitionJob(failoverGroup, objectMeta, null,
                            null, localDatabase, localOlapTable, localPartition.getName(), false);
                    job.start();
                }
            }
        } finally {
            locker.unLockDatabase(localDatabase, LockType.READ);
        }
    }

    private Partition checkPartition(Database localDatabase, OlapTable localTable,
            OlapTable remoteTable, Partition remotePartition) {
        Partition localPartition = localTable.getPartition(remotePartition.getName(), false);
        if (localPartition == null) {
            CreateReplicatedPartitionJob job = new CreateReplicatedPartitionJob(failoverGroup, objectMeta,
                    remoteDatabase, remoteTable, remotePartition, localDatabase, localTable, isReplicatedObject);
            job.start();
            return null;
        }

        if (!checkPartitionConsistency(localDatabase, localTable, localPartition, remoteTable, remotePartition)) {
            DropReplicatedPartitionJob job = new DropReplicatedPartitionJob(failoverGroup, objectMeta, remoteDatabase,
                    remoteTable, localDatabase, localTable, localPartition.getName(), isReplicatedObject);
            job.start();
            return null;
        }

        // TODO: Check sub partitions
        return localPartition;
    }

    private boolean checkTableConsistency(Database localDatabase, OlapTable localTable, OlapTable remoteTable) {
        if (localTable.getType() != remoteTable.getType()) {
            LOG.warn("Local table {}.{} has different type {} with remote table type {}",
                    localDatabase.getFullName(), localTable.getName(), localTable.getType(), remoteTable.getType());
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

        if (localTable.getDefaultDistributionInfo().getBucketNum() != remoteTable.getDefaultDistributionInfo()
                .getBucketNum()) {
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

        return true;
    }

    private boolean checkPartitionConsistency(Database localDatabase, OlapTable localTable, Partition localPartition,
            OlapTable remoteTable, Partition remotePartition) {
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

        return true;
    }
}
