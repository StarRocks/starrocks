// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateReplicatedPhysicalPartitionJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CreateReplicatedPhysicalPartitionJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final PhysicalPartition remotePhysicalPartition;
    private final Database localDatabase;
    private final OlapTable localTable;
    private final Partition localPartition;
    private final boolean isIncludeObject;

    public CreateReplicatedPhysicalPartitionJob(FailoverGroup failoverGroup,
            Database remoteDatabase, OlapTable remoteTable, PhysicalPartition remotePhysicalPartition,
            Database localDatabase, OlapTable localTable, Partition localPartition, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.remotePhysicalPartition = remotePhysicalPartition;
        this.localDatabase = localDatabase;
        this.localTable = localTable;
        this.localPartition = localPartition;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        LOG.info("Creating physical partition {}.{}.{}.{} in failover group {}", localDatabase.getFullName(),
                localTable.getName(), localPartition.getName(), remotePhysicalPartition.getName(),
                failoverGroup.getName());

        long warehouseId = GlobalStateMgr.getServingState().getWarehouseMgr().getBackgroundWarehouse().getId();
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().addSubPartitions(localDatabase, localTable,
                    localPartition, 1, new String[] { remotePhysicalPartition.getName() }, warehouseId);
        } catch (Exception e) {
            LOG.warn("Failed to create physical partition {}.{}.{}.{} in failover group {}, ",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    remotePhysicalPartition.getName(), failoverGroup.getName(), e);
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }
}
