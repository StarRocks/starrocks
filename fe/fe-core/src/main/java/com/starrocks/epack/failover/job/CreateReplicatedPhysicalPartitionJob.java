// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
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
                localTable.getName(), localPartition.getName(), remotePhysicalPartition.getId(),
                failoverGroup.getName());

        ComputeResource computeResource  = GlobalStateMgr.getServingState().getWarehouseMgr().getBackgroundComputeResource();
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().addSubPartitions(localDatabase, localTable,
                    localPartition, 1, computeResource);
        } catch (Exception e) {
            failoverGroup.addErrorMessage("Failed to create physical partition " + localDatabase.getFullName() + "." +
                    localTable.getName() + "." + localPartition.getName() + "." + remotePhysicalPartition.getId() +
                    ", error: " + e.getMessage());
            LOG.warn("Failed to create physical partition {}.{}.{}.{} in failover group {}, ",
                    localDatabase.getFullName(), localTable.getName(), localPartition.getName(),
                    remotePhysicalPartition.getId(), failoverGroup.getName(), e);
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }
}
