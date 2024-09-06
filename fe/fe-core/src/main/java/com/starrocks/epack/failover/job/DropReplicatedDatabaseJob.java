// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DropReplicatedDatabaseJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(DropReplicatedDatabaseJob.class);

    private final Database remoteDatabase;
    private final List<Table> remoteTables; // Null for whole database
    private final Database localDatabase;
    private final boolean isIncludeObject;
    private final boolean isDropForce;

    public DropReplicatedDatabaseJob(FailoverGroup failoverGroup, Database remoteDatabase,
            List<Table> remoteTables, Database localDatabase, boolean isIncludeObject,
            boolean isDropForce) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTables = remoteTables;
        this.localDatabase = localDatabase;
        this.isIncludeObject = isIncludeObject;
        this.isDropForce = isDropForce;
    }

    @Override
    public void execute() {
        LOG.info("Droping database {} in failover group {}", localDatabase.getFullName(), failoverGroup.getName());

        try {
            GlobalStateMgr.getServingState().getLocalMetastore().dropDb(localDatabase.getFullName(), isDropForce);
        } catch (Exception e) {
            failoverGroup.addErrorMessage("Failed to drop database " +
                    localDatabase.getFullName() + ", error: " + e.getMessage());
            LOG.warn("Failed to drop database {} in failover group {}, ", localDatabase.getFullName(),
                    failoverGroup.getName(), e);
            return;
        }

        if (isIncludeObject) {
            failoverGroup.getIncludeMgr().removeIncludeDatabase(localDatabase.getId());
        }

        if (remoteDatabase == null) {
            return;
        }

        CheckReplicatedDatabaseJob job = new CheckReplicatedDatabaseJob(failoverGroup,
                remoteDatabase, remoteTables, isIncludeObject);
        job.execute();
    }
}
