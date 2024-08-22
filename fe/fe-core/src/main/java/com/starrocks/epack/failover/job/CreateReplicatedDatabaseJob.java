// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CreateReplicatedDatabaseJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CreateReplicatedDatabaseJob.class);

    private final Database remoteDatabase;
    private final List<Table> remoteTables; // Null for whole database
    private final boolean isIncludeObject;

    public CreateReplicatedDatabaseJob(FailoverGroup failoverGroup, Database remoteDatabase,
            List<Table> remoteTables, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTables = remoteTables;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        LOG.info("Creating database {} in failover group {}", remoteDatabase.getFullName(), failoverGroup.getName());

        try {
            GlobalStateMgr.getServingState().getLocalMetastore().createDb(remoteDatabase.getFullName());
        } catch (Exception e) {
            LOG.warn("Failed to create database {} in failover group {}, ", remoteDatabase.getFullName(),
                    failoverGroup.getName(), e);
            return;
        }

        CheckReplicatedDatabaseJob job = new CheckReplicatedDatabaseJob(failoverGroup,
                remoteDatabase, remoteTables, isIncludeObject);
        job.execute();
    }
}
