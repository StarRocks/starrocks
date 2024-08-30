// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropPartitionClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

public class DropReplicatedPartitionJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(DropReplicatedPartitionJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Database localDatabase;
    private final OlapTable localTable;
    private final String localPartitionName;
    private final boolean isIncludeObject;
    private final boolean isDropForce;

    public DropReplicatedPartitionJob(FailoverGroup failoverGroup, Database remoteDatabase,
            OlapTable remoteTable, Database localDatabase, OlapTable localTable,
            String localPartitionName, boolean isIncludeObject, boolean isDropForce) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.localDatabase = localDatabase;
        this.localTable = localTable;
        this.localPartitionName = localPartitionName;
        this.isIncludeObject = isIncludeObject;
        this.isDropForce = isDropForce;
    }

    @Override
    public void execute() {
        LOG.info("Droping partition {}.{}.{} in failover group {}", localDatabase.getFullName(),
                localTable.getName(), localPartitionName, failoverGroup.getName());

        DropPartitionClause dropPartitionClause = new DropPartitionClause(true, localPartitionName, false,
                isDropForce);
        dropPartitionClause.setResolvedPartitionNames(Collections.singletonList(localPartitionName));
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().dropPartition(localDatabase, localTable,
                    dropPartitionClause);
        } catch (Exception e) {
            LOG.warn("Failed to drop partition {}.{}.{} in failover group {}, ", localDatabase.getFullName(),
                    localTable.getName(), localPartitionName, failoverGroup.getName(), e);
            return;
        }

        if (remoteDatabase == null || remoteTable == null) {
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }
}
