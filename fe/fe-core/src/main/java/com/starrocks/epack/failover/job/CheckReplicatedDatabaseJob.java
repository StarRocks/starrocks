// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CheckReplicatedDatabaseJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CheckReplicatedDatabaseJob.class);

    private final Database remoteDatabase;
    private final List<Table> remoteTables; // Null for whole database
    private final boolean isReplicatedObject;

    public CheckReplicatedDatabaseJob(FailoverGroup failoverGroup, Database remoteDatabase,
            boolean isReplicatedObject) {
        this(failoverGroup, remoteDatabase, null, isReplicatedObject);
    }

    public CheckReplicatedDatabaseJob(FailoverGroup failoverGroup,
            Database remoteDatabase, List<Table> remoteTables, boolean isReplicatedObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTables = remoteTables;
        this.isReplicatedObject = isReplicatedObject;
    }

    @Override
    public void execute() {
        Database localDatabase = GlobalStateMgr.getServingState().getDb(remoteDatabase.getFullName());
        if (localDatabase == null) {
            CreateReplicatedDatabaseJob job = new CreateReplicatedDatabaseJob(failoverGroup,
                    remoteDatabase, remoteTables, isReplicatedObject);
            job.start();
            return;
        }

        boolean isTableReplicatedObject = remoteTables != null;
        List<Table> tables = isTableReplicatedObject ? remoteTables : remoteDatabase.getTables();
        for (Table remoteTable : tables) {
            if (!remoteTable.isOlapTable()) {
                LOG.warn("Ignore remote table {}.{} with type {}", remoteDatabase.getFullName(),
                        remoteTable.getName(), remoteTable.getType());
                continue;
            }

            OlapTable remoteOlapTable = (OlapTable) remoteTable;
            CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                    remoteDatabase, remoteOlapTable, localDatabase, isTableReplicatedObject);
            job.start();
        }

        failoverGroup.getObjectMap().putDatabaseMap(remoteDatabase.getId(), localDatabase.getId());

        if (isReplicatedObject) {
            failoverGroup.addReplicatedDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID, localDatabase.getId());

            // Drop deleted tables in database
            for (Table localTable : localDatabase.getTables()) {
                if (remoteDatabase.getTable(localTable.getName()) == null) {
                    DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, null,
                            null, localDatabase, localTable, false, false);
                    job.start();
                }
            }
        }
    }
}
