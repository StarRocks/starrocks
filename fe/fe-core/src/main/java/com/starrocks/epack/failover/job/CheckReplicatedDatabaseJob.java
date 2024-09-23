// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CheckReplicatedDatabaseJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CheckReplicatedDatabaseJob.class);

    private final Database remoteDatabase;
    private final List<Table> remoteTables; // Null for whole database
    private final boolean isIncludeObject;

    public CheckReplicatedDatabaseJob(FailoverGroup failoverGroup, Database remoteDatabase,
            boolean isIncludeObject) {
        this(failoverGroup, remoteDatabase, null, isIncludeObject);
    }

    public CheckReplicatedDatabaseJob(FailoverGroup failoverGroup,
            Database remoteDatabase, List<Table> remoteTables, boolean isIncludeObject) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTables = remoteTables;
        this.isIncludeObject = isIncludeObject;
    }

    @Override
    public void execute() {
        Database localDatabase = GlobalStateMgr.getServingState().getLocalMetastore()
                .getDb(remoteDatabase.getFullName());
        if (localDatabase == null) {
            CreateReplicatedDatabaseJob job = new CreateReplicatedDatabaseJob(failoverGroup,
                    remoteDatabase, remoteTables, isIncludeObject);
            job.start();
            return;
        }

        boolean isTableIncludeObject = remoteTables != null;
        List<Table> tables = isTableIncludeObject ? remoteTables : remoteDatabase.getTables();
        for (Table remoteTable : tables) {
            if (failoverGroup.getExcludeMgr().isExcludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    remoteDatabase.getFullName(), remoteTable.getName())) {
                LOG.warn("Ignore remote exclude table {}.{}", remoteDatabase.getFullName(), remoteTable.getName());
                continue;
            }

            if (!remoteTable.isOlapTable()) {
                LOG.warn("Ignore remote table {}.{} with type {}", remoteDatabase.getFullName(),
                        remoteTable.getName(), remoteTable.getType());
                continue;
            }

            OlapTable remoteOlapTable = (OlapTable) remoteTable;
            CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                    remoteDatabase, remoteOlapTable, localDatabase, isTableIncludeObject);
            job.start();
        }

        failoverGroup.getObjectMap().putDatabaseMap(remoteDatabase.getId(), localDatabase.getId());

        if (isIncludeObject) {
            failoverGroup.getIncludeMgr().addIncludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID,
                    localDatabase.getId());
        }

        if (isTableIncludeObject) {
            return;
        }

        if (!Config.failover_group_allow_drop_extra_table) {
            return;
        }

        // Drop extra tables in database
        for (Table localTable : localDatabase.getTables()) {
            if (failoverGroup.getExcludeMgr().isExcludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    localDatabase.getFullName(), localTable.getName())) {
                continue;
            }
            if (remoteDatabase.getTable(localTable.getName()) == null) {
                DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, null,
                        null, localDatabase, localTable, false, false);
                job.start();
            }
        }
    }
}
