// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropTableStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropReplicatedTableJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(DropReplicatedTableJob.class);

    private final Database remoteDatabase;
    private final OlapTable remoteTable;
    private final Database localDatabase;
    private final Table localTable;
    private final boolean isIncludeObject;
    private final boolean isDropForce;

    public DropReplicatedTableJob(FailoverGroup failoverGroup, Database remoteDatabase, OlapTable remoteTable,
            Database localDatabase, Table localTable, boolean isIncludeObject, boolean isDropForce) {
        super(failoverGroup);
        this.remoteDatabase = remoteDatabase;
        this.remoteTable = remoteTable;
        this.localDatabase = localDatabase;
        this.localTable = localTable;
        this.isIncludeObject = isIncludeObject;
        this.isDropForce = isDropForce;
    }

    @Override
    public void execute() {
        LOG.info("Droping table {}.{} in failover group {}", localDatabase.getFullName(), localTable.getName(),
                failoverGroup.getName());

        TableName tableName = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                localDatabase.getFullName(), localTable.getName());
        DropTableStmt dropTableStmt = new DropTableStmt(true, tableName, isDropForce);
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception e) {
            LOG.info("Failed to drop table {}.{} in failover group {}, ", localDatabase.getFullName(),
                    localTable.getName(), failoverGroup.getName(), e);
            return;
        }

        if (isIncludeObject) {
            failoverGroup.getIncludeMgr().removeIncludeTable(localTable.getId());
        }

        if (remoteDatabase == null || remoteTable == null) {
            return;
        }

        CheckReplicatedTableJob job = new CheckReplicatedTableJob(failoverGroup,
                remoteDatabase, remoteTable, localDatabase, isIncludeObject);
        job.execute();
    }
}
