// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.parser.NodePosition;
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

        QualifiedName qualifiedName = QualifiedName.of(Lists.newArrayList(
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, localDatabase.getFullName(), localTable.getName()));
        TableRef tableRef = new TableRef(qualifiedName, null, NodePosition.ZERO);
        DropTableStmt dropTableStmt = new DropTableStmt(true, tableRef, isDropForce);
        try {
            GlobalStateMgr.getServingState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception e) {
            failoverGroup.addErrorMessage("Failed to drop table " + localDatabase.getFullName() + "." +
                    localTable.getName() + ", error: " + e.getMessage());
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
