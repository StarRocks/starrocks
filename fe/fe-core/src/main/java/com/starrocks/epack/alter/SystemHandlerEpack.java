// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.alter;

import com.google.common.base.Preconditions;
import com.starrocks.alter.SystemHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.epack.sql.ast.CancelDecommissionDiskClause;
import com.starrocks.epack.sql.ast.CancelDisableDiskClause;
import com.starrocks.epack.sql.ast.DecommissionDiskClause;
import com.starrocks.epack.sql.ast.DisableDiskClause;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterClause;

import java.util.List;

public class SystemHandlerEpack extends SystemHandler {

    @Override
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database dummyDb, OlapTable dummyTbl)
            throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        if (alterClause instanceof DecommissionDiskClause) {
            DecommissionDiskClause clause = (DecommissionDiskClause) alterClause;
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .decommissionDisks(clause.getBeHostPort(), clause.getDiskList());
        } else if (alterClause instanceof CancelDecommissionDiskClause) {
            CancelDecommissionDiskClause clause = (CancelDecommissionDiskClause) alterClause;
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .cancelDecommissionDisks(clause.getBeHostPort(), clause.getDiskList());
        } else if (alterClause instanceof DisableDiskClause) {
            DisableDiskClause clause = (DisableDiskClause) alterClause;
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .disableDisks(clause.getBeHostPort(), clause.getDiskList());
        } else if (alterClause instanceof CancelDisableDiskClause) {
            CancelDisableDiskClause clause = (CancelDisableDiskClause) alterClause;
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .cancelDisableDisks(clause.getBeHostPort(), clause.getDiskList());
        } else {
            return super.process(alterClauses, dummyDb, dummyTbl);
        }
        return null;
    }
}
