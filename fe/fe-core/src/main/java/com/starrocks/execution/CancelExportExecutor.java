// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class CancelExportExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws UserException {
        context.getGlobalStateMgr().getExportMgr().cancelExportJob((CancelExportStmt) stmt);
        return null;
    }
}

