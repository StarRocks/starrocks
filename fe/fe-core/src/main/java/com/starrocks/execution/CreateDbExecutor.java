// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.CreateExistException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateDbExecutor implements DataDefinitionExecutor {
    private static final Logger LOG = LogManager.getLogger(CreateDbExecutor.class);

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        CreateDbStmt createDbStmt = (CreateDbStmt) stmt;
        String clusterName = createDbStmt.getClusterName();
        String fullDbName = createDbStmt.getFullDbName();
        boolean isSetIfNotExists = createDbStmt.isSetIfNotExists();
        try {
            context.getGlobalStateMgr().getLocalMetastore().createDb(clusterName, fullDbName);
        } catch (CreateExistException e) {
            if (isSetIfNotExists) {
                LOG.info("create database[{}] which already exists", fullDbName);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
            }
        }
        return null;
    }
}
