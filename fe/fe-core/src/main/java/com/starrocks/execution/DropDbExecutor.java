// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.DropDbStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DropDbExecutor implements DataDefinitionExecutor {
    private static final Logger LOG = LogManager.getLogger(DropDbExecutor.class);

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        DropDbStmt dropDbStmt = (DropDbStmt) stmt;
        String dbName = dropDbStmt.getDbName();
        boolean isForceDrop = dropDbStmt.isForceDrop();
        try {
            context.getGlobalStateMgr().getMetadata().dropDb(dbName, isForceDrop);
        } catch (MetaNotFoundException e) {
            if (dropDbStmt.isSetIfExists()) {
                LOG.info("drop database[{}] which does not exist", dbName);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
        }
        return null;
    }
}
