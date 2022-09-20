// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Database;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.DropFunctionStmt;

public class DropFunctionExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws UserException {
        DropFunctionStmt dropFunctionStmt = (DropFunctionStmt) stmt;
        FunctionName name = dropFunctionStmt.getFunctionName();
        Database db = context.getGlobalStateMgr().getDb(name.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
        }
        db.dropFunction(dropFunctionStmt.getFunction());
        return null;
    }
}
