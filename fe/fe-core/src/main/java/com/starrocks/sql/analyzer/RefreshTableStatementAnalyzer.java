// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.RefreshTableStatement;

public class RefreshTableStatementAnalyzer {
    public static void analyze(RefreshTableStatement statement, ConnectContext context) {
        TableName tableName = statement.getTbl();
        if (tableName == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
        }

        if (!GlobalStateMgr.getCurrentState().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "REFRESH TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getTbl());
        }
    }
}
