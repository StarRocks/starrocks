// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.catalog.Catalog;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;

import java.util.List;

public class RefreshExternalTableStmt extends DdlStmt {
    private TableRef tableRef;

    public RefreshExternalTableStmt(TableRef tableRef) {
        this.tableRef = tableRef;
    }

    public String getDbName() {
        return tableRef.getName().getDb();
    }

    public String getTableName() {
        return tableRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        if (tableRef.getPartitionNames() != null) {
            return tableRef.getPartitionNames().getPartitionNames();
        } else {
            return null;
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (tableRef == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }

        TableName tableName = tableRef.getName();
        tableName.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "REFRESH TABLE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getTbl());
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
