// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.catalog.Database;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcService;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;

public class ShowAlterStmtAnalyzer {
    public static void analyze(ShowAlterStmt statement, ConnectContext context) {
        String dbName = statement.getDbName();
        String catalog = context.getCurrentCatalog();
        if (CatalogMgr.isInternalCatalog(catalog)) {
            dbName = ClusterNamespace.getFullName(dbName);
        }
        statement.setDbName(dbName);
        ShowAlterStmt.AlterType type = statement.getType();

        Database db = context.getGlobalStateMgr().getDb(dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // build proc path
        StringBuilder sb = new StringBuilder();
        sb.append("/jobs/");
        sb.append(db.getId());
        if (type == ShowAlterStmt.AlterType.COLUMN) {
            sb.append("/schema_change");
        } else if (type == ShowAlterStmt.AlterType.ROLLUP || type == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            sb.append("/rollup");
        }

        // create show proc stmt
        // '/jobs/db_name/rollup|schema_change/
        ProcNodeInterface node = null;
        try {
            node = ProcService.getInstance().open(sb.toString());
        } catch (AnalysisException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_PROC_PATH, sb.toString());
        }
        statement.setNode(node);
    }
}
