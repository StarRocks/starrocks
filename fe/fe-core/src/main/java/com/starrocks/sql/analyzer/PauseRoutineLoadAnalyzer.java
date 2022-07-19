// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;

public class PauseRoutineLoadAnalyzer {
    public static void analyze(PauseRoutineLoadStmt statement, ConnectContext context) {
        String db = statement.getDbFullName();
        String catalog = context.getCurrentCatalog();
        if (Strings.isNullOrEmpty(db)) {
            db = context.getDatabase();
            if (CatalogMgr.isInternalCatalog(catalog)) {
                db = ClusterNamespace.getFullName(db);
            }
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            if (CatalogMgr.isInternalCatalog(catalog)) {
                db = ClusterNamespace.getFullName(db);
            }
        }
        statement.setLabelName(new LabelName(db,  statement.getName()));
    }
}
