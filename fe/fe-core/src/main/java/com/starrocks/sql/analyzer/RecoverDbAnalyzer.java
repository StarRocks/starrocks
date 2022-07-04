// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;

public class RecoverDbAnalyzer {
    public static void analyze(RecoverDbStmt statement, ConnectContext context) {
        String dbName = statement.getDbName();
        String catalog = context.getCurrentCatalog();
        if (CatalogMgr.isInternalCatalog(catalog)) {
            dbName = ClusterNamespace.getFullName(dbName);
        }
        statement.setDbName(dbName);
    }
}
