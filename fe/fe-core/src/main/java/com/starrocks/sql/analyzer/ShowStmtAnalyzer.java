// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.ShowCreateTableStmt;
import com.starrocks.analysis.ShowDataStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowDeleteStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;

public class ShowStmtAnalyzer {

    public static void analyze(ShowStmt stmt, ConnectContext session) {
        new ShowStmtAnalyzerVisitor().visit(stmt, session);
    }

    static class ShowStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowTableStmt(ShowTableStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowVariablesStmt(ShowVariablesStmt node, ConnectContext context) {
            if (node.getType() == null) {
                node.setType(SetType.DEFAULT);
            }
            return null;
        }

        @Override
        public Void visitShowColumnStmt(ShowColumnStmt node, ConnectContext context) {
            node.init();
            String db = node.getTableName().getDb();
            db = getFullDatabaseName(db, context);
            node.getTableName().setDb(db);
            return null;
        }

        @Override
        public Void visitShowTableStatusStmt(ShowTableStatusStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStmt(ShowMaterializedViewStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowCreateTableStmt(ShowCreateTableStmt node, ConnectContext context) {
            if (node.getTbl() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            node.getTbl().normalization(context);
            return null;
        }

        @Override
        public Void visitShowDatabasesStmt(ShowDbStmt node, ConnectContext context) {
            String catalogName;
            if (node.getCatalogName() != null) {
                catalogName = node.getCatalogName();
            } else {
                catalogName = context.getCurrentCatalog();
            }
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
            return null;
        }

        @Override
        public Void visitShowDeleteStmt(ShowDeleteStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getFullDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }

        String getFullDatabaseName(String db, ConnectContext session) {
            String catalog = session.getCurrentCatalog();
            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
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
            return db;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt node, ConnectContext context) {
            String dbName = node.getDb();
            dbName = getFullDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowDataStmt(ShowDataStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getFullDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }
    }
}
