// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.SetType;
<<<<<<< HEAD
=======
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
>>>>>>> ba98870f6 ([Feature] suport SHOW AUTHENTICATION (#9996))
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
<<<<<<< HEAD
import com.starrocks.cluster.ClusterNamespace;
=======
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
>>>>>>> ba98870f6 ([Feature] suport SHOW AUTHENTICATION (#9996))
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

        String getFullDatabaseName(String db, ConnectContext session) {
            String catalog = session.getCurrentCatalog();
            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
                if (CatalogMgr.isInternalCatalog(catalog)) {
                    db = ClusterNamespace.getFullName(session.getClusterName(), db);
                }
                if (Strings.isNullOrEmpty(db)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            } else {
                if (CatalogMgr.isInternalCatalog(catalog)) {
                    db = ClusterNamespace.getFullName(session.getClusterName(), db);
                }
            }
            return db;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null) {
                try {
                    user.analyze();
                } catch (AnalysisException e) {
                    SemanticException exception = new SemanticException("failed to show authentication for " + user.toString());
                    exception.initCause(e);
                    throw exception;
                }
            } else if (! statement.isAll()) {
                statement.setUserIdent(context.getCurrentUserIdentity());
            }
            return null;
        }

    }
}
