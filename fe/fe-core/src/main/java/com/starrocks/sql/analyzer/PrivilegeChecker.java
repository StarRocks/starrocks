// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;

import java.util.Map;

public class PrivilegeChecker {
    public static void check(StatementBase statement, ConnectContext session) {
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    private static class PrivilegeCheckerVisitor extends AstVisitor<Void, ConnectContext> {
        public void check(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAlterWorkGroupStatement(AlterWorkGroupStmt statement, ConnectContext session) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(session, tableName.getDb(),
                    tableName.getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(session, tableName.getDb(),
                    tableName.getTbl(), PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }

            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitCreateWorkGroupStatement(CreateWorkGroupStmt statement, ConnectContext session) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE RESOURCE_GROUP");
            }
            return null;
        }
        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            String dbName = statement.getDbName();
            String tableName = statement.getTableName();
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(session, dbName, tableName, PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }
        @Override
        public Void visitDropWorkGroupStatement(DropWorkGroupStmt statement, ConnectContext session) {
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(session, tableName.getDb(),
                    tableName.getTbl(), PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            Map<TableName, Table> tables = AnalyzerUtils.collectAllTable(stmt);
            for (Map.Entry<TableName, Table> table : tables.entrySet()) {
                TableName tableName = table.getKey();
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(session, tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.SELECT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                            session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
                }
            }
            return null;
        }

        @Override
        public Void visitShowTableStatusStmt(ShowTableStatusStmt statement, ConnectContext session) {
            String db = statement.getDb();
            if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }
    }
}
