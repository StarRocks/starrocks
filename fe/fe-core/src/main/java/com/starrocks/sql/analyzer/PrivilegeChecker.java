// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;

public class PrivilegeChecker {
    public static void check(StatementBase statement, ConnectContext session) {
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    public static boolean checkTblPriv(ConnectContext context,
                                       TableName tableName,
                                       PrivPredicate predicate) {
        return checkTblPriv(context, tableName.getDb(), tableName.getTbl(), predicate);
    }
    public static boolean checkTblPriv(ConnectContext context,
                                       String dbName,
                                       String tableName,
                                       PrivPredicate predicate) {
        return context.getCatalog().getAuth().checkTblPriv(
                        context, dbName, tableName, predicate);
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
            new TablePrivilegeChecker(session).visit(stmt);
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

    private static class TablePrivilegeChecker extends AstVisitor<Void, Void> {
        private ConnectContext session;
        public TablePrivilegeChecker(ConnectContext session) {
            this.session = session;
        }
        @Override
        public Void visitQueryStatement(QueryStatement node, Void context) {
            return visit(node.getQueryRelation());
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            // if user has select privilege for the view, then there's no need to check base table
            if (checkTblPriv(session, node.getName(), PrivPredicate.SELECT)) {
                return null;
            }
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }

            return visit(node.getRelation());
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getRelations().forEach(this::visit);
            }
            node.getRelations().forEach(this::visit);
            return null;
        }

        @Override
        public Void visitJoin(JoinRelation node, Void context) {
            visit(node.getLeft());
            visit(node.getRight());
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            return visit(node.getCteQueryStatement());
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (!checkTblPriv(session, node.getName(), PrivPredicate.SELECT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                        session.getQualifiedUser(), session.getRemoteIP(), node.getTable());
            }
            return null;
        }
    }

}
