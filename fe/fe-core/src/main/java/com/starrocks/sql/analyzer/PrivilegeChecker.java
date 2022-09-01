// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;

public class PrivilegeChecker {
    public static void check(StatementBase statement, ConnectContext session) {
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    public static boolean checkTblPriv(ConnectContext context,
                                       TableName tableName,
                                       PrivPredicate predicate) {
        return checkTblPriv(context, tableName.getCatalog(),
                tableName.getDb(), tableName.getTbl(), predicate);
    }

    public static boolean checkTblPriv(ConnectContext context,
                                       String catalogName,
                                       String dbName,
                                       String tableName,
                                       PrivPredicate predicate) {
        return !CatalogMgr.isInternalCatalog(catalogName) ||
                GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(
                        context, dbName, tableName, predicate);
    }

    public static boolean checkDbPriv(ConnectContext context,
                                       String catalogName,
                                       String dbName,
                                       PrivPredicate predicate) {
        return !CatalogMgr.isInternalCatalog(catalogName) ||
                GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(context, dbName, predicate);
    }

    private static class PrivilegeCheckerVisitor extends AstVisitor<Void, ConnectContext> {
        public void check(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext session) {
            String dbName = statement.getDbTbl().getDb();
            String tableName = statement.getDbTbl().getTbl();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(session, dbName, tableName, PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "Alter");
            }
            return null;
        }

        @Override
        public Void visitAlterWorkGroupStatement(AlterWorkGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt statement, ConnectContext session) {
            // check auth
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext session) {
            // check auth
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
            }
            return null;
        }

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }

            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitCreateWorkGroupStatement(CreateWorkGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "CREATE RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTbl(), PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitDropWorkGroupStatement(DropWorkGroupStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(session, PrivPredicate.ADMIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP RESOURCE_GROUP");
            }
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
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
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, session.getQualifiedUser(), db);
            }
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, ConnectContext session) {
            if (!checkTblPriv(session, statement.getTableName(), PrivPredicate.CREATE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
            }
            check(statement.getQueryStatement(), session);
            return null;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, ConnectContext session) {
            if (!checkTblPriv(ConnectContext.get(), statement.getDbMvName(), PrivPredicate.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAlterSystemStmt(AlterSystemStmt statement, ConnectContext session) {
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        session.getQualifiedUser());
            }
            return null;

        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, ConnectContext session) {
            // check if current user has GRANT priv on GLOBAL level.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(
                    ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt statement, ConnectContext session) {
            // check if current user has GRANT priv on GLOBAL level.
            if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(
                    ConnectContext.get(), PrivPredicate.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            // check if current user has IMPERSONATE priv
            if (!GlobalStateMgr.getCurrentState().getAuth().canImpersonate(
                    session.getCurrentUserIdentity(), stmt.getToUser())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "IMPERSONATE");
            }
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStmt(ShowMaterializedViewStmt statement, ConnectContext session) {
            String db = statement.getDb();
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(session, db, PrivPredicate.SHOW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED, "SHOW MATERIALIZED VIEW",
                        session.getQualifiedUser(),
                        session.getRemoteIP(),
                        db);
            }
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt statement, ConnectContext session) {
            // For now, the `update` operation requires the `LOAD` privilege.
            // TODO We're planning to refactor the whole privilege framework to align with mainstream databases such as
            //      MySQL by fine-grained administrative permissions.
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt statement, ConnectContext session) {
            // For now, the `delete` operation requires the `LOAD` privilege.
            // TODO We're planning to refactor the whole privilege framework to align with mainstream databases such as
            //      MySQL by fine-grained administrative permissions.
            TableName tableName = statement.getTableName();
            if (!checkTblPriv(session, tableName, PrivPredicate.LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        session.getQualifiedUser(), session.getRemoteIP(), tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableName);
            if (!checkTblPriv(ConnectContext.get(), tableName.getCatalog(),
                    tableName.getDb(), tableName.getTbl(), PrivPredicate.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                        "REFRESH EXTERNAL TABLE",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        tableName.getTbl());
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            if (statement.isAll() || ! context.getCurrentUserIdentity().equals(statement.getUserIdent())) {
                if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.GRANT)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
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
