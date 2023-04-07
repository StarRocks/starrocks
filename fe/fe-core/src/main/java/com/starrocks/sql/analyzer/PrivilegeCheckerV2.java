// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowPluginsStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.ViewRelation;

public class PrivilegeCheckerV2 {
    private static final String EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG = "external catalog is not supported for now!";

    private PrivilegeCheckerV2() {
    }

    public static void check(StatementBase statement, ConnectContext session) {
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    public static void checkTableAction(ConnectContext context,
                                        TableName tableName,
                                        PrivilegeType.TableAction action) {
        if (!CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }
        String actionStr = action.toString();
        if (!PrivilegeManager.checkTableAction(context, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkDbAction(ConnectContext context, String catalogName, String dbName,
                              PrivilegeType.DbAction action) {
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }
        if (!PrivilegeManager.checkDbAction(context, dbName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName, action.name());
        }
    }

    static void checkAnyActionOnDb(ConnectContext context, String catalogName, String dbName) {
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }

        if (!PrivilegeManager.checkAnyActionOnDb(context, dbName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkAnyActionOnOrUnderDb(ConnectContext context, String catalogName, String dbName) {
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }

        if (!PrivilegeManager.checkAnyActionOnOrUnderDb(context, dbName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType.ViewAction action) {
        if (!CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
            throw new SemanticException("external catalog is not supported for now!");
        }
        String actionStr = action.toString();
        if (!PrivilegeManager.checkViewAction(context, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    /**
     * check privilege by AST tree
     */
    private static class PrivilegeCheckerVisitor extends AstVisitor<Void, ConnectContext> {
        public void check(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt statement, ConnectContext session) {
            checkTableAction(session, statement.getTableName(), PrivilegeType.TableAction.DELETE);
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            checkTableAction(session, statement.getTableName(), PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            new TablePrivilegeChecker(session).visit(stmt);
            return null;
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
                if (PrivilegeManager.checkViewAction(
                        session, node.getName().getDb(), node.getName().getTbl(), PrivilegeType.ViewAction.SELECT)) {
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
                checkTableAction(session, node.getName(), PrivilegeType.TableAction.SELECT);
                return null;
            }
        }

        // --------------------------------- Database Statement ---------------------------------

        @Override
        public Void visitUseDbStatement(UseDbStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, statement.getCatalogName(), statement.getDbName());
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            checkAnyActionOnDb(context, statement.getCatalogName(), statement.getDb());
            return null;
        }

        @Override
        public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.DbAction.DROP);
            // TODO(yiming): check the `CREATE_DATABASE` action on internal catalog after catalog object is added
            return null;
        }

        @Override
        public Void visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.DbAction.ALTER);
            return null;
        }

        @Override
        public Void visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.DbAction.ALTER);
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.DbAction.DROP);
            return null;
        }

        // --------------------------------- External Resource Statement ---------------------------------

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.CREATE_RESOURCE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE_RESOURCE");
            }
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkResourceAction(
                    context, statement.getResourceName(), PrivilegeType.ResourceAction.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkResourceAction(
                    context, statement.getResourceName(), PrivilegeType.ResourceAction.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        // --------------------------------------- Plugin Statement ---------------------------------------

        @Override
        public Void visitInstallPluginStatement(InstallPluginStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        @Override
        public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        @Override
        public Void visitShowPluginsStatement(ShowPluginsStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        // --------------------------------------- File Statement ----------------------------------------------------------

        @Override
        public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            return null;
        }

        // --------------------------------------- Sql BlackList And WhiteList Statement -----------------------------------

        @Override
        public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        // ---------------------------------------- Privilege Statement ----------------------------------------------------

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            PrivilegeManager privilegeManager = session.getGlobalStateMgr().getPrivilegeManager();
            if (!privilegeManager.allowGrant(session, stmt.getTypeId(), stmt.getActionList(), stmt.getObjectList())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null && !user.equals(context.getCurrentUserIdentity())
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitCreateAlterUserStatement(BaseCreateAlterUserStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowRolesStatement(ShowRolesStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null && !user.equals(context.getCurrentUserIdentity())
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowUserPropertyStatement(ShowUserPropertyStmt statement, ConnectContext context) {
            String user = statement.getUser();
            if (user != null && !user.equals(context.getCurrentUserIdentity().getQualifiedUser())
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt statement, ConnectContext context) {
            PrivilegeManager privilegeManager = context.getGlobalStateMgr().getPrivilegeManager();
            if (!privilegeManager.canExecuteAs(context, statement.getToUser())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "IMPERSONATE");
            }
            return null;
        }

        public Void visitSetUserPropertyStatement(SetUserPropertyStmt statement, ConnectContext context) {
            String user = statement.getUser();
            if (user != null && !user.equals(context.getCurrentUserIdentity().getQualifiedUser())
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        // ---------------------------------------- View Statement ---------------------------------------------------------

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext context) {
            // 1. check if user can create view in this db
            TableName tableName = statement.getTableName();
            String catalog = tableName.getCatalog();
            if (catalog == null) {
                catalog = context.getCurrentCatalog();
            }
            checkDbAction(context, catalog, tableName.getDb(), PrivilegeType.DbAction.CREATE_VIEW);
            // 2. check if user can query
            check(statement.getQueryStatement(), context);
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext context) {
            // 1. check if user can alter view in this db
            checkViewAction(context, statement.getTableName(), PrivilegeType.ViewAction.ALTER);
            // 2. check if user can query
            check(statement.getQueryStatement(), context);
            return null;
        }

        // ---------------------------------------- Table Statement --------------------------------------------------------

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext session) {
            TableName tableName = statement.getDbTbl();
            String catalog = tableName.getCatalog();
            if (catalog == null) {
                catalog = session.getCurrentCatalog();
            }
            checkDbAction(session, catalog, tableName.getDb(), PrivilegeType.DbAction.CREATE_TABLE);
            return null;
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext session) {
            if (statement.isView()) {
                checkViewAction(session, statement.getTbl(), PrivilegeType.ViewAction.DROP);
            } else {
                checkTableAction(session, statement.getTbl(), PrivilegeType.TableAction.DROP);
            }
            return null;
        }
    }
}
