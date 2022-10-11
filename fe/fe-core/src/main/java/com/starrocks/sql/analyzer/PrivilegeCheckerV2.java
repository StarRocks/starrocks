// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeTypes;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;

import java.util.Arrays;

public class PrivilegeCheckerV2 {

    private PrivilegeCheckerV2() {}

    public static void check(StatementBase statement, ConnectContext session) {
        new PrivilegeCheckerVisitor().check(statement, session);
    }

    public static void checkTableAction(ConnectContext context,
                                        TableName tableName,
                                        PrivilegeTypes.TableActions action) {
        if (!CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
            throw new SemanticException("external catalog is not supported for now!");
        }
        String actionStr = action.toString();
        if (!context.getGlobalStateMgr().getPrivilegeManager().check(
                context,
                PrivilegeTypes.TABLE.toString(),
                actionStr,
                Arrays.asList(tableName.getDb(), tableName.getTbl()))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkDbAction(ConnectContext context, TableName tableName, PrivilegeTypes.DbActions action) {
        if (!CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
            throw new SemanticException("external catalog is not supported for now!");
        }
        String db = tableName.getDb();
        if (!context.getGlobalStateMgr().getPrivilegeManager().check(
                context,
                PrivilegeTypes.DATABASE.toString(),
                action.toString(),
                Arrays.asList(db))) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), db);
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
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext session) {
            checkDbAction(session, statement.getDbTbl(), PrivilegeTypes.DbActions.CREATE_TABLE);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt statement, ConnectContext session) {
            checkTableAction(session, statement.getTableName(), PrivilegeTypes.TableActions.DELETE);
            return null;
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            checkDbAction(session, statement.getTbl(), PrivilegeTypes.DbActions.DROP);
            return null;
        }
        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            checkTableAction(session, statement.getTableName(), PrivilegeTypes.TableActions.INSERT);
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
                // TODO check select for view
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
                checkTableAction(session, node.getName(), PrivilegeTypes.TableActions.SELECT);
                return null;
            }
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            PrivilegeManager privilegeManager = session.getGlobalStateMgr().getPrivilegeManager();
            if (!privilegeManager.allowGrant(session, stmt.getTypeId(), stmt.getActionList(), stmt.getObjectList())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }
    }
}
