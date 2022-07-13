// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.analysis.AdminShowReplicaStatusStmt;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.BaseViewStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowUserPropertyStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AlterMaterializedViewStatement;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseGrantRevokeImpersonateStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStatement;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;

public class Analyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzerVisitor().analyze(statement, session);
    }

    private static class AnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
            CreateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            AlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterWorkGroupStatement(AlterWorkGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                               ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowUserPropertyStmt(ShowUserPropertyStmt statement, ConnectContext session) {
            ShowUserPropertyAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitSetUserPropertyStmt(SetUserPropertyStmt statement, ConnectContext session) {
            SetUserPropertyAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitBaseViewStatement(BaseViewStmt statement, ConnectContext session) {
            ViewAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, ConnectContext session) {
            // this phrase do not analyze insertStmt, insertStmt will analyze in
            // StmtExecutor.handleCreateTableAsSelectStmt because planner will not do meta operations
            CTASAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitSubmitTaskStmt(SubmitTaskStmt statement, ConnectContext context) {
            CreateTableAsSelectStmt createTableAsSelectStmt = statement.getCreateTableAsSelectStmt();
            QueryStatement queryStatement = createTableAsSelectStmt.getQueryStatement();
            Analyzer.analyze(queryStatement, context);
            OriginStatement origStmt = statement.getOrigStmt();
            String sqlText = origStmt.originStmt.substring(statement.getSqlBeginIndex());
            statement.setSqlText(sqlText);
            TaskAnalyzer.analyzeSubmitTaskStmt(statement, context);
            return null;
        }

        @Override
        public Void visitCreateWorkGroupStatement(CreateWorkGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            InsertAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowStatement(ShowStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            AdminStmtAnalyzer.analyze(adminSetConfigStmt, session);
            return null;
        }

        @Override
        public Void visitSetStatement(SetStmt stmt, ConnectContext session) {
            stmt.analyze();
            return null;
        }

        @Override
        public Void visitAdminShowConfigStatement(AdminShowConfigStmt adminShowConfigStmt, ConnectContext session) {
            AdminStmtAnalyzer.analyze(adminShowConfigStmt, session);
            return null;
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            DropStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            new QueryAnalyzer(session).analyze(stmt);

            QueryRelation queryRelation = stmt.getQueryRelation();
            long selectLimit = session.getSessionVariable().getSqlSelectLimit();
            if (!queryRelation.hasLimit() && selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
                queryRelation.setLimit(new LimitElement(selectLimit));
            }
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, ConnectContext context) {
            UpdateAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, ConnectContext context) {
            DeleteAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitGrantRevokeImpersonateStatement(BaseGrantRevokeImpersonateStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateMaterializedViewStmt(CreateMaterializedViewStmt statement, ConnectContext context) {
            CreateMaterializedViewStmt.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStatement statement, ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStatement statement,
                                                                ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterSystemStmt(AlterSystemStmt statement, ConnectContext context) {
            AlterSystemStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowCatalogsStmt(ShowCatalogsStmt statement, ConnectContext context) {
            CatalogAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            RefreshTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterDbQuotaStmt(AlterDatabaseQuotaStmt statement, ConnectContext context) {
            AlterDbQuotaAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext context) {
            CreateDbAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            DropStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterDatabaseRename(AlterDatabaseRename statement, ConnectContext context) {
            AlterDatabaseRenameAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRecoverDbStmt(RecoverDbStmt statement, ConnectContext context) {
            RecoverDbAnalyzer.analyze(statement, context);
            return null;
        }

        public Void visitRecoverTableStatement(RecoverTableStmt statement, ConnectContext context) {
            RecoverTableAnalyzer.analyze(statement, context);
            return null;
        }

        // ------------------------------------------- Analyze Statement ---------------------------------------------------

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }
    }
}
