// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AddSqlBlackListStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.ShowGrantsStmt;
import com.starrocks.analysis.ShowRestoreStmt;
import com.starrocks.analysis.ShowSnapshotStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRename;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.BaseViewStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UpdateStmt;

public class Analyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzerVisitor().analyze(statement, session);
    }

    private static class AnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        // ---------------------------------------- Database Statement -----------------------------------------------------


        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
            CreateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateTableLikeStatement(CreateTableLikeStmt statement, ConnectContext context) {
            CreateTableLikeAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            AlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelAlterTableStatement(CancelAlterTableStmt statement, ConnectContext context) {
            CancelAlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, ConnectContext session) {
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
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, ConnectContext session) {
            AdminStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext session) {
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
        public Void visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowResourceStatement(ShowResourcesStmt stmt, ConnectContext session) {
            ResourceAnalyzer.analyze(stmt, session);
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
        public Void visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt adminSetConfigStmt, ConnectContext session) {
            AdminStmtAnalyzer.analyze(adminSetConfigStmt, session);
            return null;
        }

        @Override
        public Void visitSetStatement(SetStmt setStmt, ConnectContext session) {
            SetStmtAnalyzer.analyze(setStmt, session);
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
        public Void visitCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt stmt, ConnectContext session) {
            PrivilegeStmtAnalyzer.analyze(stmt, session);
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            ShowStmtAnalyzer.analyze(statement, context);
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
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement,
                                                        ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            MaterializedViewAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
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
        public Void visitDropFunction(DropFunctionStmt statement, ConnectContext context) {
            DropStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateFunction(CreateFunctionStmt statement, ConnectContext context) {
            try {
                statement.analyze(context);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
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
            return null;
        }

        @Override
        public Void visitRecoverTableStatement(RecoverTableStmt statement, ConnectContext context) {
            RecoverTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext context) {
            TruncateTableAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRecoverPartitionStmt(RecoverPartitionStmt statement, ConnectContext context) {
            RecoverPartitionAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext session) {
            CreateRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext session) {
            StopRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext session) {
            ResumeRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext session) {
            PauseRoutineLoadAnalyzer.analyze(statement, session);
            return null;
        }

        // ------------------------------------------- Analyze Statement ---------------------------------------------------

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            AnalyzeStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitLoadStmt(LoadStmt statement, ConnectContext context) {
            LoadStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitCancelLoadStmt(CancelLoadStmt statement, ConnectContext context) {
            CancelLoadStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitBackupStmt(BackupStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitRestoreStmt(RestoreStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowBackupStmt(ShowBackupStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowRestoreStmt(ShowRestoreStmt statement, ConnectContext context) {
            BackupRestoreAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowSnapshotStmt(ShowSnapshotStmt statement, ConnectContext context) {
            ShowSnapshotAnalyzer.analyze(statement, context);
            return null;
        }
    }
}
