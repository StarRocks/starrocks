// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.execution;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AdminCancelRepairTableStmt;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminRepairTableStmt;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterLoadStmt;
import com.starrocks.analysis.AlterResourceStmt;
import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CancelLoadStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.RevokeStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.SyncStmt;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AlterMaterializedViewStatement;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStatement;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;

public class DataDefinitionExecutorFactory {
    private static final ImmutableMap<Class<? extends StatementBase>, DataDefinitionExecutor> executorMap =
            new ImmutableMap.Builder<Class<? extends StatementBase>, DataDefinitionExecutor>()
                    .put(CreateDbStmt.class, new CreateDbExecutor())
                    .put(DropDbStmt.class, new DropDbExecutor())
                    .put(CreateFunctionStmt.class, new CreateFunctionExecutor())
                    .put(DropFunctionStmt.class, new DropFunctionExecutor())
                    .put(CreateTableStmt.class, new CreateTableExecutor())
                    .put(CreateTableLikeStmt.class, new CreateTableLikeExecutor())
                    .put(DropTableStmt.class, new DropTableExecutor())
                    .put(CreateMaterializedViewStmt.class, new CreateMaterializedViewExecutor())
                    .put(CreateMaterializedViewStatement.class, new CreateMaterializedViewStatementExecutor())
                    .put(DropMaterializedViewStmt.class, new DropMaterializedViewExecutor())
                    .put(AlterMaterializedViewStatement.class, new AlterMaterializedViewExecutor())
                    .put(RefreshMaterializedViewStatement.class, new RefreshMaterializedViewExecutor())
                    .put(CancelRefreshMaterializedViewStatement.class, new CancelRefreshMaterializedViewExecutor())
                    .put(AlterTableStmt.class, new AlterTableExecutor())
                    .put(AlterViewStmt.class, new AlterViewExecutor())
                    .put(CancelAlterTableStmt.class, new CancelAlterTableExecutor())
                    .put(LoadStmt.class, new LoadExecutor())
                    .put(CancelLoadStmt.class, new CancelLoadExecutor())
                    .put(CreateRoutineLoadStmt.class, new CreateRoutineLoadExecutor())
                    .put(PauseRoutineLoadStmt.class, new PauseRoutineLoadExecutor())
                    .put(ResumeRoutineLoadStmt.class, new ResumeRoutineLoadExecutor())
                    .put(StopRoutineLoadStmt.class, new StopRoutineLoadExecutor())
                    .put(AlterRoutineLoadStmt.class, new AlterRoutineLoadExecutor())
                    .put(CreateUserStmt.class, new CreateUserExecutor())
                    .put(AlterUserStmt.class, new AlterUserExecutor())
                    .put(DropUserStmt.class, new DropUserExecutor())
                    .put(RevokeRoleStmt.class, new RevokeRoleExecutor())
                    .put(GrantRoleStmt.class, new GrantRoleExecutor())
                    .put(GrantStmt.class, new GrantExecutor())
                    .put(GrantImpersonateStmt.class, new GrantImpersonateExecutor())
                    .put(RevokeStmt.class, new RevokeExecutor())
                    .put(RevokeImpersonateStmt.class, new RevokeImpersonateExecutor())
                    .put(CreateRoleStmt.class, new CreateRoleExecutor())
                    .put(DropRoleStmt.class, new DropRoleExecutor())
                    .put(SetUserPropertyStmt.class, new SetUserPropertyExecutor())
                    .put(AlterSystemStmt.class, new AlterSystemExecutor())
                    .put(CancelAlterSystemStmt.class, new CancelAlterSystemExecutor())
                    .put(AlterDatabaseQuotaStmt.class, new AlterDatabaseQuotaExecutor())
                    .put(AlterDatabaseRename.class, new AlterDatabaseRenameExecutor())
                    .put(RecoverDbStmt.class, new RecoverDbExecutor())
                    .put(RecoverTableStmt.class, new RecoverTableExecutor())
                    .put(RecoverPartitionStmt.class, new RecoverPartitionExecutor())
                    .put(CreateViewStmt.class, new CreateViewExecutor())
                    .put(BackupStmt.class, new BackupExecutor())
                    .put(RestoreStmt.class, new RestoreExecutor())
                    .put(CancelBackupStmt.class, new CancelBackupExecutor())
                    .put(CreateRepositoryStmt.class, new CreateRepositoryExecutor())
                    .put(DropRepositoryStmt.class, new DropRepositoryExecutor())
                    .put(SyncStmt.class, new SyncExecutor())
                    .put(TruncateTableStmt.class, new TruncateTableExecutor())
                    .put(AdminRepairTableStmt.class, new AdminRepairTableExecutor())
                    .put(AdminCancelRepairTableStmt.class, new AdminCancelRepairTableExecutor())
                    .put(AdminSetConfigStmt.class, new AdminSetConfigExecutor())
                    .put(CreateFileStmt.class, new CreateFileExecutor())
                    .put(DropFileStmt.class, new DropFileExecutor())
                    .put(InstallPluginStmt.class, new InstallPluginExecutor())
                    .put(UninstallPluginStmt.class, new UninstallPluginExecutor())
                    .put(AdminCheckTabletsStmt.class, new AdminCheckTabletsExecutor())
                    .put(AdminSetReplicaStatusStmt.class, new AdminSetReplicaStatusExecutor())
                    .put(CreateResourceStmt.class, new CreateResourceExecutor())
                    .put(DropResourceStmt.class, new DropResourceExecutor())
                    .put(AlterResourceStmt.class, new AlterResourceExecutor())
                    .put(CancelExportStmt.class, new CancelExportExecutor())
                    .put(CreateAnalyzeJobStmt.class, new CreateAnalyzeJobExecutor())
                    .put(DropAnalyzeJobStmt.class, new DropAnalyzeJobExecutor())
                    .put(RefreshTableStmt.class, new RefreshTableExecutor())
                    .put(CreateResourceGroupStmt.class, new CreateResourceGroupExecutor())
                    .put(DropResourceGroupStmt.class, new DropResourceGroupExecutor())
                    .put(AlterResourceGroupStmt.class, new AlterResourceGroupExecutor())
                    .put(CreateCatalogStmt.class, new CreateCatalogExecutor())
                    .put(DropCatalogStmt.class, new DropCatalogExecutor())
                    .put(SubmitTaskStmt.class, new SubmitTaskExecutor())
                    .put(AlterLoadStmt.class, new AlterLoadExecutor())
                    .build();

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        DataDefinitionExecutor executor = executorMap.get(stmt.getClass());
        if (executor != null) {
            return executor.execute(stmt, context);
        } else {
            throw new DdlException("Unknown statement.");
        }
    }
}
