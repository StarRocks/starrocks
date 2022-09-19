// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRename;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.BackupStmt;
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
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.TruncateTableStmt;

public class DataDefinitionExecutorFactory {
    private static final ImmutableMap<Class<? extends StatementBase>, DataDefinitionExecutor> EXECUTOR_MAP =
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
                    .put(AlterMaterializedViewStmt.class, new AlterMaterializedViewExecutor())
                    .put(RefreshMaterializedViewStatement.class, new RefreshMaterializedViewExecutor())
                    .put(CancelRefreshMaterializedViewStmt.class, new CancelRefreshMaterializedViewExecutor())
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
                    .put(GrantPrivilegeStmt.class, new GrantExecutor())
                    .put(RevokePrivilegeStmt.class, new RevokeExecutor())
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
                    .build();

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        DataDefinitionExecutor executor = EXECUTOR_MAP.get(stmt.getClass());
        if (executor != null) {
            return executor.execute(stmt, context);
        } else {
            throw new DdlException("Unknown statement.");
        }
    }
}
