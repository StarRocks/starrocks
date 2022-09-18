// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.catalog.Database;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.Constants;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseRename;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterUserStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstVisitor;
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
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RevokePrivilegeStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SyncStmt;
import com.starrocks.sql.ast.TruncateTableStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataDefinitionExecutorFactory {

    static class StmtExecutorVisitor extends AstVisitor<ShowResultSet, ConnectContext> {

        private static final Logger LOG = LogManager.getLogger(StmtExecutorVisitor.class);

        private static final StmtExecutorVisitor INSTANCE = new StmtExecutorVisitor();

        public static StmtExecutorVisitor getInstance() {
            return INSTANCE;
        }

        @Override
        public ShowResultSet visitCreateDbStatement(CreateDbStmt stmt, ConnectContext context) {
            String fullDbName = stmt.getFullDbName();
            boolean isSetIfNotExists = stmt.isSetIfNotExists();
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().getMetadata().createDb(fullDbName);
                } catch (AlreadyExistsException e) {
                    if (isSetIfNotExists) {
                        LOG.info("create database[{}] which already exists", fullDbName);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropDbStatement(DropDbStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                String dbName = stmt.getDbName();
                boolean isForceDrop = stmt.isForceDrop();
                try {
                    context.getGlobalStateMgr().getMetadata().dropDb(dbName, isForceDrop);
                } catch (MetaNotFoundException e) {
                    if (stmt.isSetIfExists()) {
                        LOG.info("drop database[{}] which does not exist", dbName);
                    } else {
                        ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                    }
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateFunctionStmt(CreateFunctionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                FunctionName name = stmt.getFunctionName();
                Database db = context.getGlobalStateMgr().getDb(name.getDb());
                if (db == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
                }
                db.addFunction(stmt.getFunction());
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropFunctionStmt(DropFunctionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {

                FunctionName name = stmt.getFunctionName();
                Database db = context.getGlobalStateMgr().getDb(name.getDb());
                if (db == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, name.getDb());
                }
                db.dropFunction(stmt.getFunction());
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTableStatement(CreateTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().createTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateTableLikeStatement(CreateTableLikeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().createTableLike(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropTableStmt(DropTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().dropTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateMaterializedViewStmt(CreateMaterializedViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().createMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateMaterializedViewStatement(CreateMaterializedViewStatement stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().createMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().dropMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterMaterializedViewStatement(AlterMaterializedViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().alterMaterializedView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement stmt,
                                                                   ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore()
                        .refreshMaterializedView(stmt.getMvName().getDb(),
                                stmt.getMvName().getTbl(),
                                Constants.TaskRunPriority.NORMAL.value());
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt stmt,
                                                                         ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLocalMetastore()
                        .cancelRefreshMaterializedView(
                                stmt.getMvName().getDb(),
                                stmt.getMvName().getTbl());
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterTableStatement(AlterTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterViewStatement(AlterViewStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterTableStatement(CancelAlterTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitLoadStmt(LoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCancelLoadStmt(CancelLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoutineLoadStatement(CreateRoutineLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitPauseRoutineLoadStatement(PauseRoutineLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitStopRoutineLoadStatement(StopRoutineLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterRoutineLoadStmt(AlterRoutineLoadStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateUserStatement(CreateUserStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterUserStatement(AlterUserStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropUserStatement(DropUserStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRevokeRoleStatement(RevokeRoleStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitGrantRoleStatement(GrantRoleStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitGrantPrivilegeStatement(GrantPrivilegeStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRevokePrivilegeStatement(RevokePrivilegeStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropRoleStatement(DropRoleStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitSetUserPropertyStmt(SetUserPropertyStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterSystemStmt(AlterSystemStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterSystemStmt(CancelAlterSystemStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterDatabaseRename(AlterDatabaseRename stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRecoverDbStmt(RecoverDbStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRecoverTableStatement(RecoverTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRecoverPartitionStmt(RecoverPartitionStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateViewStatement(CreateViewStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitBackupStmt(BackupStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRestoreStmt(RestoreStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCancelBackupStmt(CancelBackupStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateRepositoryStmt(CreateRepositoryStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitSyncStmt(SyncStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitTruncateTableStatement(TruncateTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAdminRepairTableStatement(AdminRepairTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetConfigStatement(AdminSetConfigStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateFileStatement(CreateFileStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropFileStatement(DropFileStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitInstallPluginStatement(InstallPluginStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitUninstallPluginStatement(UninstallPluginStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAdminCheckTabletsStatement(AdminCheckTabletsStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceStatement(CreateResourceStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceStatement(DropResourceStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceStatement(AlterResourceStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCancelExportStatement(CancelExportStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitRefreshTableStatement(RefreshTableStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceGroupStatement(CreateResourceGroupStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceGroupStatement(DropResourceGroupStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceGroupStatement(AlterResourceGroupStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitCreateCatalogStatement(CreateCatalogStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitDropCatalogStatement(DropCatalogStmt stmt, ConnectContext context) {
            return null;
        }

        @Override
        public ShowResultSet visitSubmitTaskStmt(SubmitTaskStmt stmt, ConnectContext context) {
            return null;
        }

    }

    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        try {
            return stmt.accept(StmtExecutorVisitor.getInstance(), context);
        } catch (RuntimeException re) {
            if (re.getCause() instanceof DdlException) {
                throw (DdlException) re.getCause();
            } else {
                throw new DdlException(re.getCause().getMessage());
            }
        }
    }
}
