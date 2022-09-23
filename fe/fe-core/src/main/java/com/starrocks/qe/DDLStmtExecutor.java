// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.qe;

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
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.catalog.Database;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.load.EtlJobType;
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
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
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
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatsConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDateTime;

public class DDLStmtExecutor {

    /**
     * Execute various ddl statement
     */
    public static ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        try {
            return stmt.accept(StmtExecutorVisitor.getInstance(), context);
        } catch (RuntimeException re) {
            if (re.getCause() instanceof DdlException) {
                throw (DdlException) re.getCause();
            } else if (re.getCause() instanceof IOException) {
                throw (IOException) re.getCause();
            } else if (re.getCause() != null) {
                throw new DdlException(re.getCause().getMessage());
            } else {
                throw re;
            }
        }
    }

    static class StmtExecutorVisitor extends AstVisitor<ShowResultSet, ConnectContext> {

        private static final Logger LOG = LogManager.getLogger(StmtExecutorVisitor.class);

        private static final StmtExecutorVisitor INSTANCE = new StmtExecutorVisitor();

        public static StmtExecutorVisitor getInstance() {
            return INSTANCE;
        }

        @Override
        public ShowResultSet visitNode(ParseNode node, ConnectContext context) {
            throw new RuntimeException(new DdlException("unsupported statement: " + node.toSql()));
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
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().alterTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterViewStatement(AlterViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().alterView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterTableStatement(CancelAlterTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().cancelAlter(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitLoadStmt(LoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                EtlJobType jobType = stmt.getEtlJobType();
                if (jobType == EtlJobType.UNKNOWN) {
                    throw new DdlException("Unknown load job type");
                }
                if (jobType == EtlJobType.HADOOP && Config.disable_hadoop_load) {
                    throw new DdlException("Load job by hadoop cluster is disabled."
                            + " Try using broker load. See 'help broker load;'");
                }

                context.getGlobalStateMgr().getLoadManager().createLoadJobFromStmt(stmt, context);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelLoadStmt(CancelLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getLoadManager().cancelLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoutineLoadStatement(CreateRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadManager().createRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitPauseRoutineLoadStatement(PauseRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadManager().pauseRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadManager().resumeRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitStopRoutineLoadStatement(StopRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadManager().stopRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterRoutineLoadStatement(AlterRoutineLoadStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getRoutineLoadManager().alterRoutineLoadJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateAlterUserStmt(BaseCreateAlterUserStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                if (stmt instanceof CreateUserStmt) {
                    if (context.getGlobalStateMgr().isUsingNewPrivilege()) {
                        context.getGlobalStateMgr().getAuthenticationManager().createUser((CreateUserStmt) stmt);
                    } else {
                        context.getGlobalStateMgr().getAuth().createUser((CreateUserStmt) stmt);
                    }
                } else if (stmt instanceof AlterUserStmt) {
                    context.getGlobalStateMgr().getAuth().alterUser((AlterUserStmt) stmt);
                } else {
                    throw new DdlException("unsupported user stmt: " + stmt.toSql());
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropUserStatement(DropUserStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuth().dropUser(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitGrantRevokeRoleStatement(BaseGrantRevokeRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                if (stmt instanceof GrantRoleStmt) {
                    context.getGlobalStateMgr().getAuth().grantRole((GrantRoleStmt) stmt);
                } else {
                    context.getGlobalStateMgr().getAuth().revokeRole((RevokeRoleStmt) stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                if (stmt instanceof GrantPrivilegeStmt) {
                    context.getGlobalStateMgr().getAuth().grant((GrantPrivilegeStmt) stmt);
                } else {
                    context.getGlobalStateMgr().getAuth().revoke((RevokePrivilegeStmt) stmt);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRoleStatement(CreateRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuth().createRole(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropRoleStatement(DropRoleStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuth().dropRole(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitSetUserPropertyStmt(SetUserPropertyStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getAuth().updateUserProperty(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterSystemStmt(AlterSystemStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().alterCluster(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelAlterSystemStmt(CancelAlterSystemStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().cancelAlterCluster(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterDatabaseRename(AlterDatabaseRename stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().renameDatabase(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverDbStmt(RecoverDbStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().recoverDatabase(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverTableStatement(RecoverTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().recoverTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRecoverPartitionStmt(RecoverPartitionStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().recoverPartition(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateViewStatement(CreateViewStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().createView(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitBackupStmt(BackupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().backup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitRestoreStmt(RestoreStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().restore(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelBackupStmt(CancelBackupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().cancelBackup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateRepositoryStmt(CreateRepositoryStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getBackupHandler().createRepository(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitSyncStmt(SyncStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
            });
            return null;
        }

        @Override
        public ShowResultSet visitTruncateTableStatement(TruncateTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().truncateTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminRepairTableStatement(AdminRepairTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getTabletChecker().repairTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getTabletChecker().cancelRepairTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetConfigStatement(AdminSetConfigStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().setConfig(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateFileStatement(CreateFileStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getSmallFileMgr().createFile(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropFileStatement(DropFileStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getSmallFileMgr().dropFile(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitInstallPluginStatement(InstallPluginStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().installPlugin(stmt);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitUninstallPluginStatement(UninstallPluginStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                try {
                    context.getGlobalStateMgr().uninstallPlugin(stmt);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminCheckTabletsStatement(AdminCheckTabletsStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().checkTablets(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().setReplicaStatus(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceStatement(CreateResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().createResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceStatement(DropResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().dropResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceStatement(AlterResourceStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceMgr().alterResource(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCancelExportStatement(CancelExportStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getExportMgr().cancelExportJob(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                AnalyzeJob analyzeJob = new AnalyzeJob(stmt.getDbId(),
                        stmt.getTableId(),
                        stmt.getColumnNames(),
                        stmt.isSample() ? StatsConstants.AnalyzeType.SAMPLE : StatsConstants.AnalyzeType.FULL,
                        StatsConstants.ScheduleType.SCHEDULE,
                        stmt.getProperties(), StatsConstants.ScheduleStatus.PENDING,
                        LocalDateTime.MIN);

                context.getGlobalStateMgr().getAnalyzeManager().addAnalyzeJob(analyzeJob);

                Thread thread = new Thread(() -> {
                    StatisticExecutor statisticExecutor = new StatisticExecutor();
                    analyzeJob.run(statisticExecutor);
                });
                thread.start();
            });
            return null;
        }

        @Override
        public ShowResultSet visitRefreshTableStatement(RefreshTableStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().refreshExternalTable(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateResourceGroupStatement(CreateResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().createResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropResourceGroupStatement(DropResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().dropResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterResourceGroupStatement(AlterResourceGroupStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getResourceGroupMgr().alterResourceGroup(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitCreateCatalogStatement(CreateCatalogStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getCatalogMgr().createCatalog(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropCatalogStatement(DropCatalogStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                context.getGlobalStateMgr().getCatalogMgr().dropCatalog(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitSubmitTaskStmt(SubmitTaskStmt stmt, ConnectContext context) {
            try {
                return context.getGlobalStateMgr().getTaskManager().handleSubmitTaskStmt(stmt);
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
