// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Sets;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.ExportJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowPluginsStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UseWarehouseStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.StatsConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrivilegeCheckerV2 {
    private PrivilegeCheckerV2() {
    }

    public static void check(StatementBase statement, ConnectContext context) {
        new PrivilegeCheckerVisitor().check(statement, context);
    }

    public static void checkTableAction(ConnectContext context,
                                        TableName tableName,
                                        PrivilegeType action) {
        String catalogName = tableName.getCatalog();
        if (catalogName == null) {
            catalogName = context.getCurrentCatalog();
        }
        if (!PrivilegeActions.checkTableAction(context, catalogName, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    action.toString(), context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    public static void checkTableAction(ConnectContext context,
                                        String dbName, String tableName,
                                        PrivilegeType action) {
        String actionStr = action.toString();
        if (!PrivilegeActions.checkTableAction(context, dbName, tableName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkAnyActionOnTable(ConnectContext context, TableName tableName) {
        String catalogName = tableName.getCatalog();
        if (catalogName == null) {
            catalogName = context.getCurrentCatalog();
        }
        if (!PrivilegeActions.checkAnyActionOnTable(context, catalogName, tableName.getDb(), tableName.getTbl())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_TABLE_DENIED,
                    context.getQualifiedUser(), tableName);
        }
    }

    static void checkAnyActionOnTable(ConnectContext context, String dbName, String tableName) {
        if (!PrivilegeActions.checkAnyActionOnTable(context, dbName, tableName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_TABLE_DENIED,
                    context.getQualifiedUser(), tableName);
        }
    }

    public static void checkMvAction(ConnectContext context,
                                     TableName tableName,
                                     PrivilegeType action) {
        String catalogName = tableName.getCatalog();
        if (catalogName == null) {
            catalogName = context.getCurrentCatalog();
        }
        String actionStr = action.toString();
        if (!PrivilegeActions.checkMaterializedViewAction(context, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_MV_ACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static String getTableNameByRoutineLoadLabel(ConnectContext context,
                                                 String dbName, String labelName) {
        RoutineLoadJob job = null;
        String tableName = null;
        try {
            job = context.getGlobalStateMgr().getRoutineLoadMgr().getJob(dbName, labelName);
        } catch (MetaNotFoundException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ROUTINELODE_JOB_NOT_FOUND, labelName);
        }
        if (null == job) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ROUTINELODE_JOB_NOT_FOUND, labelName);
        }
        try {
            tableName = job.getTableName();
        } catch (MetaNotFoundException e) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_TABLE_NOT_FOUND);
        }
        return tableName;
    }

    static void checkDbAction(ConnectContext context, String catalogName, String dbName,
                              PrivilegeType action) {
        if (!PrivilegeActions.checkDbAction(context, catalogName, dbName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType action) {
        if (!PrivilegeActions.checkCatalogAction(context, catalogName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    context.getQualifiedUser(), catalogName);
        }
    }

    static void checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        if (!PrivilegeActions.checkAnyActionOnCatalog(context, catalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    context.getQualifiedUser(), catalogName);
        }
    }

    static void checkAnyActionOnDb(ConnectContext context, String catalogName, String dbName) {
        if (!PrivilegeActions.checkAnyActionOnDb(context, catalogName, dbName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String dbName) {
        if (!PrivilegeActions.checkAnyActionOnOrInDb(context, catalogName, dbName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType action) {
        String catalogName = tableName.getCatalog();
        if (catalogName == null) {
            catalogName = context.getCurrentCatalog();
        }
        String actionStr = action.toString();
        if (!PrivilegeActions.checkViewAction(context, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkStmtOperatePrivilege(ConnectContext context) {
        if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.OPERATE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "OPERATE");
        }
    }

    static void checkSystemRepository(ConnectContext context) {
        if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.REPOSITORY)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "REPOSITORY");
        }
    }

    static void checkStmtNodePrivilege(ConnectContext context) {
        if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.NODE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "NODE");
        }
    }

    public static Set<TableName> getAllTableNamesForAnalyzeJobStmt(long dbId, long tableId) {
        Set<TableName> tableNames = Sets.newHashSet();
        if (StatsConstants.DEFAULT_ALL_ID != tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null && !db.isSystemDatabase()) {
                Table table = db.getTable(tableId);
                if (table != null && table.isOlapOrCloudNativeTable()) {
                    tableNames.add(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getFullName(), table.getName()));
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            getTableNamesInDb(tableNames, dbId);
        } else {
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
            for (Long id : dbIds) {
                getTableNamesInDb(tableNames, id);
            }
        }

        return tableNames;
    }

    private static void getTableNamesInDb(Set<TableName> tableNames, Long id) {
        Database db = GlobalStateMgr.getCurrentState().getDb(id);
        if (db != null && !db.isSystemDatabase()) {
            for (Table table : db.getTables()) {
                if (table == null || !table.isOlapOrCloudNativeTable()) {
                    continue;
                }
                TableName tableNameNew = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        db.getFullName(), table.getName());
                tableNames.add(tableNameNew);
            }
        }
    }

    private static void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, Database db,
                                                            Table table, long analyzeId) {
        if (db != null && table != null) {
            if (!PrivilegeActions.checkTableAction(context, db.getOriginName(),
                    table.getName(), PrivilegeType.SELECT) ||
                    !PrivilegeActions.checkTableAction(context, db.getOriginName(),
                            table.getName(), PrivilegeType.INSERT)
            ) {
                throw new SemanticException(String.format(
                        "You need SELECT and INSERT action on %s.%s to kill analyze job %d",
                        db.getOriginName(), table.getName(), analyzeId));
            }
        }
    }

    public static void checkPrivilegeForKillAnalyzeStmt(ConnectContext context, long analyzeId) {
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
        AnalyzeStatus analyzeStatus = analyzeManager.getAnalyzeStatus(analyzeId);
        AnalyzeJob analyzeJob = analyzeManager.getAnalyzeJob(analyzeId);
        if (analyzeStatus != null) {
            long dbId = analyzeStatus.getDbId();
            long tableId = analyzeStatus.getTableId();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            // If the db or table doesn't exist anymore, we won't check privilege on it
            if (db != null) {
                Table table = db.getTable(tableId);
                checkTblPrivilegeForKillAnalyzeStmt(context, db, table, analyzeId);
            }
        } else if (analyzeJob != null) {
            Set<TableName> tableNames = PrivilegeCheckerV2.getAllTableNamesForAnalyzeJobStmt(analyzeJob.getDbId(),
                    analyzeJob.getTableId());
            tableNames.forEach(tableName -> {
                Database db = GlobalStateMgr.getCurrentState().getDb(tableName.getDb());
                if (db != null) {
                    Table table = db.getTable(tableName.getTbl());
                    checkTblPrivilegeForKillAnalyzeStmt(context, db, table, analyzeId);
                }
            });
        }
    }

    static void checkOperateLoadPrivilege(ConnectContext context, String dbName, String label) {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_DB_NOT_FOUND, dbName);
        }
        List<LoadJob> loadJobs = globalStateMgr.getLoadMgr().
                getLoadJobsByDb(db.getId(), label, false);
        List<String> forbiddenInsertTableList = new ArrayList<>();
        List<String> forbiddenUseResourceList = new ArrayList<>();
        loadJobs.forEach(loadJob -> {
            try {
                if (loadJob instanceof SparkLoadJob &&
                        !PrivilegeActions.checkResourceAction(context, loadJob.getResourceName(),
                                PrivilegeType.USAGE)) {
                    forbiddenUseResourceList.add(loadJob.getResourceName());
                }
                loadJob.getTableNames(true).forEach(tableName -> {
                    if (!PrivilegeActions.checkTableAction(context, dbName, tableName,
                            PrivilegeType.INSERT)) {
                        forbiddenInsertTableList.add(tableName);
                    }
                });
            } catch (MetaNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
        if (forbiddenUseResourceList.size() > 0) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_RESOURCE_DENIED,
                    PrivilegeType.USAGE.toString(),
                    context.getQualifiedUser(),
                    context.getRemoteIP(),
                    forbiddenUseResourceList.toString());
        }
        if (forbiddenInsertTableList.size() > 0) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    PrivilegeType.INSERT.toString(),
                    context.getQualifiedUser(),
                    context.getRemoteIP(),
                    forbiddenInsertTableList.toString());
        }
    }

    /**
     * check privilege by AST tree
     */
    private static class PrivilegeCheckerVisitor extends AstVisitor<Void, ConnectContext> {

        public void check(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        // --------------------------------- Query Statement -------------------------------------

        @Override
        public Void visitQueryStatement(QueryStatement statement, ConnectContext context) {
            Map<TableName, Table> allTouchedTables = AnalyzerUtils.collectAllTableAndView(statement);
            checkSelectTableAction(context, allTouchedTables);

            return null;
        }

        // ------------------------------------------- DML Statement -------------------------------------------------------

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext context) {
            // For table just created by CTAS statement, we ignore the check of 'INSERT' privilege on it.
            if (!statement.isForCTAS()) {
                checkTableAction(context, statement.getTableName(), PrivilegeType.INSERT);
            }

            visit(statement.getQueryStatement(), context);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.DELETE);
            Map<TableName, Table> allTouchedTables = AnalyzerUtils.collectAllTableAndView(statement);
            allTouchedTables.remove(statement.getTableName());
            checkSelectTableAction(context, allTouchedTables);
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.UPDATE);
            Map<TableName, Table> allTouchedTables = AnalyzerUtils.collectAllTableAndView(statement);
            allTouchedTables.remove(statement.getTableName());
            checkSelectTableAction(context, allTouchedTables);
            return null;
        }

        static void checkSelectTableAction(ConnectContext context, Map<TableName, Table> allTouchedTables) {
            for (Map.Entry<TableName, Table> tableToBeChecked : allTouchedTables.entrySet()) {
                TableName tableName = tableToBeChecked.getKey();
                Table table = tableToBeChecked.getValue();

                if (table instanceof View) {
                    if (!PrivilegeActions.checkViewAction(context, tableName.getDb(), tableName.getTbl(), PrivilegeType.SELECT)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_TABLE_DENIED,
                                context.getQualifiedUser(), tableName);
                    }
                } else if (table instanceof SystemTable && ((SystemTable) table).requireOperatePrivilege()) {
                    checkStmtOperatePrivilege(context);
                } else if (table.isMaterializedView()) {
                    checkMvAction(context, tableName, PrivilegeType.SELECT);
                } else {
                    checkTableAction(context, tableName, PrivilegeType.SELECT);
                }
            }
        }

        // --------------------------------- Routine Load Statement ---------------------------------

        public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getDBName(), statement.getTableName(), PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbName(), statement.getLabel());
            checkTableAction(context, statement.getDbName(), tableName, PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, ConnectContext context) {
            // `show routine load` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowRoutineLoad()` for details.
            return null;
        }

        @Override
        public Void visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, ConnectContext context) {
            // `show routine load task` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowRoutineLoadTask()` for details.
            return null;
        }

        @Override
        public Void visitShowDataStatement(ShowDataStmt statement, ConnectContext context) {
            // `show data` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowData()` for details.
            return null;
        }

        // --------------------------------- Load Statement -------------------------------------
        @Override
        public Void visitAlterLoadStatement(AlterLoadStmt statement, ConnectContext context) {
            checkOperateLoadPrivilege(context, statement.getDbName(), statement.getLabel());
            return null;
        }

        @Override
        public Void visitLoadStatement(LoadStmt statement, ConnectContext context) {
            // check resource privilege
            if (null != statement.getResourceDesc()) {
                String resourceName = statement.getResourceDesc().getName();
                if (!PrivilegeActions.checkResourceAction(context, resourceName, PrivilegeType.USAGE)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USAGE");
                }
            }
            // check table privilege
            String dbName = statement.getLabel().getDbName();
            List<String> forbiddenInsertTableList = new ArrayList<>();
            statement.getDataDescriptions().forEach(dataDescription -> {
                String tableName = dataDescription.getTableName();
                if (!PrivilegeActions.checkTableAction(context, dbName, tableName, PrivilegeType.INSERT)) {
                    forbiddenInsertTableList.add(tableName);
                }
            });
            if (forbiddenInsertTableList.size() > 0) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                        PrivilegeType.INSERT.toString(),
                        context.getQualifiedUser(),
                        context.getRemoteIP(),
                        forbiddenInsertTableList.toString());
            }
            return null;
        }

        @Override
        public Void visitShowLoadStatement(ShowLoadStmt statement, ConnectContext context) {
            // No authorization required
            return null;
        }

        @Override
        public Void visitCancelLoadStatement(CancelLoadStmt statement, ConnectContext context) {
            checkOperateLoadPrivilege(context, statement.getDbName(), statement.getLabel());
            return null;
        }

        // --------------------------------- Database Statement ---------------------------------

        @Override
        public Void visitUseDbStatement(UseDbStmt statement, ConnectContext context) {
            checkAnyActionOnOrInDb(context, statement.getCatalogName(), statement.getDbName());
            return null;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            checkAnyActionOnDb(context, statement.getCatalogName(), statement.getDb());
            return null;
        }

        @Override
        public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
            // Need to check the `CREATE_DATABASE` action on corresponding catalog
            checkCatalogAction(context, statement.getCatalogName(), PrivilegeType.CREATE_DATABASE);
            return null;
        }

        @Override
        public Void visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.ALTER);
            return null;
        }

        @Override
        public Void visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.ALTER);
            return null;
        }

        @Override
        public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
            checkDbAction(context, statement.getCatalogName(), statement.getDbName(), PrivilegeType.DROP);
            return null;
        }

        @Override
        public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext context) {
            checkCatalogAction(context, context.getCurrentCatalog(), PrivilegeType.CREATE_DATABASE);
            return null;
        }

        // --------------------------------- External Resource Statement ----------------------------------

        @Override
        public Void visitCreateResourceStatement(CreateResourceStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.CREATE_RESOURCE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE RESOURCE");
            }
            return null;
        }

        @Override
        public Void visitDropResourceStatement(DropResourceStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkResourceAction(
                    context, statement.getResourceName(), PrivilegeType.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAlterResourceStatement(AlterResourceStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkResourceAction(
                    context, statement.getResourceName(), PrivilegeType.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        @Override
        public Void visitShowResourceStatement(ShowResourcesStmt statement, ConnectContext context) {
            // `show resources` only show resource that user has any privilege on, we will check it in
            // the execution logic, not here, see `handleShowResources()` for details.
            return null;
        }

        // --------------------------------- Resource Group Statement -------------------------------------
        public Void visitCreateResourceGroupStatement(CreateResourceGroupStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(
                    context, PrivilegeType.CREATE_RESOURCE_GROUP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "CREATE RESOURCE GROUP");
            }
            return null;
        }

        public Void visitDropResourceGroupStatement(DropResourceGroupStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkResourceGroupAction(
                    context, statement.getName(), PrivilegeType.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        public Void visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkResourceGroupAction(
                    context, statement.getName(), PrivilegeType.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        public Void visitShowResourceGroupStatement(ShowResourceGroupStmt statement, ConnectContext context) {
            // we don't check privilege for `show resource groups` statement
            return null;
        }

        // --------------------------------- Warehouse Statement ------------------------------------------
        @Override
        public Void visitUseWarehouseStatement(UseWarehouseStmt statement, ConnectContext context) {
            return null;
        }

        // --------------------------------- Catalog Statement --------------------------------------------

        @Override
        public Void visitUseCatalogStatement(UseCatalogStmt statement, ConnectContext context) {
            // No authorization check for using default_catalog
            if (CatalogMgr.isInternalCatalog(statement.getCatalogName())) {
                return null;
            }
            if (!PrivilegeActions.checkAnyActionOnOrInCatalog(context, statement.getCatalogName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), statement.getCatalogName());
            }
            return null;
        }

        @Override
        public Void visitSetCatalogStatement(SetCatalogStmt statement, ConnectContext context) {
            String catalogName = statement.getCatalogName();
            // No authorization check for using default_catalog
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return null;
            }
            if (!PrivilegeActions.checkAnyActionOnOrInCatalog(context, catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), catalogName);
            }
            return null;
        }

        @Override
        public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.CREATE_EXTERNAL_CATALOG)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), statement.getCatalogName());
            }
            return null;
        }

        @Override
        public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkCatalogAction(context, statement.getName(), PrivilegeType.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), statement.getName());
            }
            return null;
        }

        @Override
        public Void visitShowCatalogsStatement(ShowCatalogsStmt statement, ConnectContext context) {
            // `show catalogs` only show catalog that user has any privilege on, we will check it in
            // the execution logic, not here, see `handleShowCatalogs()` for details.
            return null;
        }

        // --------------------------------------- Plugin Statement ---------------------------------------

        @Override
        public Void visitInstallPluginStatement(InstallPluginStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        // ---------------------------------------- Show Node Info Statement-------------------------------
        @Override
        public Void visitShowBackendsStatement(ShowBackendsStmt statement, ConnectContext context) {
            return checkShowNodePrivilege(context);
        }

        @Override
        public Void visitShowFrontendsStatement(ShowFrontendsStmt statement, ConnectContext context) {
            return checkShowNodePrivilege(context);
        }

        @Override
        public Void visitShowBrokerStatement(ShowBrokerStmt statement, ConnectContext context) {
            return checkShowNodePrivilege(context);
        }

        @Override
        public Void visitShowComputeNodes(ShowComputeNodesStmt statement, ConnectContext context) {
            return checkShowNodePrivilege(context);
        }

        private Void checkShowNodePrivilege(ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.OPERATE)
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.NODE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "OPERATE/NODE");
            }
            return null;
        }

        @Override
        public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        @Override
        public Void visitShowPluginsStatement(ShowPluginsStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        // --------------------------------------- File Statement ----------------------------------------

        @Override
        public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrInDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrInDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            checkAnyActionOnOrInDb(context, context.getCurrentCatalog(), statement.getDbName());
            return null;
        }

        // --------------------------------------- Analyze related statements -----------------------------

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext context) {
            Set<TableName> tableNames = getAllTableNamesForAnalyzeJobStmt(statement.getDbId(), statement.getTableId());
            tableNames.forEach(tableName -> {
                checkTableAction(context, tableName, PrivilegeType.SELECT);
                checkTableAction(context, tableName, PrivilegeType.INSERT);
            });
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.INSERT);
            return null;
        }

        @Override
        public Void visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, ConnectContext context) {
            // `show analyze job` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowAnalyzeJobStmt#showAnalyzeJobs()` for details.
            return null;
        }

        @Override
        public Void visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, ConnectContext context) {
            // `show analyze status` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowAnalyzeStatusStmt#showAnalyzeStatus()` for details.
            return null;
        }

        @Override
        public Void visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, ConnectContext context) {
            // `show stats meta` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowBasicStatsMetaStmt#showBasicStatsMeta()` for details.
            return null;
        }

        @Override
        public Void visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, ConnectContext context) {
            // `show histogram meta` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowHistogramStatsMetaStmt#showHistogramStatsMeta()` for details.
            return null;
        }

        @Override
        public Void visitKillAnalyzeStatement(KillAnalyzeStmt statement, ConnectContext context) {
            // `kill analyze {id}` can only kill analyze job that user has privileges(SELECT + INSERT) on,
            // we will check it in the execution logic, not here, see `ShowExecutor#handleKillAnalyzeStmt()`
            // for details.
            return null;
        }

        // --------------------------------------- Sql BlackList And WhiteList Statement ------------------

        @Override
        public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        // ---------------------------------------- Privilege Statement -----------------------------------

        @Override
        public Void visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropUserStatement(DropUserStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowUserStatement(ShowUserStmt statement, ConnectContext context) {
            if (statement.isAll() && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if ((user != null && !user.equals(context.getCurrentUserIdentity()) || statement.isAll())
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitExecuteAsStatement(ExecuteAsStmt statement, ConnectContext context) {
            AuthorizationMgr authorizationManager = context.getGlobalStateMgr().getAuthorizationMgr();
            if (!authorizationManager.canExecuteAs(context, statement.getToUser())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "IMPERSONATE");
            }
            return null;
        }

        @Override
        public Void visitExecuteScriptStatement(ExecuteScriptStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitCreateRoleStatement(CreateRoleStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitDropRoleStatement(DropRoleStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowRolesStatement(ShowRolesStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitGrantRoleStatement(GrantRoleStmt statement, ConnectContext context) {
            if (statement.getGranteeRole().stream().anyMatch(r -> r.equalsIgnoreCase(PrivilegeBuiltinConstants.ROOT_ROLE_NAME)
                    || r.equalsIgnoreCase(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_NAME))) {
                UserIdentity userIdentity = context.getCurrentUserIdentity();
                if (!userIdentity.equals(UserIdentity.ROOT)) {
                    throw new SemanticException("Can not grant root or cluster_admin role except root user");
                }
            }

            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitRevokeRoleStatement(RevokeRoleStmt statement, ConnectContext context) {
            if (statement.getGranteeRole().stream().anyMatch(r -> r.equalsIgnoreCase(PrivilegeBuiltinConstants.ROOT_ROLE_NAME)
                    || r.equalsIgnoreCase(PrivilegeBuiltinConstants.CLUSTER_ADMIN_ROLE_NAME))) {
                UserIdentity userIdentity = context.getCurrentUserIdentity();
                if (!userIdentity.equals(UserIdentity.ROOT)) {
                    throw new SemanticException("Can not grant root or cluster_admin role except root user");
                }
            }

            if (statement.getGranteeRole().stream().anyMatch(r -> r.equalsIgnoreCase(PrivilegeBuiltinConstants.ROOT_ROLE_NAME))) {
                if (statement.getUserIdentity() != null && statement.getUserIdentity().equals(UserIdentity.ROOT)) {
                    throw new SemanticException("Can not revoke root role from root user");
                }
            }

            if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdentity();
            if (user != null && !user.equals(context.getCurrentUserIdentity())
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext context) {
            AuthorizationMgr authorizationManager = context.getGlobalStateMgr().getAuthorizationMgr();
            if (!authorizationManager.allowGrant(context, stmt.getObjectType(), stmt.getPrivilegeTypes(), stmt.getObjectList())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext context) {
            UserIdentity user = statement.getUserIdent();
            if (user != null && !user.equals(context.getCurrentUserIdentity())
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            } else if (statement.getRole() != null) {
                AuthorizationMgr authorizationManager = context.getGlobalStateMgr().getAuthorizationMgr();
                try {
                    List<String> roleNames = authorizationManager.getRoleNamesByUser(context.getCurrentUserIdentity());
                    if (!roleNames.contains(statement.getRole())
                            && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                    }
                } catch (PrivilegeException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                }
            }
            return null;
        }

        @Override
        public Void visitShowUserPropertyStatement(ShowUserPropertyStmt statement, ConnectContext context) {
            String user = statement.getUser();
            if (user != null && !user.equals(context.getCurrentUserIdentity().getQualifiedUser())
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        @Override
        public Void visitSetUserPropertyStatement(SetUserPropertyStmt statement, ConnectContext context) {
            String user = statement.getUser();
            if (user != null && !user.equals(context.getCurrentUserIdentity().getQualifiedUser())
                    && !PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
            }
            return null;
        }

        // ---------------------------------------- View Statement ---------------------------------------

        @Override
        public Void visitCreateViewStatement(CreateViewStmt statement, ConnectContext context) {
            // 1. check if user can create view in this db
            TableName tableName = statement.getTableName();
            String catalog = tableName.getCatalog();
            if (catalog == null) {
                catalog = context.getCurrentCatalog();
            }
            checkDbAction(context, catalog, tableName.getDb(), PrivilegeType.CREATE_VIEW);
            // 2. check if user can query
            check(statement.getQueryStatement(), context);
            return null;
        }

        // ---------------------------------------- Show Transaction Statement ---------------------------

        @Override
        public Void visitShowTransactionStatement(ShowTransactionStmt statement, ConnectContext context) {
            // No authorization required
            return null;
        }

        @Override
        public Void visitAlterViewStatement(AlterViewStmt statement, ConnectContext context) {
            // 1. check if user can alter view in this db
            checkViewAction(context, statement.getTableName(), PrivilegeType.ALTER);
            // 2. check if user can query
            AlterViewClause alterViewClause = (AlterViewClause) statement.getAlterClause();
            check(alterViewClause.getQueryStatement(), context);
            return null;
        }

        // ---------------------------------------- Table Statement ---------------------------------------

        @Override
        public Void visitCreateTableStatement(CreateTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getDbTbl();
            String catalog = tableName.getCatalog();
            if (catalog == null) {
                catalog = context.getCurrentCatalog();
            }
            String dbName = tableName.getDb() == null ? context.getDatabase() : tableName.getDb();
            checkDbAction(context, catalog, dbName, PrivilegeType.CREATE_TABLE);

            if (statement.getProperties() != null && statement.getProperties().containsKey("resource")) {
                String resourceProp = statement.getProperties().get("resource");
                Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceProp);
                if (resource != null) {
                    if (!PrivilegeActions.checkResourceAction(context, resource.getName(), PrivilegeType.USAGE)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_RESOURCE_DENIED,
                                PrivilegeType.USAGE, context.getQualifiedUser(), context.getRemoteIP(), resource.getName());
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, ConnectContext context) {
            visitCreateTableStatement(statement.getCreateTableStmt(), context);
            visitQueryStatement(statement.getQueryStatement(), context);
            return null;
        }

        @Override
        public Void visitCreateTableLikeStatement(CreateTableLikeStmt statement, ConnectContext context) {
            visitCreateTableStatement(statement.getCreateTableStmt(), context);
            checkTableAction(context, statement.getExistedDbTbl(), PrivilegeType.SELECT);
            return null;
        }

        @Override
        public Void visitDropTableStatement(DropTableStmt statement, ConnectContext context) {
            if (statement.isView()) {
                checkViewAction(context, statement.getTbl(), PrivilegeType.DROP);
            } else {
                checkTableAction(context, statement.getTbl(), PrivilegeType.DROP);
            }
            return null;
        }

        @Override
        public Void visitRecoverTableStatement(RecoverTableStmt statement, ConnectContext context) {
            TableName tableName = statement.getTableNameObject();
            String catalog = tableName.getCatalog();
            if (catalog == null) {
                catalog = context.getCurrentCatalog();
            }
            checkDbAction(context, catalog, tableName.getDb(), PrivilegeType.CREATE_TABLE);
            return null;
        }

        @Override
        public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext context) {
            checkTableAction(context,
                    new TableName(context.getCurrentCatalog(), statement.getDbName(), statement.getTblName()),
                    PrivilegeType.DELETE);
            return null;
        }

        @Override
        public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.ALTER);
            return null;
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTbl(), PrivilegeType.ALTER);
            return null;
        }

        @Override
        public Void visitCancelAlterTableStatement(CancelAlterTableStmt statement, ConnectContext context) {
            if (statement.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
                Database db = GlobalStateMgr.getCurrentState().getDb(statement.getDbName());
                if (db != null) {
                    try {
                        db.readLock();
                        Table table = db.getTable(statement.getTableName());
                        if (table == null || !table.isMaterializedView()) {
                            // ignore privilege check for old mv
                            return null;
                        }
                    } finally {
                        db.readUnlock();
                    }
                }
                if (!PrivilegeActions.checkMaterializedViewAction(context,
                        statement.getDbName(),
                        statement.getTableName(),
                        PrivilegeType.ALTER)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
                }
            } else {
                checkTableAction(context, statement.getDbTableName(), PrivilegeType.ALTER);
            }
            return null;
        }

        @Override
        public Void visitDescTableStmt(DescribeStmt statement, ConnectContext context) {
            checkAnyActionOnTable(context, statement.getDbTableName());
            return null;
        }

        @Override
        public Void visitShowCreateTableStatement(ShowCreateTableStmt statement, ConnectContext context) {
            checkAnyActionOnTable(context, statement.getTbl());
            return null;
        }

        @Override
        public Void visitShowTableStatusStatement(ShowTableStatusStmt statement, ConnectContext context) {
            // `show table status` only show tables that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowTableStatus()` for details.
            return null;
        }

        @Override
        public Void visitShowIndexStatement(ShowIndexStmt statement, ConnectContext context) {
            checkAnyActionOnTable(context, statement.getTableName());
            return null;
        }

        @Override
        public Void visitShowColumnStatement(ShowColumnStmt statement, ConnectContext context) {
            checkAnyActionOnTable(context, statement.getTableName());
            return null;
        }

        @Override
        public Void visitRecoverPartitionStatement(RecoverPartitionStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getDbTblName(), PrivilegeType.INSERT);
            checkTableAction(context, statement.getDbTblName(), PrivilegeType.ALTER);
            return null;
        }

        @Override
        public Void visitShowPartitionsStatement(ShowPartitionsStmt statement, ConnectContext context) {
            checkAnyActionOnTable(context, new TableName(statement.getDbName(), statement.getTableName()));
            return null;
        }

        @Override
        public Void visitSubmitTaskStatement(SubmitTaskStmt statement, ConnectContext context) {
            if (statement.getCreateTableAsSelectStmt() != null) {
                visitCreateTableAsSelectStatement(statement.getCreateTableAsSelectStmt(), context);
            } else {
                visitInsertStatement(statement.getInsertStmt(), context);
            }
            return null;
        }

        @Override
        public Void visitShowAlterStatement(ShowAlterStmt statement, ConnectContext context) {
            // `show alter table` only show tables/views/mvs that user has any privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowAlter()` for details.
            return null;
        }

        // ---------------------------------------- Show Variables Statement ------------------------------

        @Override
        public Void visitShowVariablesStatement(ShowVariablesStmt statement, ConnectContext context) {
            // No authorization required
            return null;
        }

        // ---------------------------------------- Show tablet Statement ---------------------------------

        @Override
        public Void visitShowTabletStatement(ShowTabletStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        // ---------------------------------------- Admin operate Statement --------------------------------

        @Override
        public Void visitAdminSetConfigStatement(AdminSetConfigStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminShowConfigStatement(AdminShowConfigStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                               ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminRepairTableStatement(AdminRepairTableStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitKillStatement(KillStmt statement, ConnectContext context) {
            // Privilege is checked in execution logic, see `StatementExecutor#handleKill()` for details.
            return null;
        }

        @Override
        public Void visitAlterSystemStatement(AlterSystemStmt statement, ConnectContext context) {
            checkStmtNodePrivilege(context);
            return null;
        }

        @Override
        public Void visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, ConnectContext context) {
            checkStmtNodePrivilege(context);
            return null;
        }

        @Override
        public Void visitShowProcStmt(ShowProcStmt statement, ConnectContext context) {
            checkStmtOperatePrivilege(context);
            return null;
        }

        @Override
        public Void visitShowProcesslistStatement(ShowProcesslistStmt statement, ConnectContext context) {
            // Privilege is checked in execution logic, see `StatementExecutor#handleShowProcesslist()` for details.
            return null;
        }

        @Override
        public Void visitSetStatement(SetStmt statement, ConnectContext context) {
            List<SetListItem> varList = statement.getSetListItems();
            varList.forEach(setVar -> {
                if ((setVar instanceof SetPassVar)) {
                    UserIdentity prepareChangeUser = ((SetPassVar) setVar).getUserIdent();
                    if (!context.getCurrentUserIdentity().equals(prepareChangeUser)) {
                        if (prepareChangeUser.equals(UserIdentity.ROOT)) {
                            throw new SemanticException("Can not set password for root user, except root itself");
                        }

                        if (!PrivilegeActions.checkSystemAction(context, PrivilegeType.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    }
                } else if (setVar instanceof SystemVariable) {
                    SetType type = ((SystemVariable) setVar).getType();
                    if (type != null && type.equals(SetType.GLOBAL)) {
                        checkStmtOperatePrivilege(context);
                    }
                }
            });
            return null;
        }

        // ---------------------------------------- restore & backup Statement --------------------------------
        @Override
        public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkTableAction(context,
                    statement.getTblName().getDb(),
                    statement.getTblName().getTbl(),
                    PrivilegeType.EXPORT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "EXPORT");
            }
            return null;
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext context) {
            ExportJob exportJob = null;
            try {
                exportJob = GlobalStateMgr.getCurrentState().getExportMgr().getExportJob(statement.getDbName(),
                        statement.getQueryId());
            } catch (AnalysisException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, statement.getDbName());
            }
            if (null == exportJob) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_EXPORT_JOB_NOT_FOUND,
                        statement.getQueryId().toString());
            }
            if (!PrivilegeActions.checkTableAction(context,
                    exportJob.getTableName().getDb(),
                    exportJob.getTableName().getTbl(),
                    PrivilegeType.EXPORT)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "EXPORT");
            }
            return null;
        }

        @Override
        public Void visitShowExportStatement(ShowExportStmt statement, ConnectContext context) {
            // `show export` only show tables that user has export privilege on, we will check it in
            // the execution logic, not here, see `ExportMgr#getExportJobInfosByIdOrState()` for details.
            return null;
        }

        @Override
        public Void visitCreateRepositoryStatement(CreateRepositoryStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            return null;
        }

        @Override
        public Void visitDropRepositoryStatement(DropRepositoryStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            return null;
        }

        @Override
        public Void visitShowSnapshotStatement(ShowSnapshotStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            return null;
        }

        @Override
        public Void visitBackupStatement(BackupStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            List<TableRef> tableRefs = statement.getTableRefs();
            if (tableRefs.size() == 0) {
                String dBName = statement.getDbName();
                throw new SemanticException("Database: %s is empty", dBName);
            }
            tableRefs.forEach(tableRef -> {
                TableName tableName = tableRef.getName();
                checkTableAction(context,
                        tableName.getDb(),
                        tableName.getTbl(),
                        PrivilegeType.EXPORT);
            });
            return null;
        }

        @Override
        public Void visitShowBackupStatement(ShowBackupStmt statement, ConnectContext context) {
            // Step 1 check system.Repository
            checkSystemRepository(context);
            // Step 2 check table.export
            // `show backup` only show tables that user has export privilege on, we will check it in
            // the execution logic, not here, see `ShowExecutor#handleShowBackup()` for details.
            return null;
        }

        @Override
        public Void visitCancelBackupStatement(CancelBackupStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            AbstractJob job = null;
            try {
                job = GlobalStateMgr.getCurrentState().getBackupHandler().getAbstractJobByDbName(statement.getDbName());
            } catch (DdlException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, statement.getDbName());
            }
            if (null == job) {
                return null;
            }
            if (job instanceof BackupJob) {
                BackupJob backupJob = (BackupJob) job;
                List<TableRef> tableRefs = backupJob.getTableRef();
                tableRefs.forEach(tableRef -> {
                    TableName tableName = tableRef.getName();
                    checkTableAction(context,
                            tableName.getDb(),
                            tableName.getTbl(),
                            PrivilegeType.EXPORT);
                });
            }
            return null;
        }

        @Override
        public Void visitRestoreStatement(RestoreStmt statement, ConnectContext context) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            // check repository on system
            checkSystemRepository(context);

            List<TableRef> tableRefs = statement.getTableRefs();
            // check create_database on current catalog if we're going to restore the whole database
            if (tableRefs == null || tableRefs.isEmpty()) {
                checkCatalogAction(context, context.getCurrentCatalog(), PrivilegeType.CREATE_DATABASE);
            } else {
                // going to restore some tables in database or some partitions in table
                Database db = globalStateMgr.getDb(statement.getDbName());
                if (db != null) {
                    try {
                        db.readLock();
                        // check create_table on specified database
                        checkDbAction(context, context.getCurrentCatalog(), db.getFullName(),
                                PrivilegeType.CREATE_TABLE);
                        // check insert on specified table
                        for (TableRef tableRef : tableRefs) {
                            Table table = db.getTable(tableRef.getName().getTbl());
                            if (table != null) {
                                checkTableAction(context,
                                        statement.getDbName(),
                                        tableRef.getName().getTbl(),
                                        PrivilegeType.INSERT);
                            }
                        }
                    } finally {
                        db.readUnlock();
                    }
                }
            }

            return null;
        }

        @Override
        public Void visitShowRestoreStatement(ShowRestoreStmt statement, ConnectContext context) {
            checkSystemRepository(context);
            return null;
        }

        // ---------------------------------------- Materialized View stmt --------------------------------
        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            if (!PrivilegeActions.checkDbAction(context, statement.getTableName().getDb(),
                    PrivilegeType.CREATE_MATERIALIZED_VIEW)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "CREATE MATERIALIZED VIEW");
            }
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkMaterializedViewAction(context,
                    statement.getMvName().getDb(),
                    statement.getMvName().getTbl(),
                    PrivilegeType.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "ALTER MATERIALIZED VIEW");
            }
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            if (!PrivilegeActions.checkMaterializedViewAction(context,
                    statement.getMvName().getDb(),
                    statement.getMvName().getTbl(),
                    PrivilegeType.REFRESH)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "REFRESH MATERIALIZED VIEW");
            }
            return null;
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            if (!PrivilegeActions.checkMaterializedViewAction(context,
                    statement.getMvName().getDb(),
                    statement.getMvName().getTbl(),
                    PrivilegeType.REFRESH)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                        "REFRESH MATERIALIZED VIEW");
            }
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, ConnectContext context) {
            // `show Materialized Views` show tables user (has select privilege & show mv user has any privilege),
            // we will check it in the execution logic, not here,
            // see `ShowExecutor#handleShowMaterializedView()` for details.
            return null;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt statement, ConnectContext context) {
            // To keep compatibility with old mv, drop mv will be checked in execution logic, and only new mv is checked
            return null;
        }

        // ------------------------------------------- UDF Statement ----------------------------------------------------

        @Override
        public Void visitCreateFunctionStatement(CreateFunctionStmt statement, ConnectContext context) {
            FunctionName name = statement.getFunctionName();
            if (name.isGlobalFunction()) {
                if (!PrivilegeActions.checkSystemAction(
                        context, PrivilegeType.CREATE_GLOBAL_FUNCTION)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "CREATE GLOBAL FUNCTION");
                }
            } else {
                if (!PrivilegeActions.checkDbAction(context, name.getDb(), PrivilegeType.CREATE_FUNCTION)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE FUNCTION");
                }
            }
            return null;
        }

        @Override
        public Void visitShowFunctionsStatement(ShowFunctionsStmt statement, ConnectContext context) {
            // Privilege check is handled in `ShowExecutor#handleShowFunctions()`
            return null;
        }

        @Override
        public Void visitDropFunctionStatement(DropFunctionStmt statement, ConnectContext context) {
            FunctionName functionName = statement.getFunctionName();
            // global function.
            if (functionName.isGlobalFunction()) {
                FunctionSearchDesc functionSearchDesc = statement.getFunctionSearchDesc();
                Function function = GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().getFunction(functionSearchDesc);
                if (function != null && !PrivilegeActions.checkGlobalFunctionAction(context, function, PrivilegeType.DROP)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "DROP GLOBAL FUNCTION");
                }
                return null;
            }

            // db function.
            Database db = GlobalStateMgr.getCurrentState().getDb(functionName.getDb());
            if (db != null) {
                try {
                    db.readLock();
                    Function function = db.getFunction(statement.getFunctionSearchDesc());
                    if (null != function && !PrivilegeActions.checkFunctionAction(context, db, function, PrivilegeType.DROP)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                "DROP FUNCTION");
                    }
                } finally {
                    db.readUnlock();
                }
            }
            return null;
        }
    }
}
