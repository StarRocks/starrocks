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

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.load.ExportJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddBackendBlackListStmt;
import com.starrocks.sql.ast.AddSqlBlackListStmt;
import com.starrocks.sql.ast.AdminCancelRepairTableStmt;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AlterCatalogStmt;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterLoadStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoleStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
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
import com.starrocks.sql.ast.CancelCompactionStmt;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColumnAssignment;
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
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DelBackendBlackListStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
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
import com.starrocks.sql.ast.DropStorageVolumeStmt;
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
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetDefaultRoleStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackendBlackListStmt;
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
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AuthorizerStmtVisitor extends AstVisitor<Void, ConnectContext> {
    // For show tablet detail command, if user has any privilege on the corresponding table, user can run it
    // TODO(yiming): match "/dbs", not only show tablet detail cmd, need to change privilege check for other proc node
    private static final Pattern SHOW_TABLET_DETAIL_CMD_PATTERN =
            Pattern.compile("/dbs/\\d+/\\d+/partitions/\\d+/\\d+/\\d+/?");

    private static final Logger LOG = LogManager.getLogger(AuthorizerStmtVisitor.class);

    public AuthorizerStmtVisitor() {
    }

    public void check(StatementBase statement, ConnectContext context) {
        visit(statement, context);
    }

    // --------------------------------- Query Statement -------------------------------------

    @Override
    public Void visitQueryStatement(QueryStatement statement, ConnectContext context) {
        Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(statement);
        if (Config.authorization_enable_column_level_privilege) {
            try {
                checkSelectTableAction(context, allTablesRelations);
            } catch (ErrorReportException e) {
                Map<TableName, Set<String>> allTouchedColumns = AnalyzerUtils.collectAllSelectTableColumns(statement);
                checkCanSelectFromColumns(context, allTouchedColumns, allTablesRelations);
            }
        } else {
            checkSelectTableAction(context, allTablesRelations);
        }
        return null;
    }

    enum PrivilegeLevel {
        TABLE, COLUMN
    }

    // ------------------------------------------- DML Statement -------------------------------------------------------

    @Override
    public Void visitInsertStatement(InsertStmt statement, ConnectContext context) {
        if (Config.authorization_enable_column_level_privilege) {
            Set<String> columnNames = null;
            PrivilegeLevel insertPrivLevel = null;
            // For table just created by CTAS statement, we ignore the check of 'INSERT' privilege on it.
            if (!statement.isForCTAS()) {
                try {
                    Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            statement.getTableName(), PrivilegeType.INSERT);
                    insertPrivLevel = PrivilegeLevel.TABLE;
                } catch (AccessDeniedException e) {
                    try {
                        columnNames = statement.getTargetColumnNames() == null ? Collections.singleton("*") :
                                new HashSet<>(statement.getTargetColumnNames());
                        Authorizer.checkColumnsAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                statement.getTableName(), columnNames, PrivilegeType.INSERT);
                        insertPrivLevel = PrivilegeLevel.COLUMN;
                    } catch (AccessDeniedException exception) {
                        AccessDeniedException.reportAccessDenied(
                                statement.getTableName().getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.INSERT.name(), ObjectType.COLUMN.name(),
                                String.join(".", statement.getTableName().getTbl(),
                                        String.join(",", columnNames)));
                    }
                }
            }

            Map<TableName, Relation> allTablesRelations = AnalyzerUtils.collectAllTableAndViewRelations(statement);
            Map<TableName, Set<String>> allTouchedColumns = AnalyzerUtils.collectAllSelectTableColumns(statement);
            try {
                if (insertPrivLevel == PrivilegeLevel.TABLE) {
                    // no need to check TABLE/COLUMN SELECT privilege when user already has this TABLE's INSERT privilege
                    allTablesRelations.remove(statement.getTableName());
                    allTouchedColumns.remove(statement.getTableName());
                }
                checkSelectTableAction(context, allTablesRelations);
            } catch (ErrorReportException e) {
                if (insertPrivLevel == PrivilegeLevel.COLUMN) {
                    if (allTouchedColumns.containsKey(statement.getTableName())) {
                        // no need to check COLUMN SELECT privilege when user already has COLUMN INSERT privilege
                        Set<String> usedCols = allTouchedColumns.get(statement.getTableName());
                        if (usedCols.contains("*")) {
                            usedCols = statement.getTargetTable().getColumns().stream().map(Column::getName)
                                    .collect(Collectors.toSet());
                            allTouchedColumns.put(statement.getTableName(), usedCols);
                        }
                        if (columnNames.contains("*")) {
                            usedCols.clear();
                        } else {
                            usedCols.removeAll(columnNames);
                        }
                        if (usedCols.isEmpty()) {
                            allTouchedColumns.remove(statement.getTableName());
                            allTablesRelations.remove(statement.getTableName());
                        }
                    } else {
                        allTablesRelations.remove(statement.getTableName());
                    }
                    checkCanSelectFromColumns(context, allTouchedColumns, allTablesRelations);
                } else {
                    // insertPrivLevel == PrivilegeLevel.TABLE
                    checkCanSelectFromColumns(context, allTouchedColumns, allTablesRelations);
                }
            }
        } else {
            // For table just created by CTAS statement, we ignore the check of 'INSERT' privilege on it.
            if (!statement.isForCTAS()) {
                try {
                    Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            statement.getTableName(), PrivilegeType.INSERT);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(statement.getTableName().getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
                }
            }

            visit(statement.getQueryStatement(), context);
        }
        return null;
    }

    @Override
    public Void visitDeleteStatement(DeleteStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTableName(), PrivilegeType.DELETE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(statement.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DELETE.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
        }
        Map<TableName, Relation> allTouchedTables = AnalyzerUtils.collectAllTableAndViewRelations(statement);
        allTouchedTables.remove(statement.getTableName());
        if (Config.authorization_enable_column_level_privilege) {
            try {
                checkSelectTableAction(context, allTouchedTables);
            } catch (ErrorReportException e) {
                Map<TableName, Set<String>> tableColumns = AnalyzerUtils.collectAllSelectTableColumns(statement);
                tableColumns.remove(statement.getTableName());
                checkCanSelectFromColumns(context, tableColumns, allTouchedTables);
            }
        } else {
            checkSelectTableAction(context, allTouchedTables);
        }
        return null;
    }

    @Override
    public Void visitUpdateStatement(UpdateStmt statement, ConnectContext context) {
        Map<TableName, Relation> allTouchedTables = AnalyzerUtils.collectAllTableAndViewRelations(statement);
        if (Config.authorization_enable_column_level_privilege) {
            Set<String> assignmentColumns = null;
            PrivilegeLevel updatePrivLevel = null;
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        statement.getTableName(), PrivilegeType.UPDATE);
                updatePrivLevel = PrivilegeLevel.TABLE;
            } catch (AccessDeniedException e) {
                try {
                    assignmentColumns = statement.getAssignments().stream().map(ColumnAssignment::getColumn)
                            .collect(Collectors.toSet());
                    Authorizer.checkColumnsAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            statement.getTableName(), assignmentColumns, PrivilegeType.UPDATE);
                    updatePrivLevel = PrivilegeLevel.COLUMN;
                } catch (AccessDeniedException exception) {
                    AccessDeniedException.reportAccessDenied(
                            statement.getTableName().getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.UPDATE.name(), ObjectType.COLUMN.name(),
                            String.join(".", statement.getTableName().getTbl(),
                                    String.join(",", assignmentColumns)));
                }
            }

            Map<TableName, Set<String>> tableColumns = AnalyzerUtils.collectAllSelectTableColumns(statement);
            try {
                if (updatePrivLevel == PrivilegeLevel.TABLE) {
                    // no need to check TABLE/COLUMN SELECT privilege when user already has this TABLE's UPDATE privilege
                    allTouchedTables.remove(statement.getTableName());
                    tableColumns.remove(statement.getTableName());
                }
                checkSelectTableAction(context, allTouchedTables);
            } catch (ErrorReportException e) {
                if (updatePrivLevel == PrivilegeLevel.COLUMN) {
                    // no need to check COLUMN SELECT privilege when user already has COLUMN UPDATE privilege
                    if (tableColumns.containsKey(statement.getTableName())) {
                        Set<String> usedCols = tableColumns.get(statement.getTableName());
                        if (usedCols.contains("*")) {
                            usedCols = statement.getTable().getColumns().stream().map(Column::getName)
                                    .collect(Collectors.toSet());
                            tableColumns.put(statement.getTableName(), usedCols);
                        }
                        usedCols.removeAll(assignmentColumns);
                        if (usedCols.isEmpty()) {
                            tableColumns.remove(statement.getTableName());
                            allTouchedTables.remove(statement.getTableName());
                        }
                    } else {
                        allTouchedTables.remove(statement.getTableName());
                    }
                    checkCanSelectFromColumns(context, tableColumns, allTouchedTables);
                } else {
                    // updatePrivLevel == PrivilegeLevel.TABLE
                    checkCanSelectFromColumns(context, tableColumns, allTouchedTables);
                }
            }
        } else {
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        statement.getTableName(), PrivilegeType.UPDATE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(statement.getTableName().getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.UPDATE.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
            }
            allTouchedTables.remove(statement.getTableName());
            checkSelectTableAction(context, allTouchedTables);
        }
        return null;
    }

    void checkSelectTableAction(ConnectContext context, Map<TableName, Relation> allTouchedTables) {
        for (Map.Entry<TableName, Relation> tableToBeChecked : allTouchedTables.entrySet()) {
            TableName tableName = tableToBeChecked.getKey();
            Table table;
            if (tableToBeChecked.getValue() instanceof TableRelation) {
                table = ((TableRelation) tableToBeChecked.getValue()).getTable();
            } else {
                table = ((ViewRelation) tableToBeChecked.getValue()).getView();
            }
            if (table instanceof SystemTable && ((SystemTable) table).requireOperatePrivilege()) {
                try {
                    Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                }
            } else {
                if (table instanceof View) {
                    try {
                        Authorizer.checkViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                tableName, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.VIEW.name(), tableName.getTbl());
                    }
                } else if (table.isMaterializedView()) {
                    try {
                        Authorizer.checkMaterializedViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                tableName, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.MATERIALIZED_VIEW.name(), tableName.getTbl());
                    }
                } else {
                    try {
                        Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                tableName.getCatalog(), tableName.getDb(), table.getName(), PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName.getTbl());
                    }
                }
            }
        }
    }

    private void checkCanSelectFromColumns(ConnectContext context, Map<TableName, Set<String>> allTouchedTableColumns,
                                           Map<TableName, Relation> allTouchedTables) {
        HashSet<TableName> usedTables = new HashSet<>(allTouchedTables.keySet());
        HashSet<TableName> usedColOfTables = new HashSet<>(allTouchedTableColumns.keySet());
        usedTables.removeAll(usedColOfTables);
        if (!usedTables.isEmpty()) {
            String warnMsg = String.format("The column usage information of some actually used tables " +
                    "has not been successfully collected. tables: %s", usedTables);
            LOG.warn(warnMsg);
            // check SELECT privilege of all the columns(`*`) for these tables instead
            for (TableName usedTable : usedTables) {
                try {
                    Authorizer.checkColumnsAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            usedTable, Collections.singleton("*"), PrivilegeType.SELECT);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            usedTable.getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.SELECT.name(), ObjectType.COLUMN.name(),
                            String.join(".", usedTable.getTbl(), "*"));
                }
            }
        }
        for (Map.Entry<TableName, Set<String>> tableColumns : allTouchedTableColumns.entrySet()) {
            TableName tableName = tableColumns.getKey();
            Set<String> columns = tableColumns.getValue();
            Relation relation = allTouchedTables.get(tableName);
            if (relation == null) {
                String warnMsg = String.format("Some used columns of tables were incorrectly collected. table: %s",
                        tableName);
                LOG.warn(warnMsg);
                AccessDeniedException.reportAccessDenied(
                        tableName.getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.SELECT.name(), ObjectType.COLUMN.name(),
                        String.join(".", tableName.getTbl(),
                                String.join(",", columns)));
            } else {
                Table table = relation instanceof TableRelation ? ((TableRelation) relation).getTable() :
                        ((ViewRelation) relation).getView();
                if (table instanceof SystemTable && ((SystemTable) table).requireOperatePrivilege()) {
                    try {
                        Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.OPERATE);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.VIEW.name(), tableName.getTbl());
                    }
                } else {
                    try {
                        Authorizer.checkColumnsAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                tableName, columns, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.COLUMN.name(),
                                String.join(".", tableName.getTbl(),
                                        String.join(",", columns)));
                    }
                }
            }
        }
    }

    // --------------------------------- Routine Load Statement ---------------------------------

    public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDBName(), statement.getTableName()), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), statement.getTableName());
        }
        return null;
    }

    @Override
    public Void visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, ConnectContext context) {
        String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbName(), statement.getLabel());
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDbName(), tableName), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }
        return null;
    }

    @Override
    public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext context) {
        String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDbFullName(), tableName), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }
        return null;
    }

    @Override
    public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext context) {
        String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDbFullName(), tableName), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }
        return null;
    }

    @Override
    public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext context) {
        String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDbFullName(), tableName), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
        }
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
            try {
                Authorizer.checkResourceAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), resourceName,
                        PrivilegeType.USAGE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.USAGE.name(), ObjectType.RESOURCE.name(), resourceName);
            }
        }
        // check table privilege
        String dbName = statement.getLabel().getDbName();
        statement.getDataDescriptions().forEach(dataDescription -> {
            String tableName = dataDescription.getTableName();
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        new TableName(dbName, tableName), PrivilegeType.INSERT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
            }
        });
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
        try {
            Authorizer.checkAnyActionOnOrInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    context.getCurrentCatalog(), statement.getDbName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        return null;
    }

    @Override
    public Void visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), statement.getDb());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getCatalogName(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), statement.getDb());
        }
        return null;
    }

    @Override
    public Void visitRecoverDbStatement(RecoverDbStmt statement, ConnectContext context) {
        // Need to check the `CREATE_DATABASE` action on corresponding catalog
        try {
            Authorizer.checkCatalogAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), PrivilegeType.CREATE_DATABASE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getCatalogName(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_DATABASE.name(), ObjectType.CATALOG.name(), statement.getCatalogName());
        }
        return null;
    }

    @Override
    public Void visitAlterDatabaseQuotaStatement(AlterDatabaseQuotaStmt statement, ConnectContext context) {
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), statement.getDbName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getCatalogName(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        return null;
    }

    @Override
    public Void visitAlterDatabaseRenameStatement(AlterDatabaseRenameStatement statement, ConnectContext context) {
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), statement.getDbName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getCatalogName(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        return null;
    }

    @Override
    public Void visitDropDbStatement(DropDbStmt statement, ConnectContext context) {
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), statement.getDbName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getCatalogName(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        return null;
    }

    @Override
    public Void visitCreateDbStatement(CreateDbStmt statement, ConnectContext context) {
        try {
            Authorizer.checkCatalogAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    context.getCurrentCatalog(), PrivilegeType.CREATE_DATABASE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_DATABASE.name(), ObjectType.CATALOG.name(), context.getCurrentCatalog());
        }

        if (statement.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
            String storageVolume = statement.getProperties().get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
            try {
                Authorizer.checkStorageVolumeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        storageVolume, PrivilegeType.USAGE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.USAGE.name(), ObjectType.STORAGE_VOLUME.name(), storageVolume);
            }
        }
        return null;
    }

    // --------------------------------- External Resource Statement ----------------------------------

    @Override
    public Void visitCreateResourceStatement(CreateResourceStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_RESOURCE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_RESOURCE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropResourceStatement(DropResourceStmt statement, ConnectContext context) {
        try {
            Authorizer.checkResourceAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getResourceName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.RESOURCE.name(), statement.getResourceName());
        }
        return null;
    }

    @Override
    public Void visitAlterResourceStatement(AlterResourceStmt statement, ConnectContext context) {
        try {
            Authorizer.checkResourceAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getResourceName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.RESOURCE.name(), statement.getResourceName());
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
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_RESOURCE_GROUP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_RESOURCE_GROUP.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    public Void visitDropResourceGroupStatement(DropResourceGroupStmt statement, ConnectContext context) {
        try {
            Authorizer.checkResourceGroupAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.RESOURCE_GROUP.name(), statement.getName());
        }
        return null;
    }

    public Void visitAlterResourceGroupStatement(AlterResourceGroupStmt statement, ConnectContext context) {
        try {
            Authorizer.checkResourceGroupAction(
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.RESOURCE_GROUP.name(), statement.getName());
        }
        return null;
    }

    public Void visitShowResourceGroupStatement(ShowResourceGroupStmt statement, ConnectContext context) {
        // we don't check privilege for `show resource groups` statement
        return null;
    }

    // --------------------------------- Catalog Statement --------------------------------------------

    @Override
    public Void visitUseCatalogStatement(UseCatalogStmt statement, ConnectContext context) {
        String catalogName = statement.getCatalogName();
        // No authorization check for using default_catalog
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return null;
        }
        try {
            Authorizer.checkAnyActionOnCatalog(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), catalogName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), catalogName);
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
        try {
            Authorizer.checkAnyActionOnCatalog(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), catalogName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), catalogName);
        }
        return null;
    }

    @Override
    public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_EXTERNAL_CATALOG);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_EXTERNAL_CATALOG.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
        try {
            Authorizer.checkCatalogAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getName(),
                    PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.CATALOG.name(), statement.getName());
        }
        return null;
    }

    @Override
    public Void visitShowCatalogsStatement(ShowCatalogsStmt statement, ConnectContext context) {
        // `show catalogs` only show catalog that user has any privilege on, we will check it in
        // the execution logic, not here, see `handleShowCatalogs()` for details.
        return null;
    }

    @Override
    public Void visitAlterCatalogStatement(AlterCatalogStmt statement, ConnectContext context) {
        try {
            Authorizer.checkCatalogAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getCatalogName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.CATALOG.name(), statement.getCatalogName());
        }
        return null;
    }

    // --------------------------------------- Plugin Statement ---------------------------------------

    @Override
    public Void visitInstallPluginStatement(InstallPluginStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.PLUGIN);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.PLUGIN.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // ---------------------------------------- Show Node Info Statement-------------------------------
    @Override
    public Void visitShowBackendsStatement(ShowBackendsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.NODE);
            } catch (AccessDeniedException e2) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.OPERATE.name() + " OR " + PrivilegeType.NODE.name(),
                        ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitShowFrontendsStatement(ShowFrontendsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.NODE);
            } catch (AccessDeniedException e2) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.OPERATE.name() + " OR " + PrivilegeType.NODE.name(),
                        ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitShowBrokerStatement(ShowBrokerStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.NODE);
            } catch (AccessDeniedException e2) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.OPERATE.name() + " OR " + PrivilegeType.NODE.name(),
                        ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitShowComputeNodes(ShowComputeNodesStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.NODE);
            } catch (AccessDeniedException e2) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.OPERATE.name() + " OR " + PrivilegeType.NODE.name(),
                        ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.PLUGIN);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.PLUGIN.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowPluginsStatement(ShowPluginsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.PLUGIN);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.PLUGIN.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // --------------------------------------- File Statement ----------------------------------------

    @Override
    public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnOrInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    context.getCurrentCatalog(), statement.getDbName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.FILE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.FILE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnOrInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    context.getCurrentCatalog(), statement.getDbName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.FILE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.FILE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnOrInDb(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    context.getCurrentCatalog(), statement.getDbName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), statement.getDbName());
        }
        return null;
    }

    // --------------------------------------- Analyze related statements -----------------------------

    @Override
    public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext context) {
        Authorizer.checkActionForAnalyzeStatement(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getTableName());
        return null;
    }

    @Override
    public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext context) {
        Set<TableName> tableNames = AnalyzerUtils.getAllTableNamesForAnalyzeJobStmt(statement.getDbId(), statement.getTableId());
        tableNames.forEach(tableName -> Authorizer.checkActionForAnalyzeStatement(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), tableName));
        return null;
    }

    @Override
    public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext context) {
        Authorizer.checkActionForAnalyzeStatement(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getTableName());
        return null;
    }

    @Override
    public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext context) {
        Authorizer.checkActionForAnalyzeStatement(context.getCurrentUserIdentity(),
                context.getCurrentRoleIds(), statement.getTableName());
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
        // we will check it in the execution logic, not here, see `ShowExecutor#checkPrivilegeForKillAnalyzeStmt()`
        // for details.
        return null;
    }

    // --------------------------------------- Sql BlackList And WhiteList Statement ------------------

    @Override
    public Void visitAddSqlBlackListStatement(AddSqlBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // --------------------------------------- Backend BlackList ------------------------------------

    @Override
    public Void visitAddBackendBlackListStatement(AddBackendBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDelBackendBlackListStatement(DelBackendBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.BLACKLIST.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // ---------------------------------------- Privilege Statement -----------------------------------

    @Override
    public Void visitBaseCreateAlterUserStmt(BaseCreateAlterUserStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropUserStatement(DropUserStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowUserStatement(ShowUserStmt statement, ConnectContext context) {
        if (statement.isAll()) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
        UserIdentity user = statement.getUserIdent();
        if (user != null && !user.equals(context.getCurrentUserIdentity()) || statement.isAll()) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitExecuteAsStatement(ExecuteAsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkUserAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getToUser(),
                    PrivilegeType.IMPERSONATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.IMPERSONATE.name(), ObjectType.USER.name(), statement.getToUser().getUser());
        }
        return null;
    }

    @Override
    public Void visitExecuteScriptStatement(ExecuteScriptStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitCreateRoleStatement(CreateRoleStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAlterRoleStatement(AlterRoleStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropRoleStatement(DropRoleStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowRolesStatement(ShowRolesStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
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

        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
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

        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitSetDefaultRoleStatement(SetDefaultRoleStmt statement, ConnectContext context) {
        UserIdentity user = statement.getUserIdentity();
        if (user != null && !user.equals(context.getCurrentUserIdentity())) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitGrantRevokePrivilegeStatement(BaseGrantRevokePrivilegeStmt stmt, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
        } catch (AccessDeniedException e) {
            try {
                Authorizer.withGrantOption(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), stmt.getObjectType(),
                        stmt.getPrivilegeTypes(), stmt.getObjectList());
            } catch (AccessDeniedException e2) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext context) {
        UserIdentity user = statement.getUserIdent();
        try {
            if (user != null && !user.equals(context.getCurrentUserIdentity())) {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } else if (statement.getRole() != null) {
                AuthorizationMgr authorizationManager = context.getGlobalStateMgr().getAuthorizationMgr();
                List<String> roleNames = authorizationManager.getRoleNamesByUser(context.getCurrentUserIdentity());
                if (!roleNames.contains(statement.getRole())) {
                    Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.GRANT);
                }
            }
        } catch (AccessDeniedException | PrivilegeException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowUserPropertyStatement(ShowUserPropertyStmt statement, ConnectContext context) {
        String user = statement.getUser();
        if (user != null && !user.equals(context.getCurrentUserIdentity().getUser())) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
        }
        return null;
    }

    @Override
    public Void visitSetUserPropertyStatement(SetUserPropertyStmt statement, ConnectContext context) {
        String user = statement.getUser();
        if (user != null && !user.equals(context.getCurrentUserIdentity().getUser())) {
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.GRANT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
            }
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
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), catalog,
                    tableName.getDb(), PrivilegeType.CREATE_VIEW);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    catalog,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_VIEW.name(), ObjectType.DATABASE.name(), tableName.getDb());
        }
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
        try {
            Authorizer.checkViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTableName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.VIEW.name(), statement.getTableName().getTbl());
        }
        // 2. check if user can query
        AlterClause alterClause = statement.getAlterClause();
        if (alterClause instanceof AlterViewClause) {
            check(((AlterViewClause) alterClause).getQueryStatement(), context);
        }
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
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), catalog, dbName,
                    PrivilegeType.CREATE_TABLE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(catalog, context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_TABLE.name(), ObjectType.DATABASE.name(), dbName);
        }

        if (statement.getProperties() != null) {
            Map<String, String> properties = statement.getProperties();
            if (statement.getProperties().containsKey("resource")) {
                String resourceProp = properties.get("resource");
                Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceProp);
                if (resource != null) {
                    try {
                        Authorizer.checkResourceAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                resource.getName(), PrivilegeType.USAGE);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.USAGE.name(), ObjectType.RESOURCE.name(), resource.getName());
                    }
                }
            }
            if (statement.getProperties().containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
                String storageVolume = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
                try {
                    Authorizer.checkStorageVolumeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            storageVolume, PrivilegeType.USAGE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.USAGE.name(), ObjectType.STORAGE_VOLUME.name(), storageVolume);
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
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getExistedDbTbl(), PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getExistedDbTbl().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), statement.getExistedDbTbl().getTbl());
        }
        return null;
    }

    @Override
    public Void visitDropTableStatement(DropTableStmt statement, ConnectContext context) {
        if (statement.isView()) {
            try {
                Authorizer.checkViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        statement.getTbl(), PrivilegeType.DROP);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        statement.getTbl().getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.DROP.name(), ObjectType.VIEW.name(), statement.getTbl().getTbl());
            }
        } else {
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        statement.getTbl(), PrivilegeType.DROP);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        statement.getTbl().getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.DROP.name(), ObjectType.TABLE.name(), statement.getTbl().getTbl());
            }
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
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), catalog,
                    tableName.getDb(), PrivilegeType.CREATE_TABLE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    catalog,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_TABLE.name(), ObjectType.DATABASE.name(), tableName.getDb());
        }
        return null;
    }

    @Override
    public Void visitTruncateTableStatement(TruncateTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(context.getCurrentCatalog(), statement.getDbName(), statement.getTblName()),
                    PrivilegeType.DELETE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    context.getCurrentCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DELETE.name(), ObjectType.TABLE.name(), statement.getTblName());
        }
        return null;
    }

    @Override
    public Void visitRefreshTableStatement(RefreshTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTableName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getTbl(),
                    PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTbl().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.TABLE.name(), statement.getTbl().getTbl());
        }
        return null;
    }

    @Override
    public Void visitCancelAlterTableStatement(CancelAlterTableStmt statement, ConnectContext context) {
        if (statement.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            Database db = GlobalStateMgr.getCurrentState().getDb(statement.getDbName());
            if (db != null) {
                Locker locker = new Locker();
                try {
                    locker.lockDatabase(db, LockType.READ);
                    Table table = db.getTable(statement.getTableName());
                    if (table == null || !table.isMaterializedView()) {
                        // ignore privilege check for old mv
                        return null;
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }

            try {
                Authorizer.checkMaterializedViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        new TableName(statement.getDbName(), statement.getTableName()), PrivilegeType.ALTER);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.ALTER.name(), ObjectType.MATERIALIZED_VIEW.name(), statement.getTableName());
            }
        } else {
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        statement.getDbTableName(), PrivilegeType.ALTER);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        statement.getDbTableName().getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.ALTER.name(), ObjectType.TABLE.name(), statement.getTableName());
            }
        }
        return null;
    }

    @Override
    public Void visitDescTableStmt(DescribeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getDbTableName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getDbTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), statement.getDbTableName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitShowCreateTableStatement(ShowCreateTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), statement.getTbl());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTbl().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), statement.getTbl().getTbl());
        }
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
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTableName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitShowColumnStatement(ShowColumnStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTableName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), statement.getTableName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitRecoverPartitionStatement(RecoverPartitionStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getDbTblName(), PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getDbTblName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), statement.getTableName());
        }

        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getDbTblName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getDbTblName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.TABLE.name(), statement.getTableName());
        }
        return null;
    }

    @Override
    public Void visitShowPartitionsStatement(ShowPartitionsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    new TableName(statement.getDbName(), statement.getTableName()));
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), statement.getTableName());
        }
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
        // Privilege is checked in execution logic, see `ShowExecutor#handleShowTablet()` for details.
        return null;
    }

    // ---------------------------------------- Admin operate Statement --------------------------------

    @Override
    public Void visitAdminSetConfigStatement(AdminSetConfigStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminSetReplicaStatusStatement(AdminSetReplicaStatusStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminShowConfigStatement(AdminShowConfigStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                           ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminRepairTableStatement(AdminRepairTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminCancelRepairTableStatement(AdminCancelRepairTableStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAdminCheckTabletsStatement(AdminCheckTabletsStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitKillStatement(KillStmt statement, ConnectContext context) {
        // Privilege is checked in execution logic, see `StatementExecutor#handleKill()` for details.
        return null;
    }

    @Override
    public Void visitAlterSystemStatement(AlterSystemStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.NODE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.NODE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitCancelAlterSystemStatement(CancelAlterSystemStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.NODE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.NODE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowProcStmt(ShowProcStmt statement, ConnectContext context) {
        try {
            if (!SHOW_TABLET_DETAIL_CMD_PATTERN.matcher(statement.getPath()).matches()) {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.OPERATE);
            }
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
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

                    try {
                        Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.GRANT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.GRANT.name(), ObjectType.SYSTEM.name(), null);
                    }
                }
            } else if (setVar instanceof SystemVariable) {
                SetType type = ((SystemVariable) setVar).getType();
                if (type != null && type.equals(SetType.GLOBAL)) {
                    try {
                        Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.OPERATE);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                    }
                }
            }
        });
        return null;
    }

    // ---------------------------------------- restore & backup Statement --------------------------------
    @Override
    public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getTblName(), PrivilegeType.EXPORT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getTblName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.EXPORT.name(), ObjectType.TABLE.name(), statement.getTblName().getTbl());
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
        try {
            Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    exportJob.getTableName().getDb(), exportJob.getTableName().getTbl(), PrivilegeType.EXPORT);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    exportJob.getTableName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.EXPORT.name(), ObjectType.TABLE.name(), exportJob.getTableName().getTbl());
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
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitDropRepositoryStatement(DropRepositoryStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitShowSnapshotStatement(ShowSnapshotStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitBackupStatement(BackupStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        List<TableRef> tableRefs = statement.getTableRefs();
        if (tableRefs.size() == 0) {
            String dBName = statement.getDbName();
            throw new SemanticException("Database: %s is empty", dBName);
        }
        tableRefs.forEach(tableRef -> {
            TableName tableName = tableRef.getName();
            try {
                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), tableName,
                        PrivilegeType.EXPORT);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        tableName.getCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.EXPORT.name(), ObjectType.TABLE.name(), tableName.getTbl());
            }
        });
        return null;
    }

    @Override
    public Void visitShowBackupStatement(ShowBackupStmt statement, ConnectContext context) {
        // Step 1 check system.Repository
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        // Step 2 check table.export
        // `show backup` only show tables that user has export privilege on, we will check it in
        // the execution logic, not here, see `ShowExecutor#handleShowBackup()` for details.
        return null;
    }

    @Override
    public Void visitCancelBackupStatement(CancelBackupStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
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
                try {
                    Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), tableName,
                            PrivilegeType.EXPORT);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            tableName.getCatalog(),
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.EXPORT.name(), ObjectType.TABLE.name(), tableName.getTbl());
                }
            });
        }
        return null;
    }

    @Override
    public Void visitRestoreStatement(RestoreStmt statement, ConnectContext context) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        // check repository on system
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }

        List<TableRef> tableRefs = statement.getTableRefs();
        // check create_database on current catalog if we're going to restore the whole database
        if (tableRefs == null || tableRefs.isEmpty()) {
            try {
                Authorizer.checkCatalogAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        context.getCurrentCatalog(), PrivilegeType.CREATE_DATABASE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        context.getCurrentCatalog(),
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.CREATE_DATABASE.name(), ObjectType.CATALOG.name(), context.getCurrentCatalog());
            }
        } else {
            // going to restore some tables in database or some partitions in table
            Database db = globalStateMgr.getDb(statement.getDbName());
            Locker locker = new Locker();
            if (db != null) {
                try {
                    locker.lockDatabase(db, LockType.READ);
                    // check create_table on specified database
                    try {
                        Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                context.getCurrentCatalog(), db.getFullName(), PrivilegeType.CREATE_TABLE);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                context.getCurrentCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.CREATE_TABLE.name(), ObjectType.DATABASE.name(), db.getFullName());
                    }
                    // check insert on specified table
                    for (TableRef tableRef : tableRefs) {
                        Table table = db.getTable(tableRef.getName().getTbl());
                        if (table != null) {
                            try {
                                Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                        new TableName(statement.getDbName(), tableRef.getName().getTbl()), PrivilegeType.INSERT);
                            } catch (AccessDeniedException e) {
                                AccessDeniedException.reportAccessDenied(
                                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                        context.getCurrentUserIdentity(),
                                        context.getCurrentRoleIds(), PrivilegeType.INSERT.name(), ObjectType.TABLE.name(),
                                        tableRef.getName().getTbl());
                            }
                        }
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
            }
        }

        return null;
    }

    @Override
    public Void visitShowRestoreStatement(ShowRestoreStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.REPOSITORY);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REPOSITORY.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    // ---------------------------------------- Materialized View stmt --------------------------------
    @Override
    public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                     ConnectContext context) {
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    statement.getTableName().getDb(), PrivilegeType.CREATE_MATERIALIZED_VIEW);
            visitQueryStatement(statement.getQueryStatement(), context);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_MATERIALIZED_VIEW.name(), ObjectType.DATABASE.name(), statement.getTableName().getDb());
        }
        return null;
    }

    @Override
    public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, ConnectContext context) {
        try {
            Authorizer.checkMaterializedViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getMvName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getMvName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.MATERIALIZED_VIEW.name(), statement.getMvName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement, ConnectContext context) {
        try {
            Authorizer.checkMaterializedViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getMvName(), PrivilegeType.REFRESH);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getMvName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REFRESH.name(), ObjectType.MATERIALIZED_VIEW.name(), statement.getMvName().getTbl());
        }
        return null;
    }

    @Override
    public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement, ConnectContext context) {
        try {
            Authorizer.checkMaterializedViewAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getMvName(), PrivilegeType.REFRESH);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    statement.getMvName().getCatalog(),
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.REFRESH.name(), ObjectType.MATERIALIZED_VIEW.name(), statement.getMvName().getTbl());
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
            try {
                Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.CREATE_GLOBAL_FUNCTION);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.CREATE_GLOBAL_FUNCTION.name(), ObjectType.SYSTEM.name(), null);
            }
        } else {
            try {
                Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, name.getDb(), PrivilegeType.CREATE_FUNCTION);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.CREATE_FUNCTION.name(), ObjectType.DATABASE.name(), name.getDb());
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
            if (function != null) {
                try {
                    Authorizer.checkGlobalFunctionAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            function, PrivilegeType.DROP);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.DROP.name(), ObjectType.GLOBAL_FUNCTION.name(), function.getSignature());
                }
            }
            return null;
        }

        // db function.
        Database db = GlobalStateMgr.getCurrentState().getDb(functionName.getDb());
        if (db != null) {
            Locker locker = new Locker();
            try {
                locker.lockDatabase(db, LockType.READ);
                Function function = db.getFunction(statement.getFunctionSearchDesc());
                if (null != function) {
                    try {
                        Authorizer.checkFunctionAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                db, function, PrivilegeType.DROP);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.DROP.name(), ObjectType.FUNCTION.name(), function.getSignature());
                    }
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        return null;
    }

    // ------------------------------------------- Storage volume Statement ----------------------------------------
    @Override
    public Void visitCreateStorageVolumeStatement(CreateStorageVolumeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_STORAGE_VOLUME);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_STORAGE_VOLUME.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    @Override
    public Void visitAlterStorageVolumeStatement(AlterStorageVolumeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkStorageVolumeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.STORAGE_VOLUME.name(), statement.getName());
        }
        return null;
    }

    @Override
    public Void visitDropStorageVolumeStatement(DropStorageVolumeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkStorageVolumeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getName(), PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.STORAGE_VOLUME.name(), statement.getName());
        }
        return null;
    }

    @Override
    public Void visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkAnyActionOnStorageVolume(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getName());
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.STORAGE_VOLUME.name(), statement.getName());
        }
        return null;
    }

    @Override
    public Void visitSetDefaultStorageVolumeStatement(SetDefaultStorageVolumeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkStorageVolumeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    statement.getName(), PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.STORAGE_VOLUME.name(), statement.getName());
        }
        return null;
    }

    // -------------------------------------- Pipe Statement ---------------------------------------- //
    @Override
    public Void visitCreatePipeStatement(CreatePipeStmt statement, ConnectContext context) {
        try {
            Authorizer.checkDbAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    statement.getPipeName().getDbName(), PrivilegeType.CREATE_PIPE);
            visitInsertStatement(statement.getInsertStmt(), context);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.CREATE_PIPE.name(), ObjectType.DATABASE.name(), statement.getPipeName().getDbName());
        }
        return null;
    }

    @Override
    public Void visitDropPipeStatement(DropPipeStmt statement, ConnectContext context) {
        PipeName pipeName = statement.getPipeName();
        try {
            Authorizer.checkPipeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    pipeName, PrivilegeType.DROP);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.DROP.name(), ObjectType.PIPE.name(), pipeName.toString());
        }
        return null;
    }

    @Override
    public Void visitAlterPipeStatement(AlterPipeStmt statement, ConnectContext context) {
        PipeName pipeName = statement.getPipeName();
        try {
            Authorizer.checkPipeAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), pipeName,
                    PrivilegeType.ALTER);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ALTER.name(), ObjectType.PIPE.name(), pipeName.toString());
        }
        return null;
    }

    @Override
    public Void visitDescPipeStatement(DescPipeStmt statement, ConnectContext context) {
        PipeName pipeName = statement.getName();
        try {
            Authorizer.checkAnyActionOnPipe(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), pipeName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.PIPE.name(), pipeName.toString());
        }
        return null;
    }

    @Override
    public Void visitShowPipeStatement(ShowPipeStmt statement, ConnectContext context) {
        // show pipes with privilege, handled in ShowExecutor.handleShowPipes
        return null;
    }

    // --------------------------------- Compaction Statement ---------------------------------

    @Override
    public Void visitCancelCompactionStatement(CancelCompactionStmt statement, ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                    PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
        }
        return null;
    }

    private String getTableNameByRoutineLoadLabel(ConnectContext context,
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

    private void checkOperateLoadPrivilege(ConnectContext context, String dbName, String label) {
        GlobalStateMgr globalStateMgr = context.getGlobalStateMgr();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_DB_NOT_FOUND, dbName);
        }
        List<LoadJob> loadJobs = globalStateMgr.getLoadMgr().
                getLoadJobsByDb(db.getId(), label, false);
        loadJobs.forEach(loadJob -> {
            try {
                if (loadJob instanceof SparkLoadJob) {
                    try {
                        Authorizer.checkResourceAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                loadJob.getResourceName(), PrivilegeType.USAGE);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.USAGE.name(), ObjectType.RESOURCE.name(), loadJob.getResourceName());
                    }
                }
                loadJob.getTableNames(true).forEach(tableName -> {
                    try {
                        Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                dbName, tableName, PrivilegeType.INSERT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.INSERT.name(), ObjectType.TABLE.name(), tableName);
                    }
                });
            } catch (MetaNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
