// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.privilege.PrivilegeManager;
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
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.BaseCreateAlterUserStmt;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.BaseGrantRevokeRoleStmt;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelLoadStmt;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateFileStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateRoleStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DelSqlBlackListStmt;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFileStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.DropUserStmt;
import com.starrocks.sql.ast.ExecuteAsStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KillAnalyzeStmt;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PauseRoutineLoadStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SetUserPropertyStmt;
import com.starrocks.sql.ast.SetVar;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowPluginsStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeManager;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.StatsConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
        checkTableAction(context, tableName.getDb(), tableName.getTbl(), action);
    }

    public static void checkTableAction(ConnectContext context,
                                        String dbName, String tableName,
                                        PrivilegeType.TableAction action) {
        String actionStr = action.toString();
        if (!PrivilegeManager.checkTableAction(context, dbName, tableName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                                actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkAnyActionOnTable(ConnectContext context, TableName tableName) {
        if (!CatalogMgr.isInternalCatalog(tableName.getCatalog())) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }
        checkAnyActionOnTable(context, tableName.getDb(), tableName.getTbl());
    }

    static void checkAnyActionOnTable(ConnectContext context, String dbName, String tableName) {
        if (!PrivilegeManager.checkAnyActionOnTable(context, dbName, tableName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_TABLE_DENIED,
                                                context.getQualifiedUser(), tableName);
        }
    }

    static String getTableNameByRoutineLoadLabel(ConnectContext context,
                                                 String dbName, String labelName) {
        RoutineLoadJob job = null;
        String tableName = null;
        try {
            job = context.getGlobalStateMgr().getRoutineLoadManager().getJob(dbName, labelName);
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
                              PrivilegeType.DbAction action) {
        if (!CatalogMgr.isInternalCatalog(catalogName)) {
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }
        if (!PrivilegeManager.checkDbAction(context, dbName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    context.getQualifiedUser(), dbName);
        }
    }

    static void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType.CatalogAction action) {
        if (!PrivilegeManager.checkCatalogAction(context, catalogName, action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    context.getQualifiedUser(), catalogName);
        }
    }

    static void checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        if (!PrivilegeManager.checkAnyActionOnCatalog(context, catalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    context.getQualifiedUser(), catalogName);
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
            throw new SemanticException(EXTERNAL_CATALOG_NOT_SUPPORT_ERR_MSG);
        }
        String actionStr = action.toString();
        if (!PrivilegeManager.checkViewAction(context, tableName.getDb(), tableName.getTbl(), action)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    actionStr, context.getQualifiedUser(), context.getRemoteIP(), tableName);
        }
    }

    static void checkStmtOperatePrivilege(ConnectContext context) {
        if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.OPERATE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "OPERATE");
        }
    }

    static void checkStmtNodePrivilege(ConnectContext context) {
        if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.OPERATE)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "NODE");
        }
    }

    public static Set<TableName> getAllTableNamesForAnalyzeJobStmt(long dbId, long tableId) {
        Set<TableName> tableNames = Sets.newHashSet();
        if (StatsConstants.DEFAULT_ALL_ID != tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null && !db.isInfoSchemaDb()) {
                Table table = db.getTable(tableId);
                if (table != null) {
                    tableNames.add(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getFullName(), table.getName()));
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            getTableNamesInDb(tableNames, dbId);
        } else if (dbId == StatsConstants.DEFAULT_ALL_ID) {
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
            for (Long id : dbIds) {
                getTableNamesInDb(tableNames, id);
            }
        }

        return tableNames;
    }

    private static void getTableNamesInDb(Set<TableName> tableNames, Long id) {
        Database db = GlobalStateMgr.getCurrentState().getDb(id);
        if (db != null && !db.isInfoSchemaDb()) {
            for (Table table : db.getTables()) {
                TableName tableNameNew = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        db.getFullName(), table.getName());
                tableNames.add(tableNameNew);
            }
        }
    }

    private static void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, Database db,
                                                            Table table, long analyzeId) {
        if (db != null && table != null) {
            if (!PrivilegeManager.checkTableAction(context, db.getOriginName(),
                    table.getName(), PrivilegeType.TableAction.SELECT) ||
                    !PrivilegeManager.checkTableAction(context, db.getOriginName(),
                            table.getName(), PrivilegeType.TableAction.INSERT)
            ) {
                throw new SemanticException(String.format(
                        "You need SELECT and INSERT action on %s.%s to kill analyze job %d",
                        db.getOriginName(), table.getName(), analyzeId));
            }
        }
    }

    public static void checkPrivilegeForKillAnalyzeStmt(ConnectContext context, long analyzeId) {
        AnalyzeManager analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
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
        List<LoadJob> loadJobs = globalStateMgr.getLoadManager().
                                                getLoadJobsByDb(db.getId(), label, false);
        List<String> forbiddenInsertTableList = new ArrayList<>();
        List<String> forbiddenUseResourceList = new ArrayList<>();
        loadJobs.forEach(loadJob -> {
            try {
                if (loadJob instanceof SparkLoadJob &&
                        !PrivilegeManager.checkResourceAction(context, loadJob.getResourceName(),
                                                              PrivilegeType.ResourceAction.USAGE)) {
                    forbiddenUseResourceList.add(loadJob.getResourceName());
                }
                loadJob.getTableNames().forEach(tableName -> {
                    if (!PrivilegeManager.checkTableAction(context, dbName, tableName,
                                                           PrivilegeType.TableAction.INSERT)) {
                        forbiddenInsertTableList.add(tableName);
                    }
                });
            } catch (MetaNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
        if (forbiddenUseResourceList.size() > 0) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_ACCESS_RESOURCE_DENIED,
                                                PrivilegeType.ResourceAction.USAGE.toString(),
                                                context.getQualifiedUser(),
                                                context.getRemoteIP(),
                                                forbiddenUseResourceList.toString());
        }
        if (forbiddenInsertTableList.size() > 0) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                                PrivilegeType.TableAction.INSERT.toString(),
                                                context.getQualifiedUser(),
                                                context.getRemoteIP(),
                                                forbiddenInsertTableList.toString());
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
        public Void visitUpdateStatement(UpdateStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.UPDATE);
            return null;
        }

        // --------------------------------- Routine Load Statement ---------------------------------
        public Void visitCreateRoutineLoadStatement(CreateRoutineLoadStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getDBName(), statement.getTableName(),
                                          PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitAlterRoutineLoadStatement(AlterRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbName(), statement.getLabel());
            checkTableAction(context, statement.getDbName(), tableName, PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitStopRoutineLoadStatement(StopRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitPauseRoutineLoadStatement(PauseRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitResumeRoutineLoadStatement(ResumeRoutineLoadStmt statement, ConnectContext context) {
            String tableName = getTableNameByRoutineLoadLabel(context, statement.getDbFullName(), statement.getName());
            checkTableAction(context, statement.getDbFullName(), tableName, PrivilegeType.TableAction.INSERT);
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
                if (!PrivilegeManager.checkResourceAction(context, resourceName, PrivilegeType.ResourceAction.USAGE)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "USAGE");
                }
            }
            // check table privilege
            String dbName = statement.getLabel().getDbName();
            List<String> forbiddenInsertTableList = new ArrayList<>();
            statement.getDataDescriptions().forEach(dataDescription -> {
                String tableName = dataDescription.getTableName();
                if (!PrivilegeManager.checkTableAction(context, dbName, tableName, PrivilegeType.TableAction.INSERT)) {
                    forbiddenInsertTableList.add(tableName);
                }
            });
            if (forbiddenInsertTableList.size() > 0) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                                                    PrivilegeType.TableAction.INSERT.toString(),
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
            // Also need to check the `CREATE_DATABASE` action on corresponding catalog
            checkCatalogAction(context, statement.getCatalogName(), PrivilegeType.CatalogAction.CREATE_DATABASE);
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

        // --------------------------------- External Resource Statement ----------------------------------

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

        // --------------------------------- Catalog Statement --------------------------------------------

        @Override
        public Void visitUseCatalogStatement(UseCatalogStmt statement, ConnectContext context) {
            // No authorization check for using default_catalog
            if (CatalogMgr.isInternalCatalog(statement.getCatalogName())) {
                return null;
            }
            if (!PrivilegeManager.checkAnyActionOnCatalog(context, statement.getCatalogName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), statement.getCatalogName());
            }
            return null;
        }

        @Override
        public Void visitCreateCatalogStatement(CreateCatalogStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.CREATE_EXTERNAL_CATALOG)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                        context.getQualifiedUser(), statement.getCatalogName());
            }
            return null;
        }

        @Override
        public Void visitDropCatalogStatement(DropCatalogStmt statement, ConnectContext context) {
            if (!PrivilegeManager.checkCatalogAction(context, statement.getName(), PrivilegeType.CatalogAction.DROP)) {
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
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
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
            if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.OPERATE)
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.NODE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "OPERATE/NODE");
            }
            return null;
        }

        @Override
        public Void visitUninstallPluginStatement(UninstallPluginStmt statement, ConnectContext context) {
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        @Override
        public Void visitShowPluginsStatement(ShowPluginsStmt statement, ConnectContext context) {
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.PLUGIN)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "PLUGIN");
            }
            return null;
        }

        // --------------------------------------- File Statement ----------------------------------------

        @Override
        public Void visitCreateFileStatement(CreateFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitDropFileStatement(DropFileStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.FILE)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "FILE");
            }
            return null;
        }

        @Override
        public Void visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            checkAnyActionOnOrUnderDb(context, context.getCurrentCatalog(), statement.getDbName());
            return null;
        }

        // --------------------------------------- Analyze related statements -----------------------------

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext context) {
            Set<TableName> tableNames = getAllTableNamesForAnalyzeJobStmt(statement.getDbId(), statement.getTableId());
            tableNames.forEach(tableName -> {
                checkTableAction(context, tableName, PrivilegeType.TableAction.SELECT);
                checkTableAction(context, tableName, PrivilegeType.TableAction.INSERT);
            });
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.INSERT);
            return null;
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext context) {
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.SELECT);
            checkTableAction(context, statement.getTableName(), PrivilegeType.TableAction.INSERT);
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
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitDelSqlBlackListStatement(DelSqlBlackListStmt statement, ConnectContext context) {
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        @Override
        public Void visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, ConnectContext context) {
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.BLACKLIST)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "BLACKLIST");
            }
            return null;
        }

        // ---------------------------------------- Privilege Statement -----------------------------------

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
            if (! PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
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

        @Override
        public Void visitSetUserPropertyStatement(SetUserPropertyStmt statement, ConnectContext context) {
            String user = statement.getUser();
            if (user != null && !user.equals(context.getCurrentUserIdentity().getQualifiedUser())
                    && !PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
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
            checkDbAction(context, catalog, tableName.getDb(), PrivilegeType.DbAction.CREATE_VIEW);
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
            checkViewAction(context, statement.getTableName(), PrivilegeType.ViewAction.ALTER);
            // 2. check if user can query
            check(statement.getQueryStatement(), context);
            return null;
        }

        // ---------------------------------------- Table Statement ---------------------------------------

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
            checkStmtOperatePrivilege(context);
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
        public Void visitSetStatement(SetStmt statement, ConnectContext context) {
            List<SetVar> varList = statement.getSetVars();
            varList.forEach(setVer -> {
                if ((setVer instanceof SetPassVar)) {
                    UserIdentity prepareChangeUser = ((SetPassVar) setVer).getUserIdent();
                    try {
                        prepareChangeUser.analyze();
                    } catch (AnalysisException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_ERROR, "ANALYZE ERROR");
                    }
                    if (!context.getUserIdentity().equals(prepareChangeUser)) {
                        if (!PrivilegeManager.checkSystemAction(context, PrivilegeType.SystemAction.GRANT)) {
                            ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "GRANT");
                        }
                    }
                    return;
                }
                if (setVer.getType().equals(SetType.GLOBAL)) {
                    checkStmtOperatePrivilege(context);
                }
            });
            return null;
        }
    }
}
