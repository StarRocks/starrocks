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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.StatsConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PrivilegeChecker {
    private static final PrivilegeChecker INSTANCE = new PrivilegeChecker(new PrivilegeCheckerVisitor());

    public static PrivilegeChecker getInstance() {
        return INSTANCE;
    }

    private final PrivilegeCheckerVisitor privilegeCheckerVisitor;

    private PrivilegeChecker(PrivilegeCheckerVisitor privilegeCheckerVisitor) {
        this.privilegeCheckerVisitor = privilegeCheckerVisitor;
    }

    public static void check(StatementBase statement, ConnectContext context) {
        getInstance().privilegeCheckerVisitor.check(statement, context);
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

    private static void checkTblPrivilegeForKillAnalyzeStmt(ConnectContext context, String catalogName, String dbName,
                                                            String tableName, long analyzeId) {
        Database db = MetaUtils.getDatabase(catalogName, dbName);
        Table table = MetaUtils.getTable(catalogName, dbName, tableName);
        if (db != null && table != null) {
            if (!PrivilegeActions.checkTableAction(context, catalogName, dbName, tableName, PrivilegeType.SELECT) ||
                    !PrivilegeActions.checkTableAction(context, catalogName, dbName, tableName, PrivilegeType.INSERT)
            ) {
                throw new SemanticException(String.format(
                        "You need SELECT and INSERT action on %s.%s.%s to kill analyze job %d",
                        catalogName, dbName, tableName, analyzeId));
            }
        }
    }

    public static void checkPrivilegeForKillAnalyzeStmt(ConnectContext context, long analyzeId) {
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
        AnalyzeStatus analyzeStatus = analyzeManager.getAnalyzeStatus(analyzeId);
        AnalyzeJob analyzeJob = analyzeManager.getAnalyzeJob(analyzeId);
        if (analyzeStatus != null) {
            try {
                String catalogName = analyzeStatus.getCatalogName();
                String dbName = analyzeStatus.getDbName();
                String tableName = analyzeStatus.getTableName();
                checkTblPrivilegeForKillAnalyzeStmt(context, catalogName, dbName, tableName, analyzeId);
            } catch (MetaNotFoundException ignore) {
                // If the db or table doesn't exist anymore, we won't check privilege on it
            }
        } else if (analyzeJob != null) {
            Set<TableName> tableNames = PrivilegeChecker.getAllTableNamesForAnalyzeJobStmt(analyzeJob.getDbId(),
                    analyzeJob.getTableId());
            tableNames.forEach(tableName -> {
                checkTblPrivilegeForKillAnalyzeStmt(context, tableName.getCatalog(), tableName.getDb(),
                        tableName.getTbl(), analyzeId);
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

    static void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolumeName) {
        if (!PrivilegeActions.checkAnyActionOnStorageVolume(context, storageVolumeName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_PRIVILEGE_STORAGE_VOLUME_DENIED,
                    context.getQualifiedUser(), storageVolumeName);
        }
    }
}
