// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ShowExecutor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AdminShowConfigStmt;
import com.starrocks.analysis.AdminShowReplicaDistributionStmt;
import com.starrocks.analysis.AdminShowReplicaStatusStmt;
import com.starrocks.analysis.DescribeStmt;
import com.starrocks.analysis.HelpStmt;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.ShowAuthenticationStmt;
import com.starrocks.analysis.ShowAuthorStmt;
import com.starrocks.analysis.ShowBackendsStmt;
import com.starrocks.analysis.ShowBackupStmt;
import com.starrocks.analysis.ShowBrokerStmt;
import com.starrocks.analysis.ShowClusterStmt;
import com.starrocks.analysis.ShowCollationStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.ShowCreateTableStmt;
import com.starrocks.analysis.ShowDataStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowDeleteStmt;
import com.starrocks.analysis.ShowDynamicPartitionStmt;
import com.starrocks.analysis.ShowEnginesStmt;
import com.starrocks.analysis.ShowExportStmt;
import com.starrocks.analysis.ShowFrontendsStmt;
import com.starrocks.analysis.ShowFunctionsStmt;
import com.starrocks.analysis.ShowGrantsStmt;
import com.starrocks.analysis.ShowIndexStmt;
import com.starrocks.analysis.ShowLoadStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowMigrationsStmt;
import com.starrocks.analysis.ShowPartitionsStmt;
import com.starrocks.analysis.ShowPluginsStmt;
import com.starrocks.analysis.ShowProcStmt;
import com.starrocks.analysis.ShowProcesslistStmt;
import com.starrocks.analysis.ShowRepositoriesStmt;
import com.starrocks.analysis.ShowResourcesStmt;
import com.starrocks.analysis.ShowRestoreStmt;
import com.starrocks.analysis.ShowRolesStmt;
import com.starrocks.analysis.ShowRoutineLoadStmt;
import com.starrocks.analysis.ShowRoutineLoadTaskStmt;
import com.starrocks.analysis.ShowSmallFilesStmt;
import com.starrocks.analysis.ShowSnapshotStmt;
import com.starrocks.analysis.ShowSqlBlackListStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowTabletStmt;
import com.starrocks.analysis.ShowTransactionStmt;
import com.starrocks.analysis.ShowUserPropertyStmt;
import com.starrocks.analysis.ShowUserStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.analysis.ShowWorkGroupStmt;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MetadataViewer;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.cluster.BaseParam;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.BackendsProcDir;
import com.starrocks.common.proc.FrontendsProcNode;
import com.starrocks.common.proc.PartitionsProcDir;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.proc.TabletsProcDir;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.load.DeleteHandler;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.Load;
import com.starrocks.load.routineload.RoutineLoadFunctionalExprProvider;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.meta.BlackListSql;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.ShowAnalyzeStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.transaction.GlobalTransactionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// Execute one show statement.
public class ShowExecutor {
    private static final Logger LOG = LogManager.getLogger(ShowExecutor.class);
    private static final List<List<String>> EMPTY_SET = Lists.newArrayList();

    private ConnectContext ctx;
    private ShowStmt stmt;
    private ShowResultSet resultSet;
    private MetadataMgr metadataMgr;

    public ShowExecutor(ConnectContext ctx, ShowStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
        resultSet = null;
        metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
    }

    public ShowResultSet execute() throws AnalysisException, DdlException {
        if (stmt instanceof ShowMaterializedViewStmt) {
            handleShowMaterializedView();
        } else if (stmt instanceof ShowAuthorStmt) {
            handleShowAuthor();
        } else if (stmt instanceof ShowProcStmt) {
            handleShowProc();
        } else if (stmt instanceof HelpStmt) {
            handleHelp();
        } else if (stmt instanceof ShowDbStmt) {
            handleShowDb();
        } else if (stmt instanceof ShowTableStmt) {
            handleShowTable();
        } else if (stmt instanceof ShowTableStatusStmt) {
            handleShowTableStatus();
        } else if (stmt instanceof DescribeStmt) {
            handleDescribe();
        } else if (stmt instanceof ShowCreateTableStmt) {
            handleShowCreateTable();
        } else if (stmt instanceof ShowCreateDbStmt) {
            handleShowCreateDb();
        } else if (stmt instanceof ShowProcesslistStmt) {
            handleShowProcesslist();
        } else if (stmt instanceof ShowEnginesStmt) {
            handleShowEngines();
        } else if (stmt instanceof ShowFunctionsStmt) {
            handleShowFunctions();
        } else if (stmt instanceof ShowVariablesStmt) {
            handleShowVariables();
        } else if (stmt instanceof ShowColumnStmt) {
            handleShowColumn();
        } else if (stmt instanceof ShowLoadStmt) {
            handleShowLoad();
        } else if (stmt instanceof ShowRoutineLoadStmt) {
            handleShowRoutineLoad();
        } else if (stmt instanceof ShowRoutineLoadTaskStmt) {
            handleShowRoutineLoadTask();
        } else if (stmt instanceof ShowDeleteStmt) {
            handleShowDelete();
        } else if (stmt instanceof ShowAlterStmt) {
            handleShowAlter();
        } else if (stmt instanceof ShowUserPropertyStmt) {
            handleShowUserProperty();
        } else if (stmt instanceof ShowDataStmt) {
            handleShowData();
        } else if (stmt instanceof ShowCollationStmt) {
            handleShowCollation();
        } else if (stmt instanceof ShowPartitionsStmt) {
            handleShowPartitions();
        } else if (stmt instanceof ShowTabletStmt) {
            handleShowTablet();
        } else if (stmt instanceof ShowBackupStmt) {
            handleShowBackup();
        } else if (stmt instanceof ShowRestoreStmt) {
            handleShowRestore();
        } else if (stmt instanceof ShowClusterStmt) {
            handleShowCluster();
        } else if (stmt instanceof ShowMigrationsStmt) {
            handleShowMigrations();
        } else if (stmt instanceof ShowBrokerStmt) {
            handleShowBroker();
        } else if (stmt instanceof ShowResourcesStmt) {
            handleShowResources();
        } else if (stmt instanceof ShowExportStmt) {
            handleShowExport();
        } else if (stmt instanceof ShowBackendsStmt) {
            handleShowBackends();
        } else if (stmt instanceof ShowFrontendsStmt) {
            handleShowFrontends();
        } else if (stmt instanceof ShowRepositoriesStmt) {
            handleShowRepositories();
        } else if (stmt instanceof ShowSnapshotStmt) {
            handleShowSnapshot();
        } else if (stmt instanceof ShowGrantsStmt) {
            handleShowGrants();
        } else if (stmt instanceof ShowRolesStmt) {
            handleShowRoles();
        } else if (stmt instanceof AdminShowReplicaStatusStmt) {
            handleAdminShowTabletStatus();
        } else if (stmt instanceof AdminShowReplicaDistributionStmt) {
            handleAdminShowTabletDistribution();
        } else if (stmt instanceof AdminShowConfigStmt) {
            handleAdminShowConfig();
        } else if (stmt instanceof ShowSmallFilesStmt) {
            handleShowSmallFiles();
        } else if (stmt instanceof ShowDynamicPartitionStmt) {
            handleShowDynamicPartition();
        } else if (stmt instanceof ShowIndexStmt) {
            handleShowIndex();
        } else if (stmt instanceof ShowTransactionStmt) {
            handleShowTransaction();
        } else if (stmt instanceof ShowPluginsStmt) {
            handleShowPlugins();
        } else if (stmt instanceof ShowSqlBlackListStmt) {
            handleShowSqlBlackListStmt();
        } else if (stmt instanceof ShowAnalyzeStmt) {
            handleShowAnalyze();
        } else if (stmt instanceof ShowWorkGroupStmt) {
            handleShowWorkGroup();
        } else if (stmt instanceof ShowUserStmt) {
            handleShowUser();
        } else if (stmt instanceof ShowCatalogsStmt) {
            handleShowCatalogs();
        } else if (stmt instanceof ShowAuthenticationStmt) {
            handleShowAuthentication();
        } else {
            handleEmtpy();
        }

        return resultSet;
    }

    private void handleShowAuthentication() {
        final ShowAuthenticationStmt showAuthenticationStmt = (ShowAuthenticationStmt) stmt;
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getAuth().getAuthenticationInfo(
                showAuthenticationStmt.getUserIdent());
        resultSet = new ShowResultSet(showAuthenticationStmt.getMetaData(), rows);
    }

    private void handleShowMaterializedView() throws AnalysisException {
        ShowMaterializedViewStmt showMaterializedViewStmt = (ShowMaterializedViewStmt) stmt;
        String dbName = showMaterializedViewStmt.getDb();
        List<List<String>> rowSets = Lists.newArrayList();

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        db.readLock();
        try {
            PatternMatcher matcher = null;
            if (showMaterializedViewStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showMaterializedViewStmt.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }
            for (Table materializedView : db.getMaterializedViews()) {
                if (matcher != null && !matcher.match(materializedView.getName())) {
                    continue;
                }
                MaterializedView mvTable = (MaterializedView) materializedView;
                List<String> resultRow = Lists.newArrayList(String.valueOf(mvTable.getId()), mvTable.getName(), dbName,
                        mvTable.getViewDefineSql(), String.valueOf(mvTable.getRowCount()));
                rowSets.add(resultRow);
            }
            for (Table table : db.getTables()) {
                if (table.getType() == Table.TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) table;
                    List<MaterializedIndex> visibleMaterializedViews = olapTable.getVisibleIndex();
                    long baseIdx = olapTable.getBaseIndexId();

                    for (MaterializedIndex mvIdx : visibleMaterializedViews) {
                        if (baseIdx == mvIdx.getId()) {
                            continue;
                        }
                        if (matcher != null && !matcher.match(olapTable.getIndexNameById(mvIdx.getId()))) {
                            continue;
                        }
                        ArrayList<String> resultRow = new ArrayList<>();
                        MaterializedIndexMeta mvMeta = olapTable.getVisibleIndexIdToMeta().get(mvIdx.getId());
                        resultRow.add(String.valueOf(mvIdx.getId()));
                        resultRow.add(olapTable.getIndexNameById(mvIdx.getId()));
                        resultRow.add(dbName);
                        if (mvMeta.getOriginStmt() == null) {
                            StringBuilder originStmtBuilder = new StringBuilder(
                                    "create materialized view " + olapTable.getIndexNameById(mvIdx.getId()) +
                                            " as select ");
                            String groupByString = "";
                            for (Column column : mvMeta.getSchema()) {
                                if (column.isKey()) {
                                    groupByString += column.getName() + ",";
                                }
                            }
                            originStmtBuilder.append(groupByString);
                            for (Column column : mvMeta.getSchema()) {
                                if (!column.isKey()) {
                                    originStmtBuilder.append(column.getAggregationType().toString()).append("(")
                                            .append(column.getName()).append(")").append(",");
                                }
                            }
                            originStmtBuilder.delete(originStmtBuilder.length() - 1, originStmtBuilder.length());
                            originStmtBuilder.append(" from ").append(olapTable.getName()).append(" group by ")
                                    .append(groupByString);
                            originStmtBuilder.delete(originStmtBuilder.length() - 1, originStmtBuilder.length());
                            resultRow.add(originStmtBuilder.toString());
                        } else {
                            resultRow.add(mvMeta.getOriginStmt().replace("\n", "").replace("\t", "")
                                    .replaceAll("[ ]+", " "));
                        }
                        resultRow.add(String.valueOf(mvIdx.getRowCount()));
                        rowSets.add(resultRow);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
        resultSet = new ShowResultSet(stmt.getMetaData(), rowSets);
    }

    // Handle show process list
    private void handleShowProcesslist() {
        ShowProcesslistStmt showStmt = (ShowProcesslistStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();

        List<ConnectContext.ThreadInfo> threadInfos = ctx.getConnectScheduler().listConnection(ctx.getQualifiedUser());
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(nowMs, showStmt.showFull()));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private void handleShowUser() {
        List<List<String>> rowSet = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        row.add(ctx.getQualifiedUser());
        rowSet.add(row);
        resultSet = new ShowResultSet(stmt.getMetaData(), rowSet);
    }

    // Handle show authors
    private void handleEmtpy() {
        // Only success
        resultSet = new ShowResultSet(stmt.getMetaData(), EMPTY_SET);
    }

    // Handle show authors
    private void handleShowAuthor() {
        ShowAuthorStmt showAuthorStmt = (ShowAuthorStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();
        // Only success
        resultSet = new ShowResultSet(showAuthorStmt.getMetaData(), rowSet);
    }

    // Handle show engines
    private void handleShowEngines() {
        ShowEnginesStmt showStmt = (ShowEnginesStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();
        rowSet.add(Lists.newArrayList("OLAP", "YES", "Default storage engine of StarRocks", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("MySQL", "YES", "MySQL server which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ELASTICSEARCH", "YES", "ELASTICSEARCH cluster which data is in it", "NO", "NO",
                "NO"));
        rowSet.add(Lists.newArrayList("HIVE", "YES", "HIVE database which data is in it", "NO", "NO", "NO"));
        rowSet.add(Lists.newArrayList("ICEBERG", "YES", "ICEBERG data lake which data is in it", "NO", "NO", "NO"));

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // Handle show functions
    private void handleShowFunctions() throws AnalysisException {
        ShowFunctionsStmt showStmt = (ShowFunctionsStmt) stmt;
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        List<Function> functions = showStmt.getIsBuiltin() ? ctx.getGlobalStateMgr().getBuiltinFunctions() :
                db.getFunctions();

        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (Function function : functions) {
            List<Comparable> row = function.getInfo(showStmt.getIsVerbose());
            // like predicate
            if (showStmt.getWild() == null || showStmt.like(function.functionName())) {
                rowSet.add(row);
            }
        }

        // sort function rows by first column asc
        ListComparator<List<Comparable>> comparator = null;
        OrderByPair orderByPair = new OrderByPair(0, false);
        comparator = new ListComparator<>(orderByPair);
        Collections.sort(rowSet, comparator);
        List<List<String>> resultRowSet = Lists.newArrayList();

        Set<String> functionNameSet = new HashSet<>();
        for (List<Comparable> row : rowSet) {
            List<String> resultRow = Lists.newArrayList();
            // if not verbose, remove duplicate function name
            if (functionNameSet.contains(row.get(0).toString())) {
                continue;
            }
            for (Comparable column : row) {
                resultRow.add(column.toString());
            }
            resultRowSet.add(resultRow);
            functionNameSet.add(resultRow.get(0));
        }

        // Only success
        ShowResultSetMetaData showMetaData = showStmt.getIsVerbose() ? showStmt.getMetaData() :
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Function Name", ScalarType.createVarchar(256))).build();
        resultSet = new ShowResultSet(showMetaData, resultRowSet);
    }

    private void handleShowProc() throws AnalysisException {
        ShowProcStmt showProcStmt = (ShowProcStmt) stmt;
        ShowResultSetMetaData metaData = showProcStmt.getMetaData();
        ProcNodeInterface procNode = showProcStmt.getNode();

        List<List<String>> finalRows = procNode.fetchResult().getRows();

        resultSet = new ShowResultSet(metaData, finalRows);
    }

    // Show clusters
    private void handleShowCluster() {
        final ShowClusterStmt showStmt = (ShowClusterStmt) stmt;
        final List<List<String>> rows = Lists.newArrayList();
        final List<String> clusterNames = ctx.getGlobalStateMgr().getClusterNames();

        final Set<String> clusterNameSet = Sets.newTreeSet();
        for (String cluster : clusterNames) {
            clusterNameSet.add(cluster);
        }

        for (String clusterName : clusterNameSet) {
            rows.add(Lists.newArrayList(clusterName));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show clusters
    private void handleShowMigrations() {
        final ShowMigrationsStmt showStmt = (ShowMigrationsStmt) stmt;
        final List<List<String>> rows = Lists.newArrayList();
        final Set<BaseParam> infos = ctx.getGlobalStateMgr().getMigrations();

        for (BaseParam param : infos) {
            final int percent = (int) (param.getFloatParam(0) * 100f);
            rows.add(Lists.newArrayList(param.getStringParam(0), param.getStringParam(1), param.getStringParam(2),
                    String.valueOf(percent + "%")));
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show databases statement
    private void handleShowDb() throws AnalysisException, DdlException {
        ShowDbStmt showDbStmt = (ShowDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> dbNames = new ArrayList<>();
        String catalogName;
        if (showDbStmt.getCatalogName() == null) {
            catalogName = ctx.getCurrentCatalog();
        } else {
            catalogName = showDbStmt.getCatalogName();
        }
        dbNames = metadataMgr.listDbNames(catalogName);

        PatternMatcher matcher = null;
        if (showDbStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showDbStmt.getPattern(), CaseSensibility.DATABASE.getCaseSensibility());
        }
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String fullName : dbNames) {
            final String db = ClusterNamespace.getNameFromFullName(fullName);
            // Filter dbname
            if (matcher != null && !matcher.match(db)) {
                continue;
            }

            if (!PrivilegeChecker.checkDbPriv(ConnectContext.get(), catalogName,
                    fullName, PrivPredicate.SHOW)) {
                continue;
            }
            dbNameSet.add(db);
        }

        for (String dbName : dbNameSet) {
            rows.add(Lists.newArrayList(dbName));
        }

        resultSet = new ShowResultSet(showDbStmt.getMetaData(), rows);
    }

    // Show table statement.
    private void handleShowTable() throws AnalysisException, DdlException {
        ShowTableStmt showTableStmt = (ShowTableStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String catalog = ctx.getCurrentCatalog();
        String dbName = showTableStmt.getDb();
        Database db = metadataMgr.getDb(catalog, dbName);

        PatternMatcher matcher = null;
        if (showTableStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showTableStmt.getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
        }

        Map<String, String> tableMap = Maps.newTreeMap();
        if (db != null) {
            db.readLock();
            try {
                if (CatalogMgr.isInternalCatalog(catalog)) {
                    for (Table tbl : db.getTables()) {
                        if (matcher != null && !matcher.match(tbl.getName())) {
                            continue;
                        }
                        // check tbl privs
                        if (!PrivilegeChecker.checkTblPriv(ConnectContext.get(), catalog,
                                db.getFullName(), tbl.getName(), PrivPredicate.SHOW)) {
                            continue;
                        }
                        tableMap.put(tbl.getName(), tbl.getMysqlType());
                    }
                } else {
                    List<String> tableNames = metadataMgr.listTableNames(catalog, dbName);
                    if (matcher != null) {
                        tableNames = tableNames.stream().filter(matcher::match).collect(Collectors.toList());
                    }
                    tableNames.forEach(name -> tableMap.put(name, "BASE TABLE"));
                }
            } finally {
                db.readUnlock();
            }
        } else {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showTableStmt.getDb());
        }

        for (Map.Entry<String, String> entry : tableMap.entrySet()) {
            if (showTableStmt.isVerbose()) {
                rows.add(Lists.newArrayList(entry.getKey(), entry.getValue()));
            } else {
                rows.add(Lists.newArrayList(entry.getKey()));
            }
        }
        resultSet = new ShowResultSet(showTableStmt.getMetaData(), rows);
    }

    // Show table status statement.
    private void handleShowTableStatus() throws AnalysisException {
        ShowTableStatusStmt showStmt = (ShowTableStatusStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDb());
        if (db != null) {
            db.readLock();
            try {
                PatternMatcher matcher = null;
                if (showStmt.getPattern() != null) {
                    matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                            CaseSensibility.TABLE.getCaseSensibility());
                }
                for (Table table : db.getTables()) {
                    if (matcher != null && !matcher.match(table.getName())) {
                        continue;
                    }

                    // check tbl privs
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                            db.getFullName(), table.getName(),
                            PrivPredicate.SHOW)) {
                        continue;
                    }

                    List<String> row = Lists.newArrayList();
                    // Name
                    row.add(table.getName());
                    // Engine
                    row.add(table.getEngine());
                    // version, ra
                    for (int i = 0; i < 15; ++i) {
                        row.add(null);
                    }
                    row.add(table.getComment());
                    rows.add(row);
                }
            } finally {
                db.readUnlock();
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show variables like
    private void handleShowVariables() throws AnalysisException {
        ShowVariablesStmt showStmt = (ShowVariablesStmt) stmt;
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<List<String>> rows = VariableMgr.dump(showStmt.getType(), ctx.getSessionVariable(), matcher);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create database
    private void handleShowCreateDb() throws AnalysisException {
        ShowCreateDbStmt showStmt = (ShowCreateDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDb());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE `").append(ClusterNamespace.getNameFromFullName(showStmt.getDb())).append("`");
        rows.add(Lists.newArrayList(ClusterNamespace.getNameFromFullName(showStmt.getDb()), sb.toString()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create table
    private void handleShowCreateTable() throws AnalysisException {
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) stmt;
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDb());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDb());
        }
        List<List<String>> rows = Lists.newArrayList();
        db.readLock();
        try {
            Table table = db.getTable(showStmt.getTable());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
            }

            List<String> createTableStmt = Lists.newArrayList();
            GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true /* hide password */);
            if (createTableStmt.isEmpty()) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
                return;
            }

            if (table instanceof View) {
                View view = (View) table;
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE VIEW `").append(table.getName()).append("` AS ").append(view.getInlineViewDef());
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0), "utf8", "utf8_general_ci"));
                resultSet = new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
            } else {
                if (showStmt.isView()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                            showStmt.getTable(), "VIEW");
                }
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
            }
        } finally {
            db.readUnlock();
        }
    }

    // Describe statement
    private void handleDescribe() throws AnalysisException {
        DescribeStmt describeStmt = (DescribeStmt) stmt;
        resultSet = new ShowResultSet(describeStmt.getMetaData(), describeStmt.getResultRows());
    }

    // Show column statement.
    private void handleShowColumn() throws AnalysisException {
        ShowColumnStmt showStmt = (ShowColumnStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDb());
        if (db != null) {
            db.readLock();
            try {
                Table table = db.getTable(showStmt.getTable());
                if (table != null) {
                    PatternMatcher matcher = null;
                    if (showStmt.getPattern() != null) {
                        matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                                CaseSensibility.COLUMN.getCaseSensibility());
                    }
                    List<Column> columns = table.getBaseSchema();
                    for (Column col : columns) {
                        if (matcher != null && !matcher.match(col.getName())) {
                            continue;
                        }
                        final String columnName = col.getName();
                        final String columnType = col.getType().canonicalName().toLowerCase();
                        final String isAllowNull = col.isAllowNull() ? "YES" : "NO";
                        final String isKey = col.isKey() ? "YES" : "NO";
                        final String defaultValue = col.getMetaDefaultValue(Lists.newArrayList());
                        final String aggType = col.getAggregationType() == null
                                || col.isAggregationTypeImplicit() ? "" : col.getAggregationType().toSql();
                        if (showStmt.isVerbose()) {
                            // Field Type Collation Null Key Default Extra
                            // Privileges Comment
                            rows.add(Lists.newArrayList(columnName,
                                    columnType,
                                    "",
                                    isAllowNull,
                                    isKey,
                                    defaultValue,
                                    aggType,
                                    "",
                                    col.getComment()));
                        } else {
                            // Field Type Null Key Default Extra
                            rows.add(Lists.newArrayList(columnName,
                                    columnType,
                                    isAllowNull,
                                    isKey,
                                    defaultValue,
                                    aggType));
                        }
                    }
                } else {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR,
                            db.getFullName() + "." + showStmt.getTable());
                }
            } finally {
                db.readUnlock();
            }
        } else {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR,
                    showStmt.getDb() + "." + showStmt.getTable());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show index statement.
    private void handleShowIndex() throws AnalysisException {
        ShowIndexStmt showStmt = (ShowIndexStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        db.readLock();
        try {
            Table table = db.getTable(showStmt.getTableName().getTbl());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR,
                        db.getFullName() + "." + showStmt.getTableName().toString());
            } else if (table instanceof OlapTable) {
                List<Index> indexes = ((OlapTable) table).getIndexes();
                for (Index index : indexes) {
                    rows.add(Lists.newArrayList(showStmt.getTableName().toString(), "", index.getIndexName(),
                            "", String.join(",", index.getColumns()), "", "", "", "",
                            "", index.getIndexType().name(), index.getComment()));
                }
            } else {
                // other type view, mysql, hive, es
                // do nothing
            }
        } finally {
            db.readUnlock();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle help statement.
    private void handleHelp() {
        HelpStmt helpStmt = (HelpStmt) stmt;
        String mark = helpStmt.getMask();
        HelpModule module = HelpModule.getInstance();

        // Get topic
        HelpTopic topic = module.getTopic(mark);
        // Get by Keyword
        if (topic == null) {
            List<String> topics = module.listTopicByKeyword(mark);
            if (topics.size() == 0) {
                // assign to avoid code style problem
                topic = null;
            } else if (topics.size() == 1) {
                topic = module.getTopic(topics.get(0));
            } else {
                // Send topic list and category list
                List<List<String>> rows = Lists.newArrayList();
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> categories = module.listCategoryByName(mark);
                for (String str : categories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), rows);
                return;
            }
        }
        if (topic != null) {
            resultSet = new ShowResultSet(helpStmt.getMetaData(), Lists.<List<String>>newArrayList(
                    Lists.newArrayList(topic.getName(), topic.getDescription(), topic.getExample())));
        } else {
            List<String> categories = module.listCategoryByName(mark);
            if (categories.isEmpty()) {
                // If no category match for this name, return
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), EMPTY_SET);
            } else if (categories.size() > 1) {
                // Send category list
                resultSet = new ShowResultSet(helpStmt.getCategoryMetaData(),
                        Lists.<List<String>>newArrayList(categories));
            } else {
                // Send topic list and sub-category list
                List<List<String>> rows = Lists.newArrayList();
                List<String> topics = module.listTopicByCategory(categories.get(0));
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> subCategories = module.listCategoryByCategory(categories.get(0));
                for (String str : subCategories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), rows);
            }
        }
    }

    // Show load statement.
    private void handleShowLoad() throws AnalysisException {
        ShowLoadStmt showStmt = (ShowLoadStmt) stmt;

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        long dbId = db.getId();

        // combine the List<LoadInfo> of load(v1) and loadManager(v2)
        Set<String> statesValue = showStmt.getStates() == null ? null : showStmt.getStates().stream()
                .map(entity -> entity.name())
                .collect(Collectors.toSet());
        List<List<Comparable>> loadInfos =
                globalStateMgr.getLoadManager().getLoadJobInfosByDb(dbId, showStmt.getLabelValue(),
                        showStmt.isAccurateMatch(),
                        statesValue);

        // order the result of List<LoadInfo> by orderByPairs in show stmt
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(loadInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : loadInfos) {
            List<String> oneInfo = new ArrayList<String>(loadInfo.size());

            for (Comparable element : loadInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        // filter by limit
        long limit = showStmt.getLimit();
        long offset = showStmt.getOffset() == -1L ? 0 : showStmt.getOffset();
        if (offset >= rows.size()) {
            rows = Lists.newArrayList();
        } else if (limit != -1L) {
            if ((limit + offset) < rows.size()) {
                rows = rows.subList((int) offset, (int) (limit + offset));
            } else {
                rows = rows.subList((int) offset, rows.size());
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowRoutineLoad() throws AnalysisException {
        ShowRoutineLoadStmt showRoutineLoadStmt = (ShowRoutineLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        List<RoutineLoadJob> routineLoadJobList;
        try {
            routineLoadJobList = GlobalStateMgr.getCurrentState().getRoutineLoadManager()
                    .getJob(showRoutineLoadStmt.getDbFullName(),
                            showRoutineLoadStmt.getName(),
                            showRoutineLoadStmt.isIncludeHistory());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }

        if (routineLoadJobList != null) {
            RoutineLoadFunctionalExprProvider fProvider = showRoutineLoadStmt.getFunctionalExprProvider();
            rows = routineLoadJobList.parallelStream()
                    .filter(fProvider.getPredicateChain())
                    .sorted(fProvider.getOrderComparator())
                    .skip(fProvider.getSkipCount())
                    .limit(fProvider.getLimitCount())
                    .map(job -> job.getShowInfo())
                    .collect(Collectors.toList());
        }

        if (!Strings.isNullOrEmpty(showRoutineLoadStmt.getName()) && rows.isEmpty()) {
            // if the jobName has been specified
            throw new AnalysisException("There is no job named " + showRoutineLoadStmt.getName()
                    + " in db " + showRoutineLoadStmt.getDbFullName()
                    + ". Include history? " + showRoutineLoadStmt.isIncludeHistory());
        }
        resultSet = new ShowResultSet(showRoutineLoadStmt.getMetaData(), rows);
    }

    private void handleShowRoutineLoadTask() throws AnalysisException {
        ShowRoutineLoadTaskStmt showRoutineLoadTaskStmt = (ShowRoutineLoadTaskStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        RoutineLoadJob routineLoadJob;
        try {
            routineLoadJob =
                    GlobalStateMgr.getCurrentState().getRoutineLoadManager()
                            .getJob(showRoutineLoadTaskStmt.getDbFullName(),
                                    showRoutineLoadTaskStmt.getJobName());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        if (routineLoadJob == null) {
            throw new AnalysisException("The job named " + showRoutineLoadTaskStmt.getJobName() + "does not exists "
                    + "or job state is stopped or cancelled");
        }

        // check auth
        String dbFullName = showRoutineLoadTaskStmt.getDbFullName();
        String tableName;
        try {
            tableName = routineLoadJob.getTableName();
        } catch (MetaNotFoundException e) {
            throw new AnalysisException(
                    "The table metadata of job has been changed. The job will be cancelled automatically", e);
        }
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                dbFullName,
                tableName,
                PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName);
        }

        // get routine load task info
        rows.addAll(routineLoadJob.getTasksShowInfo());
        resultSet = new ShowResultSet(showRoutineLoadTaskStmt.getMetaData(), rows);
    }

    // Show user property statement
    private void handleShowUserProperty() throws AnalysisException {
        ShowUserPropertyStmt showStmt = (ShowUserPropertyStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getRows());
    }

    // Show delete statement.
    private void handleShowDelete() throws AnalysisException {
        ShowDeleteStmt showStmt = (ShowDeleteStmt) stmt;

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }
        long dbId = db.getId();

        DeleteHandler deleteHandler = globalStateMgr.getDeleteHandler();
        Load load = globalStateMgr.getLoadInstance();
        List<List<Comparable>> deleteInfos = deleteHandler.getDeleteInfosByDb(dbId);
        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> deleteInfo : deleteInfos) {
            List<String> oneInfo = new ArrayList<String>(deleteInfo.size());
            for (Comparable element : deleteInfo) {
                oneInfo.add(element.toString());
            }
            rows.add(oneInfo);
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show alter statement.
    private void handleShowAlter() throws AnalysisException {
        ShowAlterStmt showStmt = (ShowAlterStmt) stmt;
        ProcNodeInterface procNodeI = showStmt.getNode();
        Preconditions.checkNotNull(procNodeI);
        List<List<String>> rows;
        //Only SchemaChangeProc support where/order by/limit syntax
        if (procNodeI instanceof SchemaChangeProcDir) {
            rows = ((SchemaChangeProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderPairs(), showStmt.getLimitElement()).getRows();
        } else {
            rows = procNodeI.fetchResult().getRows();
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show alter statement.
    private void handleShowCollation() {
        ShowCollationStmt showStmt = (ShowCollationStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        // | utf8_general_ci | utf8 | 33 | Yes | Yes | 1 |
        row.add("utf8_general_ci");
        row.add("utf8");
        row.add("33");
        row.add("Yes");
        row.add("Yes");
        row.add("1");
        rows.add(row);
        // | binary | binary | 63 | Yes | Yes | 1 |
        row = Lists.newArrayList();
        row.add("binary");
        row.add("binary");
        row.add("63");
        row.add("Yes");
        row.add("Yes");
        row.add("1");
        rows.add(row);
        // | gbk_chinese_ci | gbk | 28 | Yes | Yes | 1 |
        row = Lists.newArrayList();
        row.add("gbk_chinese_ci");
        row.add("gbk");
        row.add("28");
        row.add("Yes");
        row.add("Yes");
        row.add("1");
        rows.add(row);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowData() throws AnalysisException {
        ShowDataStmt showStmt = (ShowDataStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getResultRows());
    }

    private void handleShowPartitions() throws AnalysisException {
        ShowPartitionsStmt showStmt = (ShowPartitionsStmt) stmt;
        ProcNodeInterface procNodeI = showStmt.getNode();
        Preconditions.checkNotNull(procNodeI);
        List<List<String>> rows = ((PartitionsProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                showStmt.getOrderByPairs(), showStmt.getLimitElement()).getRows();
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowTablet() throws AnalysisException {
        ShowTabletStmt showStmt = (ShowTabletStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (showStmt.isShowSingleTablet()) {
            long tabletId = showStmt.getTabletId();
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            Long dbId = tabletMeta != null ? tabletMeta.getDbId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String dbName = null;
            Long tableId = tabletMeta != null ? tabletMeta.getTableId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String tableName = null;
            Long partitionId = tabletMeta != null ? tabletMeta.getPartitionId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String partitionName = null;
            Long indexId = tabletMeta != null ? tabletMeta.getIndexId() : TabletInvertedIndex.NOT_EXIST_VALUE;
            String indexName = null;
            Boolean isSync = true;

            // check real meta
            do {
                Database db = globalStateMgr.getDb(dbId);
                if (db == null) {
                    isSync = false;
                    break;
                }
                dbName = db.getFullName();

                db.readLock();
                try {
                    Table table = db.getTable(tableId);
                    if (table == null || !(table instanceof OlapTable)) {
                        isSync = false;
                        break;
                    }
                    tableName = table.getName();

                    OlapTable olapTable = (OlapTable) table;
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null) {
                        isSync = false;
                        break;
                    }
                    partitionName = partition.getName();

                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        isSync = false;
                        break;
                    }
                    indexName = olapTable.getIndexNameById(indexId);

                    if (partition.isUseStarOS()) {
                        break;
                    }

                    LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                    if (tablet == null) {
                        isSync = false;
                        break;
                    }

                    List<Replica> replicas = tablet.getReplicas();
                    for (Replica replica : replicas) {
                        Replica tmp = invertedIndex.getReplica(tabletId, replica.getBackendId());
                        if (tmp == null) {
                            isSync = false;
                            break;
                        }
                        // use !=, not equals(), because this should be the same object.
                        if (tmp != replica) {
                            isSync = false;
                            break;
                        }
                    }

                } finally {
                    db.readUnlock();
                }
            } while (false);

            String detailCmd = String.format("SHOW PROC '/dbs/%d/%d/partitions/%d/%d/%d';",
                    dbId, tableId, partitionId, indexId, tabletId);
            rows.add(Lists.newArrayList(dbName, tableName, partitionName, indexName,
                    dbId.toString(), tableId.toString(),
                    partitionId.toString(), indexId.toString(),
                    isSync.toString(), detailCmd));
        } else {
            Database db = globalStateMgr.getDb(showStmt.getDbName());
            if (db == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
            }

            db.readLock();
            try {
                Table table = db.getTable(showStmt.getTableName());
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTableName());
                }
                if (!(table instanceof OlapTable)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, showStmt.getTableName());
                }

                OlapTable olapTable = (OlapTable) table;
                long sizeLimit = -1;
                if (showStmt.hasOffset() && showStmt.hasLimit()) {
                    sizeLimit = showStmt.getOffset() + showStmt.getLimit();
                } else if (showStmt.hasLimit()) {
                    sizeLimit = showStmt.getLimit();
                }
                boolean stop = false;
                Collection<Partition> partitions = new ArrayList<Partition>();
                if (showStmt.hasPartition()) {
                    PartitionNames partitionNames = showStmt.getPartitionNames();
                    for (String partName : partitionNames.getPartitionNames()) {
                        Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                        if (partition == null) {
                            throw new AnalysisException("Unknown partition: " + partName);
                        }
                        partitions.add(partition);
                    }
                } else {
                    partitions = olapTable.getPartitions();
                }
                List<List<Comparable>> tabletInfos = new ArrayList<>();
                String indexName = showStmt.getIndexName();
                long indexId = -1;
                if (indexName != null) {
                    Long id = olapTable.getIndexIdByName(indexName);
                    if (id == null) {
                        // invalid indexName
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getIndexName());
                    }
                    indexId = id;
                }
                for (Partition partition : partitions) {
                    if (stop) {
                        break;
                    }
                    for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.ALL)) {
                        if (indexId > -1 && index.getId() != indexId) {
                            continue;
                        }
                        TabletsProcDir procDir = new TabletsProcDir(db, partition, index);
                        tabletInfos.addAll(procDir.fetchComparableResult(
                                showStmt.getVersion(), showStmt.getBackendId(), showStmt.getReplicaState()));
                        if (sizeLimit > -1 && tabletInfos.size() >= sizeLimit) {
                            stop = true;
                            break;
                        }
                    }
                }
                if (sizeLimit > -1 && tabletInfos.size() < sizeLimit) {
                    tabletInfos.clear();
                } else if (sizeLimit > -1) {
                    tabletInfos = tabletInfos.subList((int) showStmt.getOffset(), (int) sizeLimit);
                }

                // order by
                List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
                ListComparator<List<Comparable>> comparator = null;
                if (orderByPairs != null) {
                    OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
                    comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
                } else {
                    // order by tabletId, replicaId
                    comparator = new ListComparator<>(0, 1);
                }
                Collections.sort(tabletInfos, comparator);

                for (List<Comparable> tabletInfo : tabletInfos) {
                    List<String> oneTablet = new ArrayList<String>(tabletInfo.size());
                    for (Comparable column : tabletInfo) {
                        oneTablet.add(column.toString());
                    }
                    rows.add(oneTablet);
                }
            } finally {
                db.readUnlock();
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle show brokers
    private void handleShowBroker() {
        ShowBrokerStmt showStmt = (ShowBrokerStmt) stmt;
        List<List<String>> rowSet = GlobalStateMgr.getCurrentState().getBrokerMgr().getBrokersInfo();

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // Handle show resources
    private void handleShowResources() {
        ShowResourcesStmt showStmt = (ShowResourcesStmt) stmt;
        List<List<String>> rowSet = GlobalStateMgr.getCurrentState().getResourceMgr().getResourcesInfo();

        // Only success
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private void handleShowExport() throws AnalysisException {
        ShowExportStmt showExportStmt = (ShowExportStmt) stmt;
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb(showExportStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showExportStmt.getDbName());
        }
        long dbId = db.getId();

        ExportMgr exportMgr = globalStateMgr.getExportMgr();

        Set<ExportJob.JobState> states = null;
        ExportJob.JobState state = showExportStmt.getJobState();
        if (state != null) {
            states = Sets.newHashSet(state);
        }
        List<List<String>> infos = exportMgr.getExportJobInfosByIdOrState(
                dbId, showExportStmt.getJobId(), states, showExportStmt.getQueryId(),
                showExportStmt.getOrderByPairs(), showExportStmt.getLimit());

        resultSet = new ShowResultSet(showExportStmt.getMetaData(), infos);
    }

    private void handleShowBackends() {
        final ShowBackendsStmt showStmt = (ShowBackendsStmt) stmt;
        List<List<String>> backendInfos = BackendsProcDir.getClusterBackendInfos(showStmt.getClusterName());
        resultSet = new ShowResultSet(showStmt.getMetaData(), backendInfos);
    }

    private void handleShowFrontends() {
        final ShowFrontendsStmt showStmt = (ShowFrontendsStmt) stmt;
        List<List<String>> infos = Lists.newArrayList();
        FrontendsProcNode.getFrontendsInfo(GlobalStateMgr.getCurrentState(), infos);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRepositories() {
        final ShowRepositoriesStmt showStmt = (ShowRepositoriesStmt) stmt;
        List<List<String>> repoInfos = GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getReposInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), repoInfos);
    }

    private void handleShowSnapshot() throws AnalysisException {
        final ShowSnapshotStmt showStmt = (ShowSnapshotStmt) stmt;
        Repository repo =
                GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(showStmt.getRepoName());
        if (repo == null) {
            throw new AnalysisException("Repository " + showStmt.getRepoName() + " does not exist");
        }

        List<List<String>> snapshotInfos = repo.getSnapshotInfos(showStmt.getSnapshotName(), showStmt.getTimestamp());
        resultSet = new ShowResultSet(showStmt.getMetaData(), snapshotInfos);
    }

    private void handleShowBackup() throws AnalysisException {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        Database db = GlobalStateMgr.getCurrentState().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }

        AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
        if (!(jobI instanceof BackupJob)) {
            resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
            return;
        }

        BackupJob backupJob = (BackupJob) jobI;
        List<String> info = backupJob.getInfo();
        List<List<String>> infos = Lists.newArrayList();
        infos.add(info);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRestore() throws AnalysisException {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        Database db = GlobalStateMgr.getCurrentState().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }

        AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
        if (!(jobI instanceof RestoreJob)) {
            resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
            return;
        }

        RestoreJob restoreJob = (RestoreJob) jobI;
        List<String> info = restoreJob.getInfo();
        List<List<String>> infos = Lists.newArrayList();
        infos.add(info);
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowGrants() {
        ShowGrantsStmt showStmt = (ShowGrantsStmt) stmt;
        List<List<String>> infos = GlobalStateMgr.getCurrentState().getAuth().getGrantsSQLs(showStmt.getUserIdent());
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRoles() {
        ShowRolesStmt showStmt = (ShowRolesStmt) stmt;
        List<List<String>> infos = GlobalStateMgr.getCurrentState().getAuth().getRoleInfo();
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleAdminShowTabletStatus() throws AnalysisException {
        AdminShowReplicaStatusStmt showStmt = (AdminShowReplicaStatusStmt) stmt;
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletStatus(showStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleAdminShowTabletDistribution() throws AnalysisException {
        AdminShowReplicaDistributionStmt showStmt = (AdminShowReplicaDistributionStmt) stmt;
        List<List<String>> results;
        try {
            results = MetadataViewer.getTabletDistribution(showStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleAdminShowConfig() throws AnalysisException {
        AdminShowConfigStmt showStmt = (AdminShowConfigStmt) stmt;
        List<List<String>> results;
        try {
            PatternMatcher matcher = null;
            if (showStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                        CaseSensibility.CONFIG.getCaseSensibility());
            }
            results = ConfigBase.getConfigInfo(matcher);
            // Sort all configs by config key.
            Collections.sort(results, new Comparator<List<String>>() {
                @Override
                public int compare(List<String> o1, List<String> o2) {
                    return o1.get(0).compareTo(o2.get(0));
                }
            });
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowSmallFiles() throws AnalysisException {
        ShowSmallFilesStmt showStmt = (ShowSmallFilesStmt) stmt;
        List<List<String>> results;
        try {
            results = GlobalStateMgr.getCurrentState().getSmallFileMgr().getInfo(showStmt.getDbName());
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), results);
    }

    private void handleShowDynamicPartition() {
        ShowDynamicPartitionStmt showDynamicPartitionStmt = (ShowDynamicPartitionStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = ctx.getGlobalStateMgr().getDb(showDynamicPartitionStmt.getDb());
        if (db != null) {
            db.readLock();
            try {
                for (Table tbl : db.getTables()) {
                    if (!(tbl instanceof OlapTable)) {
                        continue;
                    }

                    DynamicPartitionScheduler dynamicPartitionScheduler =
                            GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler();
                    OlapTable olapTable = (OlapTable) tbl;
                    if (!olapTable.dynamicPartitionExists()) {
                        dynamicPartitionScheduler.removeRuntimeInfo(olapTable.getName());
                        continue;
                    }
                    // check tbl privs
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                            db.getFullName(), olapTable.getName(),
                            PrivPredicate.SHOW)) {
                        continue;
                    }
                    DynamicPartitionProperty dynamicPartitionProperty =
                            olapTable.getTableProperty().getDynamicPartitionProperty();
                    String tableName = olapTable.getName();
                    int replicationNum = dynamicPartitionProperty.getReplicationNum();
                    replicationNum = (replicationNum == DynamicPartitionProperty.NOT_SET_REPLICATION_NUM) ?
                            olapTable.getDefaultReplicationNum() : FeConstants.default_replication_num;
                    rows.add(Lists.newArrayList(
                            tableName,
                            String.valueOf(dynamicPartitionProperty.getEnable()),
                            dynamicPartitionProperty.getTimeUnit().toUpperCase(),
                            String.valueOf(dynamicPartitionProperty.getStart()),
                            String.valueOf(dynamicPartitionProperty.getEnd()),
                            dynamicPartitionProperty.getPrefix(),
                            String.valueOf(dynamicPartitionProperty.getBuckets()),
                            String.valueOf(replicationNum),
                            dynamicPartitionProperty.getStartOfInfo(),
                            dynamicPartitionScheduler
                                    .getRuntimeInfo(tableName, DynamicPartitionScheduler.LAST_UPDATE_TIME),
                            dynamicPartitionScheduler
                                    .getRuntimeInfo(tableName, DynamicPartitionScheduler.LAST_SCHEDULER_TIME),
                            dynamicPartitionScheduler
                                    .getRuntimeInfo(tableName, DynamicPartitionScheduler.DYNAMIC_PARTITION_STATE),
                            dynamicPartitionScheduler
                                    .getRuntimeInfo(tableName, DynamicPartitionScheduler.CREATE_PARTITION_MSG),
                            dynamicPartitionScheduler
                                    .getRuntimeInfo(tableName, DynamicPartitionScheduler.DROP_PARTITION_MSG)));
                }
            } finally {
                db.readUnlock();
            }
            resultSet = new ShowResultSet(showDynamicPartitionStmt.getMetaData(), rows);
        }
    }

    // Show transaction statement.
    private void handleShowTransaction() throws AnalysisException {
        ShowTransactionStmt showStmt = (ShowTransactionStmt) stmt;
        Database db = ctx.getGlobalStateMgr().getDb(showStmt.getDbName());
        if (db == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, showStmt.getDbName());
        }

        long txnId = showStmt.getTxnId();
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        resultSet = new ShowResultSet(showStmt.getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
    }

    private void handleShowPlugins() throws AnalysisException {
        ShowPluginsStmt pluginsStmt = (ShowPluginsStmt) stmt;
        List<List<String>> rows = GlobalStateMgr.getCurrentPluginMgr().getPluginShowInfos();
        resultSet = new ShowResultSet(pluginsStmt.getMetaData(), rows);
    }

    // Show sql blacklist
    private void handleShowSqlBlackListStmt() throws AnalysisException {
        ShowSqlBlackListStmt showStmt = (ShowSqlBlackListStmt) stmt;

        List<List<String>> rows = new ArrayList<List<String>>();
        for (Map.Entry<String, BlackListSql> entry : SqlBlackList.getInstance().sqlBlackListMap.entrySet()) {
            List<String> oneSql = new ArrayList<String>();
            oneSql.add(String.valueOf(entry.getValue().id));
            oneSql.add(entry.getKey());
            rows.add(oneSql);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowAnalyze() {
        List<AnalyzeJob> jobs = ctx.getGlobalStateMgr().getAnalyzeManager().getAllAnalyzeJobList();
        List<List<String>> rows = Lists.newArrayList();
        jobs.sort(Comparator.comparing(AnalyzeJob::getId));
        for (AnalyzeJob job : jobs) {
            try {
                rows.add(job.showAnalyzeJobs());
            } catch (MetaNotFoundException e) {
                // pass
            }
        }
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowWorkGroup() throws AnalysisException {
        ShowWorkGroupStmt showWorkGroupStmt = (ShowWorkGroupStmt) stmt;
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getWorkGroupMgr().showWorkGroup(showWorkGroupStmt);
        resultSet = new ShowResultSet(showWorkGroupStmt.getMetaData(), rows);
    }

    private void handleShowCatalogs() {
        ShowCatalogsStmt showCatalogsStmt = (ShowCatalogsStmt) stmt;
        List<List<String>> rowSet = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogsInfo();
        rowSet.add(Arrays.asList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "Internal", "Internal Catalog"));
        rowSet.sort(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return o1.get(0).compareTo(o2.get(0));
            }
        });
        resultSet = new ShowResultSet(showCatalogsStmt.getMetaData(), rowSet);
    }

}
