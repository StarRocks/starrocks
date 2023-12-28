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
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HiveMetaStoreTable;
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
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.proc.BackendsProcDir;
import com.starrocks.common.proc.ComputeNodeProcDir;
import com.starrocks.common.proc.FrontendsProcNode;
import com.starrocks.common.proc.LakeTabletsProcDir;
import com.starrocks.common.proc.LocalTabletsProcDir;
import com.starrocks.common.proc.OptimizeProcDir;
import com.starrocks.common.proc.PartitionsProcDir;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.SchemaChangeProcDir;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.credential.CredentialUtil;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.load.routineload.RoutineLoadFunctionalExprProvider;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadFunctionalExprProvider;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.meta.BlackListSql;
import com.starrocks.meta.SqlBlackList;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ActionSet;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.CatalogPEntryObject;
import com.starrocks.privilege.DbPEntryObject;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeEntry;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.privilege.TablePEntryObject;
import com.starrocks.proto.FailPointTriggerModeType;
import com.starrocks.proto.PFailPointInfo;
import com.starrocks.proto.PFailPointTriggerMode;
import com.starrocks.proto.PListFailPointResponse;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PListFailPointRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.service.InformationSchemaDataSource;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.DescStorageVolumeStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowAuthorStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBackupStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.sql.ast.ShowCatalogsStmt;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateRoutineLoadStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataCacheRulesStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.sql.ast.ShowDictionaryStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.ShowFailPointStatement;
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
import com.starrocks.sql.ast.ShowProfilelistStmt;
import com.starrocks.sql.ast.ShowRepositoriesStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.ShowResourceGroupUsageStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import com.starrocks.sql.ast.ShowRestoreStmt;
import com.starrocks.sql.ast.ShowRolesStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowRunningQueriesStmt;
import com.starrocks.sql.ast.ShowSmallFilesStmt;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowSqlBlackListStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStorageVolumesStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.ShowUserPropertyStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.ShowWarehousesStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Table.TableType.JDBC;

// Execute one show statement.
public class ShowExecutor {
    private static final Logger LOG = LogManager.getLogger(ShowExecutor.class);
    private static final List<List<String>> EMPTY_SET = Lists.newArrayList();

    private final ConnectContext connectContext;
    private final ShowStmt stmt;
    private ShowResultSet resultSet;
    private final MetadataMgr metadataMgr;

    public ShowExecutor(ConnectContext connectContext, ShowStmt stmt) {
        this.connectContext = connectContext;
        this.stmt = stmt;
        resultSet = null;
        metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
    }

    public ShowResultSet execute() throws AnalysisException, DdlException {
        if (stmt instanceof ShowMaterializedViewsStmt) {
            handleShowMaterializedView();
        } else if (stmt instanceof ShowAuthorStmt) {
            handleShowAuthor();
        } else if (stmt instanceof ShowProcStmt) {
            handleShowProc();
        } else if (stmt instanceof HelpStmt) {
            handleHelp();
        } else if (stmt instanceof ShowWarehousesStmt) {
            handleShowWarehouses();
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
        } else if (stmt instanceof ShowProfilelistStmt) {
            handleShowProfilelist();
        } else if (stmt instanceof ShowRunningQueriesStmt) {
            handleShowRunningQueries();
        } else if (stmt instanceof ShowResourceGroupUsageStmt) {
            handleShowResourceGroupUsage();
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
        } else if (stmt instanceof ShowCreateRoutineLoadStmt) {
            handleShowCreateRoutineLoad();
        } else if (stmt instanceof ShowRoutineLoadTaskStmt) {
            handleShowRoutineLoadTask();
        } else if (stmt instanceof ShowStreamLoadStmt) {
            handleShowStreamLoad();
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
        } else if (stmt instanceof ShowDataCacheRulesStmt) {
            handleShowDataCacheRulesStmt();
        } else if (stmt instanceof ShowAnalyzeJobStmt) {
            handleShowAnalyzeJob();
        } else if (stmt instanceof ShowAnalyzeStatusStmt) {
            handleShowAnalyzeStatus();
        } else if (stmt instanceof ShowBasicStatsMetaStmt) {
            handleShowBasicStatsMeta();
        } else if (stmt instanceof ShowHistogramStatsMetaStmt) {
            handleShowHistogramStatsMeta();
        } else if (stmt instanceof ShowResourceGroupStmt) {
            handleShowResourceGroup();
        } else if (stmt instanceof ShowUserStmt) {
            handleShowUser();
        } else if (stmt instanceof ShowCatalogsStmt) {
            handleShowCatalogs();
        } else if (stmt instanceof ShowComputeNodesStmt) {
            handleShowComputeNodes();
        } else if (stmt instanceof ShowAuthenticationStmt) {
            handleShowAuthentication();
        } else if (stmt instanceof ShowCreateExternalCatalogStmt) {
            handleShowCreateExternalCatalog();
        } else if (stmt instanceof ShowCharsetStmt) {
            handleShowCharset();
        } else if (stmt instanceof ShowStorageVolumesStmt) {
            handleShowStorageVolumes();
        } else if (stmt instanceof DescStorageVolumeStmt) {
            handleDescStorageVolume();
        } else if (stmt instanceof ShowPipeStmt) {
            handleShowPipes();
        } else if (stmt instanceof DescPipeStmt) {
            handleDescPipe();
        } else if (stmt instanceof ShowFailPointStatement) {
            handleShowFailPoint();
        } else if (stmt instanceof ShowDictionaryStmt) {
            handleShowDictionary();
        } else {
            handleEmpty();
        }

        List<List<String>> rows = doPredicate(stmt, stmt.getMetaData(), resultSet.getResultRows());
        return new ShowResultSet(resultSet.getMetaData(), rows);
    }

    private void handleShowAuthentication() {
        final ShowAuthenticationStmt showAuthenticationStmt = (ShowAuthenticationStmt) stmt;
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        List<List<String>> userAuthInfos = Lists.newArrayList();

        Map<UserIdentity, UserAuthenticationInfo> authenticationInfoMap = new HashMap<>();
        if (showAuthenticationStmt.isAll()) {
            authenticationInfoMap.putAll(authenticationManager.getUserToAuthenticationInfo());
        } else {
            UserAuthenticationInfo userAuthenticationInfo;
            if (showAuthenticationStmt.getUserIdent() == null) {
                userAuthenticationInfo = authenticationManager
                        .getUserAuthenticationInfoByUserIdentity(connectContext.getCurrentUserIdentity());
            } else {
                userAuthenticationInfo =
                        authenticationManager.getUserAuthenticationInfoByUserIdentity(
                                showAuthenticationStmt.getUserIdent());
            }
            authenticationInfoMap.put(showAuthenticationStmt.getUserIdent(), userAuthenticationInfo);
        }
        for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : authenticationInfoMap.entrySet()) {
            UserAuthenticationInfo userAuthenticationInfo = entry.getValue();
            userAuthInfos.add(Lists.newArrayList(
                    entry.getKey().toString(),
                    userAuthenticationInfo.getPassword().length == 0 ? "No" : "Yes",
                    userAuthenticationInfo.getAuthPlugin(),
                    userAuthenticationInfo.getTextForAuthPlugin()));
        }

        resultSet = new ShowResultSet(showAuthenticationStmt.getMetaData(), userAuthInfos);
    }

    private void handleShowComputeNodes() {
        final ShowComputeNodesStmt showStmt = (ShowComputeNodesStmt) stmt;
        List<List<String>> computeNodesInfos = ComputeNodeProcDir.getClusterComputeNodesInfos();
        resultSet = new ShowResultSet(showStmt.getMetaData(), computeNodesInfos);
    }

    private void handleShowMaterializedView() throws AnalysisException {
        ShowMaterializedViewsStmt showMaterializedViewsStmt = (ShowMaterializedViewsStmt) stmt;
        String dbName = showMaterializedViewsStmt.getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        MetaUtils.checkDbNullAndReport(db, dbName);

        List<MaterializedView> materializedViews = Lists.newArrayList();
        List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            PatternMatcher matcher = null;
            if (showMaterializedViewsStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showMaterializedViewsStmt.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }

            for (Table table : db.getTables()) {
                if (table.isMaterializedView()) {
                    MaterializedView mvTable = (MaterializedView) table;
                    if (matcher != null && !matcher.match(mvTable.getName())) {
                        continue;
                    }

                    AtomicBoolean baseTableHasPrivilege = new AtomicBoolean(true);
                    mvTable.getBaseTableInfos().forEach(baseTableInfo -> {
                        Table baseTable = baseTableInfo.getTable();
                        // TODO: external table should check table action after AuthorizationManager support it.
                        if (baseTable != null && baseTable.isNativeTableOrMaterializedView()) {
                            try {
                                Authorizer.checkTableAction(connectContext.getCurrentUserIdentity(),
                                        connectContext.getCurrentRoleIds(), baseTableInfo.getDbName(),
                                        baseTableInfo.getTableName(),
                                        PrivilegeType.SELECT);
                            } catch (AccessDeniedException e) {
                                baseTableHasPrivilege.set(false);
                            }
                        }
                    });
                    if (!baseTableHasPrivilege.get()) {
                        continue;
                    }

                    try {
                        Authorizer.checkAnyActionOnMaterializedView(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(db.getFullName(), mvTable.getName()));
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    materializedViews.add(mvTable);
                } else if (Table.TableType.OLAP == table.getType()) {
                    OlapTable olapTable = (OlapTable) table;
                    List<MaterializedIndexMeta> visibleMaterializedViews = olapTable.getVisibleIndexMetas();
                    long baseIdx = olapTable.getBaseIndexId();
                    for (MaterializedIndexMeta mvMeta : visibleMaterializedViews) {
                        if (baseIdx == mvMeta.getIndexId()) {
                            continue;
                        }
                        if (matcher != null && !matcher.match(olapTable.getIndexNameById(mvMeta.getIndexId()))) {
                            continue;
                        }
                        singleTableMVs.add(Pair.create(olapTable, mvMeta));
                    }
                }
            }

            List<ShowMaterializedViewStatus> mvStatusList = listMaterializedViewStatus(dbName, materializedViews, singleTableMVs);
            List<List<String>> rowSets = mvStatusList.stream().map(status -> status.toResultSet()).collect(Collectors.toList());
            resultSet = new ShowResultSet(stmt.getMetaData(), rowSets);
        } catch (Exception e) {
            LOG.warn("listMaterializedViews failed:", e);
            throw e;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    public static String buildCreateMVSql(OlapTable olapTable, String mv, MaterializedIndexMeta mvMeta) {
        StringBuilder originStmtBuilder = new StringBuilder(
                "create materialized view " + mv +
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
        return originStmtBuilder.toString();
    }

    public static List<ShowMaterializedViewStatus> listMaterializedViewStatus(
            String dbName,
            List<MaterializedView> materializedViews,
            List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs) {
        List<ShowMaterializedViewStatus> rowSets = Lists.newArrayList();

        // Now there are two MV cases:
        //  1. Table's type is MATERIALIZED_VIEW, this is the new MV type which the MV table is separated from
        //     the base table and supports multi table in MV definition.
        //  2. Table's type is OLAP, this is the old MV type which the MV table is associated with the base
        //     table and only supports single table in MV definition.
        Map<String, TaskRunStatus> mvNameTaskMap = Maps.newHashMap();
        if (!materializedViews.isEmpty()) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            TaskManager taskManager = globalStateMgr.getTaskManager();
            mvNameTaskMap = taskManager.showMVLastRefreshTaskRunStatus(dbName);
        }
        for (MaterializedView mvTable : materializedViews) {
            long mvId = mvTable.getId();
            ShowMaterializedViewStatus mvStatus = new ShowMaterializedViewStatus(mvId, dbName, mvTable.getName());
            TaskRunStatus taskStatus = mvNameTaskMap.get(TaskBuilder.getMvTaskName(mvId));
            // refresh_type
            MaterializedView.MvRefreshScheme refreshScheme = mvTable.getRefreshScheme();
            if (refreshScheme == null) {
                mvStatus.setRefreshType("UNKNOWN");
            } else {
                mvStatus.setRefreshType(String.valueOf(mvTable.getRefreshScheme().getType()));
            }
            // is_active
            mvStatus.setActive(mvTable.isActive());
            mvStatus.setInactiveReason(Optional.ofNullable(mvTable.getInactiveReason()).map(String::valueOf).orElse(null));
            // partition info
            if (mvTable.getPartitionInfo() != null && mvTable.getPartitionInfo().getType() != null) {
                mvStatus.setPartitionType(mvTable.getPartitionInfo().getType().toString());
            }
            // row count
            mvStatus.setRows(mvTable.getRowCount());
            // materialized view ddl
            mvStatus.setText(mvTable.getMaterializedViewDdlStmt(true));
            // task run status
            mvStatus.setLastTaskRunStatus(taskStatus);
            rowSets.add(mvStatus);
        }

        for (Pair<OlapTable, MaterializedIndexMeta> singleTableMV : singleTableMVs) {
            OlapTable olapTable = singleTableMV.first;
            MaterializedIndexMeta mvMeta = singleTableMV.second;

            long mvId = mvMeta.getIndexId();
            ShowMaterializedViewStatus mvStatus = new ShowMaterializedViewStatus(mvId, dbName, olapTable.getIndexNameById(mvId));
            // refresh_type
            mvStatus.setRefreshType("ROLLUP");
            // is_active
            mvStatus.setActive(true);
            // partition type
            if (olapTable.getPartitionInfo() != null && olapTable.getPartitionInfo().getType() != null) {
                mvStatus.setPartitionType(olapTable.getPartitionInfo().getType().toString());
            }
            // rows
            if (olapTable.getPartitionInfo().getType() == PartitionType.UNPARTITIONED) {
                Partition partition = olapTable.getPartitions().iterator().next();
                MaterializedIndex index = partition.getIndex(mvId);
                mvStatus.setRows(index.getRowCount());
            } else {
                mvStatus.setRows(0L);
            }
            if (mvMeta.getOriginStmt() == null) {
                String mvName = olapTable.getIndexNameById(mvId);
                mvStatus.setText(buildCreateMVSql(olapTable, mvName, mvMeta));
            } else {
                mvStatus.setText(mvMeta.getOriginStmt().replace("\n", "").replace("\t", "")
                        .replaceAll("[ ]+", " "));
            }
            rowSets.add(mvStatus);
        }
        return rowSets;
    }

    // Handle show process list
    private void handleShowProcesslist() {
        ShowProcesslistStmt showStmt = (ShowProcesslistStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();

        List<ConnectContext.ThreadInfo> threadInfos = connectContext.getConnectScheduler()
                .listConnection(connectContext.getQualifiedUser());
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            List<String> row = info.toRow(nowMs, showStmt.showFull());
            if (row != null) {
                rowSet.add(row);
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private void handleShowProfilelist() {
        ShowProfilelistStmt showStmt = (ShowProfilelistStmt) stmt;
        List<List<String>> rowSet = Lists.newArrayList();

        List<ProfileManager.ProfileElement> profileElements = ProfileManager.getInstance().getAllProfileElements();
        Collections.reverse(profileElements);
        Iterator<ProfileManager.ProfileElement> iterator = profileElements.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            ProfileManager.ProfileElement element = iterator.next();
            List<String> row = element.toRow();
            rowSet.add(row);
            count++;
            if (showStmt.getLimit() >= 0 && count >= showStmt.getLimit()) {
                break;
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private void handleShowRunningQueries() {
        ShowRunningQueriesStmt showStmt = (ShowRunningQueriesStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        List<LogicalSlot> slots = GlobalStateMgr.getCurrentState().getSlotManager().getSlots();
        slots.sort(
                Comparator.comparingLong(LogicalSlot::getStartTimeMs).thenComparingLong(LogicalSlot::getExpiredAllocatedTimeMs));

        for (LogicalSlot slot : slots) {
            List<String> row =
                    ShowRunningQueriesStmt.getColumnSuppliers().stream().map(columnSupplier -> columnSupplier.apply(slot))
                            .collect(Collectors.toList());
            rows.add(row);

            if (showStmt.getLimit() >= 0 && rows.size() >= showStmt.getLimit()) {
                break;
            }
        }

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowResourceGroupUsage() {
        ShowResourceGroupUsageStmt showStmt = (ShowResourceGroupUsageStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();

        GlobalStateMgr.getCurrentSystemInfo().backendAndComputeNodeStream()
                .flatMap(worker -> worker.getResourceGroupUsages().stream()
                        .map(usage -> new ShowResourceGroupUsageStmt.ShowItem(worker, usage)))
                .filter(item -> showStmt.getGroupName() == null ||
                        showStmt.getGroupName().equals(item.getUsage().getGroup().getName()))
                .sorted()
                .forEach(item -> {
                    List<String> row = ShowResourceGroupUsageStmt.getColumnSuppliers().stream()
                            .map(columnSupplier -> columnSupplier.apply(item))
                            .collect(Collectors.toList());
                    rows.add(row);
                });

        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle show authors
    private void handleEmpty() {
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
        List<Function> functions;
        if (showStmt.getIsBuiltin()) {
            functions = connectContext.getGlobalStateMgr().getBuiltinFunctions();
        } else if (showStmt.getIsGlobal()) {
            functions = connectContext.getGlobalStateMgr().getGlobalFunctionMgr().getFunctions();
        } else {
            Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDbName());
            MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());
            functions = db.getFunctions();
        }

        List<List<Comparable>> rowSet = Lists.newArrayList();
        for (Function function : functions) {
            List<Comparable> row = function.getInfo(showStmt.getIsVerbose());
            // like predicate
            if (showStmt.getWild() == null || showStmt.like(function.functionName())) {
                if (showStmt.getIsGlobal()) {
                    try {
                        Authorizer.checkAnyActionOnGlobalFunction(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), function);
                    } catch (AccessDeniedException e) {
                        continue;
                    }
                } else if (!showStmt.getIsBuiltin()) {
                    Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDbName());
                    try {
                        Authorizer.checkAnyActionOnFunction(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), db.getFullName(), function);
                    } catch (AccessDeniedException e) {
                        continue;
                    }
                }

                rowSet.add(row);
            }
        }

        // sort function rows by first column asc
        ListComparator<List<Comparable>> comparator;
        OrderByPair orderByPair = new OrderByPair(0, false);
        comparator = new ListComparator<>(orderByPair);
        rowSet.sort(comparator);
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

    // Show databases statement
    private void handleShowDb() {
        ShowDbStmt showDbStmt = (ShowDbStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> dbNames;
        String catalogName;
        if (showDbStmt.getCatalogName() == null) {
            catalogName = connectContext.getCurrentCatalog();
        } else {
            catalogName = showDbStmt.getCatalogName();
        }
        dbNames = metadataMgr.listDbNames(catalogName);

        PatternMatcher matcher = null;
        if (showDbStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showDbStmt.getPattern(),
                    CaseSensibility.DATABASE.getCaseSensibility());
        }
        Set<String> dbNameSet = Sets.newTreeSet();
        for (String dbName : dbNames) {
            // Filter dbname
            if (matcher != null && !matcher.match(dbName)) {
                continue;
            }

            try {
                Authorizer.checkAnyActionOnOrInDb(connectContext.getCurrentUserIdentity(),
                        connectContext.getCurrentRoleIds(), catalogName, dbName);
            } catch (AccessDeniedException e) {
                continue;
            }

            dbNameSet.add(dbName);
        }

        for (String dbName : dbNameSet) {
            rows.add(Lists.newArrayList(dbName));
        }

        resultSet = new ShowResultSet(showDbStmt.getMetaData(), rows);
    }

    // Show table statement.
    private void handleShowTable() throws AnalysisException {
        ShowTableStmt showTableStmt = (ShowTableStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        String catalogName = showTableStmt.getCatalogName();
        if (catalogName == null) {
            catalogName = connectContext.getCurrentCatalog();
        }
        String dbName = showTableStmt.getDb();
        Database db = metadataMgr.getDb(catalogName, dbName);

        PatternMatcher matcher = null;
        if (showTableStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showTableStmt.getPattern(),
                    CaseSensibility.TABLE.getCaseSensibility());
        }

        Map<String, String> tableMap = Maps.newTreeMap();
        MetaUtils.checkDbNullAndReport(db, showTableStmt.getDb());

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            List<String> tableNames = metadataMgr.listTableNames(catalogName, dbName);

            for (String tableName : tableNames) {
                if (matcher != null && !matcher.match(tableName)) {
                    continue;
                }
                Table table = metadataMgr.getTable(catalogName, dbName, tableName);
                if (table == null) {
                    LOG.warn("table {}.{}.{} does not exist", catalogName, dbName, tableName);
                    continue;
                }
                try {
                    if (table.isOlapView()) {
                        Authorizer.checkAnyActionOnView(
                                connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                                new TableName(db.getFullName(), table.getName()));
                    } else if (table.isMaterializedView()) {
                        Authorizer.checkAnyActionOnMaterializedView(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(db.getFullName(), table.getName()));
                    } else {
                        Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(db.getFullName(), table.getName()));
                    }
                } catch (AccessDeniedException e) {
                    continue;
                }

                tableMap.put(tableName, table.getMysqlType());
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
    private void handleShowTableStatus() {
        ShowTableStatusStmt showStmt = (ShowTableStatusStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDb());
        ZoneId currentTimeZoneId = TimeUtils.getTimeZone().toZoneId();
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
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

                    try {
                        Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(db.getFullName(), table.getName()));
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    TTableInfo info = new TTableInfo();
                    if (table.isNativeTableOrMaterializedView() || table.getType() == Table.TableType.OLAP_EXTERNAL) {
                        InformationSchemaDataSource.genNormalTableInfo(table, info);
                    } else {
                        InformationSchemaDataSource.genDefaultConfigInfo(info);
                    }

                    List<String> row = Lists.newArrayList();
                    // Name
                    row.add(table.getName());
                    // Engine
                    row.add(table.getEngine());
                    // Version
                    row.add(null);
                    // Row_format
                    row.add("");
                    // Rows
                    row.add(String.valueOf(info.getTable_rows()));
                    // Avg_row_length
                    row.add(String.valueOf(info.getAvg_row_length()));
                    // Data_length
                    row.add(String.valueOf(info.getData_length()));
                    // Max_data_length
                    row.add(null);
                    // Index_length
                    row.add(null);
                    // Data_free
                    row.add(null);
                    // Auto_increment
                    row.add(null);
                    // Create_time
                    row.add(DateUtils.formatTimestampInSeconds(table.getCreateTime(), currentTimeZoneId));
                    // Update_time
                    row.add(DateUtils.formatTimestampInSeconds(info.getUpdate_time(), currentTimeZoneId));
                    // Check_time
                    row.add(null);
                    // Collation
                    row.add(InformationSchemaDataSource.UTF8_GENERAL_CI);
                    // Checksum
                    row.add(null);
                    // Create_options
                    row.add("");
                    // Comment
                    row.add(table.getDisplayComment());

                    rows.add(row);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show variables like
    private void handleShowVariables() {
        ShowVariablesStmt showStmt = (ShowVariablesStmt) stmt;
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<List<String>> rows = VariableMgr.dump(showStmt.getType(), connectContext.getSessionVariable(), matcher);
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create database
    private void handleShowCreateDb() throws AnalysisException {
        ShowCreateDbStmt showStmt = (ShowCreateDbStmt) stmt;
        String catalogName = showStmt.getCatalogName();
        String dbName = showStmt.getDb();
        List<List<String>> rows = Lists.newArrayList();

        Database db;
        if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
            db = connectContext.getGlobalStateMgr().getDb(dbName);
        } else {
            db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        }
        MetaUtils.checkDbNullAndReport(db, showStmt.getDb());

        StringBuilder createSqlBuilder = new StringBuilder();
        createSqlBuilder.append("CREATE DATABASE `").append(showStmt.getDb()).append("`");
        if (!Strings.isNullOrEmpty(db.getLocation())) {
            createSqlBuilder.append("\nPROPERTIES (\"location\" = \"").append(db.getLocation()).append("\")");
        } else if (RunMode.isSharedDataMode()) {
            String volume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeNameOfDb(db.getId());
            createSqlBuilder.append("\nPROPERTIES (\"storage_volume\" = \"").append(volume).append("\")");
        }
        rows.add(Lists.newArrayList(showStmt.getDb(), createSqlBuilder.toString()));
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show create table
    private void handleShowCreateTable() throws AnalysisException {
        ShowCreateTableStmt showStmt = (ShowCreateTableStmt) stmt;
        TableName tbl = showStmt.getTbl();
        String catalogName = tbl.getCatalog();
        if (catalogName == null) {
            catalogName = connectContext.getCurrentCatalog();
        }
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            showCreateInternalCatalogTable(showStmt);
        } else {
            showCreateExternalCatalogTable(tbl, catalogName);
        }
    }

    private void showCreateExternalCatalogTable(TableName tbl, String catalogName) {
        String dbName = tbl.getDb();
        String tableName = tbl.getTbl();
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Database db = metadataMgr.getDb(catalogName, dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = metadataMgr.getTable(catalogName, dbName, tableName);
        if (table == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }

        // create table catalogName.dbName.tableName (
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ")
                .append("`").append(tableName).append("`")
                .append(" (\n");

        // Columns
        List<String> columns = table.getFullSchema().stream().map(
                this::toMysqlDDL).collect(Collectors.toList());
        createTableSql.append(String.join(",\n", columns))
                .append("\n)");

        // Partition column names
        if (table.getType() != JDBC && !table.isUnPartitioned()) {
            createTableSql.append("\nPARTITION BY ( ")
                    .append(String.join(", ", table.getPartitionColumnNames()))
                    .append(" )");
        }

        // Location
        String location = null;
        if (table.isHiveTable() || table.isHudiTable()) {
            location = ((HiveMetaStoreTable) table).getTableLocation();
        } else if (table.isIcebergTable()) {
            location = table.getTableLocation();
        } else if (table.isDeltalakeTable()) {
            location = table.getTableLocation();
        } else if (table.isPaimonTable()) {
            location = table.getTableLocation();
        }

        if (!Strings.isNullOrEmpty(location)) {
            createTableSql.append("\nPROPERTIES (\"location\" = \"").append(location).append("\");");
        }

        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(tableName, createTableSql.toString()));
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private String toMysqlDDL(Column column) {
        StringBuilder sb = new StringBuilder();
        sb.append("  `").append(column.getName()).append("` ");
        sb.append(column.getType().toSql());
        sb.append(" DEFAULT NULL");

        if (!Strings.isNullOrEmpty(column.getComment())) {
            sb.append(" COMMENT \"").append(column.getDisplayComment()).append("\"");
        }

        return sb.toString();
    }

    private void showCreateInternalCatalogTable(ShowCreateTableStmt showStmt) throws AnalysisException {
        Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDb());
        MetaUtils.checkDbNullAndReport(db, showStmt.getDb());
        List<List<String>> rows = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(showStmt.getTable());
            if (table == null) {
                if (showStmt.getType() != ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
                } else {
                    // For Sync Materialized View, it is a mv index inside OLAP table,
                    // so we can not get it from database.
                    for (Table tbl : db.getTables()) {
                        if (tbl.getType() == Table.TableType.OLAP) {
                            OlapTable olapTable = (OlapTable) tbl;
                            List<MaterializedIndexMeta> visibleMaterializedViews =
                                    olapTable.getVisibleIndexMetas();
                            for (MaterializedIndexMeta mvMeta : visibleMaterializedViews) {
                                if (olapTable.getIndexNameById(mvMeta.getIndexId()).equals(showStmt.getTable())) {
                                    if (mvMeta.getOriginStmt() == null) {
                                        String mvName = olapTable.getIndexNameById(mvMeta.getIndexId());
                                        rows.add(Lists.newArrayList(showStmt.getTable(), buildCreateMVSql(olapTable,
                                                mvName, mvMeta), "utf8", "utf8_general_ci"));
                                    } else {
                                        rows.add(Lists.newArrayList(showStmt.getTable(), mvMeta.getOriginStmt(),
                                                "utf8", "utf8_general_ci"));
                                    }
                                    resultSet =
                                            new ShowResultSet(ShowCreateTableStmt.getMaterializedViewMetaData(), rows);
                                    return;
                                }
                            }
                        }
                    }
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
                }
            }

            List<String> createTableStmt = Lists.newArrayList();
            GlobalStateMgr.getDdlStmt(table, createTableStmt, null, null, false, true /* hide password */);
            if (createTableStmt.isEmpty()) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
                return;
            }

            if (table instanceof View) {
                if (showStmt.getType() == ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                            showStmt.getTable(), "MATERIALIZED VIEW");
                }
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0), "utf8", "utf8_general_ci"));
                resultSet = new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
            } else if (table instanceof MaterializedView) {
                // In order to be compatible with BI, we return the syntax supported by
                // mysql according to the standard syntax.
                if (showStmt.getType() == ShowCreateTableStmt.CreateTableType.VIEW) {
                    MaterializedView mv = (MaterializedView) table;
                    String sb = "CREATE VIEW `" + table.getName() + "` AS " + mv.getViewDefineSql();
                    rows.add(Lists.newArrayList(table.getName(), sb, "utf8", "utf8_general_ci"));
                    resultSet = new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
                } else {
                    rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                    resultSet = new ShowResultSet(ShowCreateTableStmt.getMaterializedViewMetaData(), rows);
                }
            } else {
                if (showStmt.getType() != ShowCreateTableStmt.CreateTableType.TABLE) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                            showStmt.getTable(), showStmt.getType().getValue());
                }
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
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
        String catalogName = showStmt.getCatalog();
        if (catalogName == null) {
            catalogName = connectContext.getCurrentCatalog();
        }
        String dbName = showStmt.getDb();
        Database db = metadataMgr.getDb(catalogName, dbName);
        MetaUtils.checkDbNullAndReport(db, showStmt.getDb());
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = metadataMgr.getTable(catalogName, dbName, showStmt.getTable());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR,
                        showStmt.getDb() + "." + showStmt.getTable());
            }
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
                String defaultValue = null;
                if (!col.getType().isOnlyMetricType()) {
                    defaultValue = col.getMetaDefaultValue(Lists.newArrayList());
                }
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
                            col.getDisplayComment()));
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
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Show index statement.
    private void handleShowIndex() throws AnalysisException {
        ShowIndexStmt showStmt = (ShowIndexStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDbName());
        MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(showStmt.getTableName().getTbl());
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR,
                        db.getOriginName() + "." + showStmt.getTableName().toString());
            } else if (table instanceof OlapTable) {
                List<Index> indexes = ((OlapTable) table).getIndexes();
                for (Index index : indexes) {
                    rows.add(Lists.newArrayList(showStmt.getTableName().toString(), "",
                            index.getIndexName(), "", String.join(",", index.getColumns()), "", "", "", "",
                            "", String.format("%s%s", index.getIndexType().name(), index.getPropertiesString()),
                            index.getComment()));
                }
            } else {
                // other type view, mysql, hive, es
                // do nothing
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    // Handle help statement.
    private void handleHelp() {
        HelpStmt helpStmt = (HelpStmt) stmt;
        resultSet = new ShowResultSet(helpStmt.getKeywordMetaData(), EMPTY_SET);
    }

    // Show load statement.
    private void handleShowLoad() throws AnalysisException {
        ShowLoadStmt showStmt = (ShowLoadStmt) stmt;

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        long dbId = -1;
        if (showStmt.isAll()) {
            dbId = -1;
        } else {
            Database db = globalStateMgr.getDb(showStmt.getDbName());
            MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());
            dbId = db.getId();
        }

        // combine the List<LoadInfo> of load(v1) and loadManager(v2)
        Set<String> statesValue = showStmt.getStates() == null ? null : showStmt.getStates().stream()
                .map(Enum::name)
                .collect(Collectors.toSet());
        List<List<Comparable>> loadInfos =
                globalStateMgr.getLoadMgr().getLoadJobInfosByDb(dbId, showStmt.getLabelValue(),
                        showStmt.isAccurateMatch(),
                        statesValue);

        // order the result of List<LoadInfo> by orderByPairs in show stmt
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<>(0);
        }
        loadInfos.sort(comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> loadInfo : loadInfos) {
            List<String> oneInfo = new ArrayList<>(loadInfo.size());

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

    private void handleShowCreateRoutineLoad() throws AnalysisException {
        ShowCreateRoutineLoadStmt showCreateRoutineLoadStmt = (ShowCreateRoutineLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<RoutineLoadJob> routineLoadJobList;
        try {
            routineLoadJobList = GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                    .getJob(showCreateRoutineLoadStmt.getDbFullName(),
                            showCreateRoutineLoadStmt.getName(),
                            false);
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        if (routineLoadJobList == null || routineLoadJobList.size() == 0) {
            resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
            return;
        }
        RoutineLoadJob routineLoadJob = routineLoadJobList.get(0);
        if (routineLoadJob.getDataSourceTypeName().equals("PULSAR")) {
            throw new AnalysisException("not support pulsar datasource");
        }
        StringBuilder createRoutineLoadSql = new StringBuilder();
        try {
            String dbName = routineLoadJob.getDbFullName();
            createRoutineLoadSql.append("CREATE ROUTINE LOAD ").append(dbName).append(".")
                    .append(showCreateRoutineLoadStmt.getName())
                    .append(" on ").append(routineLoadJob.getTableName());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }

        if (routineLoadJob.getColumnSeparator() != null) {
            createRoutineLoadSql.append("\n COLUMNS TERMINATED BY ")
                    .append(routineLoadJob.getColumnSeparator().toSql(true));
        }

        if (routineLoadJob.getColumnDescs() != null) {
            createRoutineLoadSql.append(",\nCOLUMNS (");
            List<ImportColumnDesc> descs = routineLoadJob.getColumnDescs();
            for (int i = 0; i < descs.size(); i++) {
                ImportColumnDesc desc = descs.get(i);
                createRoutineLoadSql.append(desc.toString());
                if (descs.size() == 1 || i == descs.size() - 1) {
                    createRoutineLoadSql.append(")");
                } else {
                    createRoutineLoadSql.append(", ");
                }
            }
        }
        if (routineLoadJob.getPartitions() != null) {
            createRoutineLoadSql.append(",\n");
            createRoutineLoadSql.append(routineLoadJob.getPartitions().toString());
        }
        if (routineLoadJob.getWhereExpr() != null) {
            createRoutineLoadSql.append(",\nWHERE ");
            createRoutineLoadSql.append(routineLoadJob.getWhereExpr().toSql());
        }

        createRoutineLoadSql.append("\nPROPERTIES\n").append(routineLoadJob.jobPropertiesToSql());
        createRoutineLoadSql.append("FROM ").append(routineLoadJob.getDataSourceTypeName()).append("\n");
        createRoutineLoadSql.append(routineLoadJob.dataSourcePropertiesToSql());
        createRoutineLoadSql.append(";");
        rows.add(Lists.newArrayList(showCreateRoutineLoadStmt.getName(), createRoutineLoadSql.toString()));
        resultSet = new ShowResultSet(showCreateRoutineLoadStmt.getMetaData(), rows);
    }

    private void handleShowRoutineLoad() throws AnalysisException {
        ShowRoutineLoadStmt showRoutineLoadStmt = (ShowRoutineLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if job exists
        List<RoutineLoadJob> routineLoadJobList;
        try {
            routineLoadJobList = GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                    .getJob(showRoutineLoadStmt.getDbFullName(),
                            showRoutineLoadStmt.getName(),
                            showRoutineLoadStmt.isIncludeHistory());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }
        // In new privilege framework(RBAC), user needs any action on the table to show routine load job on it.
        if (routineLoadJobList != null) {
            Iterator<RoutineLoadJob> iterator = routineLoadJobList.iterator();
            while (iterator.hasNext()) {
                RoutineLoadJob routineLoadJob = iterator.next();
                try {
                    try {
                        Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(routineLoadJob.getDbFullName(),
                                        routineLoadJob.getTableName()));
                    } catch (AccessDeniedException e) {
                        iterator.remove();
                    }
                } catch (MetaNotFoundException e) {
                    // ignore
                }
            }
        }

        if (routineLoadJobList != null) {
            RoutineLoadFunctionalExprProvider fProvider =
                    showRoutineLoadStmt.getFunctionalExprProvider(this.connectContext);
            rows = routineLoadJobList.parallelStream()
                    .filter(fProvider.getPredicateChain())
                    .sorted(fProvider.getOrderComparator())
                    .skip(fProvider.getSkipCount())
                    .limit(fProvider.getLimitCount())
                    .map(RoutineLoadJob::getShowInfo)
                    .collect(Collectors.toList());
        }

        if (!Strings.isNullOrEmpty(showRoutineLoadStmt.getName()) && rows.isEmpty()) {
            // if the jobName has been specified
            throw new AnalysisException("There is no running job named " + showRoutineLoadStmt.getName()
                    + " in db " + showRoutineLoadStmt.getDbFullName()
                    + ". Include history? " + showRoutineLoadStmt.isIncludeHistory()
                    +
                    ", you can try `show all routine load job for job_name` if you want to list stopped and cancelled jobs");
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
                    GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
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
        // In new privilege framework(RBAC), user needs any action on the table to show routine load job on it.
        try {
            Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                    connectContext.getCurrentRoleIds(), new TableName(dbFullName, tableName));
        } catch (AccessDeniedException e) {
            // if we have no privilege, return an empty result set
            resultSet = new ShowResultSet(showRoutineLoadTaskStmt.getMetaData(), rows);
            return;
        }

        // get routine load task info
        rows.addAll(routineLoadJob.getTasksShowInfo());
        resultSet = new ShowResultSet(showRoutineLoadTaskStmt.getMetaData(), rows);
    }

    private void handleShowStreamLoad() throws AnalysisException {
        ShowStreamLoadStmt showStreamLoadStmt = (ShowStreamLoadStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        // if task exists
        List<StreamLoadTask> streamLoadTaskList;
        try {
            streamLoadTaskList = GlobalStateMgr.getCurrentState().getStreamLoadMgr()
                    .getTask(showStreamLoadStmt.getDbFullName(),
                            showStreamLoadStmt.getName(),
                            showStreamLoadStmt.isIncludeHistory());
        } catch (MetaNotFoundException e) {
            LOG.warn(e.getMessage(), e);
            throw new AnalysisException(e.getMessage());
        }

        if (streamLoadTaskList != null) {
            StreamLoadFunctionalExprProvider fProvider =
                    showStreamLoadStmt.getFunctionalExprProvider(this.connectContext);
            rows = streamLoadTaskList.parallelStream()
                    .filter(fProvider.getPredicateChain())
                    .sorted(fProvider.getOrderComparator())
                    .skip(fProvider.getSkipCount())
                    .limit(fProvider.getLimitCount())
                    .map(StreamLoadTask::getShowInfo)
                    .collect(Collectors.toList());
        }

        if (!Strings.isNullOrEmpty(showStreamLoadStmt.getName()) && rows.isEmpty()) {
            // if the label has been specified
            throw new AnalysisException("There is no label named " + showStreamLoadStmt.getName()
                    + " in db " + showStreamLoadStmt.getDbFullName()
                    + ". Include history? " + showStreamLoadStmt.isIncludeHistory());
        }
        resultSet = new ShowResultSet(showStreamLoadStmt.getMetaData(), rows);
    }

    // Show user property statement
    private void handleShowUserProperty() throws AnalysisException {
        ShowUserPropertyStmt showStmt = (ShowUserPropertyStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getRows(connectContext));
    }

    // Show delete statement.
    private void handleShowDelete() throws AnalysisException {
        ShowDeleteStmt showStmt = (ShowDeleteStmt) stmt;

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDb(showStmt.getDbName());
        MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());
        long dbId = db.getId();

        DeleteMgr deleteHandler = globalStateMgr.getDeleteMgr();
        List<List<Comparable>> deleteInfos = deleteHandler.getDeleteInfosByDb(dbId);
        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> deleteInfo : deleteInfos) {
            List<String> oneInfo = new ArrayList<>(deleteInfo.size());
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
        // Only SchemaChangeProc support where/order by/limit syntax
        if (procNodeI instanceof SchemaChangeProcDir) {
            rows = ((SchemaChangeProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
                    showStmt.getOrderPairs(), showStmt.getLimitElement()).getRows();
        } else if (procNodeI instanceof OptimizeProcDir) {
            rows = ((OptimizeProcDir) procNodeI).fetchResultByFilter(showStmt.getFilterMap(),
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

    private void handleShowData() {
        ShowDataStmt showStmt = (ShowDataStmt) stmt;
        String dbName = showStmt.getDbName();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            String tableName = showStmt.getTableName();
            List<List<String>> totalRows = showStmt.getResultRows();
            if (tableName == null) {
                long totalSize = 0;
                long totalReplicaCount = 0;

                // sort by table name
                List<Table> tables = db.getTables();
                SortedSet<Table> sortedTables = new TreeSet<>(Comparator.comparing(Table::getName));

                for (Table table : tables) {
                    try {
                        Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), new TableName(dbName, table.getName()));
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    sortedTables.add(table);
                }

                for (Table table : sortedTables) {
                    if (!table.isNativeTableOrMaterializedView()) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    long tableSize = olapTable.getDataSize();
                    long replicaCount = olapTable.getReplicaCount();

                    Pair<Double, String> tableSizePair = DebugUtil.getByteUint(tableSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(tableSizePair.first) + " "
                            + tableSizePair.second;

                    List<String> row = Arrays.asList(table.getName(), readableSize, String.valueOf(replicaCount));
                    totalRows.add(row);

                    totalSize += tableSize;
                    totalReplicaCount += replicaCount;
                } // end for tables

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> total = Arrays.asList("Total", readableSize, String.valueOf(totalReplicaCount));
                totalRows.add(total);

                // quota
                long quota = db.getDataQuota();
                long replicaQuota = db.getReplicaQuota();
                Pair<Double, String> quotaPair = DebugUtil.getByteUint(quota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaPair.first) + " "
                        + quotaPair.second;

                List<String> quotaRow = Arrays.asList("Quota", readableQuota, String.valueOf(replicaQuota));
                totalRows.add(quotaRow);

                // left
                long left = Math.max(0, quota - totalSize);
                long replicaCountLeft = Math.max(0, replicaQuota - totalReplicaCount);
                Pair<Double, String> leftPair = DebugUtil.getByteUint(left);
                String readableLeft = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftPair.first) + " "
                        + leftPair.second;
                List<String> leftRow = Arrays.asList("Left", readableLeft, String.valueOf(replicaCountLeft));
                totalRows.add(leftRow);
            } else {
                try {
                    Authorizer.checkAnyActionOnTable(connectContext.getCurrentUserIdentity(),
                            connectContext.getCurrentRoleIds(), new TableName(dbName, tableName));
                } catch (AccessDeniedException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW DATA",
                            connectContext.getCurrentUserIdentity().getUser(),
                            connectContext.getCurrentUserIdentity().getHost(),
                            tableName);
                }

                Table table = db.getTable(tableName);
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }

                if (!table.isNativeTableOrMaterializedView()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
                }

                OlapTable olapTable = (OlapTable) table;
                int i = 0;
                long totalSize = 0;
                long totalReplicaCount = 0;

                // sort by index name
                Map<String, Long> indexNames = olapTable.getIndexNameToId();
                Map<String, Long> sortedIndexNames = new TreeMap<>(indexNames);

                for (Long indexId : sortedIndexNames.values()) {
                    long indexSize = 0;
                    long indexReplicaCount = 0;
                    long indexRowCount = 0;
                    for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
                        MaterializedIndex mIndex = partition.getIndex(indexId);
                        indexSize += mIndex.getDataSize();
                        indexReplicaCount += mIndex.getReplicaCount();
                        indexRowCount += mIndex.getRowCount();
                    }

                    Pair<Double, String> indexSizePair = DebugUtil.getByteUint(indexSize);
                    String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(indexSizePair.first) + " "
                            + indexSizePair.second;

                    List<String> row = null;
                    if (i == 0) {
                        row = Arrays.asList(tableName,
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    } else {
                        row = Arrays.asList("",
                                olapTable.getIndexNameById(indexId),
                                readableSize, String.valueOf(indexReplicaCount),
                                String.valueOf(indexRowCount));
                    }

                    totalSize += indexSize;
                    totalReplicaCount += indexReplicaCount;
                    totalRows.add(row);

                    i++;
                } // end for indices

                Pair<Double, String> totalSizePair = DebugUtil.getByteUint(totalSize);
                String readableSize = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalSizePair.first) + " "
                        + totalSizePair.second;
                List<String> row = Arrays.asList("", "Total", readableSize, String.valueOf(totalReplicaCount), "");
                totalRows.add(row);
            }
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
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

                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    Table table = db.getTable(tableId);
                    if (!(table instanceof OlapTable)) {
                        isSync = false;
                        break;
                    }
                    tableName = table.getName();

                    OlapTable olapTable = (OlapTable) table;
                    PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
                    if (physicalPartition == null) {
                        isSync = false;
                        break;
                    }
                    Partition partition = olapTable.getPartition(physicalPartition.getParentId());
                    partitionName = partition.getName();

                    MaterializedIndex index = physicalPartition.getIndex(indexId);
                    if (index == null) {
                        isSync = false;
                        break;
                    }
                    indexName = olapTable.getIndexNameById(indexId);

                    if (table.isCloudNativeTableOrMaterializedView()) {
                        break;
                    }

                    LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                    if (tablet == null) {
                        isSync = false;
                        break;
                    }

                    List<Replica> replicas = tablet.getImmutableReplicas();
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
                    locker.unLockDatabase(db, LockType.READ);
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
            MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = db.getTable(showStmt.getTableName());
                if (table == null) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTableName());
                }
                if (!table.isNativeTableOrMaterializedView()) {
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
                Collection<Partition> partitions = new ArrayList<>();
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
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                            if (indexId > -1 && index.getId() != indexId) {
                                continue;
                            }
                            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                                LakeTabletsProcDir procNode = new LakeTabletsProcDir(db, olapTable, index);
                                tabletInfos.addAll(procNode.fetchComparableResult());
                            } else {
                                LocalTabletsProcDir procDir = new LocalTabletsProcDir(db, olapTable, index);
                                tabletInfos.addAll(procDir.fetchComparableResult(
                                        showStmt.getVersion(), showStmt.getBackendId(), showStmt.getReplicaState()));
                            }
                            if (sizeLimit > -1 && CollectionUtils.isEmpty(showStmt.getOrderByPairs())
                                    && tabletInfos.size() >= sizeLimit) {
                                stop = true;
                                break;
                            }
                        }
                    }
                }

                // order by
                List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
                ListComparator<List<Comparable>> comparator;
                if (orderByPairs != null) {
                    OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
                    comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
                } else {
                    // order by tabletId, replicaId
                    comparator = new ListComparator<>(0, 1);
                }
                tabletInfos.sort(comparator);

                if (sizeLimit > -1 && tabletInfos.size() >= sizeLimit) {
                    tabletInfos = tabletInfos.subList((int) showStmt.getOffset(), (int) sizeLimit);
                }

                for (List<Comparable> tabletInfo : tabletInfos) {
                    List<String> oneTablet = new ArrayList<>(tabletInfo.size());
                    for (Comparable column : tabletInfo) {
                        oneTablet.add(column.toString());
                    }
                    rows.add(oneTablet);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
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
        MetaUtils.checkDbNullAndReport(db, showExportStmt.getDbName());
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
        List<List<String>> backendInfos = BackendsProcDir.getClusterBackendInfos();
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

        List<List<String>> snapshotInfos = repo.getSnapshotInfos(showStmt.getSnapshotName(), showStmt.getTimestamp(),
                showStmt.getSnapshotNames());
        resultSet = new ShowResultSet(showStmt.getMetaData(), snapshotInfos);
    }

    private void handleShowBackup() {
        ShowBackupStmt showStmt = (ShowBackupStmt) stmt;
        Database filterDb = GlobalStateMgr.getCurrentState().getDb(showStmt.getDbName());
        List<List<String>> infos = Lists.newArrayList();
        List<Database> dbs = Lists.newArrayList();

        if (filterDb == null) {
            for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState().getIdToDb().entrySet()) {
                dbs.add(entry.getValue());
            }
        } else {
            dbs.add(filterDb);
        }

        for (Database db : dbs) {
            AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
            if (!(jobI instanceof BackupJob)) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
                continue;
            }

            BackupJob backupJob = (BackupJob) jobI;

            // check privilege
            List<TableRef> tableRefs = backupJob.getTableRef();
            AtomicBoolean privilegeDeny = new AtomicBoolean(false);
            tableRefs.forEach(tableRef -> {
                TableName tableName = tableRef.getName();
                try {
                    Authorizer.checkTableAction(connectContext.getCurrentUserIdentity(), connectContext.getCurrentRoleIds(),
                            tableName.getDb(), tableName.getTbl(), PrivilegeType.EXPORT);
                } catch (AccessDeniedException e) {
                    privilegeDeny.set(true);
                }
            });
            if (privilegeDeny.get()) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
                return;
            }

            List<String> info = backupJob.getInfo();
            infos.add(info);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowRestore() {
        ShowRestoreStmt showStmt = (ShowRestoreStmt) stmt;
        Database filterDb = GlobalStateMgr.getCurrentState().getDb(showStmt.getDbName());
        List<List<String>> infos = Lists.newArrayList();
        List<Database> dbs = Lists.newArrayList();

        if (filterDb == null) {
            for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState().getIdToDb().entrySet()) {
                dbs.add(entry.getValue());
            }
        } else {
            dbs.add(filterDb);
        }

        for (Database db : dbs) {
            AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
            if (!(jobI instanceof RestoreJob)) {
                resultSet = new ShowResultSet(showStmt.getMetaData(), EMPTY_SET);
                continue;
            }

            RestoreJob restoreJob = (RestoreJob) jobI;
            List<String> info = restoreJob.getInfo();
            infos.add(info);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private String getCatalogNameById(long catalogId) throws MetaNotFoundException {
        if (CatalogMgr.isInternalCatalog(catalogId)) {
            return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        }

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Optional<Catalog> catalogOptional = catalogMgr.getCatalogById(catalogId);
        if (!catalogOptional.isPresent()) {
            throw new MetaNotFoundException("cannot find catalog");
        }

        return catalogOptional.get().getName();
    }

    private String getCatalogNameFromPEntry(ObjectType objectType, PrivilegeEntry privilegeEntry)
            throws MetaNotFoundException {
        if (objectType.equals(ObjectType.CATALOG)) {
            CatalogPEntryObject catalogPEntryObject =
                    (CatalogPEntryObject) privilegeEntry.getObject();
            if (catalogPEntryObject.getId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            } else {
                return getCatalogNameById(catalogPEntryObject.getId());
            }
        } else if (objectType.equals(ObjectType.DATABASE)) {
            DbPEntryObject dbPEntryObject = (DbPEntryObject) privilegeEntry.getObject();
            if (dbPEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            }
            return getCatalogNameById(dbPEntryObject.getCatalogId());
        } else if (objectType.equals(ObjectType.TABLE)) {
            TablePEntryObject tablePEntryObject = (TablePEntryObject) privilegeEntry.getObject();
            if (tablePEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            }
            return getCatalogNameById(tablePEntryObject.getCatalogId());
        } else {
            return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        }
    }

    private List<List<String>> privilegeToRowString(AuthorizationMgr authorizationManager, GrantRevokeClause userOrRoleName,
                                                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList)
            throws PrivilegeException {
        List<List<String>> infos = new ArrayList<>();
        for (Map.Entry<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntry
                : typeToPrivilegeEntryList.entrySet()) {
            for (PrivilegeEntry privilegeEntry : typeToPrivilegeEntry.getValue()) {
                ObjectType objectType = typeToPrivilegeEntry.getKey();
                String catalogName;
                try {
                    catalogName = getCatalogNameFromPEntry(objectType, privilegeEntry);
                } catch (MetaNotFoundException e) {
                    // ignore this entry
                    continue;
                }
                List<String> info = new ArrayList<>();
                info.add(userOrRoleName.getRoleName() != null ?
                        userOrRoleName.getRoleName() : userOrRoleName.getUserIdentity().toString());
                info.add(catalogName);

                GrantPrivilegeStmt grantPrivilegeStmt = new GrantPrivilegeStmt(new ArrayList<>(), objectType.name(),
                        userOrRoleName, null, privilegeEntry.isWithGrantOption());

                grantPrivilegeStmt.setObjectType(objectType);
                ActionSet actionSet = privilegeEntry.getActionSet();
                List<PrivilegeType> privList = authorizationManager.analyzeActionSet(objectType, actionSet);
                grantPrivilegeStmt.setPrivilegeTypes(privList);
                grantPrivilegeStmt.setObjectList(Lists.newArrayList(privilegeEntry.getObject()));

                try {
                    info.add(AstToSQLBuilder.toSQL(grantPrivilegeStmt));
                    infos.add(info);
                } catch (com.starrocks.sql.common.MetaNotFoundException e) {
                    //Ignore the case of MetaNotFound in the show statement, such as metadata being deleted
                }
            }
        }

        return infos;
    }

    private void handleShowGrants() {
        ShowGrantsStmt showStmt = (ShowGrantsStmt) stmt;

        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            List<List<String>> infos = new ArrayList<>();
            if (showStmt.getRole() != null) {
                List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForRole(showStmt.getRole());
                if (granteeRole != null) {
                    infos.add(granteeRole);
                }

                Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                        authorizationManager.getTypeToPrivilegeEntryListByRole(showStmt.getRole());
                infos.addAll(privilegeToRowString(authorizationManager,
                        new GrantRevokeClause(null, showStmt.getRole()), typeToPrivilegeEntryList));
            } else {
                List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForUser(showStmt.getUserIdent());
                if (granteeRole != null) {
                    infos.add(granteeRole);
                }

                Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                        authorizationManager.getTypeToPrivilegeEntryListByUser(showStmt.getUserIdent());
                infos.addAll(privilegeToRowString(authorizationManager,
                        new GrantRevokeClause(showStmt.getUserIdent(), null), typeToPrivilegeEntryList));
            }
            resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    private void handleShowRoles() {
        ShowRolesStmt showStmt = (ShowRolesStmt) stmt;

        List<List<String>> infos = new ArrayList<>();
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        List<String> roles = authorizationManager.getAllRoles();
        roles.forEach(e -> infos.add(Lists.newArrayList(e,
                authorizationManager.isBuiltinRole(e) ? "true" : "false",
                authorizationManager.getRoleComment(e))));

        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleShowUser() {
        List<List<String>> rowSet = Lists.newArrayList();

        ShowUserStmt showUserStmt = (ShowUserStmt) stmt;
        if (showUserStmt.isAll()) {
            AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            List<String> users = authorizationManager.getAllUsers();
            users.forEach(u -> rowSet.add(Lists.newArrayList(u)));
        } else {
            List<String> row = Lists.newArrayList();
            row.add(connectContext.getCurrentUserIdentity().toString());
            rowSet.add(row);
        }

        resultSet = new ShowResultSet(stmt.getMetaData(), rowSet);
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
            results.sort(Comparator.comparing(o -> o.get(0)));
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
        Database db = connectContext.getGlobalStateMgr().getDb(showDynamicPartitionStmt.getDb());
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
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

                    try {
                        Authorizer.checkAnyActionOnTable(ConnectContext.get().getCurrentUserIdentity(),
                                ConnectContext.get().getCurrentRoleIds(), new TableName(db.getFullName(), olapTable.getName()));
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    DynamicPartitionProperty dynamicPartitionProperty =
                            olapTable.getTableProperty().getDynamicPartitionProperty();
                    String tableName = olapTable.getName();
                    int replicationNum = dynamicPartitionProperty.getReplicationNum();
                    replicationNum = (replicationNum == DynamicPartitionProperty.NOT_SET_REPLICATION_NUM) ?
                            olapTable.getDefaultReplicationNum() : RunMode.defaultReplicationNum();
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
                locker.unLockDatabase(db, LockType.READ);
            }
            resultSet = new ShowResultSet(showDynamicPartitionStmt.getMetaData(), rows);
        }
    }

    // Show transaction statement.
    private void handleShowTransaction() throws AnalysisException {
        ShowTransactionStmt showStmt = (ShowTransactionStmt) stmt;
        Database db = connectContext.getGlobalStateMgr().getDb(showStmt.getDbName());
        MetaUtils.checkDbNullAndReport(db, showStmt.getDbName());

        long txnId = showStmt.getTxnId();
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        resultSet = new ShowResultSet(showStmt.getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
    }

    private void handleShowPlugins() {
        ShowPluginsStmt pluginsStmt = (ShowPluginsStmt) stmt;
        List<List<String>> rows = GlobalStateMgr.getCurrentPluginMgr().getPluginShowInfos();
        resultSet = new ShowResultSet(pluginsStmt.getMetaData(), rows);
    }

    private void handleShowCharset() {
        ShowCharsetStmt showCharsetStmt = (ShowCharsetStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        // | utf8 | UTF-8 Unicode | utf8_general_ci | 3 |
        row.add("utf8");
        row.add("UTF-8 Unicode");
        row.add("utf8_general_ci");
        row.add("3");
        rows.add(row);
        resultSet = new ShowResultSet(showCharsetStmt.getMetaData(), rows);
    }

    // Show sql blacklist
    private void handleShowSqlBlackListStmt() {
        ShowSqlBlackListStmt showStmt = (ShowSqlBlackListStmt) stmt;

        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<String, BlackListSql> entry : SqlBlackList.getInstance().sqlBlackListMap.entrySet()) {
            List<String> oneSql = new ArrayList<>();
            oneSql.add(String.valueOf(entry.getValue().id));
            oneSql.add(entry.getKey());
            rows.add(oneSql);
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowDataCacheRulesStmt() {
        ShowDataCacheRulesStmt showStmt = (ShowDataCacheRulesStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), DataCacheMgr.getInstance().getShowResultSetRows());
    }

    private void handleShowAnalyzeJob() {
        List<AnalyzeJob> jobs = connectContext.getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList();
        List<List<String>> rows = Lists.newArrayList();
        jobs.sort(Comparator.comparing(AnalyzeJob::getId));
        for (AnalyzeJob job : jobs) {
            try {
                List<String> result = ShowAnalyzeJobStmt.showAnalyzeJobs(connectContext, job);
                if (result != null) {
                    rows.add(result);
                }
            } catch (MetaNotFoundException e) {
                // pass
                LOG.warn("analyze job {} meta not found, {}", job.getId(), e);
            }
        }
        rows = doPredicate(stmt, stmt.getMetaData(), rows);
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowAnalyzeStatus() {
        List<AnalyzeStatus> statuses = new ArrayList<>(connectContext.getGlobalStateMgr().getAnalyzeMgr()
                .getAnalyzeStatusMap().values());
        List<List<String>> rows = Lists.newArrayList();
        statuses.sort(Comparator.comparing(AnalyzeStatus::getId));
        for (AnalyzeStatus status : statuses) {
            try {
                List<String> result = ShowAnalyzeStatusStmt.showAnalyzeStatus(connectContext, status);
                if (result != null) {
                    rows.add(result);
                }
            } catch (MetaNotFoundException e) {
                // pass
            }
        }
        rows = doPredicate(stmt, stmt.getMetaData(), rows);
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowBasicStatsMeta() {
        List<BasicStatsMeta> metas = new ArrayList<>(connectContext.getGlobalStateMgr().getAnalyzeMgr()
                .getBasicStatsMetaMap().values());
        List<List<String>> rows = Lists.newArrayList();
        for (BasicStatsMeta meta : metas) {
            try {
                List<String> result = ShowBasicStatsMetaStmt.showBasicStatsMeta(connectContext, meta);
                if (result != null) {
                    rows.add(result);
                }
            } catch (MetaNotFoundException e) {
                // pass
            }
        }
        List<ExternalBasicStatsMeta> externalMetas =
                new ArrayList<>(connectContext.getGlobalStateMgr().getAnalyzeMgr().getExternalBasicStatsMetaMap().values());
        for (ExternalBasicStatsMeta meta : externalMetas) {
            try {
                List<String> result = ShowBasicStatsMetaStmt.showExternalBasicStatsMeta(connectContext, meta);
                if (result != null) {
                    rows.add(result);
                }
            } catch (MetaNotFoundException e) {
                // pass
            }
        }

        rows = doPredicate(stmt, stmt.getMetaData(), rows);
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowHistogramStatsMeta() {
        List<HistogramStatsMeta> metas = new ArrayList<>(connectContext.getGlobalStateMgr().getAnalyzeMgr()
                .getHistogramStatsMetaMap().values());
        List<List<String>> rows = Lists.newArrayList();
        for (HistogramStatsMeta meta : metas) {
            try {
                List<String> result = ShowHistogramStatsMetaStmt.showHistogramStatsMeta(connectContext, meta);
                if (result != null) {
                    rows.add(result);
                }
            } catch (MetaNotFoundException e) {
                // pass
            }
        }

        rows = doPredicate(stmt, stmt.getMetaData(), rows);
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowResourceGroup() throws AnalysisException {
        ShowResourceGroupStmt showResourceGroupStmt = (ShowResourceGroupStmt) stmt;
        List<List<String>> rows =
                GlobalStateMgr.getCurrentState().getResourceGroupMgr().showResourceGroup(showResourceGroupStmt);
        resultSet = new ShowResultSet(showResourceGroupStmt.getMetaData(), rows);
    }

    private void handleShowCatalogs() {
        ShowCatalogsStmt showCatalogsStmt = (ShowCatalogsStmt) stmt;
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
        List<List<String>> rowSet = catalogMgr.getCatalogsInfo().stream()
                .filter(row -> {
                            if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(row.get(0))) {

                                try {
                                    Authorizer.checkAnyActionOnCatalog(
                                            connectContext.getCurrentUserIdentity(),
                                            connectContext.getCurrentRoleIds(), row.get(0));
                                } catch (AccessDeniedException e) {
                                    return false;
                                }

                                return true;
                            }
                            return true;
                        }
                )
                .sorted(Comparator.comparing(o -> o.get(0))).collect(Collectors.toList());
        resultSet = new ShowResultSet(showCatalogsStmt.getMetaData(), rowSet);
    }

    // show warehouse statement
    private void handleShowWarehouses() {
        ShowWarehousesStmt showStmt = (ShowWarehousesStmt) stmt;
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouseMgr = globalStateMgr.getWarehouseMgr();
        List<List<String>> rowSet = warehouseMgr.getWarehousesInfo().stream()
                .sorted(Comparator.comparing(o -> o.get(0))).collect(Collectors.toList());
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    private List<List<String>> doPredicate(ShowStmt showStmt,
                                           ShowResultSetMetaData showResultSetMetaData,
                                           List<List<String>> rows) {
        Predicate predicate = showStmt.getPredicate();
        if (predicate == null) {
            return rows;
        }

        SlotRef slotRef = (SlotRef) predicate.getChild(0);
        StringLiteral stringLiteral = (StringLiteral) predicate.getChild(1);
        List<List<String>> returnRows = new ArrayList<>();
        BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;

        int idx = showResultSetMetaData.getColumnIdx(slotRef.getColumnName());
        if (binaryPredicate.getOp().isEquivalence()) {
            for (List<String> row : rows) {
                if (row.get(idx).equals(stringLiteral.getStringValue())) {
                    returnRows.add(row);
                }
            }
        }

        return returnRows;
    }

    private void handleShowCreateExternalCatalog() throws AnalysisException {
        ShowCreateExternalCatalogStmt showStmt = (ShowCreateExternalCatalogStmt) stmt;
        String catalogName = showStmt.getCatalogName();
        List<List<String>> rows = Lists.newArrayList();
        if (InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(catalogName)) {
            resultSet = new ShowResultSet(stmt.getMetaData(), rows);
            return;
        }

        Catalog catalog = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogByName(catalogName);
        if (catalog == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
        }

        // Create external catalog catalogName (
        StringBuilder createCatalogSql = new StringBuilder();
        createCatalogSql.append("CREATE EXTERNAL CATALOG ")
                .append("`").append(catalogName).append("`")
                .append("\n");

        // Comment
        String comment = catalog.getComment();
        if (comment != null) {
            createCatalogSql.append("comment \"").append(catalog.getDisplayComment()).append("\"\n");
        }
        Map<String, String> clonedConfig = new HashMap<>(catalog.getConfig());
        CredentialUtil.maskCredential(clonedConfig);
        // Properties
        createCatalogSql.append("PROPERTIES (")
                .append(new PrintableMap<>(clonedConfig, " = ", true, true))
                .append("\n)");
        rows.add(Lists.newArrayList(catalogName, createCatalogSql.toString()));
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowStorageVolumes() throws DdlException {
        ShowStorageVolumesStmt showStmt = (ShowStorageVolumesStmt) stmt;
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        StorageVolumeMgr storageVolumeMgr = globalStateMgr.getStorageVolumeMgr();
        List<String> storageVolumeNames = storageVolumeMgr.listStorageVolumeNames();
        PatternMatcher matcher = null;
        List<List<String>> rows = Lists.newArrayList();
        if (!showStmt.getPattern().isEmpty()) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.STORAGEVOLUME.getCaseSensibility());
        }
        PatternMatcher finalMatcher = matcher;
        storageVolumeNames = storageVolumeNames.stream()
                .filter(storageVolumeName -> finalMatcher == null || finalMatcher.match(storageVolumeName))
                .filter(storageVolumeName -> {
                                try {
                                    Authorizer.checkAnyActionOnStorageVolume(connectContext.getCurrentUserIdentity(),
                                            connectContext.getCurrentRoleIds(), storageVolumeName);
                                } catch (AccessDeniedException e) {
                                    return false;
                                }
                                return true;
                            }
                ).collect(Collectors.toList());
        for (String storageVolumeName : storageVolumeNames) {
            rows.add(Lists.newArrayList(storageVolumeName));
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleDescStorageVolume() throws AnalysisException {
        DescStorageVolumeStmt desc = (DescStorageVolumeStmt) stmt;
        resultSet = new ShowResultSet(desc.getMetaData(), desc.getResultRows());
    }

    private void handleShowPipes() {
        List<List<Comparable>> rows = Lists.newArrayList();
        ShowPipeStmt showStmt = (ShowPipeStmt) stmt;
        String dbName = ((ShowPipeStmt) stmt).getDbName();
        long dbId = GlobalStateMgr.getCurrentState().mayGetDb(dbName)
                .map(Database::getId)
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName));
        PipeManager pipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        for (Pipe pipe : pipeManager.getPipesUnlock().values()) {
            // show pipes in current database
            if (pipe.getPipeId().getDbId() != dbId) {
                continue;
            }

            // check privilege
            try {
                Authorizer.checkAnyActionOnPipe(connectContext.getCurrentUserIdentity(),
                        connectContext.getCurrentRoleIds(), new PipeName(dbName, pipe.getName()));
            } catch (AccessDeniedException e) {
                continue;
            }

            // execute
            List<Comparable> row = Lists.newArrayList();
            ShowPipeStmt.handleShow(row, pipe);
            rows.add(row);
        }

        // order by
        List<OrderByPair> orderByPairs = ((ShowPipeStmt) stmt).getOrderByPairs();
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<>(0);
        }
        rows.sort(comparator);

        // limit
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

        List<List<String>> result = rows.stream().map(x -> x.stream().map(y -> (String) y)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());
        resultSet = new ShowResultSet(stmt.getMetaData(), result);
    }

    private void handleDescPipe() {
        List<List<String>> rows = Lists.newArrayList();
        DescPipeStmt descStmt = ((DescPipeStmt) stmt);
        PipeManager pipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
        Pipe pipe = pipeManager.mayGetPipe(descStmt.getName())
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_UNKNOWN_PIPE, descStmt.getName()));

        List<String> row = Lists.newArrayList();
        DescPipeStmt.handleDesc(row, pipe);
        rows.add(row);
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowFailPoint() throws AnalysisException {
        ShowFailPointStatement showStmt = ((ShowFailPointStatement) stmt);
        // send request and build resultSet
        PListFailPointRequest request = new PListFailPointRequest();
        SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
        PatternMatcher matcher = null;
        if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.VARIABLES.getCaseSensibility());
        }
        List<Backend> backends = new LinkedList<>();
        if (showStmt.getBackends() == null) {
            List<Long> backendIds = clusterInfoService.getBackendIds(true);
            if (backendIds == null) {
                throw new AnalysisException("No alive backends");
            }
            for (long backendId : backendIds) {
                Backend backend = clusterInfoService.getBackend(backendId);
                if (backend == null) {
                    continue;
                }
                backends.add(backend);
            }
        } else {
            for (String backendAddr : showStmt.getBackends()) {
                String[] tmp = backendAddr.split(":");
                if (tmp.length != 2) {
                    throw new AnalysisException("invalid backend addr");
                }
                Backend backend = clusterInfoService.getBackendWithBePort(tmp[0], Integer.parseInt(tmp[1]));
                if (backend == null) {
                    throw new AnalysisException("cannot find backend with addr " + backendAddr);
                }
                backends.add(backend);
            }
        }
        // send request
        List<Pair<Backend, Future<PListFailPointResponse>>> futures = Lists.newArrayList();
        for (Backend backend : backends) {
            try {
                futures.add(Pair.create(backend,
                        BackendServiceClient.getInstance().listFailPointAsync(backend.getBrpcAddress(), request)));
            } catch (RpcException e) {
                throw new AnalysisException("sending list failpoint request fails");
            }
        }
        // handle response
        List<List<String>> rows = Lists.newArrayList();
        for (Pair<Backend, Future<PListFailPointResponse>> future : futures) {
            try {
                final Backend backend = future.first;
                final PListFailPointResponse result = future.second.get(10, TimeUnit.SECONDS);
                if (result != null && result.status.statusCode != TStatusCode.OK.getValue()) {
                    String errMsg = String.format("list failpoint status failed, backend: %s:%d, error: %s",
                            backend.getHost(), backend.getBePort(), result.status.errorMsgs.get(0));
                    LOG.warn(errMsg);
                    throw new AnalysisException(errMsg);
                }
                Preconditions.checkNotNull(result);
                for (PFailPointInfo failPointInfo : result.failPoints) {
                    String name = failPointInfo.name;
                    PFailPointTriggerMode triggerMode = failPointInfo.triggerMode;
                    if (matcher != null && !matcher.match(name)) {
                        continue;
                    }
                    List<String> row = Lists.newArrayList();
                    row.add(failPointInfo.name);
                    row.add(triggerMode.mode.toString());
                    if (triggerMode.mode == FailPointTriggerModeType.ENABLE_N_TIMES) {
                        row.add(Integer.toString(triggerMode.nTimes));
                    } else if (triggerMode.mode == FailPointTriggerModeType.PROBABILITY_ENABLE) {
                        row.add(Double.toString(triggerMode.probability));
                    } else {
                        row.add("");
                    }
                    row.add(String.format("%s:%d", backend.getHost(), backend.getBePort()));
                    rows.add(row);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                throw new AnalysisException(e.getMessage());
            }
        }
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowDictionary() throws AnalysisException {
        ShowDictionaryStmt showStmt = (ShowDictionaryStmt) stmt;
        List<List<String>> allInfo = null;
        try {
            allInfo = GlobalStateMgr.getCurrentState().getDictionaryMgr().getAllInfo(showStmt.getDictionaryName());
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), allInfo);
    }
}
