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
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergView;
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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
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
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.service.InformationSchemaDataSource;
import com.starrocks.sql.ShowTemporaryTableStmt;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AdminShowConfigStmt;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AstVisitor;
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
import com.starrocks.sql.ast.ShowBackendBlackListStmt;
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
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalHistogramStatsMeta;
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
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Table.TableType.JDBC;

// Execute one show statement.
public class ShowExecutor {
    private static final Logger LOG = LogManager.getLogger(ShowExecutor.class);
    private static final List<List<String>> EMPTY_SET = Lists.newArrayList();
    private final ShowExecutorVisitor showExecutorVisitor;

    public ShowExecutor(ShowExecutorVisitor showExecutorVisitor) {
        this.showExecutorVisitor = showExecutorVisitor;
    }

    public static ShowResultSet execute(ShowStmt statement, ConnectContext context) {
        return GlobalStateMgr.getCurrentState().getShowExecutor().showExecutorVisitor.visit(statement, context);
    }

    public static class ShowExecutorVisitor implements AstVisitor<ShowResultSet, ConnectContext> {
        private static final Logger LOG = LogManager.getLogger(ShowExecutor.ShowExecutorVisitor.class);
        private static final ShowExecutor.ShowExecutorVisitor INSTANCE = new ShowExecutor.ShowExecutorVisitor();
        public static ShowExecutor.ShowExecutorVisitor getInstance() {
            return INSTANCE;
        }

        protected ShowExecutorVisitor() {
        }

        @Override
        public ShowResultSet visitShowStatement(ShowStmt statement, ConnectContext context) {
            return new ShowResultSet(statement.getMetaData(), EMPTY_SET);
        }

        @Override
        public ShowResultSet visitShowMaterializedViewStatement(ShowMaterializedViewsStmt statement, ConnectContext context) {
            String dbName = statement.getDb();
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            MetaUtils.checkDbNullAndReport(db, dbName);

            List<MaterializedView> materializedViews = Lists.newArrayList();
            List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs = Lists.newArrayList();
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                PatternMatcher matcher = null;
                if (statement.getPattern() != null) {
                    matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
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
                            Table baseTable = MvUtils.getTableChecked(baseTableInfo);
                            // TODO: external table should check table action after AuthorizationManager support it.
                            if (baseTable != null && baseTable.isNativeTableOrMaterializedView()) {
                                try {
                                    Authorizer.checkTableAction(context.getCurrentUserIdentity(),
                                            context.getCurrentRoleIds(), baseTableInfo.getDbName(),
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
                            Authorizer.checkAnyActionOnMaterializedView(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), new TableName(db.getFullName(), mvTable.getName()));
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

                List<ShowMaterializedViewStatus> mvStatusList =
                        listMaterializedViewStatus(dbName, materializedViews, singleTableMVs);
                List<List<String>> rowSets = mvStatusList.stream().map(ShowMaterializedViewStatus::toResultSet)
                        .collect(Collectors.toList());
                return new ShowResultSet(statement.getMetaData(), rowSets);
            } catch (Exception e) {
                LOG.warn("listMaterializedViews failed:", e);
                throw e;
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }

        @Override
        public ShowResultSet visitShowAuthorStatement(ShowAuthorStmt statement, ConnectContext context) {
            List<List<String>> rowSet = Lists.newArrayList();
            // Only success
            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowProcStmt(ShowProcStmt statement, ConnectContext context) {
            ShowResultSetMetaData metaData = statement.getMetaData();
            ProcNodeInterface procNode = statement.getNode();

            List<List<String>> finalRows = null;
            try {
                finalRows = procNode.fetchResult().getRows();
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }

            return new ShowResultSet(metaData, finalRows);
        }

        @Override
        public ShowResultSet visitHelpStatement(HelpStmt statement, ConnectContext context) {
            return new ShowResultSet(statement.getKeywordMetaData(), EMPTY_SET);
        }

        @Override
        public ShowResultSet visitShowDatabasesStatement(ShowDbStmt statement, ConnectContext context) {
            GlobalStateMgr.getCurrentState().tryLock(true);
            try {
                List<List<String>> rows = Lists.newArrayList();
                List<String> dbNames;
                String catalogName;
                if (statement.getCatalogName() == null) {
                    catalogName = context.getCurrentCatalog();
                } else {
                    catalogName = statement.getCatalogName();
                }
                dbNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listDbNames(catalogName);

                PatternMatcher matcher = null;
                if (statement.getPattern() != null) {
                    matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                            CaseSensibility.DATABASE.getCaseSensibility());
                }
                Set<String> dbNameSet = Sets.newTreeSet();
                for (String dbName : dbNames) {
                    // Filter dbname
                    if (matcher != null && !matcher.match(dbName)) {
                        continue;
                    }

                    try {
                        Authorizer.checkAnyActionOnOrInDb(context.getCurrentUserIdentity(),
                                context.getCurrentRoleIds(), catalogName, dbName);
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    dbNameSet.add(dbName);
                }

                for (String dbName : dbNameSet) {
                    rows.add(Lists.newArrayList(dbName));
                }

                return new ShowResultSet(((ShowDbStmt) statement).getMetaData(), rows);
            } finally {
                GlobalStateMgr.getCurrentState().unlock();
            }
        }

        public ShowResultSet visitShowTableStatement(ShowTableStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            String catalogName = statement.getCatalogName();
            if (catalogName == null) {
                catalogName = context.getCurrentCatalog();
            }
            String dbName = statement.getDb();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);

            PatternMatcher matcher = null;
            if (statement.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }

            Map<String, String> tableMap = Maps.newTreeMap();
            MetaUtils.checkDbNullAndReport(db, statement.getDb());

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                List<String> tableNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listTableNames(catalogName, dbName);

                for (String tableName : tableNames) {
                    if (matcher != null && !matcher.match(tableName)) {
                        continue;
                    }
                    BasicTable table = GlobalStateMgr.getCurrentState().getMetadataMgr().getBasicTable(
                            catalogName, dbName, tableName);
                    if (table == null) {
                        LOG.warn("table {}.{}.{} does not exist", catalogName, dbName, tableName);
                        continue;
                    }
                    try {
                        if (table.isOlapView()) {
                            Authorizer.checkAnyActionOnView(
                                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                    new TableName(db.getFullName(), table.getName()));
                        } else if (table.isMaterializedView()) {
                            Authorizer.checkAnyActionOnMaterializedView(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), new TableName(db.getFullName(), table.getName()));
                        } else {
                            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(),
                                    new TableName(catalogName, db.getFullName(), table.getName()));
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
                if (statement.isVerbose()) {
                    rows.add(Lists.newArrayList(entry.getKey(), entry.getValue()));
                } else {
                    rows.add(Lists.newArrayList(entry.getKey()));
                }
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowTemporaryTablesStatement(ShowTemporaryTableStmt statement, ConnectContext context) {
            statement.setSessionId(context.getSessionId());

            ShowTemporaryTableStmt showTemporaryTableStmt = statement;
            List<List<String>> rows = Lists.newArrayList();
            String catalogName = showTemporaryTableStmt.getCatalogName();
            if (catalogName == null) {
                catalogName = context.getCurrentCatalog();
            }

            String dbName = showTemporaryTableStmt.getDb();
            UUID sessionId = showTemporaryTableStmt.getSessionId();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);

            PatternMatcher matcher = null;
            if (showTemporaryTableStmt.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(showTemporaryTableStmt.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            }

            Map<String, String> tableMap = Maps.newTreeMap();
            MetaUtils.checkDbNullAndReport(db, showTemporaryTableStmt.getDb());

            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
                List<String> tableNames = temporaryTableMgr.listTemporaryTables(sessionId, db.getId());
                for (String tableName : tableNames) {
                    if (matcher != null && !matcher.match(tableName)) {
                        continue;
                    }
                    rows.add(Lists.newArrayList(tableName));
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }

            for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                rows.add(Lists.newArrayList(entry.getKey()));
            }
            return new ShowResultSet(showTemporaryTableStmt.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowTableStatusStatement(ShowTableStatusStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            Database db = context.getGlobalStateMgr().getDb(statement.getDb());
            ZoneId currentTimeZoneId = TimeUtils.getTimeZone().toZoneId();
            if (db != null) {
                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    PatternMatcher matcher = null;
                    if (statement.getPattern() != null) {
                        matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                                CaseSensibility.TABLE.getCaseSensibility());
                    }
                    for (Table table : db.getTables()) {
                        if (matcher != null && !matcher.match(table.getName())) {
                            continue;
                        }

                        try {
                            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), new TableName(db.getFullName(), table.getName()));
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
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitDescTableStmt(DescribeStmt statement, ConnectContext context) {
            try {
                return new ShowResultSet(statement.getMetaData(), statement.getResultRows());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public ShowResultSet visitShowCreateDbStatement(ShowCreateDbStmt statement, ConnectContext context) {
            String catalogName = statement.getCatalogName();
            String dbName = statement.getDb();
            List<List<String>> rows = Lists.newArrayList();

            Database db;
            if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
                db = context.getGlobalStateMgr().getDb(dbName);
            } else {
                db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
            }
            MetaUtils.checkDbNullAndReport(db, statement.getDb());

            StringBuilder createSqlBuilder = new StringBuilder();
            createSqlBuilder.append("CREATE DATABASE `").append(statement.getDb()).append("`");
            if (!Strings.isNullOrEmpty(db.getLocation())) {
                createSqlBuilder.append("\nPROPERTIES (\"location\" = \"").append(db.getLocation()).append("\")");
            } else if (RunMode.isSharedDataMode() && !db.isSystemDatabase() && Strings.isNullOrEmpty(db.getCatalogName())) {
                String volume = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeNameOfDb(db.getId());
                createSqlBuilder.append("\nPROPERTIES (\"storage_volume\" = \"").append(volume).append("\")");
            }
            rows.add(Lists.newArrayList(statement.getDb(), createSqlBuilder.toString()));
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowCreateTableStatement(ShowCreateTableStmt statement, ConnectContext context) {
            TableName tbl = statement.getTbl();
            String catalogName = tbl.getCatalog();
            if (catalogName == null) {
                catalogName = context.getCurrentCatalog();
            }
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return showCreateInternalCatalogTable(statement, context);
            } else {
                return showCreateExternalCatalogTable(statement, tbl, catalogName);
            }
        }

        private ShowResultSet showCreateInternalCatalogTable(ShowCreateTableStmt showStmt, ConnectContext connectContext) {
            Database db = GlobalStateMgr.getCurrentState().getDb(showStmt.getDb());
            MetaUtils.checkDbNullAndReport(db, showStmt.getDb());
            List<List<String>> rows = Lists.newArrayList();
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = MetaUtils.getSessionAwareTable(connectContext, db, showStmt.getTbl());
                if (table == null) {
                    if (showStmt.getType() != ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
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
                                        return new ShowResultSet(ShowCreateTableStmt.getMaterializedViewMetaData(), rows);
                                    }
                                }
                            }
                        }
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, showStmt.getTable());
                    }
                }

                List<String> createTableStmt = Lists.newArrayList();
                AstToStringBuilder.getDdlStmt(table, createTableStmt, null, null, false, true /* hide password */);
                if (createTableStmt.isEmpty()) {
                    return new ShowResultSet(showStmt.getMetaData(), rows);
                }

                if (table instanceof View) {
                    if (showStmt.getType() == ShowCreateTableStmt.CreateTableType.MATERIALIZED_VIEW) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                                showStmt.getTable(), "MATERIALIZED VIEW");
                    }
                    rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0), "utf8", "utf8_general_ci"));
                    return new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
                } else if (table instanceof MaterializedView) {
                    // In order to be compatible with BI, we return the syntax supported by
                    // mysql according to the standard syntax.
                    if (showStmt.getType() == ShowCreateTableStmt.CreateTableType.VIEW) {
                        MaterializedView mv = (MaterializedView) table;
                        String sb = "CREATE VIEW `" + table.getName() + "` AS " + mv.getViewDefineSql();
                        rows.add(Lists.newArrayList(table.getName(), sb, "utf8", "utf8_general_ci"));
                        return new ShowResultSet(ShowCreateTableStmt.getViewMetaData(), rows);
                    } else {
                        rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                        return new ShowResultSet(ShowCreateTableStmt.getMaterializedViewMetaData(), rows);
                    }
                } else {
                    if (showStmt.getType() != ShowCreateTableStmt.CreateTableType.TABLE) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_OBJECT, showStmt.getDb(),
                                showStmt.getTable(), showStmt.getType().getValue());
                    }
                    rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                    return new ShowResultSet(showStmt.getMetaData(), rows);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }

        private ShowResultSet showCreateExternalCatalogTable(ShowCreateTableStmt showStmt, TableName tbl, String catalogName) {
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
            if (table.isHiveTable() && ((HiveTable) table).getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
                createTableSql.append("CREATE EXTERNAL TABLE ");
            } else {
                createTableSql.append("CREATE TABLE ");
            }
            createTableSql.append("`").append(tableName).append("`")
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
            } else if (table instanceof IcebergView) {
                location = table.getTableLocation();
            }

            // Comment
            if (!Strings.isNullOrEmpty(table.getComment())) {
                createTableSql.append("\nCOMMENT (\"").append(table.getComment()).append("\")");
            }

            if (!Strings.isNullOrEmpty(location)) {
                createTableSql.append("\nPROPERTIES (\"location\" = \"").append(location).append("\");");
            }

            List<List<String>> rows = Lists.newArrayList();
            rows.add(Lists.newArrayList(tableName, createTableSql.toString()));
            return new ShowResultSet(showStmt.getMetaData(), rows);
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

        @Override
        public ShowResultSet visitShowProcesslistStatement(ShowProcesslistStmt statement, ConnectContext context) {
            List<List<String>> rowSet = Lists.newArrayList();

            List<ConnectContext.ThreadInfo> threadInfos = context.getConnectScheduler()
                    .listConnection(context.getQualifiedUser(), statement.getForUser());
            long nowMs = System.currentTimeMillis();
            for (ConnectContext.ThreadInfo info : threadInfos) {
                List<String> row = info.toRow(nowMs, statement.showFull());
                if (row != null) {
                    rowSet.add(row);
                }
            }

            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowProfilelistStatement(ShowProfilelistStmt statement, ConnectContext context) {
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
                if (statement.getLimit() >= 0 && count >= statement.getLimit()) {
                    break;
                }
            }

            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowRunningQueriesStatement(ShowRunningQueriesStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();

            List<LogicalSlot> slots = GlobalStateMgr.getCurrentState().getSlotManager().getSlots();
            slots.sort(Comparator.comparingLong(LogicalSlot::getStartTimeMs)
                    .thenComparingLong(LogicalSlot::getExpiredAllocatedTimeMs));

            for (LogicalSlot slot : slots) {
                List<String> row =
                        ShowRunningQueriesStmt.getColumnSuppliers().stream().map(columnSupplier -> columnSupplier.apply(slot))
                                .collect(Collectors.toList());
                rows.add(row);

                if (statement.getLimit() >= 0 && rows.size() >= statement.getLimit()) {
                    break;
                }
            }

            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowResourceGroupUsageStatement(ShowResourceGroupUsageStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();

            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().backendAndComputeNodeStream()
                    .flatMap(worker -> worker.getResourceGroupUsages().stream()
                            .map(usage -> new ShowResourceGroupUsageStmt.ShowItem(worker, usage)))
                    .filter(item -> statement.getGroupName() == null ||
                            statement.getGroupName().equals(item.getUsage().getGroup().getName()))
                    .sorted()
                    .forEach(item -> {
                        List<String> row = ShowResourceGroupUsageStmt.getColumnSuppliers().stream()
                                .map(columnSupplier -> columnSupplier.apply(item))
                                .collect(Collectors.toList());
                        rows.add(row);
                    });

            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowEnginesStatement(ShowEnginesStmt statement, ConnectContext context) {
            List<List<String>> rowSet = Lists.newArrayList();
            rowSet.add(Lists.newArrayList("OLAP", "YES", "Default storage engine of StarRocks", "NO", "NO", "NO"));
            rowSet.add(Lists.newArrayList("MySQL", "YES", "MySQL server which data is in it", "NO", "NO", "NO"));
            rowSet.add(Lists.newArrayList("ELASTICSEARCH", "YES", "ELASTICSEARCH cluster which data is in it", "NO", "NO",
                    "NO"));
            rowSet.add(Lists.newArrayList("HIVE", "YES", "HIVE database which data is in it", "NO", "NO", "NO"));
            rowSet.add(Lists.newArrayList("ICEBERG", "YES", "ICEBERG data lake which data is in it", "NO", "NO", "NO"));

            // Only success
            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowFunctionsStatement(ShowFunctionsStmt statement, ConnectContext context) {
            List<Function> functions;
            if (statement.getIsBuiltin()) {
                functions = context.getGlobalStateMgr().getBuiltinFunctions();
            } else if (statement.getIsGlobal()) {
                functions = context.getGlobalStateMgr().getGlobalFunctionMgr().getFunctions();
            } else {
                Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
                MetaUtils.checkDbNullAndReport(db, statement.getDbName());
                functions = db.getFunctions();
            }

            List<List<Comparable>> rowSet = Lists.newArrayList();
            for (Function function : functions) {
                List<Comparable> row = function.getInfo(statement.getIsVerbose());
                // like predicate
                if (statement.getWild() == null || statement.like(function.functionName())) {
                    if (statement.getIsGlobal()) {
                        try {
                            Authorizer.checkAnyActionOnGlobalFunction(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), function);
                        } catch (AccessDeniedException e) {
                            continue;
                        }
                    } else if (!statement.getIsBuiltin()) {
                        Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
                        try {
                            Authorizer.checkAnyActionOnFunction(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), db.getFullName(), function);
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
            ShowResultSetMetaData showMetaData = statement.getIsVerbose() ? statement.getMetaData() :
                    ShowResultSetMetaData.builder()
                            .addColumn(new Column("Function Name", ScalarType.createVarchar(256))).build();
            return new ShowResultSet(showMetaData, resultRowSet);
        }

        @Override
        public ShowResultSet visitShowVariablesStatement(ShowVariablesStmt statement, ConnectContext context) {
            PatternMatcher matcher = null;
            if (statement.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                        CaseSensibility.VARIABLES.getCaseSensibility());
            }
            List<List<String>> rows = VariableMgr.dump(statement.getType(), context.getSessionVariable(), matcher);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowColumnStatement(ShowColumnStmt statement, ConnectContext context) {

            List<List<String>> rows = Lists.newArrayList();
            String catalogName = statement.getCatalog();
            if (catalogName == null) {
                catalogName = context.getCurrentCatalog();
            }
            String dbName = statement.getDb();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
            MetaUtils.checkDbNullAndReport(db, statement.getDb());
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getTable(catalogName, dbName, statement.getTable());
                if (table == null) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR,
                            statement.getDb() + "." + statement.getTable());
                }
                PatternMatcher matcher = null;
                if (statement.getPattern() != null) {
                    matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
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
                    if (statement.isVerbose()) {
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
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowLoadStatement(ShowLoadStmt statement, ConnectContext context) {

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            long dbId = -1;
            if (statement.isAll()) {
                dbId = -1;
            } else {
                Database db = globalStateMgr.getDb(statement.getDbName());
                MetaUtils.checkDbNullAndReport(db, statement.getDbName());
                dbId = db.getId();
            }

            // combine the List<LoadInfo> of load(v1) and loadManager(v2)
            Set<String> statesValue = statement.getStates() == null ? null : statement.getStates().stream()
                    .map(Enum::name)
                    .collect(Collectors.toSet());
            List<List<Comparable>> loadInfos =
                    globalStateMgr.getLoadMgr().getLoadJobInfosByDb(dbId, statement.getLabelValue(),
                            statement.isAccurateMatch(),
                            statesValue);

            // order the result of List<LoadInfo> by orderByPairs in show statement
            List<OrderByPair> orderByPairs = statement.getOrderByPairs();
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
            long limit = statement.getLimit();
            long offset = statement.getOffset() == -1L ? 0 : statement.getOffset();
            if (offset >= rows.size()) {
                rows = Lists.newArrayList();
            } else if (limit != -1L) {
                if ((limit + offset) < rows.size()) {
                    rows = rows.subList((int) offset, (int) (limit + offset));
                } else {
                    rows = rows.subList((int) offset, rows.size());
                }
            }

            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowRoutineLoadStatement(ShowRoutineLoadStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            // if job exists
            List<RoutineLoadJob> routineLoadJobList;
            try {
                routineLoadJobList = GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                        .getJob(statement.getDbFullName(),
                                statement.getName(),
                                statement.isIncludeHistory());
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                throw new SemanticException(e.getMessage());
            }
            // In new privilege framework(RBAC), user needs any action on the table to show routine load job on it.
            if (routineLoadJobList != null) {
                Iterator<RoutineLoadJob> iterator = routineLoadJobList.iterator();
                while (iterator.hasNext()) {
                    RoutineLoadJob routineLoadJob = iterator.next();
                    try {
                        try {
                            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), new TableName(routineLoadJob.getDbFullName(),
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
                RoutineLoadFunctionalExprProvider fProvider = statement.getFunctionalExprProvider(context);
                rows = routineLoadJobList.parallelStream()
                        .filter(fProvider.getPredicateChain())
                        .sorted(fProvider.getOrderComparator())
                        .skip(fProvider.getSkipCount())
                        .limit(fProvider.getLimitCount())
                        .map(RoutineLoadJob::getShowInfo)
                        .collect(Collectors.toList());
            }

            if (!Strings.isNullOrEmpty(statement.getName()) && rows.isEmpty()) {
                // if the jobName has been specified
                throw new SemanticException("There is no running job named " + statement.getName()
                        + " in db " + statement.getDbFullName()
                        + ". Include history? " + statement.isIncludeHistory()
                        +
                        ", you can try `show all routine load job for job_name` if you want to list stopped and cancelled jobs");
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            List<RoutineLoadJob> routineLoadJobList;
            try {
                routineLoadJobList = GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                        .getJob(statement.getDbFullName(),
                                statement.getName(),
                                false);
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                throw new SemanticException(e.getMessage());
            }
            if (routineLoadJobList == null || routineLoadJobList.size() == 0) {
                return new ShowResultSet(statement.getMetaData(), rows);
            }
            RoutineLoadJob routineLoadJob = routineLoadJobList.get(0);
            if (routineLoadJob.getDataSourceTypeName().equals("PULSAR")) {
                throw new SemanticException("not support pulsar datasource");
            }
            StringBuilder createRoutineLoadSql = new StringBuilder();
            try {
                String dbName = routineLoadJob.getDbFullName();
                createRoutineLoadSql.append("CREATE ROUTINE LOAD ").append(dbName).append(".")
                        .append(statement.getName())
                        .append(" on ").append(routineLoadJob.getTableName());
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                throw new SemanticException(e.getMessage());
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
            rows.add(Lists.newArrayList(statement.getName(), createRoutineLoadSql.toString()));
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            // if job exists
            RoutineLoadJob routineLoadJob;
            try {
                routineLoadJob =
                        GlobalStateMgr.getCurrentState().getRoutineLoadMgr()
                                .getJob(statement.getDbFullName(),
                                        statement.getJobName());
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                throw new SemanticException(e.getMessage());
            }
            if (routineLoadJob == null) {
                throw new SemanticException("The job named " + statement.getJobName() + "does not exists "
                        + "or job state is stopped or cancelled");
            }

            // check auth
            String dbFullName = statement.getDbFullName();
            String tableName;
            try {
                tableName = routineLoadJob.getTableName();
            } catch (MetaNotFoundException e) {
                throw new SemanticException(
                        "The table metadata of job has been changed. The job will be cancelled automatically", e);
            }
            // In new privilege framework(RBAC), user needs any action on the table to show routine load job on it.
            try {
                Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                        context.getCurrentRoleIds(), new TableName(dbFullName, tableName));
            } catch (AccessDeniedException e) {
                // if we have no privilege, return an empty result set
                return new ShowResultSet(statement.getMetaData(), rows);
            }

            // get routine load task info
            rows.addAll(routineLoadJob.getTasksShowInfo());
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowStreamLoadStatement(ShowStreamLoadStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            // if task exists
            List<StreamLoadTask> streamLoadTaskList;
            try {
                streamLoadTaskList = GlobalStateMgr.getCurrentState().getStreamLoadMgr()
                        .getTask(statement.getDbFullName(),
                                statement.getName(),
                                statement.isIncludeHistory());
            } catch (MetaNotFoundException e) {
                LOG.warn(e.getMessage(), e);
                throw new SemanticException(e.getMessage());
            }

            if (streamLoadTaskList != null) {
                StreamLoadFunctionalExprProvider fProvider =
                        statement.getFunctionalExprProvider(context);
                rows = streamLoadTaskList.parallelStream()
                        .filter(fProvider.getPredicateChain())
                        .sorted(fProvider.getOrderComparator())
                        .skip(fProvider.getSkipCount())
                        .limit(fProvider.getLimitCount())
                        .map(StreamLoadTask::getShowInfo)
                        .collect(Collectors.toList());
            }

            if (!Strings.isNullOrEmpty(statement.getName()) && rows.isEmpty()) {
                // if the label has been specified
                throw new SemanticException("There is no label named " + statement.getName()
                        + " in db " + statement.getDbFullName()
                        + ". Include history? " + statement.isIncludeHistory());
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowDeleteStatement(ShowDeleteStmt statement, ConnectContext context) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            Database db = globalStateMgr.getDb(statement.getDbName());
            MetaUtils.checkDbNullAndReport(db, statement.getDbName());
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

            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowAlterStatement(ShowAlterStmt statement, ConnectContext context) {
            ProcNodeInterface procNodeI = statement.getNode();
            Preconditions.checkNotNull(procNodeI);
            List<List<String>> rows;
            try {
                // Only SchemaChangeProc support where/order by/limit syntax
                if (procNodeI instanceof SchemaChangeProcDir) {
                    rows = ((SchemaChangeProcDir) procNodeI).fetchResultByFilter(statement.getFilterMap(),
                            statement.getOrderPairs(), statement.getLimitElement()).getRows();
                } else if (procNodeI instanceof OptimizeProcDir) {
                    rows = ((OptimizeProcDir) procNodeI).fetchResultByFilter(statement.getFilterMap(),
                            statement.getOrderPairs(), statement.getLimitElement()).getRows();
                } else {
                    rows = procNodeI.fetchResult().getRows();
                }
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowUserPropertyStatement(ShowUserPropertyStmt statement, ConnectContext context) {
            return new ShowResultSet(statement.getMetaData(), statement.getRows(context));
        }

        @Override
        public ShowResultSet visitShowDataStatement(ShowDataStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getDb(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                String tableName = statement.getTableName();
                List<List<String>> totalRows = statement.getResultRows();
                if (tableName == null) {
                    long totalSize = 0;
                    long totalReplicaCount = 0;

                    // sort by table name
                    List<Table> tables = db.getTables();
                    SortedSet<Table> sortedTables = new TreeSet<>(Comparator.comparing(Table::getName));

                    for (Table table : tables) {
                        try {
                            Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                                    context.getCurrentRoleIds(), new TableName(dbName, table.getName()));
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
                        Authorizer.checkAnyActionOnTable(context.getCurrentUserIdentity(),
                                context.getCurrentRoleIds(), new TableName(dbName, tableName));
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(),
                                context.getCurrentRoleIds(),
                                PrivilegeType.ANY.name(), ObjectType.TABLE.name(), tableName);
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
            return new ShowResultSet(statement.getMetaData(), statement.getResultRows());
        }

        @Override
        public ShowResultSet visitShowCollationStatement(ShowCollationStmt statement, ConnectContext context) {
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
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowPartitionsStatement(ShowPartitionsStmt statement, ConnectContext context) {
            ProcNodeInterface procNodeI = statement.getNode();
            Preconditions.checkNotNull(procNodeI);
            try {
                List<List<String>> rows = ((PartitionsProcDir) procNodeI).fetchResultByFilter(statement.getFilterMap(),
                        statement.getOrderByPairs(), statement.getLimitElement()).getRows();
                return new ShowResultSet(statement.getMetaData(), rows);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public ShowResultSet visitShowTabletStatement(ShowTabletStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();

            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            if (statement.isShowSingleTablet()) {
                long tabletId = statement.getTabletId();
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
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
                        Pair<Boolean, Boolean> privResult = Authorizer.checkPrivForShowTablet(context, dbName, table);
                        if (!privResult.first) {
                            AccessDeniedException.reportAccessDenied(
                                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                    context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                    PrivilegeType.ANY.name(), ObjectType.TABLE.name(), null);
                        }

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
                Database db = globalStateMgr.getDb(statement.getDbName());
                MetaUtils.checkDbNullAndReport(db, statement.getDbName());

                Locker locker = new Locker();
                locker.lockDatabase(db, LockType.READ);
                try {
                    Table table = MetaUtils.getSessionAwareTable(
                            context, db, new TableName(statement.getDbName(), statement.getTableName()));
                    if (table == null) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, statement.getTableName());
                    }
                    if (!table.isNativeTableOrMaterializedView()) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_NOT_OLAP_TABLE, statement.getTableName());
                    }

                    Pair<Boolean, Boolean> privResult = Authorizer.checkPrivForShowTablet(
                            context, db.getFullName(), table);
                    if (!privResult.first) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.ANY.name(), ObjectType.TABLE.name(), null);
                    }
                    Boolean hideIpPort = privResult.second;
                    statement.setTable(table);

                    OlapTable olapTable = (OlapTable) table;
                    long sizeLimit = -1;
                    if (statement.hasOffset() && statement.hasLimit()) {
                        sizeLimit = statement.getOffset() + statement.getLimit();
                    } else if (statement.hasLimit()) {
                        sizeLimit = statement.getLimit();
                    }
                    boolean stop = false;
                    Collection<Partition> partitions = new ArrayList<>();
                    if (statement.hasPartition()) {
                        PartitionNames partitionNames = statement.getPartitionNames();
                        for (String partName : partitionNames.getPartitionNames()) {
                            Partition partition = olapTable.getPartition(partName, partitionNames.isTemp());
                            if (partition == null) {
                                throw new SemanticException("Unknown partition: " + partName);
                            }
                            partitions.add(partition);
                        }
                    } else {
                        partitions = olapTable.getPartitions();
                    }
                    List<List<Comparable>> tabletInfos = new ArrayList<>();
                    String indexName = statement.getIndexName();
                    long indexId = -1;
                    if (indexName != null) {
                        Long id = olapTable.getIndexIdByName(indexName);
                        if (id == null) {
                            // invalid indexName
                            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, statement.getIndexName());
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
                                            statement.getVersion(), statement.getBackendId(), statement.getReplicaState(),
                                            hideIpPort));
                                }
                                if (sizeLimit > -1 && CollectionUtils.isEmpty(statement.getOrderByPairs())
                                        && tabletInfos.size() >= sizeLimit) {
                                    stop = true;
                                    break;
                                }
                            }
                        }
                    }

                    // order by
                    List<OrderByPair> orderByPairs = statement.getOrderByPairs();
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
                        tabletInfos = tabletInfos.subList((int) statement.getOffset(), (int) sizeLimit);
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

            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowBackupStatement(ShowBackupStmt statement, ConnectContext context) {
            Database filterDb = GlobalStateMgr.getCurrentState().getDb(statement.getDbName());
            List<List<String>> infos = Lists.newArrayList();
            List<Database> dbs = Lists.newArrayList();

            if (filterDb == null) {
                for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState()
                        .getLocalMetastore().getIdToDb().entrySet()) {
                    dbs.add(entry.getValue());
                }
            } else {
                dbs.add(filterDb);
            }

            for (Database db : dbs) {
                AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
                if (jobI == null || !(jobI instanceof BackupJob)) {
                    // show next db
                    continue;
                }

                BackupJob backupJob = (BackupJob) jobI;

                // check privilege
                List<TableRef> tableRefs = backupJob.getTableRef();
                AtomicBoolean privilegeDeny = new AtomicBoolean(false);
                tableRefs.forEach(tableRef -> {
                    TableName tableName = tableRef.getName();
                    try {
                        Authorizer.checkTableAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                tableName.getDb(), tableName.getTbl(), PrivilegeType.EXPORT);
                    } catch (AccessDeniedException e) {
                        privilegeDeny.set(true);
                    }
                });
                if (privilegeDeny.get()) {
                    return new ShowResultSet(statement.getMetaData(), EMPTY_SET);
                }

                List<String> info = backupJob.getInfo();
                infos.add(info);
            }
            return new ShowResultSet(statement.getMetaData(), infos);
        }

        @Override
        public ShowResultSet visitShowRestoreStatement(ShowRestoreStmt statement, ConnectContext context) {
            Database filterDb = GlobalStateMgr.getCurrentState().getDb(statement.getDbName());
            List<List<String>> infos = Lists.newArrayList();
            List<Database> dbs = Lists.newArrayList();

            if (filterDb == null) {
                for (Map.Entry<Long, Database> entry : GlobalStateMgr.getCurrentState()
                        .getLocalMetastore().getIdToDb().entrySet()) {
                    dbs.add(entry.getValue());
                }
            } else {
                dbs.add(filterDb);
            }

            for (Database db : dbs) {
                AbstractJob jobI = GlobalStateMgr.getCurrentState().getBackupHandler().getJob(db.getId());
                if (jobI == null || !(jobI instanceof RestoreJob)) {
                    // show next db
                    continue;
                }

                RestoreJob restoreJob = (RestoreJob) jobI;
                List<String> info = restoreJob.getInfo();
                infos.add(info);
            }
            return new ShowResultSet(statement.getMetaData(), infos);
        }

        @Override
        public ShowResultSet visitShowBrokerStatement(ShowBrokerStmt statement, ConnectContext context) {
            List<List<String>> rowSet = GlobalStateMgr.getCurrentState().getBrokerMgr().getBrokersInfo();

            // Only success
            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowResourceStatement(ShowResourcesStmt statement, ConnectContext context) {
            List<List<String>> rowSet = GlobalStateMgr.getCurrentState().getResourceMgr().getResourcesInfo();

            // Only success
            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowExportStatement(ShowExportStmt statement, ConnectContext context) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            Database db = globalStateMgr.getDb(statement.getDbName());
            MetaUtils.checkDbNullAndReport(db, statement.getDbName());
            long dbId = db.getId();

            ExportMgr exportMgr = globalStateMgr.getExportMgr();

            Set<ExportJob.JobState> states = null;
            ExportJob.JobState state = statement.getJobState();
            if (state != null) {
                states = Sets.newHashSet(state);
            }
            List<List<String>> infos = exportMgr.getExportJobInfosByIdOrState(
                    dbId, statement.getJobId(), states, statement.getQueryId(),
                    statement.getOrderByPairs(), statement.getLimit());

            return new ShowResultSet(statement.getMetaData(), infos);
        }

        @Override
        public ShowResultSet visitShowBackendsStatement(ShowBackendsStmt statement, ConnectContext context) {
            List<List<String>> backendInfos = BackendsProcDir.getClusterBackendInfos();
            return new ShowResultSet(statement.getMetaData(), backendInfos);
        }

        @Override
        public ShowResultSet visitShowFrontendsStatement(ShowFrontendsStmt statement, ConnectContext context) {
            List<List<String>> infos = Lists.newArrayList();
            FrontendsProcNode.getFrontendsInfo(GlobalStateMgr.getCurrentState(), infos);
            return new ShowResultSet(statement.getMetaData(), infos);
        }

        @Override
        public ShowResultSet visitShowRepositoriesStatement(ShowRepositoriesStmt statement, ConnectContext context) {
            List<List<String>> repoInfos = GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getReposInfo();
            return new ShowResultSet(statement.getMetaData(), repoInfos);
        }

        @Override
        public ShowResultSet visitShowSnapshotStatement(ShowSnapshotStmt statement, ConnectContext context) {
            Repository repo = GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(statement.getRepoName());
            if (repo == null) {
                throw new SemanticException("Repository " + statement.getRepoName() + " does not exist");
            }

            List<List<String>> snapshotInfos = repo.getSnapshotInfos(statement.getSnapshotName(), statement.getTimestamp(),
                    statement.getSnapshotNames());
            return new ShowResultSet(statement.getMetaData(), snapshotInfos);
        }

        @Override
        public ShowResultSet visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext context) {
            AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            try {
                List<List<String>> infos = new ArrayList<>();
                if (statement.getRole() != null) {
                    List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForRole(statement.getRole());
                    if (granteeRole != null) {
                        infos.add(granteeRole);
                    }

                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                            authorizationManager.getTypeToPrivilegeEntryListByRole(statement.getRole());
                    infos.addAll(privilegeToRowString(authorizationManager,
                            new GrantRevokeClause(null, statement.getRole()), typeToPrivilegeEntryList));
                } else {
                    List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForUser(statement.getUserIdent());
                    if (granteeRole != null) {
                        infos.add(granteeRole);
                    }

                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                            authorizationManager.getTypeToPrivilegeEntryListByUser(statement.getUserIdent());
                    infos.addAll(privilegeToRowString(authorizationManager,
                            new GrantRevokeClause(statement.getUserIdent(), null), typeToPrivilegeEntryList));
                }
                return new ShowResultSet(statement.getMetaData(), infos);
            } catch (PrivilegeException e) {
                throw new SemanticException(e.getMessage());
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

        @Override
        public ShowResultSet visitShowRolesStatement(ShowRolesStmt statement, ConnectContext context) {
            List<List<String>> infos = new ArrayList<>();
            AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
            List<String> roles = authorizationManager.getAllRoles();
            roles.forEach(e -> infos.add(Lists.newArrayList(e,
                    authorizationManager.isBuiltinRole(e) ? "true" : "false",
                    authorizationManager.getRoleComment(e))));

            return new ShowResultSet(statement.getMetaData(), infos);
        }

        @Override
        public ShowResultSet visitAdminShowReplicaStatusStatement(AdminShowReplicaStatusStmt statement, ConnectContext context) {
            List<List<String>> results;
            try {
                results = MetadataViewer.getTabletStatus(statement);
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), results);
        }

        @Override
        public ShowResultSet visitAdminShowReplicaDistributionStatement(AdminShowReplicaDistributionStmt statement,
                                                                        ConnectContext context) {
            List<List<String>> results;
            try {
                results = MetadataViewer.getTabletDistribution(statement);
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), results);
        }

        @Override
        public ShowResultSet visitAdminShowConfigStatement(AdminShowConfigStmt statement, ConnectContext context) {
            List<List<String>> results;
            try {
                PatternMatcher matcher = null;
                if (statement.getPattern() != null) {
                    matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                            CaseSensibility.CONFIG.getCaseSensibility());
                }
                results = ConfigBase.getConfigInfo(matcher);
                // Sort all configs by config key.
                results.sort(Comparator.comparing(o -> o.get(0)));
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), results);
        }

        @Override
        public ShowResultSet visitShowSmallFilesStatement(ShowSmallFilesStmt statement, ConnectContext context) {
            List<List<String>> results;
            try {
                results = GlobalStateMgr.getCurrentState().getSmallFileMgr().getInfo(statement.getDbName());
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), results);
        }

        @Override
        public ShowResultSet visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            Database db = context.getGlobalStateMgr().getDb(statement.getDb());
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
                                    ConnectContext.get().getCurrentRoleIds(),
                                    new TableName(db.getFullName(), olapTable.getName()));
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
                                String.valueOf(dynamicPartitionProperty.isEnabled()),
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
                                        .getRuntimeInfo(tableName, DynamicPartitionScheduler.DROP_PARTITION_MSG),
                                String.valueOf(dynamicPartitionScheduler.isInScheduler(db.getId(), olapTable.getId()))));
                    }
                } finally {
                    locker.unLockDatabase(db, LockType.READ);
                }
                return new ShowResultSet(statement.getMetaData(), rows);
            }

            return new ShowResultSet(statement.getMetaData(), EMPTY_SET);
        }

        @Override
        public ShowResultSet visitShowIndexStatement(ShowIndexStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
            MetaUtils.checkDbNullAndReport(db, statement.getDbName());
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = MetaUtils.getSessionAwareTable(context, db, statement.getTableName());
                if (table == null) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR,
                            db.getOriginName() + "." + statement.getTableName().toString());
                } else if (table instanceof OlapTable) {
                    List<Index> indexes = ((OlapTable) table).getIndexes();
                    for (Index index : indexes) {
                        rows.add(Lists.newArrayList(statement.getTableName().toString(), "",
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
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowTransactionStatement(ShowTransactionStmt statement, ConnectContext context) {
            Database db = context.getGlobalStateMgr().getDb(statement.getDbName());
            MetaUtils.checkDbNullAndReport(db, statement.getDbName());

            long txnId = statement.getTxnId();
            GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
            try {
                return new ShowResultSet(statement.getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public ShowResultSet visitShowPluginsStatement(ShowPluginsStmt statement, ConnectContext context) {
            List<List<String>> rows = GlobalStateMgr.getCurrentState().getPluginMgr().getPluginShowInfos();
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowSqlBlackListStatement(ShowSqlBlackListStmt statement, ConnectContext context) {
            List<List<String>> rows = new ArrayList<>();
            for (Map.Entry<String, BlackListSql> entry : SqlBlackList.getInstance().sqlBlackListMap.entrySet()) {
                List<String> oneSql = new ArrayList<>();
                oneSql.add(String.valueOf(entry.getValue().id));
                oneSql.add(entry.getKey());
                rows.add(oneSql);
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowDataCacheRulesStatement(ShowDataCacheRulesStmt statement, ConnectContext context) {
            return new ShowResultSet(statement.getMetaData(), DataCacheMgr.getInstance().getShowResultSetRows());
        }

        @Override
        public ShowResultSet visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt statement, ConnectContext context) {
            List<AnalyzeJob> jobs = context.getGlobalStateMgr().getAnalyzeMgr().getAllAnalyzeJobList();
            List<List<String>> rows = Lists.newArrayList();
            jobs.sort(Comparator.comparing(AnalyzeJob::getId));
            for (AnalyzeJob job : jobs) {
                try {
                    List<String> result = ShowAnalyzeJobStmt.showAnalyzeJobs(context, job);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                    LOG.warn("analyze job {} meta not found, {}", job.getId(), e);
                }
            }
            rows = doPredicate(statement, statement.getMetaData(), rows);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt statement, ConnectContext context) {
            List<AnalyzeStatus> statuses = new ArrayList<>(context.getGlobalStateMgr().getAnalyzeMgr()
                    .getAnalyzeStatusMap().values());
            List<List<String>> rows = Lists.newArrayList();
            statuses.sort(Comparator.comparing(AnalyzeStatus::getId));
            for (AnalyzeStatus status : statuses) {
                try {
                    List<String> result = ShowAnalyzeStatusStmt.showAnalyzeStatus(context, status);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                }
            }
            rows = doPredicate(statement, statement.getMetaData(), rows);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt statement, ConnectContext context) {
            List<BasicStatsMeta> metas = new ArrayList<>(context.getGlobalStateMgr().getAnalyzeMgr()
                    .getBasicStatsMetaMap().values());
            List<List<String>> rows = Lists.newArrayList();
            for (BasicStatsMeta meta : metas) {
                try {
                    List<String> result = ShowBasicStatsMetaStmt.showBasicStatsMeta(context, meta);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                }
            }
            List<ExternalBasicStatsMeta> externalMetas =
                    new ArrayList<>(context.getGlobalStateMgr().getAnalyzeMgr().getExternalBasicStatsMetaMap().values());
            for (ExternalBasicStatsMeta meta : externalMetas) {
                try {
                    List<String> result = ShowBasicStatsMetaStmt.showExternalBasicStatsMeta(context, meta);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                }
            }

            rows = doPredicate(statement, statement.getMetaData(), rows);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt statement, ConnectContext context) {
            List<HistogramStatsMeta> metas = new ArrayList<>(context.getGlobalStateMgr().getAnalyzeMgr()
                    .getHistogramStatsMetaMap().values());
            List<List<String>> rows = Lists.newArrayList();
            for (HistogramStatsMeta meta : metas) {
                try {
                    List<String> result = ShowHistogramStatsMetaStmt.showHistogramStatsMeta(context, meta);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                }
            }

            List<ExternalHistogramStatsMeta> externalMetas =
                    new ArrayList<>(context.getGlobalStateMgr().getAnalyzeMgr().getExternalHistogramStatsMetaMap().values());
            for (ExternalHistogramStatsMeta meta : externalMetas) {
                try {
                    List<String> result = ShowHistogramStatsMetaStmt.showExternalHistogramStatsMeta(context, meta);
                    if (result != null) {
                        rows.add(result);
                    }
                } catch (MetaNotFoundException e) {
                    // pass
                }
            }

            rows = doPredicate(statement, statement.getMetaData(), rows);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowResourceGroupStatement(ShowResourceGroupStmt statement, ConnectContext context) {
            List<List<String>> rows = GlobalStateMgr.getCurrentState().getResourceGroupMgr().showResourceGroup(statement);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowUserStatement(ShowUserStmt statement, ConnectContext context) {
            List<List<String>> rowSet = Lists.newArrayList();

            if (statement.isAll()) {
                AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
                List<String> users = authorizationManager.getAllUsers();
                users.forEach(u -> rowSet.add(Lists.newArrayList(u)));
            } else {
                List<String> row = Lists.newArrayList();
                row.add(context.getCurrentUserIdentity().toString());
                rowSet.add(row);
            }

            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowCatalogsStatement(ShowCatalogsStmt statement, ConnectContext context) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
            List<List<String>> rowSet = catalogMgr.getCatalogsInfo().stream()
                    .filter(row -> {
                                if (!InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equals(row.get(0))) {

                                    try {
                                        Authorizer.checkAnyActionOnCatalog(
                                                context.getCurrentUserIdentity(),
                                                context.getCurrentRoleIds(), row.get(0));
                                    } catch (AccessDeniedException e) {
                                        return false;
                                    }

                                    return true;
                                }
                                return true;
                            }
                    )
                    .sorted(Comparator.comparing(o -> o.get(0))).collect(Collectors.toList());
            return new ShowResultSet(statement.getMetaData(), rowSet);
        }

        @Override
        public ShowResultSet visitShowComputeNodes(ShowComputeNodesStmt statement, ConnectContext context) {
            List<List<String>> computeNodesInfos = ComputeNodeProcDir.getClusterComputeNodesInfos();
            return new ShowResultSet(statement.getMetaData(), computeNodesInfos);
        }

        @Override
        public ShowResultSet visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
            AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            List<List<String>> userAuthInfos = Lists.newArrayList();

            Map<UserIdentity, UserAuthenticationInfo> authenticationInfoMap = new HashMap<>();
            if (statement.isAll()) {
                authenticationInfoMap.putAll(authenticationManager.getUserToAuthenticationInfo());
            } else {
                UserAuthenticationInfo userAuthenticationInfo;
                if (statement.getUserIdent() == null) {
                    userAuthenticationInfo = authenticationManager
                            .getUserAuthenticationInfoByUserIdentity(context.getCurrentUserIdentity());
                } else {
                    userAuthenticationInfo =
                            authenticationManager.getUserAuthenticationInfoByUserIdentity(
                                    statement.getUserIdent());
                }
                authenticationInfoMap.put(statement.getUserIdent(), userAuthenticationInfo);
            }
            for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : authenticationInfoMap.entrySet()) {
                UserAuthenticationInfo userAuthenticationInfo = entry.getValue();
                userAuthInfos.add(Lists.newArrayList(
                        entry.getKey().toString(),
                        userAuthenticationInfo.getPassword().length == 0 ? "No" : "Yes",
                        userAuthenticationInfo.getAuthPlugin(),
                        userAuthenticationInfo.getTextForAuthPlugin()));
            }

            return new ShowResultSet(statement.getMetaData(), userAuthInfos);
        }

        @Override
        public ShowResultSet visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt statement,
                                                                     ConnectContext context) {
            String catalogName = statement.getCatalogName();
            List<List<String>> rows = Lists.newArrayList();
            if (InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(catalogName)) {
                return new ShowResultSet(statement.getMetaData(), rows);
            }

            Catalog catalog = context.getGlobalStateMgr().getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
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
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowCharsetStatement(ShowCharsetStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            List<String> row = Lists.newArrayList();
            // | utf8 | UTF-8 Unicode | utf8_general_ci | 3 |
            row.add("utf8");
            row.add("UTF-8 Unicode");
            row.add("utf8_general_ci");
            row.add("3");
            rows.add(row);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowStorageVolumesStatement(ShowStorageVolumesStmt statement, ConnectContext context) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            StorageVolumeMgr storageVolumeMgr = globalStateMgr.getStorageVolumeMgr();
            List<String> storageVolumeNames = null;
            try {
                storageVolumeNames = storageVolumeMgr.listStorageVolumeNames();
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }
            PatternMatcher matcher = null;
            List<List<String>> rows = Lists.newArrayList();
            if (!statement.getPattern().isEmpty()) {
                matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                        CaseSensibility.STORAGEVOLUME.getCaseSensibility());
            }
            PatternMatcher finalMatcher = matcher;
            storageVolumeNames = storageVolumeNames.stream()
                    .filter(storageVolumeName -> finalMatcher == null || finalMatcher.match(storageVolumeName))
                    .filter(storageVolumeName -> {
                                    try {
                                        Authorizer.checkAnyActionOnStorageVolume(context.getCurrentUserIdentity(),
                                                context.getCurrentRoleIds(), storageVolumeName);
                                    } catch (AccessDeniedException e) {
                                        return false;
                                    }
                                    return true;
                                }
                    ).collect(Collectors.toList());
            for (String storageVolumeName : storageVolumeNames) {
                rows.add(Lists.newArrayList(storageVolumeName));
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitDescStorageVolumeStatement(DescStorageVolumeStmt statement, ConnectContext context) {
            try {
                return new ShowResultSet(statement.getMetaData(), statement.getResultRows());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public ShowResultSet visitShowPipeStatement(ShowPipeStmt statement, ConnectContext context) {
            List<List<Comparable>> rows = Lists.newArrayList();
            String dbName = statement.getDbName();
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
                    Authorizer.checkAnyActionOnPipe(context.getCurrentUserIdentity(),
                            context.getCurrentRoleIds(), new PipeName(dbName, pipe.getName()));
                } catch (AccessDeniedException e) {
                    continue;
                }

                // execute
                List<Comparable> row = Lists.newArrayList();
                ShowPipeStmt.handleShow(row, pipe);
                rows.add(row);
            }

            // order by
            List<OrderByPair> orderByPairs = statement.getOrderByPairs();
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
            long limit = statement.getLimit();
            long offset = statement.getOffset() == -1L ? 0 : statement.getOffset();
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
            return new ShowResultSet(statement.getMetaData(), result);
        }

        @Override
        public ShowResultSet visitDescPipeStatement(DescPipeStmt statement, ConnectContext context) {
            List<List<String>> rows = Lists.newArrayList();
            PipeManager pipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
            Pipe pipe = pipeManager.mayGetPipe(statement.getName())
                    .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_UNKNOWN_PIPE, statement.getName()));

            List<String> row = Lists.newArrayList();
            DescPipeStmt.handleDesc(row, pipe);
            rows.add(row);
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowFailPointStatement(ShowFailPointStatement statement, ConnectContext context) {
            // send request and build resultSet
            PListFailPointRequest request = new PListFailPointRequest();
            SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            PatternMatcher matcher = null;
            if (statement.getPattern() != null) {
                matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                        CaseSensibility.VARIABLES.getCaseSensibility());
            }
            List<Backend> backends = new LinkedList<>();
            if (statement.getBackends() == null) {
                List<Long> backendIds = clusterInfoService.getBackendIds(true);
                if (backendIds == null) {
                    throw new SemanticException("No alive backends");
                }
                for (long backendId : backendIds) {
                    Backend backend = clusterInfoService.getBackend(backendId);
                    if (backend == null) {
                        continue;
                    }
                    backends.add(backend);
                }
            } else {
                for (String backendAddr : statement.getBackends()) {
                    String[] tmp = backendAddr.split(":");
                    if (tmp.length != 2) {
                        throw new SemanticException("invalid backend addr");
                    }
                    Backend backend = clusterInfoService.getBackendWithBePort(tmp[0], Integer.parseInt(tmp[1]));
                    if (backend == null) {
                        throw new SemanticException("cannot find backend with addr " + backendAddr);
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
                    throw new SemanticException("sending list failpoint request fails");
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
                        throw new SemanticException(errMsg);
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
                    throw new SemanticException(e.getMessage());
                }
            }
            return new ShowResultSet(statement.getMetaData(), rows);
        }

        @Override
        public ShowResultSet visitShowDictionaryStatement(ShowDictionaryStmt statement, ConnectContext context) {
            List<List<String>> allInfo = null;
            try {
                allInfo = GlobalStateMgr.getCurrentState().getDictionaryMgr().getAllInfo(statement.getDictionaryName());
            } catch (Exception e) {
                throw new SemanticException(e.getMessage());
            }
            return new ShowResultSet(statement.getMetaData(), allInfo);
        }

        @Override
        public ShowResultSet visitShowBackendBlackListStatement(ShowBackendBlackListStmt statement, ConnectContext context) {
            List<List<String>> rows = SimpleScheduler.getHostBlacklist().getShowData();
            return new ShowResultSet(statement.getMetaData(), rows);
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
        Map<String, List<TaskRunStatus>> mvNameTaskMap = Maps.newHashMap();
        if (!materializedViews.isEmpty()) {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            TaskManager taskManager = globalStateMgr.getTaskManager();
            Set<String> taskNames = materializedViews.stream()
                    .map(mv -> TaskBuilder.getMvTaskName(mv.getId()))
                    .collect(Collectors.toSet());
            mvNameTaskMap = taskManager.listMVRefreshedTaskRunStatus(dbName, taskNames);
        }
        for (MaterializedView mvTable : materializedViews) {
            long mvId = mvTable.getId();
            ShowMaterializedViewStatus mvStatus = new ShowMaterializedViewStatus(mvId, dbName, mvTable.getName());
            List<TaskRunStatus> taskTaskStatusJob = mvNameTaskMap.get(TaskBuilder.getMvTaskName(mvId));
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
            mvStatus.setLastJobTaskRunStatus(taskTaskStatusJob);
            mvStatus.setQueryRewriteStatus(mvTable.getQueryRewriteStatus());
            rowSets.add(mvStatus);
        }

        for (Pair<OlapTable, MaterializedIndexMeta> singleTableMV : singleTableMVs) {
            OlapTable olapTable = singleTableMV.first;
            MaterializedIndexMeta mvMeta = singleTableMV.second;

            long mvId = mvMeta.getIndexId();
            ShowMaterializedViewStatus mvStatus =
                    new ShowMaterializedViewStatus(mvId, dbName, olapTable.getIndexNameById(mvId));
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
}
