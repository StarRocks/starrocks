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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/service/FrontendServiceImpl.java

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

package com.starrocks.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.information.AnalyzeStatusSystemTable;
import com.starrocks.catalog.system.information.ColumnStatsUsageSystemTable;
import com.starrocks.catalog.system.information.TaskRunsSystemTable;
import com.starrocks.catalog.system.information.TasksSystemTable;
import com.starrocks.catalog.system.sys.GrantsTo;
import com.starrocks.catalog.system.sys.RoleEdges;
import com.starrocks.catalog.system.sys.SysFeLocks;
import com.starrocks.catalog.system.sys.SysFeMemoryUsage;
import com.starrocks.catalog.system.sys.SysObjectDependencies;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServerContext;
import com.starrocks.common.ThriftServerEventProcessor;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.BaseAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.http.rest.WarehouseInfosBuilder;
import com.starrocks.journal.CheckpointException;
import com.starrocks.journal.CheckpointWorker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.lake.compaction.CompactionMgr;
import com.starrocks.leader.CheckpointController;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.batchwrite.RequestLoadResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.load.pipe.filelist.RepoAccessor;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.ProxyContextManager;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TAllocateAutoIncrementIdParam;
import com.starrocks.thrift.TAllocateAutoIncrementIdResult;
import com.starrocks.thrift.TAnalyzeStatusReq;
import com.starrocks.thrift.TAnalyzeStatusRes;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TBatchReportExecStatusParams;
import com.starrocks.thrift.TBatchReportExecStatusResult;
import com.starrocks.thrift.TBeginRemoteTxnRequest;
import com.starrocks.thrift.TBeginRemoteTxnResponse;
import com.starrocks.thrift.TClusterSnapshotJobsRequest;
import com.starrocks.thrift.TClusterSnapshotJobsResponse;
import com.starrocks.thrift.TClusterSnapshotsRequest;
import com.starrocks.thrift.TClusterSnapshotsResponse;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TColumnDesc;
import com.starrocks.thrift.TColumnStatsUsageReq;
import com.starrocks.thrift.TColumnStatsUsageRes;
import com.starrocks.thrift.TCommitRemoteTxnRequest;
import com.starrocks.thrift.TCommitRemoteTxnResponse;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TDBPrivDesc;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
import com.starrocks.thrift.TFeMemoryReq;
import com.starrocks.thrift.TFeMemoryRes;
import com.starrocks.thrift.TFeResult;
import com.starrocks.thrift.TFetchResourceResult;
import com.starrocks.thrift.TFinishCheckpointRequest;
import com.starrocks.thrift.TFinishCheckpointResponse;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TGetDBPrivsParams;
import com.starrocks.thrift.TGetDBPrivsResult;
import com.starrocks.thrift.TGetDbsParams;
import com.starrocks.thrift.TGetDbsResult;
import com.starrocks.thrift.TGetDictQueryParamRequest;
import com.starrocks.thrift.TGetDictQueryParamResponse;
import com.starrocks.thrift.TGetGrantsToRolesOrUserRequest;
import com.starrocks.thrift.TGetGrantsToRolesOrUserResponse;
import com.starrocks.thrift.TGetKeysRequest;
import com.starrocks.thrift.TGetKeysResponse;
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TGetPartitionsMetaRequest;
import com.starrocks.thrift.TGetPartitionsMetaResponse;
import com.starrocks.thrift.TGetProfileRequest;
import com.starrocks.thrift.TGetProfileResponse;
import com.starrocks.thrift.TGetQueryStatisticsRequest;
import com.starrocks.thrift.TGetQueryStatisticsResponse;
import com.starrocks.thrift.TGetRoleEdgesRequest;
import com.starrocks.thrift.TGetRoleEdgesResponse;
import com.starrocks.thrift.TGetRoutineLoadJobsResult;
import com.starrocks.thrift.TGetStreamLoadsResult;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.thrift.TGetTablePrivsParams;
import com.starrocks.thrift.TGetTablePrivsResult;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TGetTabletScheduleRequest;
import com.starrocks.thrift.TGetTabletScheduleResponse;
import com.starrocks.thrift.TGetTaskInfoResult;
import com.starrocks.thrift.TGetTaskRunInfoResult;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TGetTemporaryTablesInfoRequest;
import com.starrocks.thrift.TGetTemporaryTablesInfoResponse;
import com.starrocks.thrift.TGetTrackingLoadsResult;
import com.starrocks.thrift.TGetUserPrivsParams;
import com.starrocks.thrift.TGetUserPrivsResult;
import com.starrocks.thrift.TGetWarehousesRequest;
import com.starrocks.thrift.TGetWarehousesResponse;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TIsMethodSupportedRequest;
import com.starrocks.thrift.TListMaterializedViewStatusResult;
import com.starrocks.thrift.TListPipeFilesInfo;
import com.starrocks.thrift.TListPipeFilesParams;
import com.starrocks.thrift.TListPipeFilesResult;
import com.starrocks.thrift.TListPipesInfo;
import com.starrocks.thrift.TListPipesParams;
import com.starrocks.thrift.TListPipesResult;
import com.starrocks.thrift.TListSessionsOptions;
import com.starrocks.thrift.TListSessionsRequest;
import com.starrocks.thrift.TListSessionsResponse;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TLoadTxnBeginRequest;
import com.starrocks.thrift.TLoadTxnBeginResult;
import com.starrocks.thrift.TLoadTxnCommitRequest;
import com.starrocks.thrift.TLoadTxnCommitResult;
import com.starrocks.thrift.TLoadTxnRollbackRequest;
import com.starrocks.thrift.TLoadTxnRollbackResult;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TMVReportEpochResponse;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TMaterializedViewStatus;
import com.starrocks.thrift.TMergeCommitRequest;
import com.starrocks.thrift.TMergeCommitResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TPartitionMetaRequest;
import com.starrocks.thrift.TPartitionMetaResponse;
import com.starrocks.thrift.TQueryStatisticsInfo;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportAuditStatisticsResult;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TReportFragmentFinishParams;
import com.starrocks.thrift.TReportFragmentFinishResponse;
import com.starrocks.thrift.TReportLakeCompactionRequest;
import com.starrocks.thrift.TReportLakeCompactionResponse;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TSessionInfo;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TShowVariableRequest;
import com.starrocks.thrift.TShowVariableResult;
import com.starrocks.thrift.TSnapshotLoaderReportRequest;
import com.starrocks.thrift.TStartCheckpointRequest;
import com.starrocks.thrift.TStartCheckpointResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStreamLoadInfo;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TStreamLoadPutResult;
import com.starrocks.thrift.TTablePrivDesc;
import com.starrocks.thrift.TTableReplicationRequest;
import com.starrocks.thrift.TTableReplicationResponse;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.thrift.TTaskInfo;
import com.starrocks.thrift.TTrackingLoadInfo;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUpdateExportTaskStatusRequest;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUpdateResourceUsageResponse;
import com.starrocks.thrift.TUserPrivDesc;
import com.starrocks.thrift.TVerboseVariableRecord;
import com.starrocks.thrift.TWarehouseInfo;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionNotFoundException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStateSnapshot;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TStatusCode.NOT_IMPLEMENTED_ERROR;
import static com.starrocks.thrift.TStatusCode.OK;
import static com.starrocks.thrift.TStatusCode.RUNTIME_ERROR;
import static com.starrocks.thrift.TStatusCode.SERVICE_UNAVAILABLE;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(FrontendServiceImpl.class);
    private final LeaderImpl leaderImpl;
    private final ExecuteEnv exeEnv;
    private final ProxyContextManager proxyContextManager;
    public AtomicLong partitionRequestNum = new AtomicLong(0);

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        leaderImpl = new LeaderImpl();
        proxyContextManager = ProxyContextManager.getInstance();
        this.exeEnv = exeEnv;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        LOG.debug("get db request: {}", params);
        TGetDbsResult result = new TGetDbsResult();

        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.DATABASE.getCaseSensibility();
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(), caseSensitive);
            } catch (SemanticException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        if (params.isSetCatalog_name()) {
            catalogName = params.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        List<String> dbNames = metadataMgr.listDbNames(catalogName);
        LOG.debug("get db names: {}", dbNames);

        UserIdentity currentUser;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        List<String> dbs = new ArrayList<>();
        for (String fullName : dbNames) {
            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(currentUser);
                context.setCurrentRoleIds(currentUser);
                Authorizer.checkAnyActionOnOrInDb(context, catalogName, fullName);
            } catch (AccessDeniedException e) {
                continue;
            }

            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (!PatternMatcher.matchPattern(params.getPattern(), db, matcher, caseSensitive)) {
                continue;
            }

            dbs.add(fullName);
        }
        result.setDbs(dbs);
        return result;
    }

    @Override
    public TGetTablesResult getTableNames(TGetTablesParams params) throws TException {
        LOG.debug("get table name request: {}", params);
        TGetTablesResult result = new TGetTablesResult();
        List<String> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (SemanticException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phase
        String catalogName = null;
        if (params.isSetCatalog_name()) {
            catalogName = params.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Database db = metadataMgr.getDb(catalogName, params.db);

        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }

        if (db != null) {
            for (String tableName : metadataMgr.listTableNames(catalogName, params.db)) {
                LOG.debug("get table: {}, wait to check", tableName);
                Table tbl = null;
                try {
                    tbl = metadataMgr.getTable(catalogName, params.db, tableName);
                } catch (Exception e) {
                    LOG.warn(e.getMessage(), e);
                }

                if (tbl == null) {
                    continue;
                }

                try {
                    ConnectContext context = new ConnectContext();
                    context.setCurrentUserIdentity(currentUser);
                    context.setCurrentRoleIds(currentUser);
                    Authorizer.checkAnyActionOnTableLikeObject(context, params.db, tbl);
                } catch (AccessDeniedException e) {
                    continue;
                }

                if (!PatternMatcher.matchPattern(params.getPattern(), tableName, matcher, caseSensitive)) {
                    continue;
                }

                tablesResult.add(tableName);
            }
        }
        return result;
    }

    @Override
    public TListTableStatusResult listTableStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list table request: {}", params);
        TListTableStatusResult result = new TListTableStatusResult();
        List<TTableStatus> tablesResult = Lists.newArrayList();
        result.setTables(tablesResult);
        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(), caseSensitive);
            } catch (SemanticException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(params.db);
        long limit = params.isSetLimit() ? params.getLimit() : -1;
        UserIdentity currentUser;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                boolean listingViews = params.isSetType() && TTableType.VIEW.equals(params.getType());
                List<Table> tables = listingViews ? db.getViews() :
                        GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
                OUTER:
                for (Table table : tables) {
                    try {
                        ConnectContext context = new ConnectContext();
                        context.setCurrentUserIdentity(currentUser);
                        context.setCurrentRoleIds(currentUser);
                        Authorizer.checkAnyActionOnTableLikeObject(context, params.db, table);
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    if (!PatternMatcher.matchPattern(params.getPattern(), table.getName(), matcher, caseSensitive)) {
                        continue;
                    }

                    TTableStatus status = new TTableStatus();
                    status.setName(table.getName());
                    status.setType(table.getMysqlType());
                    status.setEngine(table.getEngine());
                    status.setComment(table.getComment());
                    status.setCreate_time(table.getCreateTime());
                    status.setLast_check_time(table.getLastCheckTime());
                    if (listingViews) {
                        View view = (View) table;
                        String ddlSql = view.getInlineViewDef();

                        ConnectContext connectContext = ConnectContext.buildInner();
                        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
                        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
                        connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

                        try {
                            List<TableName> allTables = view.getTableRefs();
                            for (TableName tableName : allTables) {
                                Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore()
                                        .getTable(db.getFullName(), tableName.getTbl());
                                if (tbl != null) {
                                    try {
                                        ConnectContext context = new ConnectContext();
                                        context.setCurrentUserIdentity(currentUser);
                                        context.setCurrentRoleIds(currentUser);
                                        Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), tbl);
                                    } catch (AccessDeniedException e) {
                                        continue OUTER;
                                    }
                                }
                            }
                        } catch (SemanticException e) {
                            // ignore semantic exception because view maybe invalid
                        }
                        status.setDdl_sql(ddlSql);
                    }

                    tablesResult.add(status);
                    // if user set limit, then only return limit size result
                    if (limit > 0 && tablesResult.size() >= limit) {
                        break;
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
        return result;
    }

    @Override
    public TListMaterializedViewStatusResult listMaterializedViewStatus(TGetTablesParams params) throws TException {
        LOG.debug("get list table request: {}", params);

        PatternMatcher matcher = null;
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (params.isSetPattern()) {
            matcher = PatternMatcher.createMysqlPattern(params.getPattern(), caseSensitive);
        }

        // database privs should be checked in analysis phrase
        long limit = params.isSetLimit() ? params.getLimit() : -1;
        UserIdentity currentUser;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        Preconditions.checkState(params.isSetType() && TTableType.MATERIALIZED_VIEW.equals(params.getType()));
        return listMaterializedViewStatus(limit, matcher, currentUser, params);
    }

    @Override
    public TListPipesResult listPipes(TListPipesParams params) throws TException {
        if (!params.isSetUser_ident()) {
            throw new TException("missed user_identity");
        }
        // TODO: check privilege
        UserIdentity userIdentity = UserIdentity.fromThrift(params.getUser_ident());

        PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
        Map<PipeId, Pipe> pipes = pm.getPipesUnlock();
        TListPipesResult result = new TListPipesResult();
        for (Pipe pipe : pipes.values()) {
            String databaseName = GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(pipe.getPipeId().getDbId())
                    .map(Database::getOriginName)
                    .orElse(null);

            TListPipesInfo row = new TListPipesInfo();
            row.setPipe_id(pipe.getPipeId().getId());
            row.setPipe_name(pipe.getName());
            row.setProperties(pipe.getPropertiesString());
            row.setDatabase_name(databaseName);
            row.setState(pipe.getState().toString());
            row.setTable_name(Optional.ofNullable(pipe.getTargetTable()).map(TableName::toString).orElse(""));
            row.setLast_error(pipe.getLastErrorInfo().toJson());
            row.setCreated_time(pipe.getCreatedTime());

            row.setLoad_status(pipe.getLoadStatus().toJson());
            row.setLoaded_files(pipe.getLoadStatus().loadedFiles);
            row.setLoaded_rows(pipe.getLoadStatus().loadRows);
            row.setLoaded_bytes(pipe.getLoadStatus().loadedBytes);

            result.addToPipes(row);
        }

        return result;
    }

    @Override
    public TListPipeFilesResult listPipeFiles(TListPipeFilesParams params) throws TException {
        if (!params.isSetUser_ident()) {
            throw new TException("missed user_identity");
        }
        LOG.info("listPipeFiles params={}", params);
        // TODO: check privilege
        UserIdentity userIdentity = UserIdentity.fromThrift(params.getUser_ident());
        TListPipeFilesResult result = new TListPipeFilesResult();
        PipeManager pm = GlobalStateMgr.getCurrentState().getPipeManager();
        Map<PipeId, Pipe> pipes = pm.getPipesUnlock();
        RepoAccessor repo = RepoAccessor.getInstance();
        List<PipeFileRecord> files = repo.listAllFiles();
        for (PipeFileRecord record : files) {
            TListPipeFilesInfo file = new TListPipeFilesInfo();
            Optional<Pipe> mayPipe = pm.mayGetPipe(record.pipeId);
            if (!mayPipe.isPresent()) {
                LOG.warn("Pipe not found with id {}", record.pipeId);
            }

            file.setPipe_id(record.pipeId);
            file.setDatabase_name(
                    mayPipe.flatMap(p ->
                                    GlobalStateMgr.getCurrentState().getLocalMetastore().mayGetDb(p.getDbAndName().first)
                                            .map(Database::getOriginName))
                            .orElse(""));
            file.setPipe_name(mayPipe.map(Pipe::getName).orElse(""));

            file.setFile_name(record.fileName);
            file.setFile_version(record.fileVersion);
            file.setFile_rows(0L); // TODO(murphy)
            file.setFile_size(record.fileSize);
            file.setLast_modified(DateUtils.formatDateTimeUnix(record.lastModified));

            file.setState(record.loadState.toString());
            file.setStaged_time(DateUtils.formatDateTimeUnix(record.stagedTime));
            file.setStart_load(DateUtils.formatDateTimeUnix(record.startLoadTime));
            file.setFinish_load(DateUtils.formatDateTimeUnix(record.finishLoadTime));

            file.setFirst_error_msg(record.getErrorMessage());
            file.setError_count(record.getErrorCount());
            file.setError_line(record.getErrorLine());

            result.addToPipe_files(file);
        }

        return result;
    }

    @Override
    public TObjectDependencyRes listObjectDependencies(TObjectDependencyReq params) throws TException {
        return SysObjectDependencies.listObjectDependencies(params);
    }

    @Override
    public TFeLocksRes listFeLocks(TFeLocksReq params) throws TException {
        return SysFeLocks.listLocks(params, true);
    }

    @Override
    public TFeMemoryRes listFeMemoryUsage(TFeMemoryReq request) throws TException {
        return SysFeMemoryUsage.listFeMemoryUsage(request);
    }

    @Override
    public TColumnStatsUsageRes getColumnStatsUsage(TColumnStatsUsageReq request) throws TException {
        UserIdentity currentUser = UserIdentity.fromThrift(request.getAuth_info().getCurrent_user_ident());

        TColumnStatsUsageRes result = ColumnStatsUsageSystemTable.query(request);
        result.getItems().removeIf(item -> {
            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(currentUser);
                context.setCurrentRoleIds(currentUser);
                Authorizer.checkTableAction(context, item.getTable_database(), item.getTable_name(),
                        PrivilegeType.SELECT);
                return false;
            } catch (AccessDeniedException e) {
                return true;
            }
        });

        return result;
    }

    @Override
    public TAnalyzeStatusRes getAnalyzeStatus(TAnalyzeStatusReq request) throws TException {
        TAnalyzeStatusRes res = AnalyzeStatusSystemTable.query(request);
        UserIdentity currentUser = UserIdentity.fromThrift(request.getAuth_info().getCurrent_user_ident());
        res.getItems().removeIf(item -> {
            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(currentUser);
                context.setCurrentRoleIds(currentUser);
                Authorizer.checkTableAction(context, item.getDatabase_name(), item.getTable_name(),
                        PrivilegeType.SELECT);
                return false;
            } catch (AccessDeniedException e) {
                return true;
            }
        });

        return res;
    }

    // list MaterializedView table match pattern
    private TListMaterializedViewStatusResult listMaterializedViewStatus(long limit, PatternMatcher matcher,
                                                                         UserIdentity currentUser, TGetTablesParams params) {
        TListMaterializedViewStatusResult result = new TListMaterializedViewStatusResult();
        List<TMaterializedViewStatus> tablesResult = Lists.newArrayList();
        result.setMaterialized_views(tablesResult);
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("database not exists: {}", dbName);
            return result;
        }

        listMaterializedViews(limit, matcher, currentUser, params).stream()
                .map(s -> s.toThrift())
                .forEach(t -> tablesResult.add(t));
        return result;
    }

    private void filterAsynchronousMaterializedView(PatternMatcher matcher,
                                                    UserIdentity currentUser,
                                                    String dbName,
                                                    MaterializedView mv,
                                                    TGetTablesParams params,
                                                    List<MaterializedView> result) {
        // check table name
        String mvName = params.table_name;
        if (mvName != null && !mvName.equalsIgnoreCase(mv.getName())) {
            return;
        }

        try {
            ConnectContext context = new ConnectContext();
            context.setCurrentUserIdentity(currentUser);
            context.setCurrentRoleIds(currentUser);
            Authorizer.checkAnyActionOnTableLikeObject(context, dbName, mv);
        } catch (AccessDeniedException e) {
            return;
        }

        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        if (!PatternMatcher.matchPattern(params.getPattern(), mv.getName(), matcher, caseSensitive)) {
            return;
        }
        result.add(mv);
    }

    private void filterSynchronousMaterializedView(OlapTable olapTable, PatternMatcher matcher,
                                                   TGetTablesParams params,
                                                   List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs) {
        // synchronized materialized view metadata size should be greater than 1.
        if (olapTable.getVisibleIndexMetas().size() <= 1) {
            return;
        }

        // check table name
        String mvName = params.table_name;
        if (mvName != null && !mvName.equalsIgnoreCase(olapTable.getName())) {
            return;
        }

        List<MaterializedIndexMeta> visibleMaterializedViews = olapTable.getVisibleIndexMetas();
        long baseIdx = olapTable.getBaseIndexId();
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        for (MaterializedIndexMeta mvMeta : visibleMaterializedViews) {
            if (baseIdx == mvMeta.getIndexId()) {
                continue;
            }

            if (!PatternMatcher.matchPattern(params.getPattern(), olapTable.getIndexNameById(mvMeta.getIndexId()),
                    matcher, caseSensitive)) {
                continue;
            }
            singleTableMVs.add(Pair.create(olapTable, mvMeta));
        }
    }

    private List<ShowMaterializedViewStatus> listMaterializedViews(long limit, PatternMatcher matcher,
                                                                   UserIdentity currentUser, TGetTablesParams params) {
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        List<MaterializedView> materializedViews = Lists.newArrayList();
        List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs = Lists.newArrayList();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (table.isMaterializedView()) {
                    filterAsynchronousMaterializedView(matcher, currentUser, dbName,
                            (MaterializedView) table, params, materializedViews);
                } else if (table.getType() == Table.TableType.OLAP) {
                    filterSynchronousMaterializedView((OlapTable) table, matcher, params, singleTableMVs);
                } else {
                    // continue
                }

                // check limit
                int mvSize = materializedViews.size() + singleTableMVs.size();
                if (limit > 0 && mvSize >= limit) {
                    break;
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return ShowExecutor.listMaterializedViewStatus(dbName, materializedViews, singleTableMVs);
    }

    @Override
    public TGetTaskInfoResult getTasks(TGetTasksParams params) throws TException {
        LOG.debug("get show task request: {}", params);

        List<TTaskInfo> tasksResult = TasksSystemTable.query(params);
        TGetTaskInfoResult result = new TGetTaskInfoResult();
        result.setTasks(tasksResult);

        return result;
    }

    @Override
    public TGetTaskRunInfoResult getTaskRuns(TGetTasksParams params) throws TException {
        LOG.debug("get show task run request: {}", params);
        return TaskRunsSystemTable.query(params);
    }

    @Override
    public TGetDBPrivsResult getDBPrivs(TGetDBPrivsParams params) throws TException {
        LOG.debug("get database privileges request: {}", params);
        TGetDBPrivsResult result = new TGetDBPrivsResult();
        List<TDBPrivDesc> tDBPrivs = Lists.newArrayList();
        result.setDb_privs(tDBPrivs);
        // TODO(yiming): support showing user privilege info in information_schema later
        return result;
    }

    @Override
    public TGetTablePrivsResult getTablePrivs(TGetTablePrivsParams params) throws TException {
        LOG.debug("get table privileges request: {}", params);
        TGetTablePrivsResult result = new TGetTablePrivsResult();
        List<TTablePrivDesc> tTablePrivs = Lists.newArrayList();
        result.setTable_privs(tTablePrivs);
        // TODO(yiming): support showing user privilege info in information_schema later
        return result;
    }

    @Override
    public TGetKeysResponse getKeys(TGetKeysRequest params) throws TException {
        // get encrypted keys as binary meta format
        LOG.debug("getKeys request: {}", params);
        try {
            return GlobalStateMgr.getCurrentState().getKeyMgr().getKeys(params);
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TGetProfileResponse getQueryProfile(TGetProfileRequest params) throws TException {
        LOG.debug("get query profile request: {}", params);
        List<String> queryIds = params.query_id;
        TGetProfileResponse result = new TGetProfileResponse();
        if (queryIds != null) {
            for (String queryId : queryIds) {
                String profile = ProfileManager.getInstance().getProfile(queryId);
                if (profile != null && !profile.isEmpty()) {
                    result.addToQuery_result(profile);
                } else {
                    result.addToQuery_result("");
                }
            }
        }
        return result;
    }

    @Override
    public TGetUserPrivsResult getUserPrivs(TGetUserPrivsParams params) throws TException {
        LOG.debug("get user privileges request: {}", params);
        TGetUserPrivsResult result = new TGetUserPrivsResult();
        List<TUserPrivDesc> tUserPrivs = Lists.newArrayList();
        result.setUser_privs(tUserPrivs);

        // TODO(yiming): support showing user privilege info in information_schema later
        return result;
    }

    @Override
    public TFeResult updateExportTaskStatus(TUpdateExportTaskStatusRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        return new TFeResult(FrontendServiceVersion.V1, status);
    }

    @Override
    public TDescribeTableResult describeTable(TDescribeTableParams params) throws TException {
        LOG.debug("get desc table request: {}", params);
        TDescribeTableResult result = new TDescribeTableResult();
        List<TColumnDef> columns = Lists.newArrayList();
        result.setColumns(columns);

        // database privs should be checked in analysis phrase
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        long limit = params.isSetLimit() ? params.getLimit() : -1;

        // if user query schema meta such as "select * from information_schema.columns limit 10;",
        // in this case, there is no predicate and only has limit clause,we can call the
        // describe_table interface only once, which can reduce RPC time from BE to FE, and
        // the amount of data. In additional,we need add db_name & table_name values to TColumnDesc.
        if (!params.isSetDb() && StringUtils.isBlank(params.getTable_name())) {
            describeWithoutDbAndTable(currentUser, columns, limit);
            return result;
        }

        String catalogName = null;
        if (params.isSetCatalog_name()) {
            catalogName = params.getCatalog_name();
        }

        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Database db = metadataMgr.getDb(catalogName, params.db);

        if (db != null) {
            Locker locker = new Locker();
            try {
                locker.lockDatabase(db.getId(), LockType.READ);
                Table table = metadataMgr.getTable(catalogName, params.db, params.table_name);
                if (table == null) {
                    return result;
                }
                try {
                    ConnectContext context = new ConnectContext();
                    context.setCurrentUserIdentity(currentUser);
                    context.setCurrentRoleIds(currentUser);
                    Authorizer.checkAnyActionOnTableLikeObject(context, params.db, table);
                } catch (AccessDeniedException e) {
                    return result;
                }
                setColumnDesc(columns, table, limit, false, params.db, params.table_name);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
        return result;
    }

    // get describeTable without db name and table name parameter, so we need iterate over
    // dbs and tables, when reach limit, we break;
    private void describeWithoutDbAndTable(UserIdentity currentUser, List<TColumnDef> columns, long limit) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getLocalMetastore().listDbNames();
        boolean reachLimit;
        for (String fullName : dbNames) {
            try {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(currentUser);
                context.setCurrentRoleIds(currentUser);
                Authorizer.checkAnyActionOnOrInDb(context,
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, fullName);
            } catch (AccessDeniedException e) {
                continue;
            }
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(fullName);
            if (db != null) {
                Locker locker = new Locker();
                for (String tableName : db.getTableNamesViewWithLock()) {
                    try {
                        locker.lockDatabase(db.getId(), LockType.READ);
                        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
                        if (table == null) {
                            continue;
                        }

                        try {
                            ConnectContext context = new ConnectContext();
                            context.setCurrentUserIdentity(currentUser);
                            context.setCurrentRoleIds(currentUser);
                            Authorizer.checkAnyActionOnTableLikeObject(context, fullName, table);
                        } catch (AccessDeniedException e) {
                            continue;
                        }

                        reachLimit = setColumnDesc(columns, table, limit, true, fullName, tableName);
                    } finally {
                        locker.unLockDatabase(db.getId(), LockType.READ);
                    }
                    if (reachLimit) {
                        return;
                    }
                }
            }
        }
    }

    private boolean setColumnDesc(List<TColumnDef> columns, Table table, long limit,
                                  boolean needSetDbAndTable, String db, String tbl) {
        String tableKeysType = "";
        if (table.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            tableKeysType = olapTable.getKeysType().name().substring(0, 3).toUpperCase();
        }
        for (Column column : table.getBaseSchema()) {
            final TColumnDesc desc =
                    new TColumnDesc(column.getName(), column.getPrimitiveType().toThrift());
            final Integer precision = column.getType().getPrecision();
            if (precision != null) {
                desc.setColumnPrecision(precision);
            }
            desc.setColumnDefault(column.getMetaDefaultValue(null));
            final Integer columnLength = column.getType().getColumnSize();
            if (columnLength != null) {
                desc.setColumnLength(columnLength);
            }
            final Integer decimalDigits = column.getType().getDecimalDigits();
            if (decimalDigits != null) {
                desc.setColumnScale(decimalDigits);
            }
            desc.setAllowNull(column.isAllowNull());
            if (column.isKey()) {
                // COLUMN_KEY (UNI, AGG, DUP, PRI)
                desc.setColumnKey(tableKeysType);
            } else {
                desc.setColumnKey("");
            }
            desc.setDataType(column.getType().toMysqlDataTypeString());
            desc.setColumnTypeStr(column.getType().toMysqlColumnTypeString());
            String generatedColumnExprStr = column.generatedColumnExprToString();
            if (generatedColumnExprStr != null && !generatedColumnExprStr.isEmpty()) {
                desc.setGeneratedColumnExprStr(generatedColumnExprStr);
            }
            final TColumnDef colDef = new TColumnDef(desc);
            final String comment = column.getComment();
            if (comment != null) {
                colDef.setComment(comment);
            }
            columns.add(colDef);
            // add db_name and table_name values to TColumnDesc if needed
            if (needSetDbAndTable) {
                columns.get(columns.size() - 1).columnDesc.setDbName(db);
                columns.get(columns.size() - 1).columnDesc.setTableName(tbl);
            }
            // if user set limit, then only return limit size result
            if (limit > 0 && columns.size() >= limit) {
                return true;
            }
        }

        return false;
    }

    @Override
    public TShowVariableResult showVariables(TShowVariableRequest params) throws TException {
        TShowVariableResult result = new TShowVariableResult();
        Map<String, String> map = Maps.newHashMap();
        result.setVariables(map);
        // Find connect
        ConnectContext ctx = exeEnv.getScheduler().getContext(params.getThreadId());
        if (ctx == null) {
            return result;
        }
        SetType setType = SetType.fromThrift(params.getVarType());
        List<List<String>> rows = GlobalStateMgr.getCurrentState().getVariableMgr().dump(setType,
                ctx.getSessionVariable(), null);
        if (setType != SetType.VERBOSE) {
            for (List<String> row : rows) {
                map.put(row.get(0), row.get(1));
            }
        } else {
            for (List<String> row : rows) {
                TVerboseVariableRecord record = new TVerboseVariableRecord();
                record.setVariable_name(row.get(0));
                record.setValue(row.get(1));
                record.setDefault_value(row.get(2));
                record.setIs_changed(row.get(3).equals("1"));
                result.addToVerbose_variables(record);
            }
        }
        return result;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) throws TException {
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, getClientAddr());
    }

    @Override
    public TReportAuditStatisticsResult reportAuditStatistics(TReportAuditStatisticsParams params) throws TException {
        return QeProcessorImpl.INSTANCE.reportAuditStatistics(params, getClientAddr());
    }

    @Override
    public TBatchReportExecStatusResult batchReportExecStatus(TBatchReportExecStatusParams params) throws TException {
        return QeProcessorImpl.INSTANCE.batchReportExecStatus(params, getClientAddr());
    }

    @Override
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        return leaderImpl.finishTask(request);
    }

    @Override
    public TMasterResult report(TReportRequest request) throws TException {
        return leaderImpl.report(request);
    }

    @Override
    public TFetchResourceResult fetchResource() throws TException {
        throw new TException("not supported");
    }

    @Override
    public TFeResult isMethodSupported(TIsMethodSupportedRequest request) throws TException {
        TStatus status = new TStatus(TStatusCode.OK);
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);
        switch (request.getFunction_name()) {
            case "STREAMING_MINI_LOAD":
                break;
            default:
                status.setStatus_code(NOT_IMPLEMENTED_ERROR);
                break;
        }
        return result;
    }

    @Override
    public TMasterOpResult forward(TMasterOpRequest params) throws TException {
        TNetworkAddress clientAddr = getClientAddr();
        if (clientAddr != null) {
            Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByHost(clientAddr.getHostname());
            if (fe == null) {
                LOG.warn("reject request from invalid host. client: {}", clientAddr);
                throw new TException("request from invalid host was rejected.");
            }
        }

        // add this log so that we can track this stmt
        LOG.info("receive forwarded stmt {} from FE: {}",
                params.getStmt_id(), clientAddr != null ? clientAddr.getHostname() : "unknown");
        ConnectContext context = new ConnectContext(null);
        String hostname = "";
        if (clientAddr != null) {
            hostname = clientAddr.getHostname();
        }
        context.setProxyHostName(hostname);
        boolean addToProxyManager = params.isSetConnectionId();
        final int connectionId = params.getConnectionId();

        try (var guard = proxyContextManager.guard(hostname, connectionId, context, addToProxyManager)) {
            ConnectProcessor processor = new ConnectProcessor(context);
            return processor.proxyExecute(params);
        } catch (Exception e) {
            LOG.warn("unreachable path:", e);
            final TMasterOpResult result = new TMasterOpResult();
            result.setErrorMsg(e.getMessage());
            return result;
        }
    }

    private void checkPasswordAndLoadPriv(String user, String passwd, String db, String tbl,
                                          String clientIp) throws AuthenticationException {
        UserIdentity currentUser = AuthenticationHandler.authenticate(new ConnectContext(), user, clientIp,
                passwd.getBytes(StandardCharsets.UTF_8), null);
        // check INSERT action on table
        try {
            ConnectContext context = new ConnectContext();
            context.setCurrentUserIdentity(currentUser);
            context.setCurrentRoleIds(currentUser);
            Authorizer.checkTableAction(context, db, tbl, PrivilegeType.INSERT);
        } catch (AccessDeniedException e) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the INSERT privilege(s) for this operation");
        }
    }

    @Override
    public TLoadTxnBeginResult loadTxnBegin(TLoadTxnBeginRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive txn begin request, db: {}, tbl: {}, label: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getLabel(), clientAddr);
        LOG.debug("txn begin request: {}", request);

        TLoadTxnBeginResult result = new TLoadTxnBeginResult();
        // if current node is not master, reject the request
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        if (Config.enable_sync_publish) {
            result.setTimeout(timeoutSecond);
        } else {
            result.setTimeout(0);
        }

        try {
            result.setTxnId(loadTxnBeginImpl(request, clientAddr));
        } catch (DuplicatedRequestException e) {
            // this is a duplicate request, just return previous txn id
            LOG.info("duplicate request for stream load. request id: {}, txn_id: {}", e.getDuplicatedRequestId(),
                    e.getTxnId());
            result.setTxnId(e.getTxnId());
        } catch (LabelAlreadyUsedException e) {
            status.setStatus_code(TStatusCode.LABEL_ALREADY_EXISTS);
            status.addToError_msgs(e.getMessage());
            result.setJob_status(e.getJobStatus());
        } catch (StarRocksException e) {
            LOG.warn("failed to begin: {}", e.getMessage());
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.addToError_msgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private long loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws StarRocksException {
        checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                request.getTbl(), request.getUser_ip());

        // check txn
        long limit = Config.stream_load_max_txn_num_per_be;
        if (limit >= 0) {
            long txnNumBe = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionNumByCoordinateBe(clientIp);
            LOG.info("streamload check txn num, be: {}, txn_num: {}, limit: {}", clientIp, txnNumBe, limit);
            if (txnNumBe >= limit) {
                throw new StarRocksException("streamload txn num per be exceeds limit, be: "
                        + clientIp + ", txn_num: " + txnNumBe + ", limit: " + limit);
            }
        }
        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new StarRocksException("empty label in begin request");
        }
        // check database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException("unknown database, database=" + dbName);
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), request.getTbl());
        if (table == null) {
            throw new StarRocksException("unknown table \"" + request.getDb() + "." + request.getTbl() + "\"");
        }

        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);

        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        if (request.isSetBackend_id()) {
            SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            warehouseId = Utils.getWarehouseIdByNodeId(systemInfo, request.getBackend_id())
                    .orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        } else if (request.getWarehouse() != null && !request.getWarehouse().isEmpty()) {
            // For backward, we keep this else branch. We should prioritize using the method to get the warehouse by backend.
            String warehouseName = request.getWarehouse();
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
            warehouseId = warehouse.getId();
        }

        TransactionResult resp = new TransactionResult();
        StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        streamLoadManager.beginLoadTaskFromBackend(dbName, table.getName(), request.getLabel(), request.getRequest_id(),
                request.getUser(), request.getUser_ip(), timeoutSecond * 1000, resp, false, warehouseId);
        if (!resp.stateOK()) {
            LOG.warn(resp.msg);
            throw new StarRocksException(resp.msg);
        }

        StreamLoadTask task = streamLoadManager.getTaskByLabel(request.getLabel());
        // this should't open
        if (task == null || task.getTxnId() == -1) {
            throw new StarRocksException(String.format("Load label: {} begin transacton failed", request.getLabel()));
        }
        return task.getTxnId();
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn commit request. db: {}, tbl: {}, txn_id: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), clientAddr);
        LOG.debug("txn commit request: {}", request);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        // if current node is not master, reject the request
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnCommitImpl(request, status);
        } catch (LockTimeoutException e) {
            LOG.warn("failed to commit txn_id: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatus_code(TStatusCode.TIMEOUT);
            status.addToError_msgs(e.getMessage());
        } catch (CommitRateExceededException e) {
            long allowCommitTime = e.getAllowCommitTime();
            LOG.warn("commit rate exceeded. txn_id: {}: allow commit time: {}", request.getTxnId(), allowCommitTime);
            status.setStatus_code(TStatusCode.SR_EAGAIN);
            status.addToError_msgs(e.getMessage());
            result.setRetry_interval_ms(Math.max(allowCommitTime - System.currentTimeMillis(), 0));
        } catch (StarRocksException e) {
            LOG.warn("failed to commit txn_id: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.addToError_msgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    // return true if commit success and publish success, return false if publish timeout
    void loadTxnCommitImpl(TLoadTxnCommitRequest request, TStatus status) throws StarRocksException, LockTimeoutException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }

        // get database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException("unknown database, database=" + dbName);
        }
        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;

        // Make publish timeout is less than thrift_rpc_timeout_ms
        // Otherwise, the publish process will be successful but commit timeout in BE
        // It will result in error like "call frontend service failed"
        timeoutMs = timeoutMs * 3 / 4;
        boolean ret = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().commitAndPublishTransaction(
                db, request.getTxnId(),
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                TabletFailInfo.fromThrift(request.getFailInfos()),
                timeoutMs, attachment);
        if (!ret) {
            // committed success but not visible
            status.setStatus_code(TStatusCode.PUBLISH_TIMEOUT);
            String timeoutInfo = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .getTxnPublishTimeoutDebugInfo(db.getId(), request.getTxnId());
            LOG.warn("txn {} publish timeout {}", request.getTxnId(), timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            status.addToError_msgs("Publish timeout. The data will be visible after a while, " + timeoutInfo);
            return;
        }
        // if commit and publish is success, load can be regarded as success
        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        if (null == attachment) {
            return;
        }
        // collect table-level metrics
        Table tbl = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), request.getTbl());
        if (null == tbl) {
            return;
        }
        StreamLoadTask streamLoadtask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(request.getTxnId());

        switch (request.txnCommitAttachment.getLoadType()) {
            case ROUTINE_LOAD:
                if (!(attachment instanceof RLTaskTxnCommitAttachment)) {
                    break;
                }

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(attachment, "");
                }

                break;
            case MANUAL_LOAD:
                if (!(attachment instanceof ManualLoadTxnCommitAttachment)) {
                    break;
                }

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(attachment, "");
                }

                break;
            default:
                break;
        }
    }

    @Override
    public TGetLoadTxnStatusResult getLoadTxnStatus(TGetLoadTxnStatusRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive get txn status request. db: {}, tbl: {}, txn_id: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), clientAddr);
        LOG.debug("get txn status request: {}", request);

        TGetLoadTxnStatusResult result = new TGetLoadTxnStatusResult();
        // if current node is not master, reject the request
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            LOG.warn("current fe is not leader");
            result.setStatus(TTransactionStatus.UNKNOWN);
            return result;
        }

        // get database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            LOG.warn("unknown database, database=" + dbName);
            result.setStatus(TTransactionStatus.UNKNOWN);
            return result;
        }

        try {
            TransactionStateSnapshot transactionStateSnapshot =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTxnState(db, request.getTxnId());
            LOG.debug("txn {} status is {}", request.getTxnId(), transactionStateSnapshot);
            result.setStatus(transactionStateSnapshot.getStatus().toThrift());
            result.setReason(transactionStateSnapshot.getReason());
        } catch (Throwable e) {
            result.setStatus(TTransactionStatus.UNKNOWN);
            LOG.warn("catch unknown result.", e);
        }
        return result;
    }

    @Override
    public TLoadTxnCommitResult loadTxnPrepare(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn prepare request. db: {}, tbl: {}, txn_id: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), clientAddr);
        LOG.debug("txn prepare request: {}", request);

        TLoadTxnCommitResult result = new TLoadTxnCommitResult();
        // if current node is not master, reject the request
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnPrepareImpl(request);
        } catch (StarRocksException e) {
            LOG.warn("failed to prepare txn_id: {}: {}", request.getTxnId(), e.getMessage());
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.addToError_msgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private void loadTxnPrepareImpl(TLoadTxnCommitRequest request) throws StarRocksException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }

        // get database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException("unknown database, database=" + dbName);
        }

        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().prepareTransaction(
                db.getId(), request.getTxnId(),
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                TabletFailInfo.fromThrift(request.getFailInfos()),
                attachment);

    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive txn rollback request. db: {}, tbl: {}, txn_id: {}, reason: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), request.getReason(), clientAddr);
        LOG.debug("txn rollback request: {}", request);

        TLoadTxnRollbackResult result = new TLoadTxnRollbackResult();
        // if current node is not master, reject the request
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnRollbackImpl(request);
        } catch (TransactionNotFoundException e) {
            LOG.warn("failed to rollback txn {}: {}", request.getTxnId(), e.getMessage());
            status.setStatus_code(TStatusCode.TXN_NOT_EXISTS);
            status.addToError_msgs(e.getMessage());
        } catch (StarRocksException e) {
            LOG.warn("failed to rollback txn {}: {}", request.getTxnId(), e.getMessage());
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.addToError_msgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }

        return result;
    }

    private void loadTxnRollbackImpl(TLoadTxnRollbackRequest request) throws StarRocksException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }
        String dbName = request.getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbName + " does not exist");
        }
        long dbId = db.getId();
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                TabletFailInfo.fromThrift(request.getFailInfos()),
                TxnCommitAttachment.fromThrift(request.getTxnCommitAttachment()));

        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        StreamLoadTask streamLoadtask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(request.getTxnId());

        switch (request.txnCommitAttachment.getLoadType()) {
            case ROUTINE_LOAD:
                if (!(attachment instanceof RLTaskTxnCommitAttachment)) {
                    break;
                }
                RLTaskTxnCommitAttachment routineAttachment = (RLTaskTxnCommitAttachment) attachment;

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(routineAttachment, request.getReason());

                }

                break;
            case MANUAL_LOAD:
                if (!(attachment instanceof ManualLoadTxnCommitAttachment)) {
                    break;
                }
                ManualLoadTxnCommitAttachment streamAttachment = (ManualLoadTxnCommitAttachment) attachment;

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(streamAttachment, request.getReason());
                }

                break;
            default:
                break;
        }
    }

    @Override
    public TStreamLoadPutResult streamLoadPut(TStreamLoadPutRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.debug("receive stream load put request. db:{}, tbl: {}, txn_id: {}, load id: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), DebugUtil.printId(request.getLoadId()),
                clientAddr);
        LOG.debug("stream load put request: {}", request);

        TStreamLoadPutResult result = new TStreamLoadPutResult();
        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            result.setParams(streamLoadPutImpl(request));
        } catch (LockTimeoutException e) {
            LOG.warn("failed to get stream load plan: {}", e.getMessage());
            status.setStatus_code(TStatusCode.TIMEOUT);
            status.addToError_msgs(e.getMessage());
        } catch (StarRocksException | StarRocksPlannerException e) {
            LOG.warn("failed to get stream load plan: {}", e.getMessage());
            status.setStatus_code(TStatusCode.ANALYSIS_ERROR);
            status.addToError_msgs(e.getMessage());
        } catch (Throwable e) {
            LOG.warn("catch unknown result.", e);
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(Strings.nullToEmpty(e.getMessage()));
            return result;
        }
        return result;
    }

    private Coordinator.Factory getCoordinatorFactory() {
        return new DefaultCoordinator.Factory();
    }

    TExecPlanFragmentParams streamLoadPutImpl(TStreamLoadPutRequest request) throws StarRocksException, LockTimeoutException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException("unknown database, database=" + dbName);
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), request.getTbl());
        if (table == null) {
            throw new StarRocksException("unknown table, table=" + request.getTbl());
        }
        if (!(table instanceof OlapTable)) {
            throw new StarRocksException("load table type is not OlapTable, type=" + table.getClass());
        }
        if (table instanceof MaterializedView) {
            throw new StarRocksException(String.format(
                    "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                            "and the data of materialized view must be consistent with the base table.",
                    table.getName(), table.getName()));
        }

        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;
        // Make timeout less than thrift_rpc_timeout_ms.
        // Otherwise, it will result in error like "call frontend service failed"
        timeoutMs = timeoutMs * 3 / 4;

        Locker locker = new Locker();
        if (!locker.tryLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ,
                timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new LockTimeoutException(
                    "get database read lock timeout, database=" + dbName + ", timeout=" + timeoutMs + "ms");
        }
        try {
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadInfo);
            TExecPlanFragmentParams plan = planner.plan(streamLoadInfo.getId());

            StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                    getSyncSteamLoadTaskByTxnId(request.getTxnId());
            if (streamLoadTask == null) {
                throw new StarRocksException("can not find stream load task by txnId " + request.getTxnId());
            }

            streamLoadTask.setTUniqueId(request.getLoadId());

            Coordinator coord = getCoordinatorFactory().createSyncStreamLoadScheduler(planner, getClientAddr());
            streamLoadTask.setCoordinator(coord);

            QeProcessorImpl.INSTANCE.registerQuery(streamLoadInfo.getId(), coord);

            plan.query_options.setLoad_job_type(TLoadJobType.STREAM_LOAD);
            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                            .getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new StarRocksException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes((OlapTable) table);
            plan.setImport_label(txnState.getLabel());
            plan.setDb_name(dbName);
            plan.setLoad_job_id(request.getTxnId());

            return plan;
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    @Override
    public TMergeCommitResult requestMergeCommit(TMergeCommitRequest request) throws TException {
        TMergeCommitResult result = new TMergeCommitResult();
        try {
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            Database db = globalStateMgr.getLocalMetastore().getDb(request.getDb());
            if (db == null) {
                throw new StarRocksException(String.format("unknown database [%s]", request.getDb()));
            }
            Table table = db.getTable(request.getTbl());
            if (table == null) {
                throw new StarRocksException(String.format("unknown table [%s.%s]", request.getDb(), request.getTbl()));
            }
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
            TableId tableId = new TableId(request.getDb(), request.getTbl());
            StreamLoadKvParams params = new StreamLoadKvParams(request.getParams());
            RequestLoadResult loadResult = GlobalStateMgr.getCurrentState()
                    .getBatchWriteMgr().requestLoad(tableId, params, request.getBackend_id(), request.getBackend_host());
            result.setStatus(loadResult.getStatus());
            if (loadResult.isOk()) {
                result.setLabel(loadResult.getValue());
            }
        } catch (AuthenticationException authenticationException) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.NOT_AUTHORIZED);
            status.addToError_msgs(authenticationException.getMessage());
            result.setStatus(status);
        } catch (Exception exception) {
            TStatus status = new TStatus();
            status.setStatus_code(TStatusCode.INTERNAL_ERROR);
            status.addToError_msgs(exception.getMessage());
            result.setStatus(status);
        }
        return result;
    }

    @Override
    public TStatus snapshotLoaderReport(TSnapshotLoaderReportRequest request) throws TException {
        if (GlobalStateMgr.getCurrentState().getBackupHandler().report(request.getTask_type(), request.getJob_id(),
                request.getTask_id(), request.getFinished_num(), request.getTotal_num())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TRefreshTableResponse refreshTable(TRefreshTableRequest request) throws TException {
        try {
            // Adapt to the situation that the Fe node before upgrading sends a request to the Fe node after upgrading.
            if (request.getCatalog_name() == null) {
                request.setCatalog_name(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            }
            String catalog = request.getCatalog_name();
            String db = request.getDb_name();
            String table = request.getTable_name();
            List<String> partitions = request.getPartitions() == null ? new ArrayList<>() : request.getPartitions();
            LOG.info("Start to refresh external table {}.{}.{}.{}", catalog, db, table, partitions);
            GlobalStateMgr.getCurrentState().refreshExternalTable(new TableName(catalog, db, table), partitions);
            LOG.info("Finish to refresh external table {}.{}.{}.{}", catalog, db, table, partitions);
            return new TRefreshTableResponse(new TStatus(TStatusCode.OK));
        } catch (Exception e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            return new TRefreshTableResponse(status);
        }
    }

    public TNetworkAddress getClientAddr() {
        ThriftServerContext connectionContext = ThriftServerEventProcessor.getConnectionContext();
        // For NonBlockingServer, we can not get client ip.
        if (connectionContext != null) {
            return connectionContext.getClient();
        }
        return null;
    }

    private String getClientAddrAsString() {
        TNetworkAddress addr = getClientAddr();
        return addr == null ? "unknown" : addr.hostname;
    }

    @Override
    public TGetTableMetaResponse getTableMeta(TGetTableMetaRequest request) throws TException {
        return leaderImpl.getTableMeta(request);
    }

    // Authenticate a FrontendServiceImpl#beginRemoteTxn RPC for StarRocks external table.
    // The beginRemoteTxn is sent by the source cluster, and received by the target cluster.
    // The target cluster should do authentication using the TAuthenticateParams. This method
    // will check whether the user has an authorization, and whether the user has a
    // PrivPredicate.LOAD on the given tables. The implementation is similar with that
    // of stream load, and you can refer to RestBaseAction#execute and LoadAction#executeWithoutPassword
    // to know more about the related part.
    static TStatus checkPasswordAndLoadPrivilege(TAuthenticateParams authParams) {
        if (authParams == null) {
            LOG.debug("received null TAuthenticateParams");
            return new TStatus(TStatusCode.OK);
        }

        LOG.debug("Receive TAuthenticateParams [user: {}, host: {}, db: {}, tables: {}]",
                authParams.user, authParams.getHost(), authParams.getDb_name(), authParams.getTable_names());
        if (!Config.enable_starrocks_external_table_auth_check) {
            LOG.debug("enable_starrocks_external_table_auth_check is disabled, " +
                    "and skip to check authorization and privilege for {}", authParams);
            return new TStatus(TStatusCode.OK);
        }
        String configHintMsg = "Set the configuration 'enable_starrocks_external_table_auth_check' to 'false' on the" +
                " target cluster if you don't want to check the authorization and privilege.";

        // 1. check user and password
        UserIdentity userIdentity;
        try {
            BaseAction.ActionAuthorizationInfo authInfo = BaseAction.parseAuthInfo(
                    authParams.getUser(), authParams.getPasswd(), authParams.getHost());
            userIdentity = BaseAction.checkPassword(authInfo);
        } catch (Exception e) {
            LOG.warn("Failed to check TAuthenticateParams [user: {}, host: {}, db: {}, tables: {}]",
                    authParams.user, authParams.getHost(), authParams.getDb_name(), authParams.getTable_names(), e);
            TStatus status = new TStatus(TStatusCode.NOT_AUTHORIZED);
            status.setError_msgs(Lists.newArrayList(e.getMessage(), "Please check that your user or password " +
                    "is correct", configHintMsg));
            return status;
        }

        // 2. check privilege
        try {
            String dbName = authParams.getDb_name();
            for (String tableName : authParams.getTable_names()) {
                ConnectContext context = new ConnectContext();
                context.setCurrentUserIdentity(userIdentity);
                context.setCurrentRoleIds(userIdentity);
                Authorizer.checkTableAction(context, dbName, tableName, PrivilegeType.INSERT);
            }
            return new TStatus(TStatusCode.OK);
        } catch (Exception e) {
            LOG.warn("Failed to check TAuthenticateParams [user: {}, host: {}, db: {}, tables: {}]",
                    authParams.user, authParams.getHost(), authParams.getDb_name(), authParams.getTable_names(), e);
            TStatus status = new TStatus(TStatusCode.NOT_AUTHORIZED);
            status.setError_msgs(Lists.newArrayList(e.getMessage(), configHintMsg));
            return status;
        }
    }

    @Override
    public TBeginRemoteTxnResponse beginRemoteTxn(TBeginRemoteTxnRequest request) throws TException {
        TStatus status = checkPasswordAndLoadPrivilege(request.getAuth_info());
        if (status.getStatus_code() != TStatusCode.OK) {
            TBeginRemoteTxnResponse response = new TBeginRemoteTxnResponse();
            response.setStatus(status);
            return response;
        }
        return leaderImpl.beginRemoteTxn(request);
    }

    @Override
    public TCommitRemoteTxnResponse commitRemoteTxn(TCommitRemoteTxnRequest request) throws TException {
        return leaderImpl.commitRemoteTxn(request);
    }

    @Override
    public TAbortRemoteTxnResponse abortRemoteTxn(TAbortRemoteTxnRequest request) throws TException {
        return leaderImpl.abortRemoteTxn(request);
    }

    @Override
    public TSetConfigResponse setConfig(TSetConfigRequest request) throws TException {
        try {
            Preconditions.checkState(request.getKeys().size() == request.getValues().size());
            Map<String, String> configs = new HashMap<>();
            for (int i = 0; i < request.getKeys().size(); i++) {
                String key = request.getKeys().get(i);
                String value = request.getValues().get(i);
                configs.put(key, value);
                if ("mysql_server_version".equalsIgnoreCase(key)) {
                    if (!Strings.isNullOrEmpty(value)) {
                        GlobalVariable.version = value;
                    }
                }
            }
            ConfigBase.setFrontendConfig(configs, request.isIs_persistent(),
                    request.getUser_identity());
            return new TSetConfigResponse(new TStatus(TStatusCode.OK));
        } catch (DdlException e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            return new TSetConfigResponse(status);
        }
    }

    public TAllocateAutoIncrementIdResult allocAutoIncrementId(TAllocateAutoIncrementIdParam request) throws TException {
        TAllocateAutoIncrementIdResult result = new TAllocateAutoIncrementIdResult();
        long rows = Math.max(request.rows, Config.auto_increment_cache_size);
        Long nextId = null;
        try {
            nextId = GlobalStateMgr.getCurrentState().getLocalMetastore().allocateAutoIncrementId(request.table_id, rows);
            // log the delta result.
            ConcurrentHashMap<Long, Long> deltaMap = new ConcurrentHashMap<>();
            deltaMap.put(request.table_id, nextId + rows);
            AutoIncrementInfo info = new AutoIncrementInfo(deltaMap);
            GlobalStateMgr.getCurrentState().getEditLog().logSaveAutoIncrementId(info);
        } catch (Exception e) {
            result.setAuto_increment_id(0);
            result.setAllocated_rows(0);

            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            result.setStatus(status);
            return result;
        }

        if (nextId == null) {
            result.setAuto_increment_id(0);
            result.setAllocated_rows(0);

            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("No ids have been allocated"));
            result.setStatus(status);
            return result;
        }

        result.setAuto_increment_id(nextId);
        result.setAllocated_rows(rows);

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);

        return result;
    }

    @Override
    public TImmutablePartitionResult updateImmutablePartition(TImmutablePartitionRequest request) throws TException {
        LOG.debug("Receive update immutable partition: {}", request);

        TImmutablePartitionResult result;
        try {
            result = updateImmutablePartitionInternal(request);
        } catch (Throwable t) {
            LOG.warn(t.getMessage(), t);
            result = new TImmutablePartitionResult();
            TStatus errorStatus = new TStatus(RUNTIME_ERROR);
            errorStatus.setError_msgs(Lists.newArrayList(String.format("txn_id=%d failed. %s",
                    request.getTxn_id(), t.getMessage())));
            result.setStatus(errorStatus);
        }

        LOG.info("Finish update immutable partition: {}", result);

        return result;
    }

    public synchronized TImmutablePartitionResult updateImmutablePartitionInternal(TImmutablePartitionRequest request)
            throws StarRocksException {
        long dbId = request.getDb_id();
        long tableId = request.getTable_id();
        TImmutablePartitionResult result = new TImmutablePartitionResult();
        TStatus errorStatus = new TStatus(RUNTIME_ERROR);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            errorStatus.setError_msgs(
                    Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            return result;
        }
        Locker locker = new Locker();
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            errorStatus.setError_msgs(
                    Lists.newArrayList(String.format("dbId=%d tableId=%d is not exists", dbId, tableId)));
            result.setStatus(errorStatus);
            return result;
        }
        if (!(table instanceof OlapTable)) {
            errorStatus.setError_msgs(
                    Lists.newArrayList(String.format("dbId=%d tableId=%d is not olap table", dbId, tableId)));
            result.setStatus(errorStatus);
            return result;
        }
        OlapTable olapTable = (OlapTable) table;

        if (request.partition_ids == null) {
            errorStatus.setError_msgs(Lists.newArrayList("partition_ids should not null."));
            result.setStatus(errorStatus);
            return result;
        }

        List<TOlapTablePartition> partitions = Lists.newArrayList();
        List<TTabletLocation> tablets = Lists.newArrayList();
        Set<Long> updatePartitionIds = Sets.newHashSet();

        SystemInfoService systemInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        long warehouseId = WarehouseManager.DEFAULT_WAREHOUSE_ID;
        if (request.isSetBackend_id()) {
            warehouseId = Utils.getWarehouseIdByNodeId(systemInfo, request.getBackend_id())
                    .orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        }

        // immute partitions and create new sub partitions
        for (Long id : request.partition_ids) {
            PhysicalPartition p = table.getPhysicalPartition(id);
            if (p == null) {
                LOG.warn("physical partition id {} does not exist", id);
                continue;
            }
            Partition partition = olapTable.getPartition(p.getParentId());
            if (partition == null) {
                LOG.warn("partition id {} does not exist", p.getParentId());
                continue;
            }
            updatePartitionIds.add(p.getParentId());
            p.setImmutable(true);

            List<PhysicalPartition> mutablePartitions;
            try {
                locker.lockDatabase(db.getId(), LockType.READ);
                mutablePartitions = partition.getSubPartitions().stream()
                        .filter(physicalPartition -> !physicalPartition.isImmutable())
                        .collect(Collectors.toList());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
            if (mutablePartitions.size() <= 0) {
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .addSubPartitions(db, olapTable, partition, 1, warehouseId);
            }
        }

        // return all mutable partitions
        for (Long id : updatePartitionIds) {
            Partition partition = olapTable.getPartition(id);
            if (partition == null) {
                LOG.warn("partition id {} does not exist", id);
                continue;
            }

            long mutablePartitionNum = 0;
            try {
                locker.lockDatabase(db.getId(), LockType.READ);
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    if (physicalPartition.isImmutable()) {
                        continue;
                    }
                    if (mutablePartitionNum >= 8) {
                        continue;
                    }
                    ++mutablePartitionNum;

                    TOlapTablePartition tPartition = new TOlapTablePartition();
                    tPartition.setId(physicalPartition.getId());
                    buildPartitions(olapTable, physicalPartition, partitions, tPartition);
                    buildTablets(physicalPartition, tablets, olapTable, warehouseId);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
        result.setPartitions(partitions);
        result.setTablets(tablets);

        // build nodes
        TNodesInfo nodesInfo = GlobalStateMgr.getCurrentState().createNodesInfo(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        result.setNodes(nodesInfo.nodes);
        result.setStatus(new TStatus(OK));
        return result;
    }

    private static void buildPartitions(OlapTable olapTable, PhysicalPartition physicalPartition,
                                        List<TOlapTablePartition> partitions, TOlapTablePartition tPartition) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            Range<PartitionKey> range = rangePartitionInfo.getRange(physicalPartition.getParentId());
            int partColNum = rangePartitionInfo.getPartitionColumnsSize();
            // set start keys
            if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToStart_keys(
                            range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
            // set end keys
            if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToEnd_keys(
                            range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
            List<List<TExprNode>> inKeysExprNodes = new ArrayList<>();

            List<List<LiteralExpr>> multiValues = listPartitionInfo.getMultiLiteralExprValues().get(
                    physicalPartition.getParentId());
            if (multiValues != null && !multiValues.isEmpty()) {
                inKeysExprNodes = multiValues.stream()
                        .map(values -> values.stream()
                                .map(value -> value.treeToThrift().getNodes().get(0))
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
                tPartition.setIn_keys(inKeysExprNodes);
            }

            List<LiteralExpr> values = listPartitionInfo.getLiteralExprValues().get(physicalPartition.getParentId());
            if (values != null && !values.isEmpty()) {
                inKeysExprNodes = values.stream()
                        .map(value -> Lists.newArrayList(value).stream()
                                .map(value1 -> value1.treeToThrift().getNodes().get(0))
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
            }

            if (!inKeysExprNodes.isEmpty()) {
                tPartition.setIn_keys(inKeysExprNodes);
            }
        }
        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                    index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
            tPartition.setNum_buckets(index.getTablets().size());
        }
        partitions.add(tPartition);
    }

    private static void buildTablets(PhysicalPartition physicalPartition, List<TTabletLocation> tablets,
                                     OlapTable olapTable, long warehouseId) throws StarRocksException {
        int quorum = olapTable.getPartitionInfo().getQuorumNum(physicalPartition.getParentId(), olapTable.writeQuorum());
        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                MaterializedIndex.IndexExtState.ALL)) {
            if (olapTable.isCloudNativeTable()) {
                for (Tablet tablet : index.getTablets()) {
                    try {
                        // use default warehouse nodes
                        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                                .getComputeNodeAssignedToTablet(warehouseId, (LakeTablet) tablet);
                        tablets.add(new TTabletLocation(tablet.getId(), Collections.singletonList(computeNode.getId())));
                    } catch (Exception exception) {
                        throw new StarRocksException("Check if any backend is down or not. tablet_id: " + tablet.getId());
                    }
                }
            } else {
                for (Tablet tablet : index.getTablets()) {
                    // we should ensure the replica backend is alive
                    // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                    LocalTablet localTablet = (LocalTablet) tablet;
                    Multimap<Replica, Long> bePathsMap =
                            localTablet.getNormalReplicaBackendPathMap(
                                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
                    if (bePathsMap.keySet().size() < quorum) {
                        throw new StarRocksException(String.format("Tablet lost replicas. Check if any backend is down or not. " +
                                        "tablet_id: %s, replicas: %s. Check quorum number failed(buildTablets): " +
                                        "BeReplicaSize:%s, quorum:%s", tablet.getId(), localTablet.getReplicaInfos(),
                                bePathsMap.size(), quorum));
                    }
                    // replicas[0] will be the primary replica
                    // getNormalReplicaBackendPathMap returns a linkedHashMap, it's keysets is stable
                    List<Replica> replicas = Lists.newArrayList(bePathsMap.keySet());
                    tablets.add(new TTabletLocation(tablet.getId(), replicas.stream().map(Replica::getBackendId)
                            .collect(Collectors.toList())));
                }
            }
        }

    }

    @Override
    public TCreatePartitionResult createPartition(TCreatePartitionRequest request) throws TException {

        LOG.info("Receive create partition: {}", request);

        TCreatePartitionResult result;
        try {
            if (partitionRequestNum.incrementAndGet() >= Config.thrift_server_max_worker_threads / 4) {
                result = new TCreatePartitionResult();
                TStatus errorStatus = new TStatus(SERVICE_UNAVAILABLE);
                errorStatus.setError_msgs(Lists.newArrayList(
                        String.format("Too many create partition requests, please try again later txn_id=%d",
                                request.getTxn_id())));
                result.setStatus(errorStatus);
                return result;
            }

            result = createPartitionProcess(request);
        } catch (Exception t) {
            LOG.warn(DebugUtil.getStackTrace(t));
            result = new TCreatePartitionResult();
            TStatus errorStatus = new TStatus(RUNTIME_ERROR);
            errorStatus.setError_msgs(Lists.newArrayList(String.format("txn_id=%d failed. %s",
                    request.getTxn_id(), t.getMessage())));
            result.setStatus(errorStatus);
        } finally {
            partitionRequestNum.decrementAndGet();
        }

        return result;
    }

    @NotNull
    private static TCreatePartitionResult createPartitionProcess(TCreatePartitionRequest request) {
        long dbId = request.getDb_id();
        long tableId = request.getTable_id();
        TCreatePartitionResult result = new TCreatePartitionResult();
        TStatus errorStatus = new TStatus(RUNTIME_ERROR);
        String partitionNamePrefix = null;
        boolean isTemp = false;
        if (request.isSetIs_temp() && request.isIs_temp()) {
            isTemp = true;
            partitionNamePrefix = "txn" + request.getTxn_id();
        }

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            errorStatus.setError_msgs(Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            return result;
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        if (table == null) {
            errorStatus.setError_msgs(Lists.newArrayList(String.format("dbId=%d tableId=%d is not exists", dbId, tableId)));
            result.setStatus(errorStatus);
            return result;
        }
        if (!(table instanceof OlapTable)) {
            errorStatus.setError_msgs(Lists.newArrayList(String.format("dbId=%d tableId=%d is not olap table", dbId, tableId)));
            result.setStatus(errorStatus);
            return result;
        }
        OlapTable olapTable = (OlapTable) table;

        if (request.partition_values == null) {
            errorStatus.setError_msgs(Lists.newArrayList("partition_values should not null."));
            result.setStatus(errorStatus);
            return result;
        }

        // Now only supports the case of automatically creating single range partition
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.isRangePartition() && olapTable.getPartitionColumnNames().size() != 1) {
            errorStatus.setError_msgs(Lists.newArrayList(
                    "automatic partition only support single column for range partition."));
            result.setStatus(errorStatus);
            return result;
        }

        AddPartitionClause addPartitionClause;
        List<String> partitionColNames = Lists.newArrayList();
        try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()),
                LockType.READ)) {
            addPartitionClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(olapTable,
                    request.partition_values, isTemp, partitionNamePrefix);
            PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
            if (partitionDesc instanceof RangePartitionDesc) {
                partitionColNames = ((RangePartitionDesc) partitionDesc).getPartitionColNames();
            } else if (partitionDesc instanceof ListPartitionDesc) {
                partitionColNames = ((ListPartitionDesc) partitionDesc).getPartitionColNames();
            }
            if (olapTable.getNumberOfPartitions() + partitionColNames.size() > Config.max_partition_number_per_table) {
                throw new AnalysisException("Table " + olapTable.getName() +
                        " automatically created partitions exceeded the maximum limit: " +
                        Config.max_partition_number_per_table + ". You can modify this restriction on by setting" +
                        " max_partition_number_per_table larger.");
            }
        } catch (AnalysisException ex) {
            errorStatus.setError_msgs(Lists.newArrayList(ex.getMessage()));
            result.setStatus(errorStatus);
            return result;
        }

        GlobalStateMgr state = GlobalStateMgr.getCurrentState();

        TransactionState txnState = state.getGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxn_id());
        if (txnState == null) {
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("automatic create partition failed. error: txn %d not exist", request.getTxn_id())));
            result.setStatus(errorStatus);
            return result;
        }

        if (txnState.getPartitionNameToTPartition().size() > Config.max_partitions_in_one_batch) {
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("Table %s automatic create partition failed. error: partitions in one batch exceed limit %d," +
                                    "You can modify this restriction on by setting" + " max_partitions_in_one_batch larger.",
                            olapTable.getName(), Config.max_partitions_in_one_batch)));
            result.setStatus(errorStatus);
            return result;
        }

        Set<String> creatingPartitionNames = CatalogUtils.getPartitionNamesFromAddPartitionClause(addPartitionClause);

        try {
            // creating partition names is ordered
            for (String partitionName : creatingPartitionNames) {
                olapTable.lockCreatePartition(partitionName);
            }

            // ingestion is top priority, if schema change or rollup is running, cancel it
            try {
                if (olapTable.getState() == OlapTable.OlapTableState.ROLLUP) {
                    LOG.info("cancel rollup for automatic create partition txn_id={}", request.getTxn_id());
                    state.getLocalMetastore().cancelAlter(
                            new CancelAlterTableStmt(
                                    ShowAlterStmt.AlterType.ROLLUP,
                                    new TableName(db.getFullName(), olapTable.getName())),
                            "conflict with expression partition");
                }

                if (olapTable.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE) {
                    LOG.info("cancel schema change for automatic create partition txn_id={}", request.getTxn_id());
                    state.getLocalMetastore().cancelAlter(
                            new CancelAlterTableStmt(
                                    ShowAlterStmt.AlterType.COLUMN,
                                    new TableName(db.getFullName(), olapTable.getName())),
                            "conflict with expression partition");
                }
            } catch (Exception e) {
                LOG.warn("cancel schema change or rollup failed. error: {}", e.getMessage());
            }

            // If a create partition request is from BE or CN, the warehouse information may be lost, we can get it from txn state.
            ConnectContext ctx = Util.getOrCreateInnerContext();
            if (txnState.getWarehouseId() != WarehouseManager.DEFAULT_WAREHOUSE_ID) {
                ctx.setCurrentWarehouseId(txnState.getWarehouseId());
            }
            try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()),
                    LockType.READ)) {
                AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(olapTable);
                analyzer.analyze(ctx, addPartitionClause);
            }
            state.getLocalMetastore().addPartitions(ctx, db, olapTable.getName(), addPartitionClause);
        } catch (Exception e) {
            LOG.warn("failed to add partitions", e);
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("automatic create partition failed. error:%s", e.getMessage())));
            result.setStatus(errorStatus);
            return result;
        } finally {
            for (String partitionName : creatingPartitionNames) {
                olapTable.unlockCreatePartition(partitionName);
            }
        }

        // build partition & tablets
        List<TOlapTablePartition> partitions = Lists.newArrayList();
        List<TTabletLocation> tablets = Lists.newArrayList();

        if (txnState.getTransactionStatus().isFinalStatus()) {
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("automatic create partition failed. error: txn %d is %s", request.getTxn_id(),
                            txnState.getTransactionStatus().name())));
            result.setStatus(errorStatus);
            return result;
        }

        // update partition info snapshot for txn should be synchronized
        synchronized (txnState) {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                return buildCreatePartitionResponse(
                        olapTable, txnState, partitions, tablets, partitionColNames, isTemp);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
    }

    private static TCreatePartitionResult buildCreatePartitionResponse(OlapTable olapTable,
                                                                       TransactionState txnState,
                                                                       List<TOlapTablePartition> partitions,
                                                                       List<TTabletLocation> tablets,
                                                                       List<String> partitionColNames,
                                                                       boolean isTemp) {
        TCreatePartitionResult result = new TCreatePartitionResult();
        TStatus errorStatus = new TStatus(RUNTIME_ERROR);
        for (String partitionName : partitionColNames) {
            // get partition info from snapshot
            TOlapTablePartition tPartition = txnState.getPartitionNameToTPartition().get(partitionName);
            if (tPartition != null) {
                partitions.add(tPartition);
                for (TOlapTableIndexTablets index : tPartition.getIndexes()) {
                    for (long tabletId : index.getTablets()) {
                        TTabletLocation tablet = txnState.getTabletIdToTTabletLocation().get(tabletId);
                        if (tablet != null) {
                            tablets.add(tablet);
                        }
                    }
                }
                continue;
            }

            Partition partition = olapTable.getPartition(partitionName, isTemp);
            tPartition = new TOlapTablePartition();
            tPartition.setId(partition.getDefaultPhysicalPartition().getId());
            buildPartitionInfo(olapTable, partitions, partition, tPartition, txnState);
            // tablet
            int quorum = olapTable.getPartitionInfo().getQuorumNum(partition.getId(), olapTable.writeQuorum());
            for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                if (olapTable.isCloudNativeTable()) {
                    for (Tablet tablet : index.getTablets()) {
                        LakeTablet cloudNativeTablet = (LakeTablet) tablet;
                        try {
                            // use default warehouse nodes
                            long computeNodeId = GlobalStateMgr.getCurrentState().getWarehouseMgr().getComputeNodeId(
                                    txnState.getWarehouseId(), cloudNativeTablet);
                            TTabletLocation tabletLocation = new TTabletLocation(tablet.getId(),
                                    Collections.singletonList(computeNodeId));
                            tablets.add(tabletLocation);
                            txnState.getTabletIdToTTabletLocation().put(tablet.getId(), tabletLocation);
                        } catch (Exception exception) {
                            errorStatus.setError_msgs(Lists.newArrayList(
                                    "Tablet lost replicas. Check if any backend is down or not. tablet_id: "
                                            + tablet.getId() + ", backends: none(cloud native table)"));
                            result.setStatus(errorStatus);
                            return result;
                        }
                    }
                } else {
                    for (Tablet tablet : index.getTablets()) {
                        // we should ensure the replica backend is alive
                        // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                        LocalTablet localTablet = (LocalTablet) tablet;
                        Multimap<Replica, Long> bePathsMap =
                                localTablet.getNormalReplicaBackendPathMap(
                                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
                        if (bePathsMap.keySet().size() < quorum) {
                            String errorMsg = String.format("Tablet lost replicas. Check if any backend is down or not. " +
                                            "tablet_id: %s, replicas: %s. Check quorum number failed" +
                                            "(buildCreatePartitionResponse): BeReplicaSize:%s, quorum:%s",
                                    tablet.getId(), localTablet.getReplicaInfos(), bePathsMap.size(), quorum);
                            errorStatus.setError_msgs(Lists.newArrayList(errorMsg));
                            result.setStatus(errorStatus);
                            return result;
                        }
                        // replicas[0] will be the primary replica
                        List<Replica> replicas = Lists.newArrayList(bePathsMap.keySet());
                        Collections.shuffle(replicas);
                        TTabletLocation tabletLocation = new TTabletLocation(tablet.getId(),
                                replicas.stream().map(Replica::getBackendId).collect(Collectors.toList()));
                        tablets.add(tabletLocation);
                        txnState.getTabletIdToTTabletLocation().put(tablet.getId(), tabletLocation);
                    }
                }
            }
        }
        result.setPartitions(partitions);
        result.setTablets(tablets);

        // build nodes
        TNodesInfo nodesInfo = GlobalStateMgr.getCurrentState().createNodesInfo(txnState.getWarehouseId(),
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo());
        result.setNodes(nodesInfo.nodes);
        result.setStatus(new TStatus(OK));
        return result;
    }

    private static List<TExprNode> literalExprsToTExprNodes(List<LiteralExpr> values) {
        return values.stream()
                .map(value -> value.treeToThrift().getNodes().get(0))
                .collect(Collectors.toList());
    }

    private static void buildPartitionInfo(OlapTable olapTable, List<TOlapTablePartition> partitions,
                                           Partition partition, TOlapTablePartition tPartition, TransactionState txnState) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
            int partColNum = rangePartitionInfo.getPartitionColumnsSize();
            // set start keys
            if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToStart_keys(
                            range.lowerEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
            // set end keys
            if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
                for (int i = 0; i < partColNum; i++) {
                    tPartition.addToEnd_keys(
                            range.upperEndpoint().getKeys().get(i).treeToThrift().getNodes().get(0));
                }
            }
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) olapTable.getPartitionInfo();
            List<List<TExprNode>> inKeysExprNodes = new ArrayList<>();

            List<List<LiteralExpr>> multiValues = listPartitionInfo.getMultiLiteralExprValues().get(partition.getId());
            if (multiValues != null && !multiValues.isEmpty()) {
                inKeysExprNodes = multiValues.stream()
                        .map(values -> values.stream()
                                .map(value -> value.treeToThrift().getNodes().get(0))
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
                tPartition.setIn_keys(inKeysExprNodes);
            }

            List<LiteralExpr> values = listPartitionInfo.getLiteralExprValues().get(partition.getId());
            if (values != null && !values.isEmpty()) {
                inKeysExprNodes = values.stream()
                        .map(value -> Lists.newArrayList(value).stream()
                                .map(value1 -> value1.treeToThrift().getNodes().get(0))
                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
            }

            if (!inKeysExprNodes.isEmpty()) {
                tPartition.setIn_keys(inKeysExprNodes);
            }
        }
        for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            tPartition.addToIndexes(new TOlapTableIndexTablets(index.getId(), Lists.newArrayList(
                    index.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()))));
            tPartition.setNum_buckets(index.getTablets().size());
        }
        partitions.add(tPartition);
        txnState.getPartitionNameToTPartition().put(partition.getName(), tPartition);
    }

    @Override
    public TGetTablesConfigResponse getTablesConfig(TGetTablesConfigRequest request) throws TException {
        return InformationSchemaDataSource.generateTablesConfigResponse(request);
    }

    @Override
    public TGetPartitionsMetaResponse getPartitionsMeta(TGetPartitionsMetaRequest request) throws TException {
        return InformationSchemaDataSource.generatePartitionsMetaResponse(request);
    }

    @Override
    public TGetTablesInfoResponse getTablesInfo(TGetTablesInfoRequest request) throws TException {
        return InformationSchemaDataSource.generateTablesInfoResponse(request);
    }

    /**
     * This RPC method does nothing. It is just for compatibility.
     * From the version 3.2, the resource usage is only maintained in the master FE, and isn't synchronized between FEs anymore.
     */
    @Override
    @Deprecated
    public TUpdateResourceUsageResponse updateResourceUsage(TUpdateResourceUsageRequest request) throws TException {
        TUpdateResourceUsageResponse res = new TUpdateResourceUsageResponse();
        TStatus status = new TStatus(TStatusCode.OK);
        res.setStatus(status);
        return res;
    }

    @Override
    public TGetWarehousesResponse getWarehouses(TGetWarehousesRequest request) throws TException {
        Map<Long, WarehouseInfo> warehouseToInfo = WarehouseInfosBuilder.makeBuilderFromMetricAndMgrs().build();
        List<TWarehouseInfo> warehouseInfos = warehouseToInfo.values().stream()
                .map(WarehouseInfo::toThrift)
                .collect(Collectors.toList());

        TGetWarehousesResponse res = new TGetWarehousesResponse();
        res.setStatus(new TStatus(OK));
        res.setWarehouse_infos(warehouseInfos);

        return res;
    }

    @Override
    public TGetQueryStatisticsResponse getQueryStatistics(TGetQueryStatisticsRequest request) throws TException {
        try {
            List<QueryStatisticsInfo> queryStatisticsInfos = QueryStatisticsInfo.makeListFromMetricsAndMgrs();
            List<TQueryStatisticsInfo> queryStatisticsThrift = queryStatisticsInfos
                    .stream()
                    .map(QueryStatisticsInfo::toThrift)
                    .collect(Collectors.toList());

            TGetQueryStatisticsResponse res = new TGetQueryStatisticsResponse();
            res.setStatus(new TStatus(OK));
            res.setQueryStatistics_infos(queryStatisticsThrift);

            return res;
        } catch (AnalysisException e) {
            throw new TException(e.getMessage());
        }
    }

    @Override
    public TMVReportEpochResponse mvReport(TMVMaintenanceTasks request) throws TException {
        LOG.info("Recieve mvReport: {}", request);
        if (!request.getTask_type().equals(MVTaskType.REPORT_EPOCH)) {
            throw new TException("Only support report_epoch task");
        }
        GlobalStateMgr.getCurrentState().getMaterializedViewMgr().onReportEpoch(request);
        return new TMVReportEpochResponse();
    }

    @Override
    public TGetLoadsResult getLoads(TGetLoadsParams request) throws TException {
        LOG.debug("Recieve getLoads: {}", request);

        TGetLoadsResult result = new TGetLoadsResult();
        List<TLoadInfo> loads = Lists.newArrayList();
        try {
            if (request.isSetJob_id()) {
                LoadJob job = GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJob(request.getJob_id());
                if (job != null) {
                    loads.add(job.toThrift());
                }
            } else if (request.isSetDb()) {
                long dbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(request.getDb()).getId();
                if (request.isSetLabel()) {
                    loads.addAll(GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobsByDb(
                                    dbId, request.getLabel(), true).stream()
                            .map(LoadJob::toThrift).collect(Collectors.toList()));
                } else {
                    loads.addAll(GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobsByDb(
                                    dbId, null, false).stream().map(LoadJob::toThrift)
                            .collect(Collectors.toList()));
                }
            } else {
                if (request.isSetLabel()) {
                    loads.addAll(GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobs(request.getLabel())
                            .stream().map(LoadJob::toThrift).collect(Collectors.toList()));
                } else {
                    loads.addAll(GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobs(null)
                            .stream().map(LoadJob::toThrift).collect(Collectors.toList()));
                }
            }

            if (request.isSetJob_id()) {
                StreamLoadTask task = GlobalStateMgr.getCurrentState().getStreamLoadMgr().getTaskById(request.getJob_id());
                if (task != null) {
                    loads.add(task.toThrift());
                }
            } else {
                List<StreamLoadTask> streamLoadTaskList = GlobalStateMgr.getCurrentState().getStreamLoadMgr()
                        .getTaskByName(request.getLabel());
                if (streamLoadTaskList != null) {
                    loads.addAll(
                            streamLoadTaskList.stream().map(StreamLoadTask::toThrift).collect(Collectors.toList()));
                }
            }

            result.setLoads(loads);
        } catch (Exception e) {
            LOG.warn("Failed to getLoads", e);
            throw e;
        }
        return result;
    }

    @Override
    public TGetTrackingLoadsResult getTrackingLoads(TGetLoadsParams request) throws TException {
        LOG.debug("Receive getTrackingLoads: {}", request);
        TGetTrackingLoadsResult result = new TGetTrackingLoadsResult();
        List<TTrackingLoadInfo> trackingLoadInfoList = Lists.newArrayList();

        // Since job_id is globally unique, when one job has been found, no need to go forward.
        if (request.isSetJob_id()) {
            RESULT:
            {
                // BROKER, INSERT
                LoadMgr loadManager = GlobalStateMgr.getCurrentState().getLoadMgr();
                LoadJob loadJob = loadManager.getLoadJob(request.getJob_id());
                if (loadJob != null) {
                    trackingLoadInfoList = convertLoadInfoList(request);
                    break RESULT;
                }

                // ROUTINE LOAD
                RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
                RoutineLoadJob routineLoadJob = routineLoadManager.getJob(request.getJob_id());
                if (routineLoadJob != null) {
                    trackingLoadInfoList = convertRoutineLoadInfoList(request);
                    break RESULT;
                }

                // STREAM LOAD
                StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
                StreamLoadTask streamLoadTask = streamLoadManager.getTaskById(request.getJob_id());
                if (streamLoadTask != null) {
                    trackingLoadInfoList = convertStreamLoadInfoList(request);
                }
            }
        } else {
            // iterate all types of loads to find the matching records
            trackingLoadInfoList.addAll(convertLoadInfoList(request));
            trackingLoadInfoList.addAll(convertRoutineLoadInfoList(request));
            trackingLoadInfoList.addAll(convertStreamLoadInfoList(request));
        }

        result.setTrackingLoads(trackingLoadInfoList);
        LOG.debug("get tracking load jobs size: {}", trackingLoadInfoList.size());
        return result;
    }

    private List<TTrackingLoadInfo> convertLoadInfoList(TGetLoadsParams request) throws TException {
        TGetLoadsResult loadsResult = getLoads(request);
        List<TLoadInfo> loads = loadsResult.loads;
        if (loads == null) {
            return Lists.newArrayList();
        }
        return loads.stream().map(load -> convertToTrackingLoadInfo(load.getJob_id(),
                        load.getDb(), load.getLabel(), load.getType(), load.getUrl()))
                .collect(Collectors.toList());
    }

    private List<TTrackingLoadInfo> convertRoutineLoadInfoList(TGetLoadsParams request) throws TException {
        TGetRoutineLoadJobsResult loadsResult = getRoutineLoadJobs(request);
        List<TRoutineLoadJobInfo> loads = loadsResult.loads;
        if (loads == null) {
            return Lists.newArrayList();
        }
        return loads.stream().map(load -> convertToTrackingLoadInfo(load.getId(),
                        load.getDb_name(), load.getName(), EtlJobType.ROUTINE_LOAD.name(), load.getError_log_urls()))
                .collect(Collectors.toList());
    }

    private List<TTrackingLoadInfo> convertStreamLoadInfoList(TGetLoadsParams request) throws TException {
        TGetStreamLoadsResult loadsResult = getStreamLoads(request);
        List<TStreamLoadInfo> loads = loadsResult.loads;
        if (loads == null) {
            return Lists.newArrayList();
        }
        return loads.stream().map(load -> convertToTrackingLoadInfo(load.getId(),
                        load.getDb_name(), load.getLabel(), EtlJobType.STREAM_LOAD.name(), load.getTracking_url()))
                .collect(Collectors.toList());
    }

    private TTrackingLoadInfo convertToTrackingLoadInfo(long jobId, String dbName, String label, String type, String url) {
        TTrackingLoadInfo trackingLoad = new TTrackingLoadInfo();
        trackingLoad.setJob_id(jobId);
        trackingLoad.setDb(dbName);
        trackingLoad.setLabel(label);
        trackingLoad.setLoad_type(type);
        if (url != null) {
            if (url.contains(",")) {
                trackingLoad.setUrls(Arrays.asList(url.split(",")));
            } else {
                trackingLoad.addToUrls(url);
            }
        }
        return trackingLoad;
    }

    @Override
    public TGetRoutineLoadJobsResult getRoutineLoadJobs(TGetLoadsParams request) throws TException {
        LOG.debug("Receive getRoutineLoadJobs: {}", request);
        TGetRoutineLoadJobsResult result = new TGetRoutineLoadJobsResult();
        RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();
        List<TRoutineLoadJobInfo> loads = Lists.newArrayList();
        try {
            if (request.isSetJob_id()) {
                RoutineLoadJob job = routineLoadManager.getJob(request.getJob_id());
                if (job != null) {
                    loads.add(job.toThrift());
                }
            } else {
                List<RoutineLoadJob> loadJobList;
                if (request.isSetDb()) {
                    if (request.isSetLabel()) {
                        loadJobList = routineLoadManager.getJob(request.getDb(), request.getLabel(), true);
                    } else {
                        loadJobList = routineLoadManager.getJob(request.getDb(), null, true);
                    }
                } else {
                    if (request.isSetLabel()) {
                        loadJobList = routineLoadManager.getJob(null, request.getLabel(), true);
                    } else {
                        loadJobList = routineLoadManager.getJob(null, null, true);
                    }
                }
                loads.addAll(loadJobList.stream().map(RoutineLoadJob::toThrift).collect(Collectors.toList()));
            }
            result.setLoads(loads);
        } catch (MetaNotFoundException e) {
            LOG.warn("Failed to getRoutineLoadJobs", e);
            throw new TException();
        }
        return result;
    }

    @Override
    public TGetStreamLoadsResult getStreamLoads(TGetLoadsParams request) throws TException {
        LOG.debug("Receive getStreamLoads: {}", request);
        TGetStreamLoadsResult result = new TGetStreamLoadsResult();
        StreamLoadMgr loadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
        List<TStreamLoadInfo> loads = Lists.newArrayList();
        try {
            if (request.isSetJob_id()) {
                StreamLoadTask task = loadManager.getTaskById(request.getJob_id());
                if (task != null) {
                    loads.add(task.toStreamLoadThrift());
                }
            } else {
                List<StreamLoadTask> streamLoadTaskList = loadManager.getTaskByName(request.getLabel());
                if (streamLoadTaskList != null) {
                    loads.addAll(
                            streamLoadTaskList.stream().map(StreamLoadTask::toStreamLoadThrift).collect(Collectors.toList()));
                }
            }
            result.setLoads(loads);
        } catch (Exception e) {
            LOG.warn("Failed to getStreamLoads", e);
        }
        return result;
    }

    @Override
    public TGetTabletScheduleResponse getTabletSchedule(TGetTabletScheduleRequest request) throws TException {
        TGetTabletScheduleResponse response = GlobalStateMgr.getCurrentState().getTabletScheduler().getTabletSchedule(request);
        LOG.info("getTabletSchedule: {} return {} TabletSchedule", request, response.getTablet_schedulesSize());
        return response;
    }

    @Override
    public TGetRoleEdgesResponse getRoleEdges(TGetRoleEdgesRequest request) {
        return RoleEdges.getRoleEdges(request);
    }

    @Override
    public TGetGrantsToRolesOrUserResponse getGrantsTo(TGetGrantsToRolesOrUserRequest request) {
        return GrantsTo.getGrantsTo(request);
    }

    @Override
    public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws TException {
        LogicalSlot slot = LogicalSlot.fromThrift(request.getSlot());
        GlobalStateMgr.getCurrentState().getSlotManager().requireSlotAsync(slot);

        return new TRequireSlotResponse();
    }

    @Override
    public TFinishSlotRequirementResponse finishSlotRequirement(TFinishSlotRequirementRequest request) throws TException {
        Status status = GlobalStateMgr.getCurrentState().getGlobalSlotProvider()
                .finishSlotRequirement(request.getSlot_id(), request.getPipeline_dop(), new Status(request.getStatus()));

        TFinishSlotRequirementResponse res = new TFinishSlotRequirementResponse();
        res.setStatus(status.toThrift());

        return res;
    }

    @Override
    public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws TException {
        GlobalStateMgr.getCurrentState().getSlotManager().releaseSlotAsync(request.getSlot_id());

        TStatus tstatus = new TStatus(OK);
        TReleaseSlotResponse res = new TReleaseSlotResponse();
        res.setStatus(tstatus);

        return res;
    }

    @Override
    public TGetDictQueryParamResponse getDictQueryParam(TGetDictQueryParamRequest request) throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(request.getDb_name());
        if (db == null) {
            throw new SemanticException("Database %s is not found", request.getDb_name());
        }
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), request.getTable_name());
        if (table == null) {
            throw new SemanticException("dict table %s is not found", request.getTable_name());
        }
        if (!(table instanceof OlapTable)) {
            throw new SemanticException("dict table type is not OlapTable, type=" + table.getClass());
        }
        OlapTable dictTable = (OlapTable) table;
        TupleDescriptor tupleDescriptor = new TupleDescriptor(TupleId.createGenerator().getNextId());
        IdGenerator<SlotId> slotIdIdGenerator = SlotId.createGenerator();

        for (Column column : dictTable.getBaseSchema()) {
            SlotDescriptor slotDescriptor = new SlotDescriptor(slotIdIdGenerator.getNextId(), tupleDescriptor);
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsMaterialized(true);
            tupleDescriptor.addSlot(slotDescriptor);
        }

        TGetDictQueryParamResponse response = new TGetDictQueryParamResponse();
        response.setSchema(OlapTableSink.createSchema(db.getId(), dictTable, tupleDescriptor));
        try {
            List<Long> allPartitions = dictTable.getAllPartitionIds();
            TOlapTablePartitionParam partitionParam = OlapTableSink.createPartition(
                    db.getId(), dictTable, tupleDescriptor, dictTable.supportedAutomaticPartition(),
                    dictTable.getAutomaticBucketSize(), allPartitions);
            response.setPartition(partitionParam);
            response.setLocation(OlapTableSink.createLocation(dictTable, partitionParam, dictTable.enableReplicatedStorage()));
            response.setNodes_info(GlobalStateMgr.getCurrentState().createNodesInfo(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()));
        } catch (StarRocksException e) {
            SemanticException semanticException = new SemanticException("build DictQueryParams error in dict_query_expr.");
            semanticException.initCause(e);
            throw semanticException;
        }
        return response;
    }

    @Override
    public TTableReplicationResponse startTableReplication(TTableReplicationRequest request) throws TException {
        return leaderImpl.startTableReplication(request);
    }

    @Override
    public TReportLakeCompactionResponse reportLakeCompaction(TReportLakeCompactionRequest request) throws TException {
        TReportLakeCompactionResponse resp = new TReportLakeCompactionResponse();
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            CompactionMgr compactionMgr = GlobalStateMgr.getCurrentState().getCompactionMgr();
            resp.setValid(compactionMgr.existCompaction(request.getTxn_id()));
        } else {
            // if not leader, treat all compactions as valid in case compactions is cancelled in the middle
            resp.setValid(true);
        }
        return resp;
    }

    @Override
    public TListSessionsResponse listSessions(TListSessionsRequest request) throws TException {
        TListSessionsResponse response = new TListSessionsResponse();
        if (!request.isSetOptions()) {
            TStatus status = new TStatus(TStatusCode.INVALID_ARGUMENT);
            status.addToError_msgs("options must be set");
            response.setStatus(status);
            return response;
        }
        TListSessionsOptions options = request.options;
        if (options.isSetTemporary_table_only() && options.temporary_table_only) {
            TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
            Set<UUID> sessions = ExecuteEnv.getInstance().getScheduler().listAllSessionsId();
            sessions.retainAll(temporaryTableMgr.listSessions().keySet());
            List<TSessionInfo> sessionInfos = new ArrayList<>();
            for (UUID session : sessions) {
                TSessionInfo sessionInfo = new TSessionInfo();
                sessionInfo.setSession_id(session.toString());
                sessionInfos.add(sessionInfo);
            }
            response.setStatus(new TStatus(TStatusCode.OK));
            response.setSessions(sessionInfos);
        } else {
            TStatus status = new TStatus(NOT_IMPLEMENTED_ERROR);
            status.addToError_msgs("only support temporary_table_only options now");
            response.setStatus(status);
        }
        return response;
    }

    @Override
    public TGetTemporaryTablesInfoResponse getTemporaryTablesInfo(TGetTemporaryTablesInfoRequest request)
            throws TException {
        return InformationSchemaDataSource.generateTemporaryTablesInfoResponse(request);
    }

    @Override
    public TReportFragmentFinishResponse reportFragmentFinish(TReportFragmentFinishParams request) throws TException {
        return QeProcessorImpl.INSTANCE.reportFragmentFinish(request);
    }

    @Override
    public TStartCheckpointResponse startCheckpoint(TStartCheckpointRequest request) throws TException {
        CheckpointWorker worker;
        if (request.is_global_state_mgr) {
            worker = GlobalStateMgr.getCurrentState().getCheckpointWorker();
        } else {
            worker = StarMgrServer.getCurrentState().getCheckpointWorker();
        }

        TStartCheckpointResponse response = new TStartCheckpointResponse();
        try {
            worker.setNextCheckpoint(request.getEpoch(), request.getJournal_id());
            response.setStatus(new TStatus(OK));
            return response;
        } catch (CheckpointException e) {
            LOG.warn("set next checkpoint failed", e);
            TStatus status = new TStatus(TStatusCode.CANCELLED);
            status.addToError_msgs(e.getMessage());
            response.setStatus(status);
            return response;
        }
    }

    @Override
    public TFinishCheckpointResponse finishCheckpoint(TFinishCheckpointRequest request) throws TException {
        TFinishCheckpointResponse response = new TFinishCheckpointResponse();
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            TStatus status = new TStatus(TStatusCode.CANCELLED);
            status.addToError_msgs("current node is not leader");
            response.setStatus(status);
            return response;
        }

        CheckpointController controller;
        if (request.is_global_state_mgr) {
            controller = GlobalStateMgr.getCurrentState().getCheckpointController();
        } else {
            controller = StarMgrServer.getCurrentState().getCheckpointController();
        }

        try {
            controller.finishCheckpoint(request.getJournal_id(), request.getNode_name());
            response.setStatus(new TStatus(OK));
            return response;
        } catch (CheckpointException e) {
            LOG.warn("finishCheckpoint failed", e);
            TStatus status = new TStatus(TStatusCode.CANCELLED);
            status.addToError_msgs(e.getMessage());
            response.setStatus(status);
            return response;
        }
    }

    @Override
    public TPartitionMetaResponse getPartitionMeta(TPartitionMetaRequest request) throws TException {
        TPartitionMetaResponse response = new TPartitionMetaResponse();
        if (!request.isSetTablet_ids() || request.getTablet_ids().isEmpty()) {
            String errMsg = "Invalid parameter from getPartitionMeta request, tablet_ids is required";
            LOG.info(errMsg);
            TStatus status = new TStatus(TStatusCode.INVALID_ARGUMENT);
            status.addToError_msgs(errMsg);
            response.setStatus(status);
            return response;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        TabletInvertedIndex invertedIndex = globalStateMgr.getTabletInvertedIndex();

        Map<Long, TabletMeta> tabletMetas = new HashMap<>();
        List<Long> tabletIds = request.getTablet_ids();
        tabletIds.forEach(id -> tabletMetas.put(id, invertedIndex.getTabletMeta(id)));
        // build a list of the partitionMeta from the tabletMetaList
        List<TPartitionMeta> partitionMetaList = getPartitionMetaImpl(tabletMetas.values());
        if (partitionMetaList.isEmpty()) {
            TStatus status = new TStatus(TStatusCode.NOT_FOUND);
            response.setStatus(status);
            return response;
        }

        // build the index for partitionId -> offset of partitionMeta array
        Map<Long, Integer> partitionId2ArrayIndex = new HashMap<>();
        for (int i = 0; i < partitionMetaList.size(); ++i) {
            partitionId2ArrayIndex.put(partitionMetaList.get(i).getPartition_id(), i);
        }
        // build tabletId -> offset of partitionMeta array
        Map<Long, Integer> tabletIdMetaIndex = new HashMap<>();
        tabletMetas.forEach((key, value) -> {
            if (value != null) {
                long phyPartitionId = value.getPhysicalPartitionId();
                if (partitionId2ArrayIndex.containsKey(phyPartitionId)) {
                    tabletIdMetaIndex.put(key, partitionId2ArrayIndex.get(phyPartitionId));
                }
            }
        });
        response.setTablet_id_partition_meta_index(tabletIdMetaIndex);
        response.setPartition_metas(partitionMetaList);
        response.setStatus(new TStatus(OK));
        return response;
    }

    static List<TPartitionMeta> getPartitionMetaImpl(Collection<TabletMeta> tabletMetas) {
        List<TPartitionMeta> result = new ArrayList<>();
        Set<Long> donePartitionIds = new HashSet<>();
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        for (TabletMeta tabletMeta : tabletMetas) {
            if (tabletMeta == null) {
                continue;
            }
            long partitionId = tabletMeta.getPhysicalPartitionId();
            if (donePartitionIds.contains(partitionId)) {
                continue;
            }
            long dbId = tabletMeta.getDbId();
            long tableId = tabletMeta.getTableId();

            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(dbId, tableId, LockType.READ);
            try {
                Database db = metastore.getDb(dbId);
                if (db == null) {
                    donePartitionIds.add(partitionId);
                    continue;
                }
                Table table = db.getTable(tableId);
                if (table == null) {
                    donePartitionIds.add(partitionId);
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
                if (physicalPartition == null) {
                    donePartitionIds.add(partitionId);
                    continue;
                }
                long parentPartitionId = physicalPartition.getParentId();
                Partition partition = olapTable.getPartition(parentPartitionId);
                if (partition == null) {
                    donePartitionIds.add(partitionId);
                    continue;
                }
                TPartitionMeta partitionMeta = new TPartitionMeta();
                partitionMeta.setPartition_id(partitionId);
                partitionMeta.setPartition_name(physicalPartition.getName());
                partitionMeta.setState(partition.getState().name());
                partitionMeta.setVisible_version(physicalPartition.getVisibleVersion());
                partitionMeta.setVisible_time(physicalPartition.getVisibleVersionTime());
                partitionMeta.setNext_version(physicalPartition.getNextVersion());
                partitionMeta.setIs_temp(olapTable.isTempPartition(parentPartitionId));
                result.add(partitionMeta);
                donePartitionIds.add(partitionId);
            } finally {
                locker.unLockTableWithIntensiveDbLock(dbId, tableId, LockType.READ);
            }
        }
        return result;
    }

    @Override
    public TClusterSnapshotsResponse getClusterSnapshotsInfo(TClusterSnapshotsRequest params) {
        return GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotsInfo();
    }

    @Override
    public TClusterSnapshotJobsResponse getClusterSnapshotJobsInfo(TClusterSnapshotJobsRequest params) {
        return GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo();
    }
}
