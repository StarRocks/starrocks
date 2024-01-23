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
import com.starrocks.authentication.AuthenticationMgr;
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
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.sys.GrantsTo;
import com.starrocks.catalog.system.sys.RoleEdges;
import com.starrocks.catalog.system.sys.SysFeLocks;
import com.starrocks.catalog.system.sys.SysObjectDependencies;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.AuthenticationException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServerContext;
import com.starrocks.common.ThriftServerEventProcessor;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.BaseAction;
import com.starrocks.http.rest.TransactionResult;
import com.starrocks.lake.LakeTablet;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.load.EtlJobType;
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
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.DbPrivEntry;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.mysql.privilege.TablePrivEntry;
import com.starrocks.mysql.privilege.UserPrivTable;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowMaterializedViewStatus;
import com.starrocks.qe.VariableMgr;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TAllocateAutoIncrementIdParam;
import com.starrocks.thrift.TAllocateAutoIncrementIdResult;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TBatchReportExecStatusParams;
import com.starrocks.thrift.TBatchReportExecStatusResult;
import com.starrocks.thrift.TBeginRemoteTxnRequest;
import com.starrocks.thrift.TBeginRemoteTxnResponse;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TColumnDesc;
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
import com.starrocks.thrift.TFeResult;
import com.starrocks.thrift.TFetchResourceResult;
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
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
import com.starrocks.thrift.TGetLoadsParams;
import com.starrocks.thrift.TGetLoadsResult;
import com.starrocks.thrift.TGetProfileRequest;
import com.starrocks.thrift.TGetProfileResponse;
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
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNodesInfo;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TOlapTableIndexTablets;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportAuditStatisticsResult;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TRoutineLoadJobInfo;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TShowVariableRequest;
import com.starrocks.thrift.TShowVariableResult;
import com.starrocks.thrift.TSnapshotLoaderReportRequest;
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
import com.starrocks.thrift.TTaskRunInfo;
import com.starrocks.thrift.TTrackingLoadInfo;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUpdateExportTaskStatusRequest;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUpdateResourceUsageResponse;
import com.starrocks.thrift.TUserPrivDesc;
import com.starrocks.thrift.TVerboseVariableRecord;
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import com.starrocks.transaction.TransactionNotFoundException;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TStatusCode.NOT_IMPLEMENTED_ERROR;
import static com.starrocks.thrift.TStatusCode.OK;
import static com.starrocks.thrift.TStatusCode.RUNTIME_ERROR;
import static com.starrocks.thrift.TStatusCode.SERVICE_UNAVAILABLE;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(LeaderImpl.class);
    private final LeaderImpl leaderImpl;
    private final ExecuteEnv exeEnv;
    public AtomicLong partitionRequestNum = new AtomicLong(0);

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        leaderImpl = new LeaderImpl();
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
                Authorizer.checkAnyActionOnOrInDb(currentUser, null, catalogName, fullName);
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
                    LOG.warn(e.getMessage());
                }

                if (tbl == null) {
                    continue;
                }

                try {
                    Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                            null, params.db, tbl);
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

        Database db = GlobalStateMgr.getCurrentState().getDb(params.db);
        long limit = params.isSetLimit() ? params.getLimit() : -1;
        UserIdentity currentUser;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                boolean listingViews = params.isSetType() && TTableType.VIEW.equals(params.getType());
                List<Table> tables = listingViews ? db.getViews() : db.getTables();
                OUTER:
                for (Table table : tables) {
                    try {
                        Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                                null, params.db, table);
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

                        ConnectContext connectContext = new ConnectContext();
                        connectContext.setQualifiedUser(AuthenticationMgr.ROOT_USER);
                        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
                        connectContext.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

                        try {
                            List<TableName> allTables = view.getTableRefs();
                            for (TableName tableName : allTables) {
                                Table tbl = db.getTable(tableName.getTbl());
                                if (tbl != null) {
                                    try {
                                        Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                                                null, tableName.getDb(), tbl);
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
                locker.unLockDatabase(db, LockType.READ);
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
            String databaseName = GlobalStateMgr.getCurrentState().mayGetDb(pipe.getPipeId().getDbId())
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
                                    GlobalStateMgr.getCurrentState().mayGetDb(p.getDbAndName().first)
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

    // list MaterializedView table match pattern
    private TListMaterializedViewStatusResult listMaterializedViewStatus(long limit, PatternMatcher matcher,
                                                                         UserIdentity currentUser, TGetTablesParams params) {
        TListMaterializedViewStatusResult result = new TListMaterializedViewStatusResult();
        List<TMaterializedViewStatus> tablesResult = Lists.newArrayList();
        result.setMaterialized_views(tablesResult);
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            LOG.warn("database not exists: {}", dbName);
            return result;
        }

        listMaterializedViews(limit, matcher, currentUser, params).stream()
                .map(s -> s.toThrift())
                .forEach(t -> tablesResult.add(t));
        return result;
    }

    private List<ShowMaterializedViewStatus> listMaterializedViews(long limit, PatternMatcher matcher,
                                                                   UserIdentity currentUser, TGetTablesParams params) {
        String dbName = params.getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        List<MaterializedView> materializedViews = Lists.newArrayList();
        List<Pair<OlapTable, MaterializedIndexMeta>> singleTableMVs = Lists.newArrayList();
        boolean caseSensitive = CaseSensibility.TABLE.getCaseSensibility();
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            for (Table table : db.getTables()) {
                if (table.isMaterializedView()) {
                    MaterializedView mvTable = (MaterializedView) table;
                    try {
                        Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                                null, dbName, mvTable);
                    } catch (AccessDeniedException e) {
                        continue;
                    }

                    if (!PatternMatcher.matchPattern(params.getPattern(), mvTable.getName(), matcher, caseSensitive)) {
                        continue;
                    }

                    materializedViews.add(mvTable);
                } else if (table.getType() == Table.TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) table;
                    // synchronized materialized view metadata size should be greater than 1.
                    if (olapTable.getVisibleIndexMetas().size() <= 1) {
                        continue;
                    }
                    List<MaterializedIndexMeta> visibleMaterializedViews = olapTable.getVisibleIndexMetas();
                    long baseIdx = olapTable.getBaseIndexId();
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

                // check limit
                int mvSize = materializedViews.size() + singleTableMVs.size();
                if (limit > 0 && mvSize >= limit) {
                    break;
                }
            }
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        return ShowExecutor.listMaterializedViewStatus(dbName, materializedViews, singleTableMVs);
    }

    @Override
    public TGetTaskInfoResult getTasks(TGetTasksParams params) throws TException {
        LOG.debug("get show task request: {}", params);
        TGetTaskInfoResult result = new TGetTaskInfoResult();
        List<TTaskInfo> tasksResult = Lists.newArrayList();
        result.setTasks(tasksResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        TaskManager taskManager = globalStateMgr.getTaskManager();
        List<Task> taskList = taskManager.showTasks(null);

        for (Task task : taskList) {
            if (task.getDbName() == null) {
                LOG.warn("Ignore the task db because information is incorrect: " + task);
                continue;
            }

            try {
                Authorizer.checkAnyActionOnOrInDb(currentUser, null, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        task.getDbName());
            } catch (AccessDeniedException e) {
                continue;
            }

            TTaskInfo info = new TTaskInfo();
            info.setTask_name(task.getName());
            info.setCreate_time(task.getCreateTime() / 1000);
            String scheduleStr = "UNKNOWN";
            if (task.getType() != null) {
                scheduleStr = task.getType().name();
            }
            if (task.getType() == Constants.TaskType.PERIODICAL) {
                scheduleStr += task.getSchedule();
            }
            info.setSchedule(scheduleStr);
            info.setDatabase(ClusterNamespace.getNameFromFullName(task.getDbName()));
            info.setDefinition(task.getDefinition());
            info.setExpire_time(task.getExpireTime() / 1000);
            info.setProperties(task.getPropertiesString());
            tasksResult.add(info);
        }

        return result;
    }

    @Override
    public TGetTaskRunInfoResult getTaskRuns(TGetTasksParams params) throws TException {
        LOG.debug("get show task run request: {}", params);
        TGetTaskRunInfoResult result = new TGetTaskRunInfoResult();
        List<TTaskRunInfo> tasksResult = Lists.newArrayList();
        result.setTask_runs(tasksResult);

        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        TaskManager taskManager = globalStateMgr.getTaskManager();
        List<TaskRunStatus> taskRunList = taskManager.showTaskRunStatus(null);

        for (TaskRunStatus status : taskRunList) {
            if (status.getDbName() == null) {
                LOG.warn("Ignore the task status because db information is incorrect: " + status);
                continue;
            }

            try {
                Authorizer.checkAnyActionOnOrInDb(currentUser, null, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        status.getDbName());
            } catch (AccessDeniedException e) {
                continue;
            }

            String taskName = status.getTaskName();
            TTaskRunInfo info = new TTaskRunInfo();
            info.setQuery_id(status.getQueryId());
            info.setTask_name(taskName);
            info.setCreate_time(status.getCreateTime() / 1000);
            info.setFinish_time(status.getFinishTime() / 1000);
            info.setState(status.getState().toString());
            info.setDatabase(ClusterNamespace.getNameFromFullName(status.getDbName()));
            try {
                // NOTE: use task's definition to display task-run's definition here
                Task task = taskManager.getTaskWithoutLock(taskName);
                if (task != null) {
                    info.setDefinition(task.getDefinition());
                }
            } catch (Exception e) {
                LOG.warn("Get taskName {} definition failed: {}", taskName, e);
            }
            info.setError_code(status.getErrorCode());
            info.setError_message(status.getErrorMessage());
            info.setExpire_time(status.getExpireTime() / 1000);
            info.setProgress(status.getProgress() + "%");
            info.setExtra_message(status.getExtraMessage());
            info.setProperties(status.getPropertiesJson());
            tasksResult.add(info);
        }
        return result;
    }

    @Override
    public TGetDBPrivsResult getDBPrivs(TGetDBPrivsParams params) throws TException {
        LOG.debug("get database privileges request: {}", params);
        TGetDBPrivsResult result = new TGetDBPrivsResult();
        List<TDBPrivDesc> tDBPrivs = Lists.newArrayList();
        result.setDb_privs(tDBPrivs);
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            // TODO(yiming): support showing user privilege info in information_schema later
            return result;
        }

        UserIdentity currentUser = UserIdentity.fromThrift(params.current_user_ident);
        List<DbPrivEntry> dbPrivEntries = GlobalStateMgr.getCurrentState().getAuth().getDBPrivEntries(currentUser);
        // flatten privileges
        for (DbPrivEntry entry : dbPrivEntries) {
            PrivBitSet savedPrivs = entry.getPrivSet();
            String clusterPrefix = SystemInfoService.DEFAULT_CLUSTER + ClusterNamespace.CLUSTER_DELIMITER;
            String userIdentStr = currentUser.toString().replace(clusterPrefix, "");
            String dbName = entry.getOrigDb();
            boolean isGrantable = savedPrivs.satisfy(PrivPredicate.GRANT);
            List<TDBPrivDesc> tPrivs = savedPrivs.toPrivilegeList().stream().map(
                    priv -> {
                        TDBPrivDesc privDesc = new TDBPrivDesc();
                        privDesc.setDb_name(dbName);
                        privDesc.setIs_grantable(isGrantable);
                        privDesc.setUser_ident_str(userIdentStr);
                        privDesc.setPriv(priv.getUpperNameForMysql());
                        return privDesc;
                    }
            ).collect(Collectors.toList());
            if (savedPrivs.satisfy(PrivPredicate.LOAD)) {
                // add `INSERT` `UPDATE` and `DELETE` to adapt `Aliyun DTS`
                tPrivs.addAll(Lists.newArrayList("INSERT", "UPDATE", "DELETE").stream().map(priv -> {
                    TDBPrivDesc privDesc = new TDBPrivDesc();
                    privDesc.setDb_name(dbName);
                    privDesc.setIs_grantable(isGrantable);
                    privDesc.setUser_ident_str(userIdentStr);
                    privDesc.setPriv(priv);
                    return privDesc;
                }).collect(Collectors.toList()));
            }
            tDBPrivs.addAll(tPrivs);
        }
        return result;
    }

    @Override
    public TGetTablePrivsResult getTablePrivs(TGetTablePrivsParams params) throws TException {
        LOG.debug("get table privileges request: {}", params);
        TGetTablePrivsResult result = new TGetTablePrivsResult();
        List<TTablePrivDesc> tTablePrivs = Lists.newArrayList();
        result.setTable_privs(tTablePrivs);
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            // TODO(yiming): support showing user privilege info in information_schema later
            return result;
        }

        UserIdentity currentUser = UserIdentity.fromThrift(params.current_user_ident);
        List<TablePrivEntry> tablePrivEntries =
                GlobalStateMgr.getCurrentState().getAuth().getTablePrivEntries(currentUser);
        // flatten privileges
        for (TablePrivEntry entry : tablePrivEntries) {
            PrivBitSet savedPrivs = entry.getPrivSet();
            String clusterPrefix = SystemInfoService.DEFAULT_CLUSTER + ClusterNamespace.CLUSTER_DELIMITER;
            String userIdentStr = currentUser.toString().replace(clusterPrefix, "");
            String dbName = entry.getOrigDb();
            boolean isGrantable = savedPrivs.satisfy(PrivPredicate.GRANT);
            List<TTablePrivDesc> tPrivs = savedPrivs.toPrivilegeList().stream().map(
                    priv -> {
                        TTablePrivDesc privDesc = new TTablePrivDesc();
                        privDesc.setDb_name(dbName);
                        privDesc.setTable_name(entry.getOrigTbl());
                        privDesc.setIs_grantable(isGrantable);
                        privDesc.setUser_ident_str(userIdentStr);
                        privDesc.setPriv(priv.getUpperNameForMysql());
                        return privDesc;
                    }
            ).collect(Collectors.toList());
            if (savedPrivs.satisfy(PrivPredicate.LOAD)) {
                // add `INSERT` `UPDATE` and `DELETE` to adapt `Aliyun DTS`
                tPrivs.addAll(Lists.newArrayList("INSERT", "UPDATE", "DELETE").stream().map(priv -> {
                    TTablePrivDesc privDesc = new TTablePrivDesc();
                    privDesc.setDb_name(dbName);
                    privDesc.setTable_name(entry.getOrigTbl());
                    privDesc.setIs_grantable(isGrantable);
                    privDesc.setUser_ident_str(userIdentStr);
                    privDesc.setPriv(priv);
                    return privDesc;
                }).collect(Collectors.toList()));
            }
            tTablePrivs.addAll(tPrivs);
        }
        return result;
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
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            // TODO(yiming): support showing user privilege info in information_schema later
            return result;
        }

        UserIdentity currentUser = UserIdentity.fromThrift(params.current_user_ident);
        Auth currAuth = GlobalStateMgr.getCurrentState().getAuth();
        UserPrivTable userPrivTable = currAuth.getUserPrivTable();
        List<UserIdentity> userIdents = Lists.newArrayList();
        // users can only see the privileges of themselves at this moment
        userIdents.add(currentUser);

        // TODO: users with super privilege can view all the privileges like below
        // if (!userPrivTable.hasPriv(currentUser.getHost(), currentUser.getQualifiedUser(), PrivPredicate.GRANT)) {
        //     // user who doesn't have GRANT privilege could only see the privilege of it self
        //     userIdents.add(currentUser);
        // } else {
        //     // user who has GRANT privilege will get all user privileges
        //     userIdents = Lists.newArrayList(currAuth.getAllUserIdents(false /* get all user */));
        // }

        // fullfill the result
        for (UserIdentity userIdent : userIdents) {
            PrivBitSet savedPrivs = new PrivBitSet();
            userPrivTable.getPrivs(userIdent, savedPrivs);
            String clusterPrefix = SystemInfoService.DEFAULT_CLUSTER + ClusterNamespace.CLUSTER_DELIMITER;
            String userIdentStr = currentUser.toString().replace(clusterPrefix, "");
            // flatten privileges
            List<TUserPrivDesc> tPrivs = savedPrivs.toPrivilegeList().stream().map(
                    priv -> {
                        boolean isGrantable =
                                Privilege.NODE_PRIV != priv // NODE_PRIV counld not be granted event with GRANT_PRIV
                                        && userPrivTable.hasPriv(userIdent,
                                        PrivPredicate.GRANT);
                        TUserPrivDesc privDesc = new TUserPrivDesc();
                        privDesc.setIs_grantable(isGrantable);
                        privDesc.setUser_ident_str(userIdentStr);
                        privDesc.setPriv(priv.getUpperNameForMysql());
                        return privDesc;
                    }
            ).collect(Collectors.toList());
            tUserPrivs.addAll(tPrivs);
        }
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
                locker.lockDatabase(db, LockType.READ);
                Table table = metadataMgr.getTable(catalogName, params.db, params.table_name);
                if (table == null) {
                    return result;
                }
                try {
                    Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                            null, params.db, table);
                } catch (AccessDeniedException e) {
                    return result;
                }
                setColumnDesc(columns, table, limit, false, params.db, params.table_name);
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        return result;
    }

    // get describeTable without db name and table name parameter, so we need iterate over
    // dbs and tables, when reach limit, we break;
    private void describeWithoutDbAndTable(UserIdentity currentUser, List<TColumnDef> columns, long limit) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getDbNames();
        boolean reachLimit;
        for (String fullName : dbNames) {
            try {
                Authorizer.checkAnyActionOnOrInDb(currentUser, null,
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, fullName);
            } catch (AccessDeniedException e) {
                continue;
            }
            Database db = GlobalStateMgr.getCurrentState().getDb(fullName);
            if (db != null) {
                Locker locker = new Locker();
                for (String tableName : db.getTableNamesViewWithLock()) {
                    try {
                        locker.lockDatabase(db, LockType.READ);
                        Table table = db.getTable(tableName);
                        if (table == null) {
                            continue;
                        }

                        try {
                            Authorizer.checkAnyActionOnTableLikeObject(currentUser,
                                    null, fullName, table);
                        } catch (AccessDeniedException e) {
                            continue;
                        }

                        reachLimit = setColumnDesc(columns, table, limit, true, fullName, tableName);
                    } finally {
                        locker.unLockDatabase(db, LockType.READ);
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
        List<List<String>> rows = VariableMgr.dump(setType, ctx.getSessionVariable(),
                null);
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
            Frontend fe = GlobalStateMgr.getCurrentState().getFeByHost(clientAddr.getHostname());
            if (fe == null) {
                LOG.warn("reject request from invalid host. client: {}", clientAddr);
                throw new TException("request from invalid host was rejected.");
            }
        }

        // add this log so that we can track this stmt
        LOG.info("receive forwarded stmt {} from FE: {}",
                params.getStmt_id(), clientAddr != null ? clientAddr.getHostname() : "unknown");
        ConnectContext context = new ConnectContext(null);
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        ConnectContext.remove();
        return result;
    }

    private void checkPasswordAndLoadPriv(String user, String passwd, String db, String tbl,
                                          String clientIp) throws AuthenticationException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        UserIdentity currentUser =
                globalStateMgr.getAuthenticationMgr().checkPlainPassword(user, clientIp, passwd);
        if (currentUser == null) {
            throw new AuthenticationException("Access denied for " + user + "@" + clientIp);
        }
        // check INSERT action on table
        try {
            Authorizer.checkTableAction(currentUser, null, db, tbl, PrivilegeType.INSERT);
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
        } catch (UserException e) {
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

    private long loadTxnBeginImpl(TLoadTxnBeginRequest request, String clientIp) throws UserException {
        checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                request.getTbl(), request.getUser_ip());

        // check txn
        long limit = Config.stream_load_max_txn_num_per_be;
        if (limit >= 0) {
            long txnNumBe = GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionNumByCoordinateBe(clientIp);
            LOG.info("streamload check txn num, be: {}, txn_num: {}, limit: {}", clientIp, txnNumBe, limit);
            if (txnNumBe >= limit) {
                throw new UserException("streamload txn num per be exceeds limit, be: "
                        + clientIp + ", txn_num: " + txnNumBe + ", limit: " + limit);
            }
        }
        // check label
        if (Strings.isNullOrEmpty(request.getLabel())) {
            throw new UserException("empty label in begin request");
        }
        // check database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new UserException("unknown database, database=" + dbName);
        }
        Table table = db.getTable(request.getTbl());
        if (table == null) {
            throw new UserException("unknown table \"" + request.getDb() + "." + request.getTbl() + "\"");
        }

        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);

        // just use default value of session variable
        // as there is no connectContext for sync stream load
        ConnectContext connectContext = new ConnectContext();
        if (connectContext.getSessionVariable().isEnableLoadProfile()) {
            TransactionResult resp = new TransactionResult();
            StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();
            streamLoadManager.beginLoadTask(dbName, table.getName(), request.getLabel(), timeoutSecond * 1000, resp, false);
            if (!resp.stateOK()) {
                LOG.warn(resp.msg);
                throw new UserException(resp.msg);
            }

            StreamLoadTask task = streamLoadManager.getTaskByLabel(request.getLabel());
            // this should't open
            if (task == null || task.getTxnId() == -1) {
                throw new UserException(String.format("Load label: {} begin transacton failed", request.getLabel()));
            }
            return task.getTxnId();
        }

        return GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(
                db.getId(), Lists.newArrayList(table.getId()), request.getLabel(), request.getRequest_id(),
                new TxnCoordinator(TxnSourceType.BE, clientIp),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, timeoutSecond);
    }

    @Override
    public TLoadTxnCommitResult loadTxnCommit(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive txn commit request. db: {}, tbl: {}, txn_id: {}, backend: {}",
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
        } catch (UserException e) {
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
    void loadTxnCommitImpl(TLoadTxnCommitRequest request, TStatus status) throws UserException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }

        // get database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new UserException("unknown database, database=" + dbName);
        }
        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;

        // Make publish timeout is less than thrift_rpc_timeout_ms
        // Otherwise, the publish process will be successful but commit timeout in BE
        // It will result in error like "call frontend service failed"
        timeoutMs = timeoutMs * 3 / 4;
        boolean ret = GlobalStateMgr.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                db, request.getTxnId(),
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                TabletFailInfo.fromThrift(request.getFailInfos()),
                timeoutMs, attachment);
        if (!ret) {
            // committed success but not visible
            status.setStatus_code(TStatusCode.PUBLISH_TIMEOUT);
            String timeoutInfo = GlobalStateMgr.getCurrentGlobalTransactionMgr()
                    .getTxnPublishTimeoutDebugInfo(db.getId(), request.getTxnId());
            LOG.warn("txn {} publish timeout {}", request.getTxnId(), timeoutInfo);
            if (timeoutInfo.length() > 240) {
                timeoutInfo = timeoutInfo.substring(0, 240) + "...";
            }
            status.addToError_msgs("Publish timeout. The data will be visible after a while" + timeoutInfo);
            return;
        }
        // if commit and publish is success, load can be regarded as success
        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        if (null == attachment) {
            return;
        }
        // collect table-level metrics
        Table tbl = db.getTable(request.getTbl());
        if (null == tbl) {
            return;
        }
        TableMetricsEntity entity = TableMetricsRegistry.getInstance().getMetricsEntity(tbl.getId());
        StreamLoadTask streamLoadtask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                getSyncSteamLoadTaskByTxnId(request.getTxnId());

        switch (request.txnCommitAttachment.getLoadType()) {
            case ROUTINE_LOAD:
                if (!(attachment instanceof RLTaskTxnCommitAttachment)) {
                    break;
                }
                RLTaskTxnCommitAttachment routineAttachment = (RLTaskTxnCommitAttachment) attachment;
                entity.counterRoutineLoadFinishedTotal.increase(1L);
                entity.counterRoutineLoadBytesTotal.increase(routineAttachment.getReceivedBytes());
                entity.counterRoutineLoadRowsTotal.increase(routineAttachment.getLoadedRows());
                entity.counterRoutineLoadErrorRowsTotal.increase(routineAttachment.getFilteredRows());
                entity.counterRoutineLoadUnselectedRowsTotal.increase(routineAttachment.getUnselectedRows());

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(routineAttachment.getLoadedBytes(),
                            routineAttachment.getLoadedRows(),
                            routineAttachment.getFilteredRows(),
                            routineAttachment.getUnselectedRows(),
                            routineAttachment.getErrorLogUrl(), "");
                }

                break;
            case MANUAL_LOAD:
                if (!(attachment instanceof ManualLoadTxnCommitAttachment)) {
                    break;
                }
                ManualLoadTxnCommitAttachment streamAttachment = (ManualLoadTxnCommitAttachment) attachment;
                entity.counterStreamLoadFinishedTotal.increase(1L);
                entity.counterStreamLoadBytesTotal.increase(streamAttachment.getReceivedBytes());
                entity.counterStreamLoadRowsTotal.increase(streamAttachment.getLoadedRows());

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(streamAttachment.getLoadedBytes(),
                            streamAttachment.getLoadedRows(),
                            streamAttachment.getFilteredRows(),
                            streamAttachment.getUnselectedRows(),
                            streamAttachment.getErrorLogUrl(), "");
                }

                break;
            default:
                break;
        }
    }

    @Override
    public TGetLoadTxnStatusResult getLoadTxnStatus(TGetLoadTxnStatusRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive get txn status request. db: {}, tbl: {}, txn_id: {}, backend: {}",
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
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            LOG.warn("unknown database, database=" + dbName);
            result.setStatus(TTransactionStatus.UNKNOWN);
            return result;
        }

        try {
            TTransactionStatus status = GlobalStateMgr.getCurrentGlobalTransactionMgr().getTxnStatus(db, request.getTxnId());
            LOG.debug("txn {} status is {}", request.getTxnId(), status);
            result.setStatus(status);
        } catch (Throwable e) {
            result.setStatus(TTransactionStatus.UNKNOWN);
            LOG.warn("catch unknown result.", e);
        }
        return result;
    }

    @Override
    public TLoadTxnCommitResult loadTxnPrepare(TLoadTxnCommitRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive txn prepare request. db: {}, tbl: {}, txn_id: {}, backend: {}",
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
        } catch (UserException e) {
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

    private void loadTxnPrepareImpl(TLoadTxnCommitRequest request) throws UserException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }

        // get database
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new UserException("unknown database, database=" + dbName);
        }

        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        GlobalStateMgr.getCurrentGlobalTransactionMgr().prepareTransaction(
                db.getId(), request.getTxnId(),
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                TabletFailInfo.fromThrift(request.getFailInfos()),
                attachment);

    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive txn rollback request. db: {}, tbl: {}, txn_id: {}, reason: {}, backend: {}",
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
        } catch (UserException e) {
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

    private void loadTxnRollbackImpl(TLoadTxnRollbackRequest request) throws UserException {
        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndLoadPriv(request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip());
        }
        String dbName = request.getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbName + " does not exist");
        }
        long dbId = db.getId();
        GlobalStateMgr.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
                request.isSetReason() ? request.getReason() : "system cancel",
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
                    streamLoadtask.setLoadState(routineAttachment.getLoadedBytes(),
                            routineAttachment.getLoadedRows(),
                            routineAttachment.getFilteredRows(),
                            routineAttachment.getUnselectedRows(),
                            routineAttachment.getErrorLogUrl(), request.getReason());

                }

                break;
            case MANUAL_LOAD:
                if (!(attachment instanceof ManualLoadTxnCommitAttachment)) {
                    break;
                }
                ManualLoadTxnCommitAttachment streamAttachment = (ManualLoadTxnCommitAttachment) attachment;

                if (streamLoadtask != null) {
                    streamLoadtask.setLoadState(streamAttachment.getLoadedBytes(),
                            streamAttachment.getLoadedRows(),
                            streamAttachment.getFilteredRows(),
                            streamAttachment.getUnselectedRows(),
                            streamAttachment.getErrorLogUrl(), request.getReason());
                }

                break;
            default:
                break;
        }
    }

    @Override
    public TStreamLoadPutResult streamLoadPut(TStreamLoadPutRequest request) {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive stream load put request. db:{}, tbl: {}, txn_id: {}, load id: {}, backend: {}",
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
        } catch (UserException | StarRocksPlannerException e) {
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

    TExecPlanFragmentParams streamLoadPutImpl(TStreamLoadPutRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        String dbName = request.getDb();
        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new UserException("unknown database, database=" + dbName);
        }

        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;
        // Make timeout less than thrift_rpc_timeout_ms.
        // Otherwise, it will result in error like "call frontend service failed"
        timeoutMs = timeoutMs * 3 / 4;

        Locker locker = new Locker();
        if (!locker.tryLockDatabase(db, LockType.READ, timeoutMs)) {
            throw new LockTimeoutException(
                    "get database read lock timeout, database=" + dbName + ", timeout=" + timeoutMs + "ms");
        }
        try {
            Table table = db.getTable(request.getTbl());
            if (table == null) {
                throw new UserException("unknown table, table=" + request.getTbl());
            }
            if (!(table instanceof OlapTable)) {
                throw new UserException("load table type is not OlapTable, type=" + table.getClass());
            }
            if (table instanceof MaterializedView) {
                throw new UserException(String.format(
                        "The data of '%s' cannot be inserted because '%s' is a materialized view," +
                                "and the data of materialized view must be consistent with the base table.",
                        table.getName(), table.getName()));
            }
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromTStreamLoadPutRequest(request, db);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadInfo);
            TExecPlanFragmentParams plan = planner.plan(streamLoadInfo.getId());

            if (plan.query_options.enable_profile) {
                StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().getStreamLoadMgr().
                        getSyncSteamLoadTaskByTxnId(request.getTxnId());
                if (streamLoadTask == null) {
                    throw new UserException("can not find stream load task by txnId " + request.getTxnId());
                }

                streamLoadTask.setTUniqueId(request.getLoadId());

                Coordinator coord = getCoordinatorFactory().createSyncStreamLoadScheduler(planner, getClientAddr());
                streamLoadTask.setCoordinator(coord);

                QeProcessorImpl.INSTANCE.registerQuery(streamLoadInfo.getId(), coord);
            }

            plan.query_options.setLoad_job_type(TLoadJobType.STREAM_LOAD);
            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes((OlapTable) table);
            plan.setImport_label(txnState.getLabel());
            plan.setDb_name(dbName);
            plan.setLoad_job_id(request.getTxnId());

            return plan;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
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

    private TNetworkAddress getClientAddr() {
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
                Authorizer.checkTableAction(userIdentity, null, dbName, tableName, PrivilegeType.INSERT);
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

            GlobalStateMgr.getCurrentState().setFrontendConfig(configs);
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
            nextId = GlobalStateMgr.getCurrentState().allocateAutoIncrementId(request.table_id, rows);
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
        LOG.info("Receive update immutable partition: {}", request);

        TImmutablePartitionResult result;
        try {
            result = updateImmutablePartitionInternal(request);
        } catch (Throwable t) {
            LOG.warn(t);
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
            throws UserException {
        long dbId = request.getDb_id();
        long tableId = request.getTable_id();
        TImmutablePartitionResult result = new TImmutablePartitionResult();
        TStatus errorStatus = new TStatus(RUNTIME_ERROR);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            errorStatus.setError_msgs(
                    Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            return result;
        }
        Locker locker = new Locker();
        Table table = db.getTable(tableId);
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

            List<PhysicalPartition> mutablePartitions = Lists.newArrayList();
            try {
                locker.lockDatabase(db, LockType.READ);
                mutablePartitions = partition.getSubPartitions().stream()
                        .filter(physicalPartition -> !physicalPartition.isImmutable())
                        .collect(Collectors.toList());
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
            if (mutablePartitions.size() <= 1) {
                GlobalStateMgr.getCurrentState().addSubPartitions(db, olapTable.getName(), partition, 1);
            }
            p.setImmutable(true);
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
                locker.lockDatabase(db, LockType.READ);
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
                    buildTablets(physicalPartition, tablets, olapTable);
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
        result.setPartitions(partitions);
        result.setTablets(tablets);

        // build nodes
        TNodesInfo nodesInfo = GlobalStateMgr.getCurrentState().createNodesInfo(olapTable.getClusterId());
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
            int partColNum = rangePartitionInfo.getPartitionColumns().size();
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
                                     OlapTable olapTable) throws UserException {
        int quorum = olapTable.getPartitionInfo().getQuorumNum(physicalPartition.getParentId(), olapTable.writeQuorum());
        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(
                MaterializedIndex.IndexExtState.ALL)) {
            if (olapTable.isCloudNativeTable()) {
                for (Tablet tablet : index.getTablets()) {
                    try {
                        // use default warehouse nodes
                        long primaryId = ((LakeTablet) tablet).getPrimaryComputeNodeId();
                        tablets.add(new TTabletLocation(tablet.getId(), Collections.singletonList(primaryId)));
                    } catch (UserException exception) {
                        throw new UserException("Check if any backend is down or not. tablet_id: " + tablet.getId());
                    }
                }
            } else {
                for (Tablet tablet : index.getTablets()) {
                    // we should ensure the replica backend is alive
                    // otherwise, there will be a 'unknown node id, id=xxx' error for stream load
                    LocalTablet localTablet = (LocalTablet) tablet;
                    Multimap<Replica, Long> bePathsMap =
                            localTablet.getNormalReplicaBackendPathMap(olapTable.getClusterId());
                    if (bePathsMap.keySet().size() < quorum) {
                        throw new UserException(String.format("Tablet lost replicas. Check if any backend is down or not. " +
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

        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            errorStatus.setError_msgs(Lists.newArrayList(String.format("dbId=%d is not exists", dbId)));
            result.setStatus(errorStatus);
            return result;
        }
        Table table = db.getTable(tableId);
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
        try {
            addPartitionClause = AnalyzerUtils.getAddPartitionClauseFromPartitionValues(olapTable,
                    request.partition_values);
            PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
            if (partitionDesc instanceof RangePartitionDesc) {
                partitionColNames = ((RangePartitionDesc) partitionDesc).getPartitionColNames();
            } else if (partitionDesc instanceof ListPartitionDesc) {
                partitionColNames = ((ListPartitionDesc) partitionDesc).getPartitionColNames();
            }
            if (olapTable.getNumberOfPartitions() + partitionColNames.size() > Config.max_automatic_partition_number) {
                throw new AnalysisException(" Automatically created partitions exceeded the maximum limit: " +
                        Config.max_automatic_partition_number + ". You can modify this restriction on by setting" +
                        " max_automatic_partition_number larger.");
            }
        } catch (AnalysisException ex) {
            errorStatus.setError_msgs(Lists.newArrayList(ex.getMessage()));
            result.setStatus(errorStatus);
            return result;
        }

        GlobalStateMgr state = GlobalStateMgr.getCurrentState();

        try {
            state.addPartitions(db, olapTable.getName(), addPartitionClause);
        } catch (Exception e) {
            LOG.warn(e);
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("automatic create partition failed. error:%s", e.getMessage())));
            result.setStatus(errorStatus);
            return result;
        }


        // build partition & tablets
        List<TOlapTablePartition> partitions = Lists.newArrayList();
        List<TTabletLocation> tablets = Lists.newArrayList();

        TransactionState txnState =
                GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxn_id());
        if (txnState == null) {
            errorStatus.setError_msgs(Lists.newArrayList(
                    String.format("automatic create partition failed. error: txn %d not exist", request.getTxn_id())));
            result.setStatus(errorStatus);
            return result;
        }

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
            locker.lockDatabase(db, LockType.READ);
            try {
                return buildCreatePartitionResponse(olapTable, txnState, partitions, tablets, partitionColNames);
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
        }
    }

    private static TCreatePartitionResult buildCreatePartitionResponse(OlapTable olapTable,
                                                                       TransactionState txnState,
                                                                       List<TOlapTablePartition> partitions,
                                                                       List<TTabletLocation> tablets,
                                                                       List<String> partitionColNames) {
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

            Partition partition = olapTable.getPartition(partitionName);
            tPartition = new TOlapTablePartition();
            tPartition.setId(partition.getId());
            buildPartitionInfo(olapTable, partitions, partition, tPartition, txnState);
            // tablet
            int quorum = olapTable.getPartitionInfo().getQuorumNum(partition.getId(), olapTable.writeQuorum());
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                if (olapTable.isCloudNativeTable()) {
                    for (Tablet tablet : index.getTablets()) {
                        LakeTablet cloudNativeTablet = (LakeTablet) tablet;
                        try {
                            // use default warehouse nodes
                            long primaryId = cloudNativeTablet.getPrimaryComputeNodeId();
                            TTabletLocation tabletLocation = new TTabletLocation(tablet.getId(),
                                    Collections.singletonList(primaryId));
                            tablets.add(tabletLocation);
                            txnState.getTabletIdToTTabletLocation().put(tablet.getId(), tabletLocation);
                        } catch (UserException exception) {
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
                                localTablet.getNormalReplicaBackendPathMap(olapTable.getClusterId());
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
        TNodesInfo nodesInfo = GlobalStateMgr.getCurrentState().createNodesInfo(olapTable.getClusterId());
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
            int partColNum = rangePartitionInfo.getPartitionColumns().size();
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
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
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

    /**
     * Returns the empty warehouse info.
     * Maintaining this method is just to avoid problems at grayscale upgrading.
     */
    @Override
    public TGetWarehousesResponse getWarehouses(TGetWarehousesRequest request) throws TException {
        TGetWarehousesResponse res = new TGetWarehousesResponse();
        res.setStatus(new TStatus(OK));
        res.setWarehouse_infos(Collections.emptyList());

        return res;
    }

    @Override
    public TMVReportEpochResponse mvReport(TMVMaintenanceTasks request) throws TException {
        LOG.info("Recieve mvReport: {}", request);
        if (!request.getTask_type().equals(MVTaskType.REPORT_EPOCH)) {
            throw new TException("Only support report_epoch task");
        }
        MaterializedViewMgr.getInstance().onReportEpoch(request);
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
                long dbId = GlobalStateMgr.getCurrentState().getDb(request.getDb()).getId();
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
                    loads.add(task.toThrift());
                }
            } else {
                List<StreamLoadTask> streamLoadTaskList = loadManager.getTaskByName(request.getLabel());
                if (streamLoadTaskList != null) {
                    loads.addAll(
                            streamLoadTaskList.stream().map(StreamLoadTask::toThrift).collect(Collectors.toList()));
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
        Status status = GlobalStateMgr.getCurrentState().getSlotProvider()
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
        Database db = GlobalStateMgr.getCurrentState().getDb(request.getDb_name());
        if (db == null) {
            throw new SemanticException("Database %s is not found", request.getDb_name());
        }
        Table table = db.getTable(request.getTable_name());
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
            response.setLocation(OlapTableSink.createLocation(
                    dictTable, dictTable.getClusterId(), partitionParam, dictTable.enableReplicatedStorage()));
            response.setNodes_info(GlobalStateMgr.getCurrentState().createNodesInfo(dictTable.getClusterId()));
        } catch (UserException e) {
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
}
