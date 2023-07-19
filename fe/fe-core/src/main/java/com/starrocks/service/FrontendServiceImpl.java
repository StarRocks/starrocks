// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.View;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.AuthenticationException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.DuplicatedRequestException;
import com.starrocks.common.LabelAlreadyUsedException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.ThriftServerContext;
import com.starrocks.common.ThriftServerEventProcessor;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.http.BaseAction;
import com.starrocks.http.UnauthorizedException;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.streamload.StreamLoadInfo;
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
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.SetType;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TBatchReportExecStatusParams;
import com.starrocks.thrift.TBatchReportExecStatusResult;
import com.starrocks.thrift.TBeginRemoteTxnRequest;
import com.starrocks.thrift.TBeginRemoteTxnResponse;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TColumnDesc;
import com.starrocks.thrift.TCommitRemoteTxnRequest;
import com.starrocks.thrift.TCommitRemoteTxnResponse;
import com.starrocks.thrift.TDBPrivDesc;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFeResult;
import com.starrocks.thrift.TFetchResourceResult;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TGetDBPrivsParams;
import com.starrocks.thrift.TGetDBPrivsResult;
import com.starrocks.thrift.TGetDbsParams;
import com.starrocks.thrift.TGetDbsResult;
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
import com.starrocks.thrift.TGetUserPrivsParams;
import com.starrocks.thrift.TGetUserPrivsResult;
import com.starrocks.thrift.TIsMethodSupportedRequest;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TLoadTxnBeginRequest;
import com.starrocks.thrift.TLoadTxnBeginResult;
import com.starrocks.thrift.TLoadTxnCommitRequest;
import com.starrocks.thrift.TLoadTxnCommitResult;
import com.starrocks.thrift.TLoadTxnRollbackRequest;
import com.starrocks.thrift.TLoadTxnRollbackResult;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TMasterResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TRefreshTableRequest;
import com.starrocks.thrift.TRefreshTableResponse;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TReportExecStatusResult;
import com.starrocks.thrift.TReportRequest;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TShowVariableRequest;
import com.starrocks.thrift.TShowVariableResult;
import com.starrocks.thrift.TSnapshotLoaderReportRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TStreamLoadPutResult;
import com.starrocks.thrift.TTablePrivDesc;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTaskInfo;
import com.starrocks.thrift.TTaskRunInfo;
import com.starrocks.thrift.TUpdateExportTaskStatusRequest;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUpdateResourceUsageResponse;
import com.starrocks.thrift.TUserPrivDesc;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TStatusCode.NOT_IMPLEMENTED_ERROR;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(LeaderImpl.class);
    private LeaderImpl leaderImpl;
    private ExecuteEnv exeEnv;

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        leaderImpl = new LeaderImpl();
        this.exeEnv = exeEnv;
    }

    @Override
    public TGetDbsResult getDbNames(TGetDbsParams params) throws TException {
        LOG.debug("get db request: {}", params);
        TGetDbsResult result = new TGetDbsResult();

        List<String> dbs = Lists.newArrayList();
        PatternMatcher matcher = null;
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.DATABASE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<String> dbNames = globalStateMgr.getDbNames();
        LOG.debug("get db names: {}", dbNames);

        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        for (String fullName : dbNames) {
            if (!globalStateMgr.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                continue;
            }

            final String db = ClusterNamespace.getNameFromFullName(fullName);
            if (matcher != null && !matcher.match(db)) {
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
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format: " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase

        Database db = GlobalStateMgr.getCurrentState().getDb(params.db);
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            for (String tableName : db.getTableNamesViewWithLock()) {
                LOG.debug("get table: {}, wait to check", tableName);
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, params.db,
                        tableName, PrivPredicate.SHOW)) {
                    continue;
                }

                if (matcher != null && !matcher.match(tableName)) {
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
        if (params.isSetPattern()) {
            try {
                matcher = PatternMatcher.createMysqlPattern(params.getPattern(),
                        CaseSensibility.TABLE.getCaseSensibility());
            } catch (AnalysisException e) {
                throw new TException("Pattern is in bad format " + params.getPattern());
            }
        }

        // database privs should be checked in analysis phrase

        Database db = GlobalStateMgr.getCurrentState().getDb(params.db);
        long limit = params.isSetLimit() ? params.getLimit() : -1;
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (params.isSetType() && TTableType.MATERIALIZED_VIEW.equals(params.getType())) {
            listMaterializedViewStatus(tablesResult, limit, matcher, currentUser, params.db);
            return result;
        }
        if (db != null) {
            db.readLock();
            try {
                boolean listingViews = params.isSetType() && TTableType.VIEW.equals(params.getType());
                List<Table> tables = listingViews ? db.getViews() : db.getTables();
                for (Table table : tables) {
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, params.db,
                            table.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                    if (matcher != null && !matcher.match(table.getName())) {
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
                        Map<TableName, Table> allTables = AnalyzerUtils.collectAllTable(view.getQueryStatement());
                        for (TableName tableName : allTables.keySet()) {
                            if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(
                                    currentUser, tableName.getDb(), tableName.getTbl(), PrivPredicate.SHOW)) {
                                ddlSql = "";
                                break;
                            }
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
                db.readUnlock();
            }
        }
        return result;
    }

    // list MaterializedView table match pattern
    public void listMaterializedViewStatus(List<TTableStatus> tablesResult, long limit, PatternMatcher matcher,
                                           UserIdentity currentUser, String dbName) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            LOG.warn("database not exists: {}", dbName);
            return;
        }
        db.readLock();
        try {
            for (MaterializedView mvTable : db.getMaterializedViews()) {
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, dbName,
                        mvTable.getName(), PrivPredicate.SHOW)) {
                    continue;
                }
                if (matcher != null && !matcher.match(mvTable.getName())) {
                    continue;
                }
                List<String> createTableStmt = Lists.newArrayList();
                GlobalStateMgr.getDdlStmt(mvTable, createTableStmt, null, null, false, true);
                String ddlSql = createTableStmt.get(0);
                TTableStatus status = new TTableStatus();
                status.setId(String.valueOf(mvTable.getId()));
                status.setName(mvTable.getName());
                status.setDdl_sql(ddlSql);
                status.setRows(String.valueOf(mvTable.getRowCount()));
                status.setType(mvTable.getMysqlType());
                status.setComment(mvTable.getComment());
                tablesResult.add(status);
                if (limit > 0 && tablesResult.size() >= limit) {
                    return;
                }
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
                        MaterializedIndexMeta mvMeta = olapTable.getVisibleIndexIdToMeta().get(mvIdx.getId());
                        TTableStatus status = new TTableStatus();
                        status.setId(String.valueOf(mvIdx.getId()));
                        status.setName(olapTable.getIndexNameById(mvIdx.getId()));
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
                            status.setDdl_sql(originStmtBuilder.toString());
                        } else {
                            status.setDdl_sql(mvMeta.getOriginStmt().replace("\n", "").replace("\t", "")
                                    .replaceAll("[ ]+", " "));
                        }
                        status.setRows(String.valueOf(mvIdx.getRowCount()));
                        // for materialized view used old logic
                        status.setType("");
                        status.setComment("");
                        tablesResult.add(status);
                        if (limit > 0 && tablesResult.size() >= limit) {
                            return;
                        }
                    }
                }
            }
        } finally {
            db.readUnlock();
        }
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
            if (!globalStateMgr.getAuth().checkDbPriv(currentUser, task.getDbName(), PrivPredicate.SHOW)) {
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
            if (!globalStateMgr.getAuth().checkDbPriv(currentUser, status.getDbName(), PrivPredicate.SHOW)) {
                continue;
            }

            TTaskRunInfo info = new TTaskRunInfo();
            info.setQuery_id(status.getQueryId());
            info.setTask_name(status.getTaskName());
            info.setCreate_time(status.getCreateTime() / 1000);
            info.setFinish_time(status.getFinishTime() / 1000);
            info.setState(status.getState().toString());
            info.setDatabase(ClusterNamespace.getNameFromFullName(status.getDbName()));
            info.setDefinition(status.getDefinition());
            info.setError_code(status.getErrorCode());
            info.setError_message(status.getErrorMessage());
            info.setExpire_time(status.getExpireTime() / 1000);
            info.setProgress(status.getProgress() + "%");
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
    public TGetUserPrivsResult getUserPrivs(TGetUserPrivsParams params) throws TException {
        LOG.debug("get user privileges request: {}", params);
        TGetUserPrivsResult result = new TGetUserPrivsResult();
        List<TUserPrivDesc> tUserPrivs = Lists.newArrayList();
        result.setUser_privs(tUserPrivs);
        UserIdentity currentUser = UserIdentity.fromThrift(params.current_user_ident);
        Auth currAuth = GlobalStateMgr.getCurrentState().getAuth();
        UserPrivTable userPrivTable = currAuth.getUserPrivTable();
        List<UserIdentity> userIdents = Lists.newArrayList();
        // users can only see the privileges of themself at this moment
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
        TFeResult result = new TFeResult(FrontendServiceVersion.V1, status);

        return result;
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
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, params.db,
                params.getTable_name(), PrivPredicate.SHOW)) {
            return result;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(params.db);
        if (db != null) {
            db.readLock();
            try {
                Table table = db.getTable(params.getTable_name());
                setColumnDesc(columns, table, limit, false, params.db, params.getTable_name());
            } finally {
                db.readUnlock();
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
            if (!GlobalStateMgr.getCurrentState().getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
                continue;
            }
            Database db = GlobalStateMgr.getCurrentState().getDb(fullName);
            if (db != null) {
                for (String tableName : db.getTableNamesViewWithLock()) {
                    LOG.debug("get table: {}, wait to check", tableName);
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser, fullName,
                            tableName, PrivPredicate.SHOW)) {
                        continue;
                    }
                    db.readLock();
                    try {
                        Table table = db.getTable(tableName);
                        reachLimit = setColumnDesc(columns, table, limit, true, fullName, tableName);
                    } finally {
                        db.readUnlock();
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
        if (table != null) {
            String tableKeysType = "";
            if (TableType.OLAP.equals(table.getType())) {
                OlapTable olapTable = (OlapTable) table;
                tableKeysType = olapTable.getKeysType().name().substring(0, 3).toUpperCase();
            }
<<<<<<< HEAD
            for (Column column : table.getBaseSchema()) {
                final TColumnDesc desc =
                        new TColumnDesc(column.getName(), column.getPrimitiveType().toThrift());
                final Integer precision = column.getType().getPrecision();
                if (precision != null) {
                    desc.setColumnPrecision(precision);
                }
                final Integer columnLength = column.getType().getColumnSize();
                if (columnLength != null) {
                    desc.setColumnLength(columnLength);
                }
                final Integer decimalDigits = column.getType().getDecimalDigits();
                if (decimalDigits != null) {
                    desc.setColumnScale(decimalDigits);
                }
                if (column.isKey()) {
                    // COLUMN_KEY (UNI, AGG, DUP, PRI)
                    desc.setColumnKey(tableKeysType);
                } else {
                    desc.setColumnKey("");
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
=======
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
>>>>>>> b4be96ad36 ([BugFix] Fix the wrong IS_NULLABLE result of system table (#27573))
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
        List<List<String>> rows = VariableMgr.dump(SetType.fromThrift(params.getVarType()), ctx.getSessionVariable(),
                null);
        for (List<String> row : rows) {
            map.put(row.get(0), row.get(1));
        }
        return result;
    }

    @Override
    public TReportExecStatusResult reportExecStatus(TReportExecStatusParams params) throws TException {
        return QeProcessorImpl.INSTANCE.reportExecStatus(params, getClientAddr());
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
        LOG.info("receive forwarded stmt {} from FE: {}", params.getStmt_id(), clientAddr.getHostname());
        ConnectContext context = new ConnectContext(null);
        ConnectProcessor processor = new ConnectProcessor(context);
        TMasterOpResult result = processor.proxyExecute(params);
        ConnectContext.remove();
        return result;
    }

    private void checkPasswordAndPrivs(String cluster, String user, String passwd, String db, String tbl,
                                       String clientIp, PrivPredicate predicate) throws AuthenticationException {

        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!GlobalStateMgr.getCurrentState().getAuth()
                .checkPlainPassword(user, clientIp, passwd, currentUser)) {
            throw new AuthenticationException("Access denied for " + user + "@" + clientIp);
        }

        Preconditions.checkState(currentUser.size() == 1);
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(currentUser.get(0), db, tbl, predicate)) {
            throw new AuthenticationException(
                    "Access denied; you need (at least one of) the LOAD privilege(s) for this operation");
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
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);

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

        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
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
    private void loadTxnCommitImpl(TLoadTxnCommitRequest request, TStatus status) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);
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
        // Otherwise, the publish will be successful but commit timeout in BE
        // It will results as error like "call frontend service failed"
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
                break;
            case MANUAL_LOAD:
                if (!(attachment instanceof ManualLoadTxnCommitAttachment)) {
                    break;
                }
                ManualLoadTxnCommitAttachment streamAttachment = (ManualLoadTxnCommitAttachment) attachment;
                entity.counterStreamLoadFinishedTotal.increase(1L);
                entity.counterStreamLoadBytesTotal.increase(streamAttachment.getReceivedBytes());
                entity.counterStreamLoadRowsTotal.increase(streamAttachment.getLoadedRows());

                break;
            default:
                break;
        }
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
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);
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
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }

        if (request.isSetAuth_code()) {
            // TODO: find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);
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
        } catch (UserException e) {
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

    private TExecPlanFragmentParams streamLoadPutImpl(TStreamLoadPutRequest request) throws UserException {
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
        if (!db.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException("get database read lock timeout, database=" + dbName);
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
            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxnId());
            if (txnState == null) {
                throw new UserException("txn does not exist: " + request.getTxnId());
            }
            txnState.addTableIndexes((OlapTable) table);

            return plan;
        } finally {
            db.readUnlock();
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
            GlobalStateMgr.getCurrentState().refreshExternalTable(new TableName(request.getCatalog_name(),
                    request.getDb_name(), request.getTable_name()), request.getPartitions());
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
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(
                        userIdentity, dbName, tableName, PrivPredicate.LOAD)) {
                    String errMsg = String.format("Access denied; user '%s'@'%s' need (at least one of) the " +
                                    "privilege(s) in [%s] for table '%s' in database '%s'", userIdentity.getQualifiedUser(),
                            userIdentity.getHost(), PrivPredicate.LOAD.getPrivs().toString().trim(), tableName, dbName);
                    throw new UnauthorizedException(errMsg);
                }
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
                configs.put(request.getKeys().get(i), request.getValues().get(i));
            }

            GlobalStateMgr.getCurrentState().setFrontendConfig(configs);
            return new TSetConfigResponse(new TStatus(TStatusCode.OK));
        } catch (DdlException e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            return new TSetConfigResponse(status);
        }
    }

    @Override
    public TGetTablesConfigResponse getTablesConfig(TGetTablesConfigRequest request) throws TException {
        return InformationSchemaDataSource.generateTablesConfigResponse(request);
    }

    @Override
    public TGetTablesInfoResponse getTablesInfo(TGetTablesInfoRequest request) throws TException {

        return InformationSchemaDataSource.generateTablesInfoResponse(request);
    }

    @Override
    public TUpdateResourceUsageResponse updateResourceUsage(TUpdateResourceUsageRequest request) throws TException {
        TResourceUsage usage = request.getResource_usage();
        QueryQueueManager.getInstance().updateResourceUsage(request.getBackend_id(),
                usage.getNum_running_queries(), usage.getMem_limit_bytes(), usage.getMem_used_bytes(),
                usage.getCpu_used_permille());

        TUpdateResourceUsageResponse res = new TUpdateResourceUsageResponse();
        TStatus status = new TStatus(TStatusCode.OK);
        res.setStatus(status);
        return res;
    }

    @Override
    public TGetTabletScheduleResponse getTabletSchedule(TGetTabletScheduleRequest request) throws TException {
        TGetTabletScheduleResponse response = GlobalStateMgr.getCurrentState().getTabletScheduler().getTabletSchedule(request);
        LOG.info("getTabletSchedule: {} return {} TabletSchedule", request, response.getTablet_schedulesSize());
        return response;
    }
}
