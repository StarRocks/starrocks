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
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
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
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.master.MasterImpl;
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
import com.starrocks.qe.VariableMgr;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.StreamLoadTask;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TAbortRemoteTxnRequest;
import com.starrocks.thrift.TAbortRemoteTxnResponse;
import com.starrocks.thrift.TAuthenticateParams;
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
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
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
import com.starrocks.thrift.TUpdateExportTaskStatusRequest;
import com.starrocks.thrift.TUserPrivDesc;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.transaction.TxnCommitAttachment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.thrift.TStatusCode.NOT_IMPLEMENTED_ERROR;

// Frontend service used to serve all request for this frontend through
// thrift protocol
public class FrontendServiceImpl implements FrontendService.Iface {
    private static final Logger LOG = LogManager.getLogger(MasterImpl.class);
    private MasterImpl masterImpl;
    private ExecuteEnv exeEnv;

    public FrontendServiceImpl(ExecuteEnv exeEnv) {
        masterImpl = new MasterImpl();
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

        Catalog catalog = Catalog.getCurrentCatalog();
        List<String> dbNames = catalog.getDbNames();
        LOG.debug("get db names: {}", dbNames);

        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        for (String fullName : dbNames) {
            if (!catalog.getAuth().checkDbPriv(currentUser, fullName, PrivPredicate.SHOW)) {
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

        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            for (String tableName : db.getTableNamesWithLock()) {
                LOG.debug("get table: {}, wait to check", tableName);
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
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

        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        UserIdentity currentUser = null;
        if (params.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(params.current_user_ident);
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(params.user, params.user_ip);
        }
        if (db != null) {
            db.readLock();
            try {
                boolean listingViews = params.isSetType() && TTableType.VIEW.equals(params.getType());
                List<Table> tables = listingViews ? db.getViews() : db.getTables();
                for (Table table : tables) {
                    if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
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
                        List<TableRef> tblRefs = new ArrayList<>();
                        view.getQueryStmt().collectTableRefs(tblRefs);
                        for (TableRef tblRef : tblRefs) {
                            if (!Catalog.getCurrentCatalog().getAuth()
                                    .checkTblPriv(currentUser, tblRef.getName().getDb(),
                                            tblRef.getName().getTbl(), PrivPredicate.SHOW)) {
                                ddlSql = "";
                                break;
                            }
                        }
                        status.setDdl_sql(ddlSql);
                    }
                    tablesResult.add(status);
                }
            } finally {
                db.readUnlock();
            }
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
        List<DbPrivEntry> dbPrivEntries = Catalog.getCurrentCatalog().getAuth().getDBPrivEntries(currentUser);
        // flatten privileges
        for (DbPrivEntry entry : dbPrivEntries) {
            PrivBitSet savedPrivs = entry.getPrivSet();
            String clusterPrefix = SystemInfoService.DEFAULT_CLUSTER + ClusterNamespace.CLUSTER_DELIMITER;
            String userIdentStr = currentUser.toString().replace(clusterPrefix, "");
            String dbName = ClusterNamespace.getNameFromFullName(entry.getOrigDb());
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
        List<TablePrivEntry> tablePrivEntries = Catalog.getCurrentCatalog().getAuth().getTablePrivEntries(currentUser);
        // flatten privileges
        for (TablePrivEntry entry : tablePrivEntries) {
            PrivBitSet savedPrivs = entry.getPrivSet();
            String clusterPrefix = SystemInfoService.DEFAULT_CLUSTER + ClusterNamespace.CLUSTER_DELIMITER;
            String userIdentStr = currentUser.toString().replace(clusterPrefix, "");
            String dbName = ClusterNamespace.getNameFromFullName(entry.getOrigDb());
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
        Auth currAuth = Catalog.getCurrentCatalog().getAuth();
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
                                        && userPrivTable.hasPriv(userIdent.getHost(), userIdent.getQualifiedUser(),
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
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser, params.db,
                params.getTable_name(), PrivPredicate.SHOW)) {
            return result;
        }
        Database db = Catalog.getCurrentCatalog().getDb(params.db);
        if (db != null) {
            db.readLock();
            try {
                Table table = db.getTable(params.getTable_name());
                if (table != null) {
                    String tableKeysType = "";
                    if (TableType.OLAP.equals(table.getType())) {
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
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return result;
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
    public TMasterResult finishTask(TFinishTaskRequest request) throws TException {
        return masterImpl.finishTask(request);
    }

    @Override
    public TMasterResult report(TReportRequest request) throws TException {
        return masterImpl.report(request);
    }

    @Override
    public TFetchResourceResult fetchResource() throws TException {
        return masterImpl.fetchResource();
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
            Frontend fe = Catalog.getCurrentCatalog().getFeByHost(clientAddr.getHostname());
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

        final String fullUserName = ClusterNamespace.getFullName(cluster, user);
        final String fullDbName = ClusterNamespace.getFullName(cluster, db);
        List<UserIdentity> currentUser = Lists.newArrayList();
        if (!Catalog.getCurrentCatalog().getAuth().checkPlainPassword(fullUserName, clientIp, passwd, currentUser)) {
            throw new AuthenticationException("Access denied for " + fullUserName + "@" + clientIp);
        }

        Preconditions.checkState(currentUser.size() == 1);
        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(currentUser.get(0), fullDbName, tbl, predicate)) {
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
        if (!Catalog.getCurrentCatalog().isMaster()) {
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
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }

        Table table = null;
        db.readLock();
        try {
            table = db.getTable(request.tbl);
            if (table == null || table.getType() != TableType.OLAP) {
                throw new UserException("unknown table, table=" + request.tbl);
            }
        } finally {
            db.readUnlock();
        }

        // begin
        long timeoutSecond = request.isSetTimeout() ? request.getTimeout() : Config.stream_load_default_timeout_second;
        MetricRepo.COUNTER_LOAD_ADD.increase(1L);
        return Catalog.getCurrentGlobalTransactionMgr().beginTransaction(
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
        if (!Catalog.getCurrentCatalog().isMaster()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            if (!loadTxnCommitImpl(request)) {
                // committed success but not visible
                status.setStatus_code(TStatusCode.PUBLISH_TIMEOUT);
                status.addToError_msgs("Publish timeout. The data will be visible after a while");
            }
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
    private boolean loadTxnCommitImpl(TLoadTxnCommitRequest request) throws UserException {
        String cluster = request.getCluster();
        if (Strings.isNullOrEmpty(cluster)) {
            cluster = SystemInfoService.DEFAULT_CLUSTER;
        }
        if (request.isSetAuth_code()) {
            // TODO(cmy): find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);
        }

        // get database
        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        TxnCommitAttachment attachment = TxnCommitAttachment.fromThrift(request.txnCommitAttachment);
        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;
        // Make publish timeout is less than thrift_rpc_timeout_ms
        // Otherwise, the publish will be successful but commit timeout in BE
        // It will results as error like "call frontend service failed"
        timeoutMs = timeoutMs * 3 / 4;
        boolean ret = Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                db, request.getTxnId(),
                TabletCommitInfo.fromThrift(request.getCommitInfos()),
                timeoutMs, attachment);
        if (!ret) {
            return ret;
        }
        // if commit and publish is success, load can be regarded as success
        MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
        if (null == attachment) {
            return ret;
        }
        // collect table-level metrics
        Table tbl = db.getTable(request.getTbl());
        if (null == tbl) {
            return ret;
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
        return ret;
    }

    @Override
    public TLoadTxnRollbackResult loadTxnRollback(TLoadTxnRollbackRequest request) throws TException {
        String clientAddr = getClientAddrAsString();
        LOG.info("receive txn rollback request. db: {}, tbl: {}, txn_id: {}, reason: {}, backend: {}",
                request.getDb(), request.getTbl(), request.getTxnId(), request.getReason(), clientAddr);
        LOG.debug("txn rollback request: {}", request);

        TLoadTxnRollbackResult result = new TLoadTxnRollbackResult();
        // if current node is not master, reject the request
        if (!Catalog.getCurrentCatalog().isMaster()) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList("current fe is not master"));
            result.setStatus(status);
            return result;
        }

        TStatus status = new TStatus(TStatusCode.OK);
        result.setStatus(status);
        try {
            loadTxnRollbackImpl(request);
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
            // TODO(cmy): find a way to check
        } else {
            checkPasswordAndPrivs(cluster, request.getUser(), request.getPasswd(), request.getDb(),
                    request.getTbl(), request.getUser_ip(), PrivPredicate.LOAD);
        }
        String dbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new MetaNotFoundException("db " + request.getDb() + " does not exist");
        }
        long dbId = db.getId();
        Catalog.getCurrentGlobalTransactionMgr().abortTransaction(dbId, request.getTxnId(),
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

        Catalog catalog = Catalog.getCurrentCatalog();
        String fullDbName = ClusterNamespace.getFullName(cluster, request.getDb());
        Database db = catalog.getDb(fullDbName);
        if (db == null) {
            String dbName = fullDbName;
            if (Strings.isNullOrEmpty(request.getCluster())) {
                dbName = request.getDb();
            }
            throw new UserException("unknown database, database=" + dbName);
        }
        long timeoutMs = request.isSetThrift_rpc_timeout_ms() ? request.getThrift_rpc_timeout_ms() : 5000;
        if (!db.tryReadLock(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new UserException("get database read lock timeout, database=" + fullDbName);
        }
        try {
            Table table = db.getTable(request.getTbl());
            if (table == null) {
                throw new UserException("unknown table, table=" + request.getTbl());
            }
            if (!(table instanceof OlapTable)) {
                throw new UserException("load table type is not OlapTable, type=" + table.getClass());
            }
            StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request, db);
            StreamLoadPlanner planner = new StreamLoadPlanner(db, (OlapTable) table, streamLoadTask);
            TExecPlanFragmentParams plan = planner.plan(streamLoadTask.getId());
            // add table indexes to transaction state
            TransactionState txnState =
                    Catalog.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), request.getTxnId());
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
        if (Catalog.getCurrentCatalog().getBackupHandler().report(request.getTask_type(), request.getJob_id(),
                request.getTask_id(), request.getFinished_num(), request.getTotal_num())) {
            return new TStatus(TStatusCode.OK);
        }
        return new TStatus(TStatusCode.CANCELLED);
    }

    @Override
    public TRefreshTableResponse refreshTable(TRefreshTableRequest request) throws TException {
        try {
            Catalog.getCurrentCatalog().refreshExternalTable(request.getDb_name(),
                    request.getTable_name(),
                    request.getPartitions());
            return new TRefreshTableResponse(new TStatus(TStatusCode.OK));
        } catch (DdlException e) {
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

    // Authenticate a FrontendServiceImpl#beginRemoteTxn RPC for StarRocks external table.
    // The beginRemoteTxn is sent by the source cluster, and received by the target cluster.
    // The target cluster should do authentication using the TAuthenticateParams. This method
    // will check whether the user has an authorization, and whether the user has a
    // PrivPredicate.LOAD on the given tables. The implementation is similar with that
    // of stream load, and you can refer to RestBaseAction#execute and LoadAction#executeWithoutPassword
    // to know more about the related part.
    static TStatus checkPasswordAndPrivilege(TAuthenticateParams authParams) {
        if (authParams == null) {
            LOG.debug("received null TAuthenticateParams");
            return new TStatus(TStatusCode.OK);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Receive parameter {}", authParams);
        }
        if (!Config.enable_starrocks_external_table_auth_check) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("enable_starrocks_external_table_auth_check is disabled, " +
                        "and skip to check authorization and privilege for {}", authParams);
            }
            return new TStatus(TStatusCode.OK);
        }

        try {
            BaseAction.ActionAuthorizationInfo authInfo = BaseAction.parseAuthInfo(
                    authParams.getUser(), authParams.getPasswd(), authParams.getHost());
            UserIdentity userIdentity = BaseAction.checkPassword(authInfo);
            String clusterName = authInfo.cluster;
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new DdlException("No cluster selected");
            }
            String fullDbName = ClusterNamespace.getFullName(clusterName, authParams.getDb_name());
            for (String tableName : authParams.getTable_names()) {
                if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(
                        userIdentity, fullDbName, tableName, PrivPredicate.LOAD)) {
                    String errMsg = String.format("Access denied; user '%s'@'%s' need (at least one of) the %s " +
                            "privilege(s) for table '%s' in db '%s'", userIdentity.getQualifiedUser(),
                                userIdentity.getHost(), PrivPredicate.LOAD, tableName, fullDbName);
                    throw new UnauthorizedException(errMsg);
                }
            }
            return new TStatus(TStatusCode.OK);
        } catch (Exception e) {
            LOG.warn("Failed to check parameter [user: {}, host: {}, db: {}, tables: {}]",
                    authParams.getUser(), authParams.getHost(), authParams.getDb_name(), authParams.getTable_names(), e);

            TStatus status = new TStatus(TStatusCode.NOT_AUTHORIZED);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            return status;
        }
    }

    @Override
    public TGetTableMetaResponse getTableMeta(TGetTableMetaRequest request) throws TException {
        return masterImpl.getTableMeta(request);
    }

    @Override
    public TBeginRemoteTxnResponse beginRemoteTxn(TBeginRemoteTxnRequest request) throws TException {
        TStatus status = checkPasswordAndPrivilege(request.getAuth_info());
        if (status.getStatus_code() != TStatusCode.OK) {
            TBeginRemoteTxnResponse response = new TBeginRemoteTxnResponse();
            response.setStatus(status);
            return response;
        }
        return masterImpl.beginRemoteTxn(request);
    }

    @Override
    public TCommitRemoteTxnResponse commitRemoteTxn(TCommitRemoteTxnRequest request) throws TException {
        return masterImpl.commitRemoteTxn(request);
    }

    @Override
    public TAbortRemoteTxnResponse abortRemoteTxn(TAbortRemoteTxnRequest request) throws TException {
        return masterImpl.abortRemoteTxn(request);
    }

    @Override
    public TSetConfigResponse setConfig(TSetConfigRequest request) throws TException {
        try {
            Preconditions.checkState(request.getKeys().size() == request.getValues().size());
            Map<String, String> configs = new HashMap<>();
            for (int i = 0; i < request.getKeys().size(); i++) {
                configs.put(request.getKeys().get(i), request.getValues().get(i));
            }

            Catalog.getCurrentCatalog().setFrontendConfig(configs);
            return new TSetConfigResponse(new TStatus(TStatusCode.OK));
        } catch (DdlException e) {
            TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
            status.setError_msgs(Lists.newArrayList(e.getMessage()));
            return new TSetConfigResponse(status);
        }
    }
}
