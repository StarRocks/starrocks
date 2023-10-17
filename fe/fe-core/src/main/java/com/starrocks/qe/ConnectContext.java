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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectContext.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.ssl.SSLChannel;
import com.starrocks.mysql.ssl.SSLChannelImpClassLoader;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.net.ssl.SSLContext;

// When one client connect in, we create a connection context for it.
// We store session information here. Meanwhile, ConnectScheduler all
// connect with its connection id.
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<>();

    // set this id before analyze
    protected long stmtId;
    protected long forwardedStmtId;

    // The queryId of the last query processed by this session.
    // In some scenarios, the user can get the output of a request by queryId,
    // such as Insert, export requests
    protected UUID lastQueryId;

    // The queryId is used to track a user's request. A user request will only have one queryId
    // in the entire StarRocks system. in some scenarios, a user request may be forwarded to multiple
    // nodes for processing or be processed repeatedly, but each execution instance will have
    // the same queryId
    protected UUID queryId;

    // A request will be executed multiple times because of retry or redirect.
    // This id is used to distinguish between different execution instances
    protected TUniqueId executionId;

    // id for this connection
    protected int connectionId;
    // Time when the connection is make
    protected long connectionStartTime;

    // mysql net
    protected MysqlChannel mysqlChannel;
    // state
    protected QueryState state;
    protected long returnRows;

    // error code
    protected String errorCode = "";

    // the protocol capability which server say it can support
    protected MysqlCapability serverCapability;
    // the protocol capability after server and client negotiate
    protected MysqlCapability capability;
    // Indicate if this client is killed.
    protected volatile boolean isKilled;
    // catalog
    protected volatile String currentCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    // Db
    protected String currentDb = "";
    // warehouse
    protected String currentWarehouse;
    // `qualifiedUser` is the user used when the user establishes connection and authentication.
    // It is the real user used for this connection.
    // Different from the `currentUserIdentity` authentication user of execute as,
    // `qualifiedUser` should not be changed during the entire session.
    protected String qualifiedUser;
    // `currentUserIdentity` is the user used for authorization. Under normal circumstances,
    // `currentUserIdentity` and `qualifiedUser` are the same user,
    // but currentUserIdentity may be modified by execute as statement.
    protected UserIdentity currentUserIdentity;
    // currentRoleIds is the role that has taken effect in the current session.
    // Note that this set is not all roles belonging to the current user.
    // `execute as` will modify currentRoleIds and assign the active role of the impersonate user to currentRoleIds.
    // For specific logic, please refer to setCurrentRoleIds.
    protected Set<Long> currentRoleIds = new HashSet<>();
    // Serializer used to pack MySQL packet.
    protected MysqlSerializer serializer;
    // Variables belong to this session.
    protected SessionVariable sessionVariable;
    // all the modified session variables, will forward to leader
    protected Map<String, SystemVariable> modifiedSessionVariables = new HashMap<>();
    // user define variable in this session
    protected HashMap<String, UserVariable> userVariables;
    // Scheduler this connection belongs to
    protected ConnectScheduler connectScheduler;
    // Executor
    protected StmtExecutor executor;
    // Command this connection is processing.
    protected MysqlCommand command;
    // Timestamp in millisecond last command starts at
    protected long startTime = System.currentTimeMillis();
    // Cache thread info for this connection.
    protected ThreadInfo threadInfo;

    // GlobalStateMgr: put globalStateMgr here is convenient for unit test,
    // because globalStateMgr is singleton, hard to mock
    protected GlobalStateMgr globalStateMgr;
    protected boolean isSend;

    protected AuditEventBuilder auditEventBuilder = new AuditEventBuilder();

    protected String remoteIP;

    protected volatile boolean closed;

    // set with the randomstring extracted from the handshake data at connecting stage
    // used for authdata(password) salting
    protected byte[] authDataSalt;

    protected QueryDetail queryDetail;

    // isLastStmt is true when original stmt is single stmt
    //    or current processing stmt is the last stmt for multi stmts
    // used to set mysql result package
    protected boolean isLastStmt;
    // set true when user dump query through HTTP
    protected boolean isHTTPQueryDump = false;

    protected boolean isStatisticsConnection = false;
    protected boolean isStatisticsJob = false;
    protected boolean isStatisticsContext = false;
    protected boolean needQueued = true;

    protected DumpInfo dumpInfo;

    // The related db ids for current sql
    protected Set<Long> currentSqlDbIds = Sets.newHashSet();

    protected PlannerProfile plannerProfile;
    protected StatementBase.ExplainLevel explainLevel;

    protected TWorkGroup resourceGroup;

    protected volatile boolean isPending = false;

    protected SSLContext sslContext;

    private ConnectContext parent;

    private boolean relationAliasCaseInsensitive = false;

    public StmtExecutor getExecutor() {
        return executor;
    }

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public boolean isSend() {
        return this.isSend;
    }

    public ConnectContext() {
        this(null, null);
    }

    public ConnectContext(SocketChannel channel) {
        this(channel, null);
    }

    public ConnectContext(SocketChannel channel, SSLContext sslContext) {
        closed = false;
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        serializer = MysqlSerializer.newInstance();
        sessionVariable = VariableMgr.newSessionVariable();
        userVariables = new HashMap<>();
        command = MysqlCommand.COM_SLEEP;
        queryDetail = null;
        plannerProfile = new PlannerProfile();

        mysqlChannel = new MysqlChannel(channel);
        if (channel != null) {
            remoteIP = mysqlChannel.getRemoteIp();
        }

        this.sslContext = sslContext;

        if (shouldDumpQuery()) {
            this.dumpInfo = new QueryDumpInfo(this);
        }
    }

    public long getStmtId() {
        return stmtId;
    }

    public void setStmtId(long stmtId) {
        this.stmtId = stmtId;
    }

    public long getForwardedStmtId() {
        return forwardedStmtId;
    }

    public void setForwardedStmtId(long forwardedStmtId) {
        this.forwardedStmtId = forwardedStmtId;
    }

    public String getRemoteIP() {
        return remoteIP;
    }

    public void setRemoteIP(String remoteIP) {
        this.remoteIP = remoteIP;
    }

    public void setQueryDetail(QueryDetail queryDetail) {
        this.queryDetail = queryDetail;
    }

    public QueryDetail getQueryDetail() {
        return queryDetail;
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return auditEventBuilder;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    public GlobalStateMgr getGlobalStateMgr() {
        return globalStateMgr;
    }

    public String getQualifiedUser() {
        return qualifiedUser;
    }

    public void setQualifiedUser(String qualifiedUser) {
        this.qualifiedUser = qualifiedUser;
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    public Set<Long> getCurrentRoleIds() {
        return currentRoleIds;
    }

    public void setCurrentRoleIds(UserIdentity user) {
        try {
            Set<Long> defaultRoleIds;
            if (GlobalVariable.isActivateAllRolesOnLogin()) {
                defaultRoleIds = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(user);
            } else {
                defaultRoleIds = GlobalStateMgr.getCurrentState().getAuthorizationMgr().getDefaultRoleIdsByUser(user);
            }
            this.currentRoleIds = defaultRoleIds;
        } catch (PrivilegeException e) {
            LOG.warn("Set current role fail : {}", e.getMessage());
        }
    }

    public void setCurrentRoleIds(Set<Long> roleIds) {
        this.currentRoleIds = roleIds;
    }

    public void modifySystemVariable(SystemVariable setVar, boolean onlySetSessionVar) throws DdlException {
        VariableMgr.setSystemVariable(sessionVariable, setVar, onlySetSessionVar);
        if (!SetType.GLOBAL.equals(setVar.getType()) && VariableMgr.shouldForwardToLeader(setVar.getVariable())) {
            modifiedSessionVariables.put(setVar.getVariable(), setVar);
        }
    }

    public void modifyUserVariable(UserVariable userVariable) {
        if (userVariables.size() > 1024 && !userVariables.containsKey(userVariable.getVariable())) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        userVariables.put(userVariable.getVariable(), userVariable);
    }

    public SetStmt getModifiedSessionVariables() {
        List<SetListItem> sessionVariables = new ArrayList<>();
        if (!modifiedSessionVariables.isEmpty()) {
            sessionVariables.addAll(modifiedSessionVariables.values());
        }
        if (!userVariables.isEmpty()) {
            sessionVariables.addAll(userVariables.values());
        }

        if (sessionVariables.isEmpty()) {
            return null;
        } else {
            return new SetStmt(sessionVariables);
        }
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public UserVariable getUserVariables(String variable) {
        return userVariables.get(variable);
    }

    public void resetSessionVariable() {
        this.sessionVariable = VariableMgr.newSessionVariable();
        modifiedSessionVariables.clear();
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public ConnectScheduler getConnectScheduler() {
        return connectScheduler;
    }

    public void setConnectScheduler(ConnectScheduler connectScheduler) {
        this.connectScheduler = connectScheduler;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
        returnRows = 0;
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetRetureRows() {
        returnRows = 0;
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }

    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    public void resetConnectionStartTime() {
        this.connectionStartTime = System.currentTimeMillis();
    }

    public long getConnectionStartTime() {
        return connectionStartTime;
    }

    public MysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public QueryState getState() {
        return state;
    }

    public void setState(QueryState state) {
        this.state = state;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public void setErrorCodeOnce(String errorCode) {
        if (this.errorCode == null || this.errorCode.isEmpty()) {
            this.errorCode = errorCode;
        }
    }

    public MysqlCapability getCapability() {
        return capability;
    }

    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    public MysqlCapability getServerCapability() {
        return serverCapability;
    }

    public String getDatabase() {
        return currentDb;
    }

    public void setDatabase(String db) {
        currentDb = db;
    }

    public void setExecutor(StmtExecutor executor) {
        this.executor = executor;
    }

    public synchronized void cleanup() {
        if (closed) {
            return;
        }
        closed = true;
        mysqlChannel.close();
        threadLocalInfo.remove();
        returnRows = 0;
    }

    public boolean isKilled() {
        return (parent != null && parent.isKilled()) || isKilled;
    }

    // Set kill flag to true;
    public void setKilled() {
        isKilled = true;
    }

    public TUniqueId getExecutionId() {
        return executionId;
    }

    public void setExecutionId(TUniqueId executionId) {
        this.executionId = executionId;
    }

    public UUID getQueryId() {
        return queryId;
    }

    public void setQueryId(UUID queryId) {
        this.queryId = queryId;
    }

    public UUID getLastQueryId() {
        return lastQueryId;
    }

    public void setLastQueryId(UUID queryId) {
        this.lastQueryId = queryId;
    }

    public byte[] getAuthDataSalt() {
        return authDataSalt;
    }

    public void setAuthDataSalt(byte[] authDataSalt) {
        this.authDataSalt = authDataSalt;
    }

    public boolean getIsLastStmt() {
        return this.isLastStmt;
    }

    public void setIsLastStmt(boolean isLastStmt) {
        this.isLastStmt = isLastStmt;
    }

    public void setIsHTTPQueryDump(boolean isHTTPQueryDump) {
        this.isHTTPQueryDump = isHTTPQueryDump;
    }

    public boolean isHTTPQueryDump() {
        return isHTTPQueryDump;
    }

    public boolean shouldDumpQuery() {
        return this.isHTTPQueryDump || sessionVariable.getEnableQueryDump();
    }

    public DumpInfo getDumpInfo() {
        return this.dumpInfo;
    }

    public void setDumpInfo(DumpInfo dumpInfo) {
        this.dumpInfo = dumpInfo;
    }

    public Set<Long> getCurrentSqlDbIds() {
        return currentSqlDbIds;
    }

    public void setCurrentSqlDbIds(Set<Long> currentSqlDbIds) {
        this.currentSqlDbIds = currentSqlDbIds;
    }

    public PlannerProfile getPlannerProfile() {
        return plannerProfile;
    }

    public StatementBase.ExplainLevel getExplainLevel() {
        return explainLevel;
    }

    public void setExplainLevel(StatementBase.ExplainLevel explainLevel) {
        this.explainLevel = explainLevel;
    }

    public TWorkGroup getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(TWorkGroup resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public String getCurrentCatalog() {
        return currentCatalog;
    }

    public void setCurrentCatalog(String currentCatalog) {
        this.currentCatalog = currentCatalog;
    }

    public String getCurrentWarehouse() {
        if (currentWarehouse != null) {
            return currentWarehouse;
        }
        return WarehouseManager.DEFAULT_WAREHOUSE_NAME;
    }

    public void setCurrentWarehouse(String currentWarehouse) {
        this.currentWarehouse = currentWarehouse;
    }

    public void setParentConnectContext(ConnectContext parent) {
        this.parent = parent;
    }

    public boolean isStatisticsConnection() {
        return isStatisticsConnection;
    }

    public void setStatisticsConnection(boolean statisticsConnection) {
        isStatisticsConnection = statisticsConnection;
    }

    public boolean isStatisticsJob() {
        return isStatisticsJob || isStatisticsContext;
    }

    public void setStatisticsJob(boolean statisticsJob) {
        isStatisticsJob = statisticsJob;
    }

    public void setStatisticsContext(boolean isStatisticsContext) {
        this.isStatisticsContext = isStatisticsContext;
    }

    public boolean isNeedQueued() {
        return needQueued;
    }

    public void setNeedQueued(boolean needQueued) {
        this.needQueued = needQueued;
    }

    public ConnectContext getParent() {
        return parent;
    }

    public void setRelationAliasCaseInSensitive(boolean relationAliasCaseInsensitive) {
        this.relationAliasCaseInsensitive = relationAliasCaseInsensitive;
    }

    public boolean isRelationAliasCaseInsensitive() {
        return relationAliasCaseInsensitive;
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        LOG.warn("kill query, {}, kill connection: {}",
                getMysqlChannel().getRemoteHostPortString(), killConnection);
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (killConnection) {
            isKilled = true;
        }
        if (executorRef != null) {
            executorRef.cancel();
        }
        if (killConnection) {
            int times = 0;
            while (!closed) {
                try {
                    Thread.sleep(10);
                    times++;
                    if (times > 100) {
                        LOG.warn("wait for close fail, break.");
                        break;
                    }
                } catch (InterruptedException e) {
                    LOG.warn(e);
                    LOG.warn("sleep exception, ignore.");
                    break;
                }
            }
            // Close channel to break connection with client
            getMysqlChannel().close();
        }
    }

    public void checkTimeout(long now) {
        if (startTime <= 0) {
            return;
        }

        long delta = now - startTime;
        boolean killFlag = false;
        boolean killConnection = false;
        if (command == MysqlCommand.COM_SLEEP) {
            if (delta > sessionVariable.getWaitTimeoutS() * 1000L) {
                // Need kill this connection.
                LOG.warn("kill wait timeout connection, remote: {}, wait timeout: {}",
                        getMysqlChannel().getRemoteHostPortString(), sessionVariable.getWaitTimeoutS());

                killFlag = true;
                killConnection = true;
            }
        } else {
            long timeoutSecond = sessionVariable.getQueryTimeoutS();
            if (delta > timeoutSecond * 1000L) {
                LOG.warn("kill query timeout, remote: {}, query timeout: {}",
                        getMysqlChannel().getRemoteHostPortString(), sessionVariable.getQueryTimeoutS());

                // Only kill
                killFlag = true;
            }
        }
        if (killFlag) {
            kill(killConnection);
        }
    }

    // Helper to dump connection information.
    public ThreadInfo toThreadInfo() {
        if (threadInfo == null) {
            threadInfo = new ThreadInfo();
        }
        return threadInfo;
    }

    public int getAliveBackendNumber() {
        int v = sessionVariable.getCboDebugAliveBackendNumber();
        if (v > 0) {
            return v;
        }
        return globalStateMgr.getClusterInfo().getAliveBackendNumber();
    }

    public int getTotalBackendNumber() {
        return globalStateMgr.getClusterInfo().getTotalBackendNumber();
    }

    public void setPending(boolean pending) {
        isPending = pending;
    }

    public boolean isPending() {
        return isPending;
    }

    public boolean supportSSL() {
        return sslContext != null;
    }

    public boolean enableSSL() throws IOException {
        Class<? extends SSLChannel> clazz = SSLChannelImpClassLoader.loadSSLChannelImpClazz();
        if (clazz == null) {
            LOG.warn("load SSLChannelImp class failed");
            throw new IOException("load SSLChannelImp class failed");
        }

        try {
            SSLChannel sslChannel = (SSLChannel) clazz.getConstructors()[0]
                    .newInstance(sslContext.createSSLEngine(), mysqlChannel);
            if (!sslChannel.init()) {
                return false;
            } else {
                mysqlChannel.setSSLChannel(sslChannel);
                return true;
            }
        } catch (Exception e) {
            LOG.warn("construct SSLChannelImp class failed");
            throw new IOException("construct SSLChannelImp class failed");
        }
    }

    public StmtExecutor executeSql(String sql) throws Exception {
        StatementBase sqlStmt = SqlParser.parse(sql, getSessionVariable()).get(0);
        sqlStmt.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = new StmtExecutor(this, sqlStmt);
        setExecutor(executor);
        setThreadLocalInfo();
        executor.execute();
        return executor;
    }

    public class ThreadInfo {
        public boolean isRunning() {
            return state.isRunning();
        }

        public List<String> toRow(long nowMs, boolean full) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(ClusterNamespace.getNameFromFullName(qualifiedUser));
            row.add(getMysqlChannel().getRemoteHostPortString());
            row.add(ClusterNamespace.getNameFromFullName(currentDb));
            // Command
            row.add(command.toString());
            // connection start Time
            row.add(TimeUtils.longToTimeString(connectionStartTime));
            // Time
            row.add("" + (nowMs - startTime) / 1000);
            // State
            row.add(state.toString());
            // Info
            String stmt = "";
            if (executor != null) {
                stmt = executor.getOriginStmtInString();
                // refers to https://mariadb.com/kb/en/show-processlist/
                // `show full processlist` will output full SQL
                // and `show processlist` will output at most 100 chars.
                if (!full && stmt.length() > 100) {
                    stmt = stmt.substring(0, 100);
                }
            }
            row.add(stmt);
            row.add(Boolean.toString(isPending));
            row.add(currentWarehouse);
            return row;
        }
    }
}
