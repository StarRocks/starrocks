// This file is made available under Elastic License 2.0.
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
import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.SetVar;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.PlannerProfile;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

// When one client connect in, we create a connect context for it.
// We store session information here. Meanwhile ConnectScheduler all
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
    protected boolean isKilled;
    // catalog
    protected volatile String currentCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    // Db
    protected String currentDb = "";

    // username@host of current login user
    protected String qualifiedUser;
    // username@host combination for the StarRocks account
    // that the server used to authenticate the current client.
    // In other word, currentUserIdentity is the entry that matched in StarRocks auth table.
    // This account determines user's access privileges.
    protected UserIdentity currentUserIdentity;
    // Serializer used to pack MySQL packet.
    protected MysqlSerializer serializer;
    // Variables belong to this session.
    protected SessionVariable sessionVariable;
    // all the modified session variables, will forward to leader
    protected Map<String, SetVar> modifiedSessionVariables = new HashMap<>();
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

    // set with the randomstring extracted from the handshake data at connecting stage
    // used for authdata(password) salting
    protected byte[] authDataSalt;

    protected QueryDetail queryDetail;

    // isLastStmt is true when original stmt is single stmt
    //    or current processing stmt is the last stmt for multi stmts
    // used to set mysql result package
    protected boolean isLastStmt;
    // set true when user dump query through HTTP
    protected boolean isQueryDump = false;

    protected DumpInfo dumpInfo;

    // The related db ids for current sql
    protected Set<Long> currentSqlDbIds = Sets.newHashSet();

    protected PlannerProfile plannerProfile;

    protected ResourceGroup resourceGroup;

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
        this(null);
    }

    public ConnectContext(SocketChannel channel) {
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        serializer = MysqlSerializer.newInstance();
        sessionVariable = VariableMgr.newSessionVariable();
        userVariables = new HashMap<>();
        command = MysqlCommand.COM_SLEEP;
        queryDetail = null;
        dumpInfo = new QueryDumpInfo(sessionVariable);
        plannerProfile = new PlannerProfile();
        plannerProfile.init(this);

        mysqlChannel = new MysqlChannel(channel);
        if (channel != null) {
            remoteIP = mysqlChannel.getRemoteIp();
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

    // for USER() function
    public UserIdentity getUserIdentity() {
        return new UserIdentity(qualifiedUser, remoteIP);
    }

    public UserIdentity getCurrentUserIdentity() {
        return currentUserIdentity;
    }

    public void setCurrentUserIdentity(UserIdentity currentUserIdentity) {
        this.currentUserIdentity = currentUserIdentity;
    }

    public void modifySessionVariable(SetVar setVar, boolean onlySetSessionVar) throws DdlException {
        VariableMgr.setVar(sessionVariable, setVar, onlySetSessionVar);
        if (!setVar.getType().equals(SetType.GLOBAL) && VariableMgr.shouldForwardToLeader(setVar.getVariable())) {
            modifiedSessionVariables.put(setVar.getVariable(), setVar);
        }
    }

    public void modifyUserVariable(SetVar setVar) {
        UserVariable userDefineVariable = (UserVariable) setVar;
        if (userVariables.size() > 1024 && !userVariables.containsKey(setVar.getVariable())) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        userVariables.put(setVar.getVariable(), userDefineVariable);
    }

    public SetStmt getModifiedSessionVariables() {
        if (!modifiedSessionVariables.isEmpty()) {
            return new SetStmt(new ArrayList<>(modifiedSessionVariables.values()));
        }
        return null;
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

    public void cleanup() {
        mysqlChannel.close();
        threadLocalInfo.remove();
        returnRows = 0;
    }

    public boolean isKilled() {
        return isKilled;
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

    public void setIsQueryDump(boolean isQueryDump) {
        this.isQueryDump = isQueryDump;
    }

    public boolean isQueryDump() {
        return this.isQueryDump;
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

    public ResourceGroup getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(ResourceGroup resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public String getCurrentCatalog() {
        return currentCatalog;
    }

    public void setCurrentCatalog(String currentCatalog) {
        this.currentCatalog = currentCatalog;
    }

    // kill operation with no protect.
    public void kill(boolean killConnection) {
        LOG.warn("kill timeout query, {}, kill connection: {}",
                getMysqlChannel().getRemoteHostPortString(), killConnection);

        if (killConnection) {
            isKilled = true;
            // Close channel to break connection with client
            getMysqlChannel().close();
        }
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (executorRef != null) {
            executorRef.cancel();
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
            if (delta > sessionVariable.getQueryTimeoutS() * 1000L) {
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

    public class ThreadInfo {
        public List<String> toRow(long nowMs, boolean full) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(ClusterNamespace.getNameFromFullName(qualifiedUser));
            row.add(getMysqlChannel().getRemoteHostPortString());
            row.add(SystemInfoService.DEFAULT_CLUSTER);
            row.add(ClusterNamespace.getNameFromFullName(currentDb));
            // Command
            row.add(command.toString());
            // connection start Time
            row.add(TimeUtils.longToTimeString(connectionStartTime));
            // Time
            row.add("" + (nowMs - startTime) / 1000);
            // State
            row.add("");
            // Info
            String stmt = "";
            if (executor != null) {
                stmt = executor.getOriginStmtInString();
                // refers to https://mariadb.com/kb/en/show-processlist/
                // `show full processlist` will output full SQL
                // and `show processlist will` output at most 100 chars.
                if (!full && stmt.length() > 100) {
                    stmt = stmt.substring(0, 100);
                }
            }
            row.add(stmt);
            return row;
        }
    }
}
