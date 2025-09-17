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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.VariableExpr;
import com.starrocks.authentication.OAuth2Context;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.mysql.ssl.SSLChannel;
import com.starrocks.mysql.ssl.SSLChannelImp;
import com.starrocks.mysql.ssl.SSLContextLoader;
import com.starrocks.plugin.AuditEvent.AuditEventBuilder;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CleanTemporaryTableStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.expression.StringLiteral;
>>>>>>> b70c85739c ([BugFix] Fix the bug where UserProperty priority is lower than Session Variable (#63173))
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.spm.SQLPlanStorage;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.common.util.Util.normalizeName;

// When one client connect in, we create a connection context for it.
// We store session information here. Meanwhile, ConnectScheduler all
// connect with its connection id.
public class ConnectContext {
    private static final Logger LOG = LogManager.getLogger(ConnectContext.class);
    protected static ThreadLocal<ConnectContext> threadLocalInfo = new ThreadLocal<>();

    // set this id before analyze
    protected long stmtId;
    protected long forwardedStmtId;
    private int forwardTimes = 0;

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
    // Db
    protected String currentDb = "";
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
    // groups of current user
    protected Set<String> groups = new HashSet<>();

    // The Token in the OpenIDConnect authentication method is obtained
    // from the authentication logic and stored in the ConnectContext.
    // If the downstream system needs it, it needs to be obtained from the ConnectContext.
    protected volatile String authToken = null;

    // Only used in OAuth2 authentication mode to store
    // relevant information of OAuth2 authentication.
    // Ensure that necessary information can be obtained during OAuth2 http callback process.
    private volatile OAuth2Context oAuth2Context = null;

    // After negotiate and switching with the client,
    // the auth plugin type used for this authentication is finally determined.
    private String authPlugin = null;

    //Auth Data salt generated at mysql negotiate used for password salting
    private byte[] authDataSalt = null;

    // The security integration method used for authentication.
    protected String securityIntegration = "native";

    // Distinguished name (DN) used for LDAP authentication and group resolution
    // In LDAP context, this represents the unique identifier of a user in the directory
    // For non-LDAP authentication, this typically defaults to the username
    // Used by group providers to resolve user group memberships
    protected String distinguishedName = "";

    // Serializer used to pack MySQL packet.
    protected MysqlSerializer serializer;
    // Variables belong to this session.
    protected SessionVariable sessionVariable;
    // all the modified session variables, will forward to leader
    protected Map<String, SystemVariable> modifiedSessionVariables = new HashMap<>();
    // user define variable in this session
    protected Map<String, UserVariable> userVariables;
    protected Map<String, UserVariable> userVariablesCopyInWrite;
    // Executor
    protected StmtExecutor executor;
    // Command this connection is processing.
    protected MysqlCommand command;
    // last command start time
    protected volatile Instant startTime = Instant.now();
    // last command end time
    protected volatile Instant endTime = Instant.ofEpochMilli(0);
    // last command pending time(s), query's timeout should not contain pending time.
    protected volatile int pendingTimeSecond = 0;
    // Cache thread info for this connection.
    protected ThreadInfo threadInfo;

    protected GlobalStateMgr globalStateMgr;
    protected boolean isSend;

    protected AuditEventBuilder auditEventBuilder = new AuditEventBuilder();

    protected String remoteIP;

    protected volatile boolean closed;

    protected QueryDetail queryDetail;

    // isLastStmt is true when original stmt is single stmt
    //    or current processing stmt is the last stmt for multi stmts
    // used to set mysql result package
    protected boolean isLastStmt = true;
    // set true when user dump query through HTTP
    protected boolean isHTTPQueryDump = false;

    protected boolean isStatisticsConnection = false;
    protected boolean isStatisticsJob = false;
    protected boolean isStatisticsContext = false;

    protected boolean isMetadataContext = false;
    protected boolean needQueued = true;

    // Bypass the authorizer check for certain cases
    protected boolean bypassAuthorizerCheck = false;

    protected DumpInfo dumpInfo;

    // The related db ids for current sql
    protected Set<Long> currentSqlDbIds = Sets.newHashSet();

    protected StatementBase.ExplainLevel explainLevel;

    protected TWorkGroup resourceGroup;

    protected volatile boolean isPending = false;
    protected volatile boolean isForward = false;

    private ConnectContext parent;

    private boolean relationAliasCaseInsensitive = false;

    private final Map<String, PrepareStmtContext> preparedStmtCtxs = Maps.newHashMap();

    private UUID sessionId;

    private String proxyHostName;
    private AtomicInteger pendingForwardRequests = new AtomicInteger(0);

    // QueryMaterializationContext is different from MaterializationContext that it keeps the context during the query
    // lifecycle instead of per materialized view.
    private QueryMaterializationContext queryMVContext;

    // In order to ensure the correctness of imported data, in some cases, we don't use connector metadata cache for
    // `insert into table select external table`. Currently, this feature only supports hive table.
    private Optional<Boolean> useConnectorMetadataCache = Optional.empty();

    // running explicit transaction in a session.
    // The temporary state generated by multiple statements in a transaction is recorded in
    // GlobalTransactionMgr#ExplicitTxnState, and the transaction state is recorded in TransactionState.
    private long txnId;

    // session level SPM storage
    private SQLPlanStorage sqlPlanStorage = SQLPlanStorage.create(false);

    // Whether leader is transferred during executing stmt
    private boolean isLeaderTransferred = false;

    private AtomicLong currentThreadAllocatedMemory = new AtomicLong(0);

    // thread id is the thread who created this ConnectContext's id
    private AtomicLong currentThreadId = null;

    // The cnResource of the current session.
    private ComputeResource computeResource = null;

    // listeners for this connection
    private List<Listener> listeners = Lists.newArrayList();

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public StmtExecutor getExecutor() {
        return executor;
    }

    public static ConnectContext get() {
        return threadLocalInfo.get();
    }

    public static SessionVariable getSessionVariableOrDefault() {
        ConnectContext ctx = get();
        return (ctx != null) ? ctx.sessionVariable : SessionVariable.DEFAULT_SESSION_VARIABLE;
    }

    public static void remove() {
        threadLocalInfo.remove();
    }

    public boolean isQueryStmt(StatementBase statement) {
        if (statement instanceof QueryStatement) {
            return true;
        }

        if (statement instanceof ExecuteStmt) {
            ExecuteStmt executeStmt = (ExecuteStmt) statement;
            PrepareStmtContext prepareStmtContext = getPreparedStmt(executeStmt.getStmtName());
            if (prepareStmtContext != null) {
                return prepareStmtContext.getStmt().getInnerStmt() instanceof QueryStatement;
            }
        }
        return false;
    }

    public boolean isSend() {
        return this.isSend;
    }

    public ConnectContext() {
        this(null);
    }

    public ConnectContext(StreamConnection connection) {
        // `globalStateMgr` is used in many cases, so we should explicitly make sure it is not null
        globalStateMgr = GlobalStateMgr.getCurrentState();
        closed = false;
        state = new QueryState();
        returnRows = 0;
        serverCapability = MysqlCapability.DEFAULT_CAPABILITY;
        isKilled = false;
        serializer = MysqlSerializer.newInstance();
        sessionVariable = globalStateMgr.getVariableMgr().newSessionVariable();
        userVariables = new ConcurrentHashMap<>();
        command = MysqlCommand.COM_SLEEP;
        queryDetail = null;

        if (shouldDumpQuery()) {
            this.dumpInfo = new QueryDumpInfo(this);
        }
        this.sessionId = UUIDUtil.genUUID();

        mysqlChannel = new MysqlChannel(connection);
        if (connection != null) {
            remoteIP = mysqlChannel.getRemoteIp();
        }
    }

    /**
     * Build a ConnectContext for normal query.
     */
    public static ConnectContext build() {
        return new ConnectContext();
    }

    /**
     * Build a ConnectContext for inner query which is used for StarRocks internal query.
     */
    public static ConnectContext buildInner() {
        ConnectContext connectContext = new ConnectContext();
        // disable materialized view rewrite for inner query
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        return connectContext;
    }

    public SQLPlanStorage getSqlPlanStorage() {
        return sqlPlanStorage;
    }

    public void putPreparedStmt(String stmtName, PrepareStmtContext ctx) {
        this.preparedStmtCtxs.put(stmtName, ctx);
    }

    public PrepareStmtContext getPreparedStmt(String stmtName) {
        return this.preparedStmtCtxs.get(stmtName);
    }

    public void removePreparedStmt(String stmtName) {
        this.preparedStmtCtxs.remove(stmtName);
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

    public void setAuditEventBuilder(AuditEventBuilder auditEventBuilder) {
        this.auditEventBuilder = auditEventBuilder;
    }

    public AuditEventBuilder getAuditEventBuilder() {
        return auditEventBuilder;
    }

    public void setThreadLocalInfo() {
        threadLocalInfo.set(this);
    }

    public Optional<Boolean> getUseConnectorMetadataCache() {
        return useConnectorMetadataCache;
    }

    public void setUseConnectorMetadataCache(Optional<Boolean> useConnectorMetadataCache) {
        this.useConnectorMetadataCache = useConnectorMetadataCache;
    }

    public static ConnectContext exchangeThreadLocalInfo(ConnectContext ctx) {
        ConnectContext prev = threadLocalInfo.get();
        threadLocalInfo.set(ctx);
        return prev;
    }

    public void setGlobalStateMgr(GlobalStateMgr globalStateMgr) {
        Preconditions.checkState(globalStateMgr != null);
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

    public void setDistinguishedName(String distinguishedName) {
        this.distinguishedName = distinguishedName;
    }

    public String getDistinguishedName() {
        return distinguishedName;
    }

    public Set<Long> getCurrentRoleIds() {
        return currentRoleIds;
    }

    public void setCurrentRoleIds(UserIdentity user) {
        if (user.isEphemeral()) {
            this.currentRoleIds = new HashSet<>();
        } else {
            try {
                Set<Long> defaultRoleIds;
                if (GlobalVariable.isActivateAllRolesOnLogin()) {
                    defaultRoleIds = globalStateMgr.getAuthorizationMgr().getRoleIdsByUser(user);
                } else {
                    defaultRoleIds = globalStateMgr.getAuthorizationMgr().getDefaultRoleIdsByUser(user);
                }
                this.currentRoleIds = defaultRoleIds;
            } catch (PrivilegeException e) {
                LOG.warn("Set current role fail : {}", e.getMessage());
            }
        }
    }

    public void setCurrentRoleIds(Set<Long> roleIds) {
        this.currentRoleIds = roleIds;
    }

    public void setCurrentRoleIds(UserIdentity userIdentity, Set<String> groups) {
        setCurrentRoleIds(userIdentity);
    }

    public void setAuthInfoFromThrift(TAuthInfo authInfo) {
        if (authInfo.isSetCurrent_user_ident()) {
            setAuthInfoFromThrift(authInfo.getCurrent_user_ident());
        } else {
            currentUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
            setCurrentRoleIds(currentUserIdentity);
        }
    }

    public void setAuthInfoFromThrift(TUserIdentity tUserIdent) {
        currentUserIdentity = UserIdentity.fromThrift(tUserIdent);
        if (tUserIdent.isSetCurrent_role_ids()) {
            currentRoleIds = new HashSet<>(tUserIdent.current_role_ids.getRole_id_list());
        } else {
            setCurrentRoleIds(currentUserIdentity);
        }
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public void setOAuth2Context(OAuth2Context oAuth2Context) {
        this.oAuth2Context = oAuth2Context;
    }

    public OAuth2Context getOAuth2Context() {
        return oAuth2Context;
    }

    public void setAuthPlugin(String authPlugin) {
        this.authPlugin = authPlugin;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public void setAuthDataSalt(byte[] authDataSalt) {
        this.authDataSalt = authDataSalt;
    }

    public byte[] getAuthDataSalt() {
        return authDataSalt;
    }

    public String getSecurityIntegration() {
        return securityIntegration;
    }

    public void setSecurityIntegration(String securityIntegration) {
        this.securityIntegration = securityIntegration;
    }

    public void modifySystemVariable(SystemVariable setVar, boolean onlySetSessionVar) throws DdlException {
        globalStateMgr.getVariableMgr().setSystemVariable(sessionVariable, setVar, onlySetSessionVar);
        if (!SetType.GLOBAL.equals(setVar.getType())
                && globalStateMgr.getVariableMgr().shouldForwardToLeader(setVar.getVariable())) {
            modifiedSessionVariables.put(setVar.getVariable(), setVar);
        }
    }

    public void modifyUserVariable(UserVariable userVariable) {
        if (userVariables.size() > 1024 && !userVariables.containsKey(userVariable.getVariable())) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        userVariables.put(userVariable.getVariable(), userVariable);
    }

    /**
     * 1. The {@link ConnectContext#userVariables} in the current session should not be modified
     * until you are sure that the set sql was executed successfully.
     * 2. Changes to user variables during set sql execution should
     * be effected in the {@link ConnectContext#userVariablesCopyInWrite}.
     */
    public void modifyUserVariableCopyInWrite(UserVariable userVariable) {
        if (userVariablesCopyInWrite != null) {
            if (userVariablesCopyInWrite.size() > 1024) {
                throw new SemanticException("User variable exceeds the maximum limit of 1024");
            }
            userVariablesCopyInWrite.put(userVariable.getVariable(), userVariable);
        }
    }

    /**
     * The SQL execution that sets the variable must reset userVariablesCopyInWrite when it finishes,
     * either normally or abnormally.
     * <p>
     * This method needs to be called at the time of setting the user variable.
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void resetUserVariableCopyInWrite() {
        userVariablesCopyInWrite = null;
    }

    /**
     * After the successful execution of the SQL that set the variable,
     * the result of the change to the copy of userVariables is set back to the current session.
     * <p>
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void modifyUserVariables(Map<String, UserVariable> userVarCopyInWrite) {
        if (userVarCopyInWrite.size() > 1024) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        this.userVariables = userVarCopyInWrite;
    }

    /**
     * Instead of using {@link ConnectContext#userVariables} when set userVariables,
     * use a copy of it, the purpose of which is to ensure atomicity/isolation of modifications to userVariables
     * <p>
     * This method needs to be called at the time of setting the user variable.
     * call by {@link SetExecutor#execute()}, {@link StmtExecutor#processQueryScopeHint()}
     */
    public void modifyUserVariablesCopyInWrite(Map<String, UserVariable> userVariables) {
        this.userVariablesCopyInWrite = userVariables;
    }

    public SetStmt getModifiedSessionVariables() {
        List<SetListItem> sessionVariables = new ArrayList<>();
        if (MapUtils.isNotEmpty(modifiedSessionVariables)) {
            sessionVariables.addAll(modifiedSessionVariables.values());
        }
        if (MapUtils.isNotEmpty(userVariables)) {
            for (UserVariable userVariable : userVariables.values()) {
                if (!userVariable.isFromHint()) {
                    sessionVariables.add(userVariable);
                }
            }
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

    public Map<String, UserVariable> getUserVariables() {
        return userVariables;
    }

    public UserVariable getUserVariable(String variable) {
        return userVariables.get(variable);
    }

    public void resetSessionVariable() {
        this.sessionVariable = globalStateMgr.getVariableMgr().newSessionVariable();
        modifiedSessionVariables.clear();
    }

    public UserVariable getUserVariableCopyInWrite(String variable) {
        if (userVariablesCopyInWrite == null) {
            return null;
        }

        return userVariablesCopyInWrite.get(variable);
    }

    public Map<String, UserVariable> getUserVariablesCopyInWrite() {
        if (userVariablesCopyInWrite == null) {
            return null;
        }

        return userVariablesCopyInWrite;
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public MysqlCommand getCommand() {
        return command;
    }

    public void setCommand(MysqlCommand command) {
        this.command = command;
    }

    public long getStartTime() {
        return startTime.toEpochMilli();
    }

    public Instant getStartTimeInstant() {
        return startTime;
    }

    public void setStartTime() {
        startTime = Instant.now();
        returnRows = 0;
        pendingTimeSecond = 0;
    }

    @VisibleForTesting
    public void setStartTime(Instant start) {
        startTime = start;
        returnRows = 0;
        pendingTimeSecond = 0;
    }

    public void setEndTime() {
        endTime = Instant.now();
    }

    public void updateReturnRows(int returnRows) {
        this.returnRows += returnRows;
    }

    public long getReturnRows() {
        return returnRows;
    }

    public void resetReturnRows() {
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

    public String getProxyHostName() {
        return proxyHostName;
    }

    public void setProxyHostName(String address) {
        this.proxyHostName = address;
    }

    public boolean hasPendingForwardRequest() {
        return pendingForwardRequests.intValue() > 0;
    }

    public void incPendingForwardRequest() {
        pendingForwardRequests.incrementAndGet();
    }

    public void decPendingForwardRequest() {
        pendingForwardRequests.decrementAndGet();
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

    public String getNormalizedErrorCode() {
        // TODO: how to unify TStatusCode, ErrorCode, ErrType, ConnectContext.errorCode
        if (StringUtils.isNotEmpty(errorCode)) {
            // error happens in BE execution.
            return errorCode;
        }

        if (state.getErrType() != QueryState.ErrType.UNKNOWN) {
            // error happens in FE execution.
            return state.getErrType().name();
        }

        return "";
    }

    public void resetErrorCode() {
        this.errorCode = "";
    }

    public void setErrorCodeOnce(String errorCode) {
        if (Strings.isNullOrEmpty(this.errorCode)) {
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
        computeResource = null;
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

    public String getCustomQueryId() {
        return sessionVariable != null ? sessionVariable.getCustomQueryId() : "";
    }

    public boolean isProfileEnabled() {
        if (sessionVariable == null) {
            return false;
        }
        if (sessionVariable.isEnableProfile()) {
            return true;
        }
        if (!sessionVariable.isEnableBigQueryProfile()) {
            return false;
        }
        return System.currentTimeMillis() - getStartTime() > sessionVariable.getBigQueryProfileMilliSecondThreshold();
    }

    public boolean needMergeProfile() {
        return sessionVariable.getPipelineProfileLevel() < TPipelineProfileLevel.DETAIL.getValue();
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
        return this.sessionVariable.getCatalog();
    }

    public void setCurrentCatalog(String currentCatalog) {
        this.sessionVariable.setCatalog(currentCatalog);
    }

    public long getCurrentWarehouseId() {
        String warehouseName = this.sessionVariable.getWarehouseName();
        if (warehouseName.equalsIgnoreCase(WarehouseManager.DEFAULT_WAREHOUSE_NAME)) {
            return WarehouseManager.DEFAULT_WAREHOUSE_ID;
        }

        Warehouse warehouse = globalStateMgr.getWarehouseMgr().getWarehouseAllowNull(warehouseName);
        if (warehouse == null) {
            throw new SemanticException("Warehouse " + warehouseName + " not exist");
        }
        return warehouse.getId();
    }

    public String getCurrentWarehouseName() {
        return this.sessionVariable.getWarehouseName();
    }

    public void setCurrentWarehouse(String currentWarehouse) {
        this.sessionVariable.setWarehouseName(currentWarehouse);
        this.computeResource = null;
    }

    public void setCurrentWarehouseId(long warehouseId) {
        Warehouse warehouse = globalStateMgr.getWarehouseMgr().getWarehouse(warehouseId);
        this.sessionVariable.setWarehouseName(warehouse.getName());
        this.computeResource = null;
    }

    public void setCurrentComputeResource(ComputeResource computeResource) {
        this.computeResource = computeResource;
    }

    public synchronized void tryAcquireResource(boolean force) {
        if (!force && this.computeResource != null) {
            return;
        }
        // once warehouse is set, needs to choose the available cngroup
        // try to acquire cn group id once the warehouse is set
        final long warehouseId = this.getCurrentWarehouseId();
        final WarehouseManager warehouseManager = globalStateMgr.getWarehouseMgr();
        this.computeResource = warehouseManager.acquireComputeResource(warehouseId, this.computeResource);
    }

    /**
     * Get the current compute resource, acquire it if not set.
     * NOTE: This method will acquire compute resource if it is not set.
     *
     * @return: the current compute resource, or the default resource if not in shared data mode.
     */
    public ComputeResource getCurrentComputeResource() {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseManager.DEFAULT_RESOURCE;
        }
        if (this.computeResource == null) {
            tryAcquireResource(false);
        }
        return this.computeResource;
    }

    /**
     * Get the name of the current compute resource.
     * NOTE: this method will not acquire compute resource if it is not set.
     *
     * @return: the name of the current compute resource, or empty string if not set.
     */
    public String getCurrentComputeResourceName() {
        if (!RunMode.isSharedDataMode() || this.computeResource == null) {
            return "";
        }
        final WarehouseManager warehouseManager = globalStateMgr.getWarehouseMgr();
        return warehouseManager.getComputeResourceName(this.computeResource);
    }

    /**
     * Get the current compute resource without acquiring it.
     *
     * @return: the current compute resource(null if not set), or the default resource if not in shared data mode.
     */
    public ComputeResource getCurrentComputeResourceNoAcquire() {
        if (!RunMode.isSharedDataMode()) {
            return WarehouseManager.DEFAULT_RESOURCE;
        }
        return this.computeResource;
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

    public boolean isMetadataContext() {
        return isMetadataContext;
    }

    public void setMetadataContext(boolean metadataContext) {
        isMetadataContext = metadataContext;
    }

    public boolean isNeedQueued() {
        return needQueued;
    }

    public void setNeedQueued(boolean needQueued) {
        this.needQueued = needQueued;
    }

    public boolean isBypassAuthorizerCheck() {
        return bypassAuthorizerCheck;
    }

    public void setBypassAuthorizerCheck(boolean value) {
        this.bypassAuthorizerCheck = value;
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

    public void setForwardTimes(int forwardTimes) {
        this.forwardTimes = forwardTimes;
    }

    public int getForwardTimes() {
        return this.forwardTimes;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getSessionId() {
        return this.sessionId;
    }

    public QueryMaterializationContext getQueryMVContext() {
        return queryMVContext;
    }

    public void setQueryMVContext(QueryMaterializationContext queryMVContext) {
        this.queryMVContext = queryMVContext;
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        mysqlChannel.startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        mysqlChannel.suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        mysqlChannel.resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        mysqlChannel.stopAcceptQuery();
    }

    // kill operation with no protect.
    public void kill(boolean killConnection, String cancelledMessage) {
        LOG.warn("kill query, {}, kill connection: {}",
                getMysqlChannel().getRemoteHostPortString(), killConnection);
        // Now, cancel running process.
        StmtExecutor executorRef = executor;
        if (killConnection) {
            isKilled = true;
        }
        if (executorRef != null) {
            executorRef.cancel(cancelledMessage);
        }
        if (killConnection) {
            int times = 0;
            while (!closed) {
                try {
                    Thread.sleep(10);
                    times++;
                    if (times > 100) {
                        LOG.warn("kill queryId={} connectId={} wait for close fail, break.", queryId, connectionId);
                        break;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("sleep exception, ignore.", e);
                    break;
                }
            }
            // Close channel to break connection with client
            getMysqlChannel().close();
        }
    }

    /**
     * NOTE: The ExecTimeout should not contain the pending time which may be caused by QueryQueue's scheduler.
     * </p>
     *
     * @return Get the timeout for this session, unit: second
     */
    public int getExecTimeout() {
        return pendingTimeSecond + getExecTimeoutWithoutPendingTime();
    }

    private int getExecTimeoutWithoutPendingTime() {
        return executor != null ? executor.getExecTimeout() : sessionVariable.getQueryTimeoutS();
    }

    /**
     * update the pending time for this session, unit: second
     *
     * @param pendingTimeSecond: the pending time for this session
     */
    public void setPendingTimeSecond(int pendingTimeSecond) {
        this.pendingTimeSecond = pendingTimeSecond;
    }

    public long getPendingTimeSecond() {
        return this.pendingTimeSecond;
    }

    private String getExecType() {
        return executor != null ? executor.getExecType() : "Query";
    }

    private boolean isExecLoadType() {
        return executor != null && executor.isExecLoadType();
    }

    /**
     * Check the connect context is timeout or not. If true, kill the connection, otherwise, return false.
     *
     * @param now : current time in milliseconds
     * @return true if timeout, false otherwise
     */
    public boolean checkTimeout(long now) {
        long startTimeMillis = getStartTime();
        if (startTimeMillis <= 0) {
            return false;
        }

        long delta = now - startTimeMillis;
        boolean killFlag = false;
        boolean killConnection = false;
        String sql = "";
        if (executor != null) {
            sql = executor.getOriginStmtInString();
        }
        String errMsg = "";
        if (command == MysqlCommand.COM_SLEEP) {
            int waitTimeout = sessionVariable.getWaitTimeoutS();
            if (delta > waitTimeout * 1000L) {
                // Need kill this connection.
                LOG.warn("kill wait timeout connection, remote: {}, wait timeout: {}, query id: {}, sql: {}",
                        getMysqlChannel().getRemoteHostPortString(), waitTimeout, queryId, SqlUtils.sqlPrefix(sql));

                killFlag = true;
                killConnection = true;

                errMsg = String.format("Connection reached its wait timeout of %d seconds", waitTimeout);
            }
        } else {
            long timeoutSecond = getExecTimeout();
            if (delta > timeoutSecond * 1000L) {
                final long pendingTime = getPendingTimeSecond();
                final long execTimeout = getExecTimeoutWithoutPendingTime();
                LOG.warn("kill timeout {}, remote: {}, execute timeout: {}, exec timeout: {}, pending time:{}, " +
                                "query id: {}, sql: {}",
                        getExecType().toLowerCase(), getMysqlChannel().getRemoteHostPortString(), timeoutSecond,
                        execTimeout, pendingTime, queryId, SqlUtils.sqlPrefix(sql));

                // Only kill
                killFlag = true;

                String suggestedMsg = String.format("please increase the '%s' session variable, pending time:%s",
                        isExecLoadType() ? SessionVariable.INSERT_TIMEOUT : SessionVariable.QUERY_TIMEOUT, pendingTime);
                errMsg = ErrorCode.ERR_TIMEOUT.formatErrorMsg(getExecType(), execTimeout, suggestedMsg);
            }
        }
        if (killFlag) {
            kill(killConnection, errMsg);
        }
        return killFlag;
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
        return globalStateMgr.getNodeMgr().getClusterInfo().getAliveBackendNumber();
    }

    public int getTotalBackendNumber() {
        return globalStateMgr.getNodeMgr().getClusterInfo().getTotalBackendNumber();
    }

    public int getAliveComputeNumber() {
        return globalStateMgr.getNodeMgr().getClusterInfo().getAliveComputeNodeNumber();
    }

    /**
     * BackendNode + ComputeNode
     */
    public int getAliveExecutionNodesNumber() {
        return getAliveBackendNumber() +
                (RunMode.isSharedDataMode() ?
                        getGlobalStateMgr().getNodeMgr().getClusterInfo().getAliveComputeNodeNumber() : 0);
    }

    public void setPending(boolean pending) {
        isPending = pending;
    }

    public boolean isPending() {
        return isPending;
    }

    public void setIsForward(boolean forward) {
        isForward = forward;
    }

    public boolean isForward() {
        return isForward;
    }

    public boolean enableSSL() throws IOException {
        SSLChannel sslChannel = new SSLChannelImp(SSLContextLoader.getSslContext().createSSLEngine(), mysqlChannel);
        if (!sslChannel.init()) {
            return false;
        } else {
            mysqlChannel.setSSLChannel(sslChannel);
            return true;
        }
    }

    public StmtExecutor executeSql(String sql) throws Exception {
        StatementBase sqlStmt = SqlParser.parse(sql, getSessionVariable()).get(0);
        sqlStmt.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = StmtExecutor.newInternalExecutor(this, sqlStmt);
        setExecutor(executor);
        setThreadLocalInfo();
        executor.execute();
        return executor;
    }

    /**
     * Bind the context to current scope, exchange the context if it's already existed
     * Sample usage:
     * try (var guard = context.bindScope()) {
     * ......
     * }
     */
    public ScopeGuard bindScope() {
        return ScopeGuard.bind(this);
    }

    // Change current catalog of this session, and reset current database.
    // We can support "use 'catalog <catalog_name>'" from mysql client or "use catalog <catalog_name>" from jdbc.
    public void changeCatalog(String newCatalogName) throws DdlException {
        CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
        newCatalogName = normalizeName(newCatalogName);
        if (!catalogMgr.catalogExists(newCatalogName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
        }
        if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
            try {
                Authorizer.checkAnyActionOnCatalog(this, newCatalogName);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(newCatalogName, this.getCurrentUserIdentity(), this.getCurrentRoleIds(),
                        PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), newCatalogName);
            }
        }
        this.setCurrentCatalog(newCatalogName);
        this.setDatabase("");
    }

    // Change current catalog and database of this session.
    // identifier could be "CATALOG.DB" or "DB".
    // For "CATALOG.DB", we change the current catalog database.
    // For "DB", we keep the current catalog and change the current database.
    public void changeCatalogDb(String identifier) throws DdlException {
        CatalogMgr catalogMgr = globalStateMgr.getCatalogMgr();
        MetadataMgr metadataMgr = globalStateMgr.getMetadataMgr();

        String dbName;

        String[] parts = identifier.split("\\.", 2); // at most 2 parts
        if (parts.length != 1 && parts.length != 2) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, identifier);
        }

        if (parts.length == 1) { // use database
            dbName = identifier;
        } else { // use catalog.database
            String newCatalogName = normalizeName(parts[0]);
            if (!catalogMgr.catalogExists(newCatalogName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_CATALOG_ERROR, newCatalogName);
            }
            if (!CatalogMgr.isInternalCatalog(newCatalogName)) {
                try {
                    Authorizer.checkAnyActionOnCatalog(this, newCatalogName);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(newCatalogName,
                            this.getCurrentUserIdentity(), this.getCurrentRoleIds(),
                            PrivilegeType.ANY.name(), ObjectType.CATALOG.name(), newCatalogName);
                }
            }
            this.setCurrentCatalog(newCatalogName);
            dbName = parts[1];
        }

        dbName = normalizeName(dbName);

        if (!Strings.isNullOrEmpty(dbName) && metadataMgr.getDb(this, this.getCurrentCatalog(), dbName) == null) {
            LOG.debug("Unknown catalog {} and db {}", this.getCurrentCatalog(), dbName);
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // Here we check the request permission that sent by the mysql client or jdbc.
        // So we didn't check UseDbStmt permission in PrivilegeCheckerV2.
        try {
            Authorizer.checkAnyActionOnOrInDb(this, this.getCurrentCatalog(), dbName);
        } catch (AccessDeniedException e) {
            AccessDeniedException.reportAccessDenied(this.getCurrentCatalog(),
                    this.getCurrentUserIdentity(), this.getCurrentRoleIds(),
                    PrivilegeType.ANY.name(), ObjectType.DATABASE.name(), dbName);
        }

        this.setDatabase(dbName);
    }

    public void cleanTemporaryTable() {
        if (sessionId == null) {
            return;
        }
        if (!globalStateMgr.getTemporaryTableMgr().sessionExists(sessionId)) {
            return;
        }
        LOG.debug("clean temporary table on session {}", sessionId);
        try {
            setQueryId(UUIDUtil.genUUID());
            CleanTemporaryTableStmt cleanTemporaryTableStmt = new CleanTemporaryTableStmt(sessionId);
            cleanTemporaryTableStmt.setOrigStmt(
                    new OriginStatement("clean temporary table on session '" + sessionId.toString() + "'"));
            executor = StmtExecutor.newInternalExecutor(this, cleanTemporaryTableStmt);
            executor.execute();
        } catch (Throwable e) {
            LOG.warn("Failed to clean temporary table on session {}, {}", sessionId, e);
        }
    }

    // We can not make sure the set variables are all valid. Even if some variables are invalid, we should let user continue
    // to execute SQL.
    public void updateByUserProperty(UserProperty userProperty) {
        try {
            // set session variables
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
                SystemVariable variable = new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue()));
                globalStateMgr.getVariableMgr().setSystemVariable(sessionVariable, variable, true);
            }

            // set catalog and database
            String catalog = userProperty.getCatalog();
            String database = userProperty.getDatabase();
            if (catalog.equals(UserProperty.CATALOG_DEFAULT_VALUE)) {
                if (!database.equals(UserProperty.DATABASE_DEFAULT_VALUE)) {
                    changeCatalogDb(userProperty.getCatalogDbName());
                }
            } else {
                if (database.equals(UserProperty.DATABASE_DEFAULT_VALUE)) {
                    changeCatalog(catalog);
                } else {
                    changeCatalogDb(userProperty.getCatalogDbName());
                }
                SystemVariable variable = new SystemVariable(SessionVariable.CATALOG, new StringLiteral(catalog));
                globalStateMgr.getVariableMgr().setSystemVariable(sessionVariable, variable, true);
            }
        } catch (Exception e) {
            LOG.warn("set session env failed: ", e);
            // In handshake, we will send error message to client. But it seems that client will ignore it.
            getState().setOk(0L, 0,
                    String.format("set session variables from user property failed: %s", e.getMessage()));
        }
    }

    public boolean isLeaderTransferred() {
        return isLeaderTransferred;
    }

    public void setIsLeaderTransferred(boolean isLeaderTransferred) {
        this.isLeaderTransferred = isLeaderTransferred;
    }

    /**
     * Set thread-local context for the scope, and remove it after leaving the scope
     */
    public static class ScopeGuard implements AutoCloseable {

        private boolean set = false;
        private ConnectContext prev;

        private ScopeGuard() {
        }

        private static ScopeGuard bind(ConnectContext session) {
            ScopeGuard res = new ScopeGuard();
            res.prev = exchangeThreadLocalInfo(session);
            res.set = true;
            return res;
        }

        public ConnectContext prev() {
            return prev;
        }

        @Override
        public void close() {
            if (set) {
                ConnectContext.remove();
            }
            if (prev != null) {
                prev.setThreadLocalInfo();
            }
        }
    }

    public class ThreadInfo {
        public boolean isRunning() {
            return state.isRunning();
        }

        public List<String> toRow(long nowMs, boolean full) {
            List<String> row = Lists.newArrayList();
            row.add("" + connectionId);
            row.add(ClusterNamespace.getNameFromFullName(qualifiedUser));
            // Ip + port
            if (ConnectContext.this instanceof HttpConnectContext) {
                String remoteAddress = ((HttpConnectContext) (ConnectContext.this)).getRemoteAddress();
                row.add(remoteAddress);
            } else {
                row.add(getMysqlChannel().getRemoteHostPortString());
            }
            row.add(ClusterNamespace.getNameFromFullName(currentDb));
            // Command
            row.add(command.toString());
            // connection start Time
            row.add(TimeUtils.longToTimeString(connectionStartTime));
            // Time
            row.add("" + (nowMs - getStartTime()) / 1000);
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
            if (isForward) {
                // if query is forward to leader, we can't know its accurate status in query queue,
                // so isPending should not be displayed
                row.add("");
            } else {
                row.add(Boolean.toString(isPending));
            }
            // warehouse
            row.add(sessionVariable.getWarehouseName());
            // cngroup
            row.add(getCurrentComputeResourceName());
            // catalog
            row.add(sessionVariable.getCatalog());
            // query id
            row.add(queryId == null ? null : queryId.toString());
            return row;
        }
    }

    public boolean isIdleLastFor(long milliSeconds) {
        if (command != MysqlCommand.COM_SLEEP) {
            return false;
        }

        return endTime.isAfter(startTime)
                && endTime.plusMillis(milliSeconds).isBefore(Instant.now());
    }

    public long getCurrentThreadAllocatedMemory() {
        return currentThreadAllocatedMemory.get();
    }

    public void setCurrentThreadAllocatedMemory(long currentThreadAllocatedMemory) {
        this.currentThreadAllocatedMemory.set(currentThreadAllocatedMemory);
    }

    public long getCurrentThreadId() {
        if (currentThreadId == null) {
            return 0;
        }
        return currentThreadId.get();
    }

    public void setCurrentThreadId(long currentThreadId) {
        this.currentThreadId = new AtomicLong(currentThreadId);
    }

    public interface Listener {
        /**
         * Trigger when query is finished
         */
        void onQueryFinished(ConnectContext state);
    }

    public void registerListener(Listener listener) {
        this.listeners.add(listener);
    }

    public void onQueryFinished() {
        for (Listener listener : listeners) {
            try {
                listener.onQueryFinished(this);
            } catch (Exception e) {
                // ignore
                LOG.warn("onQueryFinished error", e);
            }
        }

        try {
            auditEventBuilder.setCNGroup(getCurrentComputeResourceName());
        } catch (Exception e) {
            LOG.warn("set cn group name failed", e);
        }
    }
}
