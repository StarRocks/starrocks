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

package com.starrocks.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Version;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.starrocks.StarRocksRemoteScanWire;
import com.starrocks.http.HttpUtils;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.RemoteScanResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.CloneExpr;
import com.starrocks.sql.ast.expression.DictMappingExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TAccessPathType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStarRocksRemoteScanOutput;
import com.starrocks.thrift.TStarRocksRemoteScanRequiredOutput;
import com.starrocks.thrift.TStarRocksRemoteScanWireShape;
import com.starrocks.thrift.TStarRocksScanTransport;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeDeserializer;
import com.starrocks.type.TypeSerializer;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Business logic for the StarRocks catalog control plane, invoked from the HTTP
 * action {@code StarRocksCatalogControlAction}. The wire format is JSON; the HTTP
 * layer (RestBaseAction) authenticates the caller and hands an authenticated
 * {@link ConnectContext} to each entry point, which then runs the same per-object
 * authorization as before. The plan-carrying request fields are converted from their
 * wire DTOs to the thrift plan types the BE consumes
 * ({@link TStarRocksRemoteScanRequiredOutput}, {@link TStarRocksRemoteScanOutput})
 * once at the top of {@code createAndPrepareRemoteScan}.
 */
public final class StarRocksRemoteScanService extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StarRocksRemoteScanService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_AUTHORIZED = 401;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_ERROR = 500;

    private static final Gson GSON = new Gson();
    private static final String CONTROL_PATH_PREFIX = "/api/_starrocks_remote";
    // Bounds the entry FE's per-peer forwarding effort to roughly what the calling side
    // waits for (the client's default control-plane http timeout is also 10s).
    // Below this, a slice of the forwarding budget cannot even complete a TCP handshake, so the
    // probe stops instead of burning the remainder on calls that are guaranteed to time out.
    private static final int MIN_FORWARD_SLICE_MS = 500;

    // Prepared / running remote-scan sessions on this FE, keyed by session id. Owned by the
    // GlobalStateMgr-held instance; the inherited daemon thread sweeps expired ones once a second.
    private final Map<String, RemoteScanSession> remoteSessions = new ConcurrentHashMap<>();

    public StarRocksRemoteScanService() {
        super("remote-scan-http-session-cleaner", 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        cleanupExpiredSessions();
    }

    public StarRocksRemoteScanWire.CapabilitiesResponse getCapabilities(ConnectContext authedContext) {
        StarRocksRemoteScanWire.CapabilitiesResponse response = new StarRocksRemoteScanWire.CapabilitiesResponse();
        response.status = STATUS_OK;
        response.clusterId = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterId();
        response.version = Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH;
        response.supportedTransports = ImmutableList.of(
                StarRocksRemoteScanWire.TRANSPORT_ARROW_FLIGHT, StarRocksRemoteScanWire.TRANSPORT_BRPC_CHUNK);
        response.featureFlags = ImmutableList.of("remote_scan", "catalog_stats");
        // Only the Arrow Flight port is cluster-wide: the brpc endpoint of every serving BE
        // travels per stream in the prepare_scan response instead.
        response.arrowFlightPort = Config.arrow_flight_port;
        return response;
    }

    public StarRocksRemoteScanWire.ListDatabasesResponse listDatabases(ConnectContext authedContext) {
        StarRocksRemoteScanWire.ListDatabasesResponse response = new StarRocksRemoteScanWire.ListDatabasesResponse();
        ConnectContext context = buildInnerContext(authedContext);
        response.status = STATUS_OK;
        response.databases = GlobalStateMgr.getCurrentState().getLocalMetastore().getAllDbs().stream()
                .map(Database::getFullName)
                .filter(dbName -> hasDbVisibility(context, dbName))
                .collect(Collectors.toList());
        return response;
    }

    public StarRocksRemoteScanWire.ListTablesResponse listTables(ConnectContext authedContext, String db) {
        StarRocksRemoteScanWire.ListTablesResponse response = new StarRocksRemoteScanWire.ListTablesResponse();
        if (Strings.isNullOrEmpty(db)) {
            response.status = STATUS_BAD_REQUEST;
            response.exception = "missing db";
            return response;
        }
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db);
        ConnectContext context = buildInnerContext(authedContext);
        // Answer "not found" for a database the caller may not see, so that a 404 versus an empty
        // listing cannot be used to enumerate database names.
        if (database == null || !hasDbVisibility(context, db)) {
            response.status = STATUS_NOT_FOUND;
            response.exception = "database not found: " + db;
            return response;
        }
        response.status = STATUS_OK;
        // Only native tables can be scanned (prepare_scan rejects everything else), so listing a
        // remote view here would make it show up in SHOW TABLES and then fail at planning.
        response.tables = database.getTables().stream()
                .filter(Table::isNativeTableOrMaterializedView)
                .filter(table -> hasTableVisibility(context, database.getFullName(), table))
                .map(Table::getName)
                .collect(Collectors.toList());
        return response;
    }

    public StarRocksRemoteScanWire.GetTableResponse getTable(ConnectContext authedContext, String db,
                                                                    String table) {
        StarRocksRemoteScanWire.GetTableResponse response = new StarRocksRemoteScanWire.GetTableResponse();
        if (Strings.isNullOrEmpty(db) || Strings.isNullOrEmpty(table)) {
            response.status = STATUS_BAD_REQUEST;
            response.exception = "missing db or table";
            return response;
        }
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db);
        Table olapTable = database == null ? null : database.getTable(table);
        if (olapTable == null) {
            response.status = STATUS_NOT_FOUND;
            response.exception = "table not found: " + db + "." + table;
            return response;
        }
        try {
            ConnectContext context = buildInnerContext(authedContext);
            checkTableVisibility(context, db, table);
            response.status = STATUS_OK;
            response.table = buildWireTable(db, table, database, olapTable);
        } catch (AccessDeniedException e) {
            // Deliberately the same answer as a missing table: telling an unauthorized caller
            // that the table exists turns this endpoint into a name-enumeration oracle.
            LOG.info("hiding table {}.{} from an unauthorized caller as not found", db, table);
            response.status = STATUS_NOT_FOUND;
            response.exception = "table not found: " + db + "." + table;
        } catch (StarRocksException e) {
            response.status = STATUS_BAD_REQUEST;
            response.exception = e.getMessage();
        }
        return response;
    }

    public StarRocksRemoteScanWire.PrepareScanResponse prepareRemoteScan(
            ConnectContext authedContext, StarRocksRemoteScanWire.PrepareScanRequest request) {
        StarRocksRemoteScanWire.PrepareScanResponse response = new StarRocksRemoteScanWire.PrepareScanResponse();
        ConnectContext previousContext = ConnectContext.get();
        try {
            validatePrepareRemoteScanRequest(request);
            ConnectContext context = buildInnerContext(authedContext);
            context.setThreadLocalInfo();
            applyRemoteSessionVars(context, request.sessionVars);
            Authorizer.checkTableAction(context, request.db, request.table, PrivilegeType.SELECT);

            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(request.db);
            Table olapTable = database == null ? null : database.getTable(request.table);
            if (olapTable == null) {
                response.status = STATUS_NOT_FOUND;
                response.exception = "table not found: " + request.db + "." + request.table;
                return response;
            }
            if (!olapTable.isNativeTableOrMaterializedView()) {
                response.status = STATUS_BAD_REQUEST;
                response.exception = "remote scan only supports native tables";
                return response;
            }
            StarRocksRemoteScanWire.Table wireTable = buildWireTable(request.db, request.table, database, olapTable);
            // Drift detection compares the two sides' schema_version as values, not sentinels.
            // A freshly created OlapTable legitimately starts at schema_version=0; the first
            // ALTER bumps it to 1+. When both sides report 0 they truly agree. The mismatch
            // check below still catches real drift such as request=0 vs remote=1 (remote was
            // ALTERed after Local FE cached metadata).
            if (request.schemaVersion != wireTable.schemaVersion) {
                response.status = STATUS_BAD_REQUEST;
                response.exception = "schema version changed: expected "
                        + request.schemaVersion + ", actual " + wireTable.schemaVersion;
                return response;
            }

            RemoteScanContext remoteScanContext =
                    createAndPrepareRemoteScan(context, request, columnTypesOf(olapTable));
            registerRemoteScan(remoteScanContext);

            LOG.info("StarRocks remote scan prepared: db={} table={} session={} streams={}",
                    request.db, request.table, remoteScanContext.sessionId, remoteScanContext.streams.size());

            response.status = STATUS_OK;
            response.sessionId = remoteScanContext.sessionId;
            response.scanToken = remoteScanContext.scanTokenPrefix;
            response.remoteBes = remoteScanContext.streams.stream()
                    .map(stream -> stream.remoteBe)
                    .collect(Collectors.toList());
            response.streams = remoteScanContext.streams;
            response.outputSchema = toOutputSchema(wireTable, request.requiredColumns);
            response.outputs = remoteScanContext.outputs.stream()
                    .map(StarRocksRemoteScanWire::toDto)
                    .collect(Collectors.toList());
            response.properties = Collections.singletonMap("query_id", remoteScanContext.queryIdString);
            return response;
        } catch (AccessDeniedException e) {
            response.status = STATUS_NOT_AUTHORIZED;
            response.exception = e.getMessage();
            return response;
        } catch (StarRocksException e) {
            response.status = STATUS_BAD_REQUEST;
            response.exception = e.getMessage();
            return response;
        } catch (Exception e) {
            LOG.warn("failed to prepare remote scan", e);
            response.status = STATUS_ERROR;
            response.exception = "failed to prepare remote scan: " + e.getMessage();
            return response;
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    public StarRocksRemoteScanWire.SimpleResponse startRemoteScan(
            ConnectContext authedContext, StarRocksRemoteScanWire.ScanControlRequest request,
            boolean forwardRequest, String authorizationHeader) {
        StarRocksRemoteScanWire.SimpleResponse response = new StarRocksRemoteScanWire.SimpleResponse();
        try {
            validateControlRequest(request);
            if (!remoteSessions.containsKey(request.sessionId)) {
                // Sessions live in the memory of the one FE that served prepare_scan. Behind a
                // load balancer that request may have landed on a different FE, so the entry FE
                // looks the session up on its peers; STATUS_NOT_FOUND (as opposed to a processing
                // failure on the FE that owns the session) is what tells the forwarder to keep
                // trying the next peer.
                if (forwardRequest) {
                    return forwardStartScan(request, authorizationHeader);
                }
                response.status = STATUS_NOT_FOUND;
                response.exception = "remote scan session not found: " + request.sessionId;
                return response;
            }
            ConnectContext context = buildInnerContext(authedContext);
            checkSessionOwner(request.sessionId, context);
            startRemoteScanSession(request.sessionId);
            response.status = STATUS_OK;
            return response;
        } catch (AccessDeniedException e) {
            response.status = STATUS_NOT_AUTHORIZED;
            response.exception = e.getMessage();
            return response;
        } catch (StarRocksException e) {
            response.status = STATUS_ERROR;
            response.exception = e.getMessage();
            return response;
        } catch (Exception e) {
            LOG.warn("failed to start remote scan", e);
            response.status = STATUS_ERROR;
            response.exception = "failed to start remote scan: " + e.getMessage();
            return response;
        }
    }

    public StarRocksRemoteScanWire.SimpleResponse batchCleanupScanSessions(
            ConnectContext authedContext, StarRocksRemoteScanWire.BatchCleanupRequest request,
            boolean forwardRequest, String authorizationHeader) {
        StarRocksRemoteScanWire.SimpleResponse response = new StarRocksRemoteScanWire.SimpleResponse();
        if (request == null) {
            response.status = STATUS_BAD_REQUEST;
            response.exception = "empty batch cleanup request";
            return response;
        }
        ConnectContext context = buildInnerContext(authedContext);
        if (request.items == null || request.items.isEmpty()) {
            response.status = STATUS_OK;
            return response;
        }
        List<String> failures = new ArrayList<>();
        for (StarRocksRemoteScanWire.CleanupItem item : request.items) {
            if (item == null || Strings.isNullOrEmpty(item.sessionId)) {
                failures.add((item == null ? "null" : item.sessionId) + ": missing session_id");
                continue;
            }
            try {
                checkSessionOwner(item.sessionId, context);
            } catch (AccessDeniedException e) {
                failures.add(item.sessionId + ": " + e.getMessage());
                continue;
            }
            try {
                // A clean client finish (cancel=false) tears the query down with the engine's
                // internal QUERY_FINISHED semantics: any producer still running (e.g. blocked on
                // a full result queue after a client-side LIMIT) is stopped all the same, but the
                // query is not recorded as user-cancelled.
                cleanupRemoteScanSession(item.sessionId,
                        item.cancel ? PPlanFragmentCancelReason.USER_CANCEL : PPlanFragmentCancelReason.QUERY_FINISHED,
                        item.cancel ? "cancel remote scan session" : "release remote scan session");
            } catch (Exception e) {
                LOG.warn("failed to cleanup remote scan session {}", item.sessionId, e);
                failures.add(item.sessionId + ": " + e.getMessage());
            }
        }
        if (forwardRequest) {
            // Sessions in one batch may live on different FEs (a load balancer spreads the
            // prepare_scan calls), and cleaning an unknown session is a no-op, so the batch is
            // broadcast to every peer instead of searched for sequentially like start_scan.
            forwardCleanupToPeers(request, authorizationHeader, failures);
        }
        if (failures.isEmpty()) {
            response.status = STATUS_OK;
        } else {
            response.status = STATUS_ERROR;
            response.exception = String.join("; ", failures);
        }
        return response;
    }

    /**
     * Per-peer slice of the shared forwarding budget: the remaining time divided by the number of
     * peers still to probe. Dividing (rather than giving every peer the full budget) is what keeps
     * one alive-but-unresponsive peer from consuming the whole budget and starving the FE that
     * actually owns the session.
     */
    @VisibleForTesting
    static int forwardSliceMs(long deadlineMs, int remainingPeers) {
        if (remainingPeers <= 0) {
            return 0;
        }
        return (int) Math.max(0, (deadlineMs - System.currentTimeMillis()) / remainingPeers);
    }

    private static StarRocksRemoteScanWire.SimpleResponse forwardStartScan(
            StarRocksRemoteScanWire.ScanControlRequest request, String authorizationHeader) {
        String body = GSON.toJson(request);
        List<Frontend> peers = peerFrontends();
        long deadlineMs = System.currentTimeMillis() + Config.starrocks_catalog_forward_timeout_ms;
        int probed = 0;
        for (int i = 0; i < peers.size(); i++) {
            int sliceMs = forwardSliceMs(deadlineMs, peers.size() - i);
            if (sliceMs < MIN_FORWARD_SLICE_MS) {
                break;
            }
            Frontend frontend = peers.get(i);
            String url = "http://" + frontend.getHost() + ":" + Config.http_port +
                    CONTROL_PATH_PREFIX + "/start_scan?forward_request=false";
            probed++;
            try {
                StarRocksRemoteScanWire.SimpleResponse peerResponse = GSON.fromJson(
                        forwardPost(url, body, authorizationHeader, sliceMs),
                        StarRocksRemoteScanWire.SimpleResponse.class);
                if (peerResponse.status == STATUS_NOT_FOUND) {
                    // This peer does not hold the session; keep looking.
                    continue;
                }
                // The FE that owns the session answered — success or a real processing
                // failure, final either way.
                return peerResponse;
            } catch (Exception e) {
                // Unreachable peer: its in-memory sessions died with the process (or expire
                // by the sweeper), so it cannot be the live owner — keep looking.
                LOG.warn("failed to forward start_scan to {}", url, e);
            }
        }
        StarRocksRemoteScanWire.SimpleResponse response = new StarRocksRemoteScanWire.SimpleResponse();
        if (probed < peers.size()) {
            // Never report "not found" without having asked everyone: the session may well live on
            // a peer we ran out of budget for, and calling that absence sends whoever debugs this
            // after session expiry instead of after the unresponsive FE.
            response.status = STATUS_ERROR;
            response.exception = "remote scan session lookup exceeded its "
                    + Config.starrocks_catalog_forward_timeout_ms + "ms budget after probing "
                    + probed + " of " + peers.size() + " frontends: " + request.sessionId;
            LOG.warn("{}; unprobed frontends: {}", response.exception,
                    peers.subList(probed, peers.size()).stream()
                            .map(Frontend::getHost).collect(Collectors.toList()));
        } else {
            response.status = STATUS_NOT_FOUND;
            response.exception = "remote scan session not found on any frontend: " + request.sessionId;
        }
        return response;
    }

    private static void forwardCleanupToPeers(StarRocksRemoteScanWire.BatchCleanupRequest request,
                                              String authorizationHeader, List<String> failures) {
        String body = GSON.toJson(request);
        List<Frontend> peers = peerFrontends();
        // Cleanup is a broadcast, so there is no early exit — but it still shares one budget: it
        // runs on the caller's cleanup thread, and 10s per peer would back that queue up.
        long deadlineMs = System.currentTimeMillis() + Config.starrocks_catalog_forward_timeout_ms;
        for (int i = 0; i < peers.size(); i++) {
            int sliceMs = forwardSliceMs(deadlineMs, peers.size() - i);
            if (sliceMs < MIN_FORWARD_SLICE_MS) {
                // Whatever is left on the unvisited peers expires through the session sweeper.
                LOG.warn("cleanup_sessions forwarding exceeded its {}ms budget; skipped frontends: {}",
                        Config.starrocks_catalog_forward_timeout_ms,
                        peers.subList(i, peers.size()).stream().map(Frontend::getHost)
                                .collect(Collectors.toList()));
                break;
            }
            Frontend frontend = peers.get(i);
            String url = "http://" + frontend.getHost() + ":" + Config.http_port +
                    CONTROL_PATH_PREFIX + "/cleanup_sessions?forward_request=false";
            try {
                StarRocksRemoteScanWire.SimpleResponse peerResponse = GSON.fromJson(
                        forwardPost(url, body, authorizationHeader, sliceMs),
                        StarRocksRemoteScanWire.SimpleResponse.class);
                if (peerResponse.status != STATUS_OK) {
                    failures.add("frontend " + frontend.getHost() + ": " + peerResponse.exception);
                }
            } catch (Exception e) {
                // Unreachable peer: nothing to clean there — its in-memory sessions died with
                // the process or expire by the sweeper.
                LOG.warn("failed to forward cleanup_sessions to {}", url, e);
            }
        }
    }

    /**
     * Alive peer frontends, addressed as host + the local {@link Config#http_port}: FE
     * metadata does not track per-FE http ports, the engine assumes a cluster-uniform
     * port (SHOW FRONTENDS prints the local Config.http_port for every row on the same
     * assumption).
     */
    private static List<Frontend> peerFrontends() {
        // Identify self by node name rather than host: several FEs on one host is a normal
        // development / test layout, and comparing hosts would drop those legitimate peers and
        // make their sessions unreachable.
        Frontend self = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf();
        String selfNodeName = self == null ? null : self.getNodeName();
        return GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null).stream()
                .filter(Frontend::isAlive)
                .filter(frontend -> !frontend.getNodeName().equals(selfNodeName))
                .collect(Collectors.toList());
    }

    private static String forwardPost(String url, String jsonBody, String authorizationHeader, int timeoutMs)
            throws Exception {
        StringEntity entity = new StringEntity(jsonBody, StandardCharsets.UTF_8);
        entity.setContentType("application/json");
        HttpPost httpPost = new HttpPost(url);
        // The FE Netty HTTP server never acks Expect: 100-continue; disable it so the POST
        // does not stall for the expect-continue timeout (same as the client-side FeClient).
        httpPost.setConfig(RequestConfig.custom()
                .setExpectContinueEnabled(false)
                .setConnectTimeout(timeoutMs)
                .setSocketTimeout(timeoutMs)
                .setConnectionRequestTimeout(timeoutMs)
                .build());
        httpPost.setEntity(entity);
        if (!Strings.isNullOrEmpty(authorizationHeader)) {
            // Pass the caller's credentials through so the peer authenticates the same user
            // and runs the same session-owner check.
            httpPost.addHeader("Authorization", authorizationHeader);
        }
        try (CloseableHttpResponse httpResponse = HttpUtils.getInstance().execute(httpPost)) {
            int code = httpResponse.getStatusLine().getStatusCode();
            String responseBody = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
            // The control action always replies HTTP 200 with the logical status inside the
            // JSON envelope; anything else is a transport/auth-level failure.
            if (code != 200 || Strings.isNullOrEmpty(responseBody)) {
                throw new StarRocksException("forward http status " + code);
            }
            return responseBody;
        }
    }

    /**
     * Build an inner planning context seeded from the HTTP-authenticated caller. The
     * RestBaseAction has already verified the password; the per-object authorization
     * below runs against this identity.
     */
    private static ConnectContext buildInnerContext(ConnectContext authedContext) {
        ConnectContext context = ConnectContext.buildInner();
        if (authedContext != null) {
            UserIdentity userIdentity = authedContext.getCurrentUserIdentity();
            context.setCurrentUserIdentity(userIdentity);
            // Carry the caller's resolved roles into the planning context. Authorization
            // resolves privileges through currentRoleIds, so copying only the identity (as an
            // earlier revision did) leaves the inner context with no roles and the Authorizer
            // denies everything except always-visible schemas. Prefer the roles the HTTP auth
            // already resolved; fall back to the user's default roles.
            Set<Long> roleIds = authedContext.getCurrentRoleIds();
            if (roleIds != null && !roleIds.isEmpty()) {
                context.setCurrentRoleIds(roleIds);
            } else if (userIdentity != null) {
                context.setCurrentRoleIds(userIdentity);
            }
            if (!Strings.isNullOrEmpty(authedContext.getQualifiedUser())) {
                context.setQualifiedUser(authedContext.getQualifiedUser());
            }
        }
        return context;
    }

    private static void applyRemoteSessionVars(ConnectContext context, Map<String, String> variables)
            throws StarRocksException {
        if (context == null || variables == null || variables.isEmpty()) {
            return;
        }
        SessionVariable sessionVariable = context.getSessionVariable();
        String queryTimeout = variables.get(SessionVariable.QUERY_TIMEOUT);
        if (!Strings.isNullOrEmpty(queryTimeout)) {
            try {
                sessionVariable.setQueryTimeoutS(Integer.parseInt(queryTimeout));
            } catch (NumberFormatException e) {
                // A malformed value is the caller's mistake, so it must surface as a bad request
                // rather than fall through to the generic 500 envelope.
                throw new StarRocksException("invalid remote session variable "
                        + SessionVariable.QUERY_TIMEOUT + ": " + queryTimeout);
            }
        }
        String timeZone = variables.get(SessionVariable.TIME_ZONE);
        if (!Strings.isNullOrEmpty(timeZone)) {
            sessionVariable.setTimeZone(timeZone);
        }
        String enableStrictType = variables.get(SessionVariable.ENABLE_STRICT_TYPE);
        if (!Strings.isNullOrEmpty(enableStrictType)) {
            sessionVariable.setEnableStrictType(Boolean.parseBoolean(enableStrictType));
        }
    }

    private static void validateControlRequest(StarRocksRemoteScanWire.ScanControlRequest request)
            throws StarRocksException {
        if (request == null || Strings.isNullOrEmpty(request.sessionId)) {
            throw new StarRocksException("missing session_id");
        }
    }

    private void checkSessionOwner(String sessionId, ConnectContext context) throws AccessDeniedException {
        RemoteScanOwner currentOwner = RemoteScanOwner.fromContext(context);
        RemoteScanSession session = remoteSessions.get(sessionId);
        if (session != null && !session.owner.matches(currentOwner) && !hasSystemOperatePrivilege(context)) {
            throw new AccessDeniedException("remote scan session owner mismatch: " + sessionId);
        }
    }

    private static boolean hasSystemOperatePrivilege(ConnectContext context) {
        try {
            Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    private static void validatePrepareRemoteScanRequest(StarRocksRemoteScanWire.PrepareScanRequest request)
            throws StarRocksException {
        if (request == null) {
            throw new StarRocksException("empty prepare_remote_scan request");
        }
        if (Strings.isNullOrEmpty(request.db) || Strings.isNullOrEmpty(request.table)) {
            throw new StarRocksException("missing db or table");
        }
    }

    private static boolean hasDbVisibility(ConnectContext context, String dbName) {
        try {
            Authorizer.checkAnyActionOnOrInDb(context, InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    private static boolean hasTableVisibility(ConnectContext context, String dbName, Table table) {
        try {
            Authorizer.checkAnyActionOnTableLikeObject(context, dbName, table);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    private static void checkTableVisibility(ConnectContext context, String dbName, String tableName)
            throws AccessDeniedException, StarRocksException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException("database not found: " + dbName);
        }
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new StarRocksException("table not found: " + dbName + "." + tableName);
        }
        Authorizer.checkAnyActionOnTableLikeObject(context, db.getFullName(), table);
    }

    private static RemoteScanContext createAndPrepareRemoteScan(
            ConnectContext context, StarRocksRemoteScanWire.PrepareScanRequest request,
            Map<String, Type> columnTypes) throws Exception {
        TUniqueId queryId = prepareRemoteQueryId(context);
        String sessionId = Strings.isNullOrEmpty(request.sessionId) ?
                UUID.randomUUID().toString() : request.sessionId;
        String scanTokenPrefix = UUID.randomUUID().toString();
        long expireMs = System.currentTimeMillis() + Math.max(context.getExecTimeout(), 60) * 1000L;
        // Convert the plan-carrying wire fields to their thrift forms once here; the
        // planning helpers below operate on the thrift plan types the BE consumes.
        TStarRocksScanTransport transport = parseTransport(request.transport);
        List<TStarRocksRemoteScanRequiredOutput> requiredOutputs = parseRequiredOutputs(request.requiredOutputs);
        List<ColumnAccessPath> columnAccessPaths = parseColumnAccessPaths(request.columnAccessPaths);
        if (columnAccessPaths.isEmpty()) {
            columnAccessPaths = parseColumnAccessPathsFromRequiredOutputs(requiredOutputs);
        }
        String sql = buildScanSql(request, requiredOutputs, columnTypes);

        ExecPlan execPlan;
        boolean oldSingleNodeExecPlan = context.getSessionVariable().isSingleNodeExecPlan();
        long oldSelectLimit = context.getSessionVariable().getSqlSelectLimit();
        boolean oldEnableJSONV2DictOpt = context.getSessionVariable().isEnableJSONV2DictOpt();
        boolean oldEnableShortCircuit = context.getSessionVariable().isEnableShortCircuit();
        String oldDatabase = context.getDatabase();
        try {
            context.getSessionVariable().setSingleNodeExecPlan(false);
            context.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
            // Remote scan returns serialized chunks instead of mysql result rows. Avoid JSON
            // string dictionary rewrite for synthesized scan plans until that path is stable
            // with RemoteScanResultSink.
            context.getSessionVariable().setEnableJSONV2DictOpt(false);
            // The synthesized SQL has exactly the shape short-circuit reads target (single-table
            // SELECT with a point predicate). A short-circuit plan is meant to run through
            // startScheduling's execShortCircuit() branch, which the deferred-deploy path
            // (prepareExec + deployPreparedFragments) never reaches, so it would be deployed as
            // an ordinary plan. Keep it off regardless of what the source cluster set globally.
            context.getSessionVariable().setEnableShortCircuit(false);
            context.setDatabase(request.db);
            StatementBase statementBase =
                    parseAndValidateRemoteScanStatement(sql, context.getSessionVariable(), request);
            execPlan = StatementPlanner.plan(statementBase, context);
            applyColumnAccessPaths(execPlan, columnAccessPaths);
        } finally {
            context.getSessionVariable().setSingleNodeExecPlan(oldSingleNodeExecPlan);
            context.getSessionVariable().setSqlSelectLimit(oldSelectLimit);
            context.getSessionVariable().setEnableJSONV2DictOpt(oldEnableJSONV2DictOpt);
            context.getSessionVariable().setEnableShortCircuit(oldEnableShortCircuit);
            context.setDatabase(oldDatabase);
        }
        if (execPlan == null || execPlan.getScanNodes().isEmpty() || execPlan.getFragments().isEmpty()) {
            // The remote optimizer collapsed the scan to EMPTYSET (e.g., empty OlapTable or a
            // predicate that constant-folded to false). Return a successful context with no
            // streams; the local FE/BE treat zero streams as "no rows" and emit EOS without
            // ever invoking start_scan or fetch_remote_scan_chunk.
            List<TStarRocksRemoteScanOutput> outputs = buildRemoteScanOutputs(requiredOutputs, null);
            return new RemoteScanContext(sessionId, request.db, request.table, scanTokenPrefix, queryId,
                    DebugUtil.printId(queryId), null, Collections.emptyList(), outputs, expireMs,
                    RemoteScanOwner.fromContext(context));
        }

        List<TStarRocksRemoteScanOutput> outputs = buildRemoteScanOutputs(requiredOutputs, execPlan.getOutputExprs());
        PlanFragment topFragment = execPlan.getTopFragment();
        topFragment.setSink(new RemoteScanResultSink(transport, scanTokenPrefix, expireMs));

        Coordinator coordinator = new DefaultCoordinator.Factory().createQueryScheduler(
                context, execPlan.getFragments(), execPlan.getScanNodes(), execPlan.getDescTbl().toThrift(), execPlan);
        boolean registered = false;
        try {
            QeProcessorImpl.INSTANCE.registerQuery(queryId,
                    new QeProcessorImpl.QueryInfo(context, sql, coordinator));
            registered = true;
            coordinator.setExecPlan(execPlan);
            if (!(coordinator instanceof DefaultCoordinator)) {
                throw new StarRocksException("remote scan requires DefaultCoordinator");
            }
            ((DefaultCoordinator) coordinator).prepareExec();
            List<StarRocksRemoteScanWire.ScanStream> streams =
                    collectRemoteScanStreams(coordinator, transport, scanTokenPrefix);
            if (streams.isEmpty()) {
                throw new StarRocksException("remote scan has no prepared backend");
            }
            return new RemoteScanContext(sessionId, request.db, request.table, scanTokenPrefix, queryId,
                    DebugUtil.printId(queryId), coordinator, streams, outputs, expireMs,
                    RemoteScanOwner.fromContext(context));
        } catch (Exception e) {
            if (registered) {
                cleanupCoordinator(queryId, coordinator, PPlanFragmentCancelReason.INTERNAL_ERROR,
                        "prepare remote scan failed");
            }
            throw e;
        }
    }

    @VisibleForTesting
    static TStarRocksScanTransport parseTransport(String transport) throws StarRocksException {
        // An absent transport means the catalog default, which is brpc_chunk
        // (StarRocksConnectorConfig.SCAN_TRANSPORT). Anything else unrecognized is rejected
        // rather than silently mapped onto a transport whose ports may not even be configured.
        if (Strings.isNullOrEmpty(transport)
                || StarRocksRemoteScanWire.TRANSPORT_BRPC_CHUNK.equalsIgnoreCase(transport)) {
            return TStarRocksScanTransport.STARROCKS_BRPC_CHUNK;
        }
        if (StarRocksRemoteScanWire.TRANSPORT_ARROW_FLIGHT.equalsIgnoreCase(transport)) {
            return TStarRocksScanTransport.STARROCKS_ARROW_FLIGHT;
        }
        throw new StarRocksException("unsupported remote scan transport: " + transport);
    }

    private static List<TStarRocksRemoteScanRequiredOutput> parseRequiredOutputs(
            List<StarRocksRemoteScanWire.RequiredOutput> requiredOutputs) throws StarRocksException {
        if (requiredOutputs == null || requiredOutputs.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            return requiredOutputs.stream()
                    .map(StarRocksRemoteScanWire::toThrift)
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            throw new StarRocksException("invalid remote scan request payload: " + e.getMessage());
        }
    }

    @VisibleForTesting
    static List<ColumnAccessPath> parseColumnAccessPaths(
            List<StarRocksRemoteScanWire.ColumnAccessPathDto> wirePaths) throws StarRocksException {
        if (wirePaths == null || wirePaths.isEmpty()) {
            return Collections.emptyList();
        }
        List<ColumnAccessPath> columnAccessPaths = new ArrayList<>();
        try {
            for (StarRocksRemoteScanWire.ColumnAccessPathDto wirePath : wirePaths) {
                columnAccessPaths.add(StarRocksRemoteScanWire.toDomain(wirePath));
            }
        } catch (IllegalArgumentException e) {
            throw new StarRocksException("invalid remote scan column access path: " + e.getMessage());
        }
        return columnAccessPaths;
    }

    @VisibleForTesting
    static List<ColumnAccessPath> parseColumnAccessPathsFromRequiredOutputs(
            List<TStarRocksRemoteScanRequiredOutput> requiredOutputs) throws StarRocksException {
        if (requiredOutputs == null || requiredOutputs.isEmpty()) {
            return Collections.emptyList();
        }
        List<ColumnAccessPath> columnAccessPaths = new ArrayList<>();
        try {
            for (TStarRocksRemoteScanRequiredOutput output : requiredOutputs) {
                if (output != null && output.wire_shape == TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT &&
                        output.isSetAccess_path()) {
                    columnAccessPaths.add(ColumnAccessPath.fromThrift(output.access_path));
                }
            }
        } catch (IllegalArgumentException e) {
            throw new StarRocksException("invalid remote scan required output access path: " + e.getMessage());
        }
        return columnAccessPaths;
    }

    private static List<TStarRocksRemoteScanOutput> buildRemoteScanOutputs(
            List<TStarRocksRemoteScanRequiredOutput> requiredOutputs, List<Expr> outputExprs)
            throws StarRocksException {
        if (requiredOutputs.isEmpty()) {
            return Collections.emptyList();
        }
        if (outputExprs != null && outputExprs.size() != requiredOutputs.size()) {
            throw new StarRocksException("remote scan output expr count " + outputExprs.size()
                    + " does not match required output count " + requiredOutputs.size());
        }

        List<TStarRocksRemoteScanOutput> outputs = new ArrayList<>();
        for (int i = 0; i < requiredOutputs.size(); i++) {
            TStarRocksRemoteScanRequiredOutput required = requiredOutputs.get(i);
            validateRequiredOutput(required, i);
            Type actualType = outputExprs == null ? TypeDeserializer.fromThrift(required.expected_wire_type) :
                    outputExprs.get(i).getType();
            validateRemoteOutputType(required, actualType, i);

            TStarRocksRemoteScanOutput output = new TStarRocksRemoteScanOutput();
            output.setOutput_index(i);
            output.setLocal_slot_id(required.local_slot_id);
            output.setName("__sr_out_" + i);
            output.setActual_wire_type(TypeSerializer.toThrift(actualType));
            TStarRocksRemoteScanWireShape wireShape = required.wire_shape == null ?
                    TStarRocksRemoteScanWireShape.FULL_ROOT : required.wire_shape;
            output.setNullable(wireShape == TStarRocksRemoteScanWireShape.ROW_MARKER ? false :
                    outputExprs == null || outputExprs.get(i).isNullable());
            output.setIs_const(false);
            output.setWire_shape(wireShape);
            outputs.add(output);
        }
        return outputs;
    }

    @VisibleForTesting
    static void validateRequiredOutput(TStarRocksRemoteScanRequiredOutput output, int outputIndex)
            throws StarRocksException {
        if (output == null) {
            throw new StarRocksException("remote scan required output " + outputIndex + " is null");
        }
        if (!output.isSetLocal_slot_id()) {
            throw new StarRocksException("remote scan required output " + outputIndex + " misses local_slot_id");
        }
        TStarRocksRemoteScanWireShape wireShape = output.wire_shape == null ?
                TStarRocksRemoteScanWireShape.FULL_ROOT : output.wire_shape;
        if (wireShape == TStarRocksRemoteScanWireShape.ROW_MARKER) {
            if (!output.isSetExpected_wire_type()) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " misses expected_wire_type");
            }
            return;
        }
        if (Strings.isNullOrEmpty(output.root_column)) {
            throw new StarRocksException("remote scan required output " + outputIndex + " misses root_column");
        }
        if (!output.isSetExpected_wire_type()) {
            throw new StarRocksException("remote scan required output " + outputIndex + " misses expected_wire_type");
        }
    }

    private static void validateRemoteOutputType(TStarRocksRemoteScanRequiredOutput required, Type actualType,
                                                 int outputIndex) throws StarRocksException {
        Type expectedType = TypeDeserializer.fromThrift(required.expected_wire_type);
        if (!actualType.matchesType(expectedType)) {
            throw new StarRocksException("remote scan output type mismatch at index " + outputIndex
                    + ": expected " + expectedType.toSql() + ", actual " + actualType.toSql());
        }
    }

    private static void applyColumnAccessPaths(ExecPlan execPlan, List<ColumnAccessPath> columnAccessPaths) {
        if (execPlan == null || columnAccessPaths == null || columnAccessPaths.isEmpty()) {
            return;
        }
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            List<ColumnAccessPath> mergedPaths = mergeColumnAccessPaths(scanNode.getColumnAccessPaths(),
                    columnAccessPaths);
            scanNode.setColumnAccessPaths(mergedPaths);
            pruneRemoteScanSlotTypes(scanNode, mergedPaths);
        }
    }

    @VisibleForTesting
    static List<ColumnAccessPath> mergeColumnAccessPaths(List<ColumnAccessPath> plannedPaths,
                                                                 List<ColumnAccessPath> requestedPaths) {
        List<ColumnAccessPath> merged = new ArrayList<>();
        mergeColumnAccessPathsInto(merged, plannedPaths);
        mergeColumnAccessPathsInto(merged, requestedPaths);
        return merged;
    }

    private static void mergeColumnAccessPathsInto(List<ColumnAccessPath> merged,
                                                   List<ColumnAccessPath> incomingPaths) {
        if (incomingPaths == null || incomingPaths.isEmpty()) {
            return;
        }
        for (ColumnAccessPath incoming : incomingPaths) {
            if (incoming == null) {
                continue;
            }
            ColumnAccessPath existing = findColumnAccessPath(merged, incoming);
            if (existing == null) {
                merged.add(cloneColumnAccessPath(incoming));
            } else {
                mergeColumnAccessPath(existing, incoming);
            }
        }
    }

    private static void mergeColumnAccessPath(ColumnAccessPath target, ColumnAccessPath incoming) {
        // AND is deliberate: a path is predicate-only exactly when EVERY contributor is
        // predicate-only, so one output contributor (fromPredicate=false) makes the merged path an
        // output path — which is what outputStructAccessPaths keys off. The children below are
        // unioned, so the pruned struct ends up covering predicate and output subfields alike.
        target.setFromPredicate(target.isFromPredicate() && incoming.isFromPredicate());
        for (ColumnAccessPath incomingChild : incoming.getChildren()) {
            ColumnAccessPath targetChild = findColumnAccessPath(target.getChildren(), incomingChild);
            if (targetChild == null) {
                target.addChildPath(cloneColumnAccessPath(incomingChild));
            } else {
                mergeColumnAccessPath(targetChild, incomingChild);
            }
        }
    }

    private static ColumnAccessPath findColumnAccessPath(List<ColumnAccessPath> paths, ColumnAccessPath target) {
        if (paths == null || target == null) {
            return null;
        }
        for (ColumnAccessPath path : paths) {
            if (path.getType() == target.getType() &&
                    path.isExtended() == target.isExtended() &&
                    path.getPath().equalsIgnoreCase(target.getPath())) {
                return path;
            }
        }
        return null;
    }

    private static ColumnAccessPath cloneColumnAccessPath(ColumnAccessPath path) {
        return ColumnAccessPath.fromThrift(path.toThrift());
    }

    private static void pruneRemoteScanSlotTypes(ScanNode scanNode, List<ColumnAccessPath> columnAccessPaths) {
        Map<String, ColumnAccessPath> outputPaths = outputStructAccessPaths(columnAccessPaths);
        if (outputPaths.isEmpty()) {
            return;
        }
        for (SlotDescriptor slot : scanNode.getDesc().getSlots()) {
            if (!slot.isMaterialized() || slot.getColumn() == null || !slot.getType().isStructType()) {
                continue;
            }
            ColumnAccessPath path = outputPaths.get(slot.getColumn().getName().toLowerCase(Locale.ROOT));
            if (path == null) {
                continue;
            }
            Type prunedType = pruneStructType(slot.getType(), path);
            if (prunedType.isStructType()) {
                slot.setOriginType(prunedType);
                slot.setType(prunedType);
            }
        }
    }

    @VisibleForTesting
    static Type pruneStructType(Type type, ColumnAccessPath path) {
        if (!type.isStructType() || !path.hasChildPath()) {
            return type;
        }
        StructType structType = (StructType) type;
        List<StructField> fields = new ArrayList<>();
        for (StructField field : structType.getFields()) {
            ColumnAccessPath childPath = findChildPath(path, field.getName());
            if (childPath == null || childPath.getType() != TAccessPathType.FIELD) {
                continue;
            }
            Type fieldType = field.getType().clone();
            if (fieldType.isStructType() && childPath.hasChildPath()) {
                fieldType = pruneStructType(fieldType, childPath);
            }
            fields.add(new StructField(field.getName(), field.getFieldId(),
                    field.getFieldPhysicalName(), fieldType, field.getComment()));
        }
        return fields.isEmpty() ? type : new StructType(fields, structType.isNamed());
    }

    private static TUniqueId prepareRemoteQueryId(ConnectContext context) {
        UUID queryUuid = context.getQueryId();
        if (queryUuid == null) {
            queryUuid = UUIDUtil.genUUID();
            context.setQueryId(queryUuid);
        }
        TUniqueId queryId = UUIDUtil.toTUniqueId(queryUuid);
        context.setExecutionId(queryId);
        return queryId;
    }

    private static List<StarRocksRemoteScanWire.ScanStream> collectRemoteScanStreams(
            Coordinator coordinator, TStarRocksScanTransport transport, String scanTokenPrefix)
            throws StarRocksException {
        if (!(coordinator instanceof DefaultCoordinator)) {
            throw new StarRocksException("remote scan requires DefaultCoordinator");
        }
        DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;
        ExecutionFragment rootFragment = defaultCoordinator.getExecutionDAG().getRootFragment();
        List<FragmentInstance> instances = new ArrayList<>(rootFragment.getInstances());
        instances.sort(Comparator.comparing(FragmentInstance::getIndexInFragment));
        List<StarRocksRemoteScanWire.ScanStream> streams = new ArrayList<>();
        for (FragmentInstance instance : instances) {
            TNetworkAddress endpoint = toRemoteBeEndpoint(instance.getWorker(), transport);
            if (endpoint.getPort() <= 0) {
                throw new StarRocksException("invalid remote BE endpoint port: " + endpoint);
            }
            StarRocksRemoteScanWire.ScanStream stream = new StarRocksRemoteScanWire.ScanStream();
            stream.scanToken = scanTokenPrefix + ":" + DebugUtil.printId(instance.getInstanceId());
            stream.remoteBe = StarRocksRemoteScanWire.toDto(endpoint);
            stream.transport = transport == TStarRocksScanTransport.STARROCKS_BRPC_CHUNK
                    ? StarRocksRemoteScanWire.TRANSPORT_BRPC_CHUNK : StarRocksRemoteScanWire.TRANSPORT_ARROW_FLIGHT;
            streams.add(stream);
        }
        return streams;
    }

    private static TNetworkAddress toRemoteBeEndpoint(ComputeNode worker, TStarRocksScanTransport transport) {
        if (transport == TStarRocksScanTransport.STARROCKS_BRPC_CHUNK) {
            return new TNetworkAddress(worker.getHost(), worker.getBrpcPort());
        }
        return new TNetworkAddress(worker.getHost(), worker.getArrowFlightPort());
    }

    @VisibleForTesting
    static String buildScanSql(StarRocksRemoteScanWire.PrepareScanRequest request,
                               List<TStarRocksRemoteScanRequiredOutput> requiredOutputs,
                               Map<String, Type> columnTypes)
            throws StarRocksException {
        // The client always sends at least one required output (a hidden __sr_row_marker
        // BIGINT when no column is materialized, e.g. count(*) / constant projection), and the
        // local BE reads the returned chunks positionally by those outputs. An empty list would
        // otherwise force a "SELECT *" whose schema-order columns would not line up with what
        // the local side expects, silently scrambling the result — reject it loudly instead.
        if (requiredOutputs.isEmpty()) {
            throw new StarRocksException("remote scan request has no required outputs");
        }
        return buildScanSqlWithRequiredOutputs(request, requiredOutputs, columnTypes);
    }

    /** Lowercase column name to its declared type, the trusted source of every emitted name. */
    private static Map<String, Type> columnTypesOf(Table table) {
        Map<String, Type> columnTypes = new HashMap<>();
        for (Column column : table.getBaseSchema()) {
            columnTypes.put(column.getName().toLowerCase(Locale.ROOT), column.getType());
        }
        return columnTypes;
    }

    private static String buildScanSqlWithRequiredOutputs(StarRocksRemoteScanWire.PrepareScanRequest request,
                                                          List<TStarRocksRemoteScanRequiredOutput> requiredOutputs,
                                                          Map<String, Type> columnTypes)
            throws StarRocksException {
        List<String> projections = new ArrayList<>();
        for (int i = 0; i < requiredOutputs.size(); i++) {
            projections.add(buildProjection(requiredOutputs.get(i), i, columnTypes));
        }
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", projections))
                .append(" FROM ").append(quoteIdentifier(request.db))
                .append(".").append(quoteIdentifier(request.table));
        if (request.softLimit > 0) {
            sql.append(" LIMIT ").append(request.softLimit);
        }
        return sql.toString();
    }

    @VisibleForTesting
    static String buildProjection(TStarRocksRemoteScanRequiredOutput output, int outputIndex,
                                  Map<String, Type> columnTypes)
            throws StarRocksException {
        validateRequiredOutput(output, outputIndex);
        TStarRocksRemoteScanWireShape wireShape = output.wire_shape == null ?
                TStarRocksRemoteScanWireShape.FULL_ROOT : output.wire_shape;
        String alias = quoteIdentifier("__sr_out_" + outputIndex);
        if (wireShape == TStarRocksRemoteScanWireShape.ROW_MARKER) {
            Type expectedType = TypeDeserializer.fromThrift(output.expected_wire_type);
            if (!expectedType.matchesType(IntegerType.BIGINT)) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " ROW_MARKER expected BIGINT, actual " + expectedType.toSql());
            }
            return "CAST(1 AS BIGINT) AS " + alias;
        }
        if (wireShape == TStarRocksRemoteScanWireShape.FULL_ROOT) {
            return quoteIdentifier(output.root_column) + " AS " + alias;
        }
        if (wireShape == TStarRocksRemoteScanWireShape.PRUNED_ROOT_STRUCT) {
            if (!output.isSetAccess_path()) {
                throw new StarRocksException("remote scan required output " + outputIndex + " misses access_path");
            }
            ColumnAccessPath path;
            try {
                path = ColumnAccessPath.fromThrift(output.access_path);
            } catch (IllegalArgumentException e) {
                throw new StarRocksException("invalid remote scan required output access path: " + e.getMessage());
            }
            if (path.getType() != TAccessPathType.ROOT || !path.hasChildPath()) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " PRUNED_ROOT_STRUCT requires a root access path with children");
            }
            if (!output.root_column.equalsIgnoreCase(path.getPath())) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " root_column does not match access_path root");
            }
            // Emit field names from the LOCAL schema, never from the request: root_column, the
            // access-path names and expected_wire_type's field names are all caller-controlled
            // strings, and the projection has no deny-list behind it (findUnsafeReason only guards
            // the pushdown WHERE). The access path is used solely to decide WHICH declared fields
            // to keep. The client derives expected_wire_type by pruning the same declared type in
            // the same field order, so validateRemoteOutputType still checks its claim afterwards.
            Type columnType = columnTypes.get(output.root_column.toLowerCase(Locale.ROOT));
            if (columnType == null) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " references unknown column: " + output.root_column);
            }
            if (!columnType.isStructType()) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " PRUNED_ROOT_STRUCT requires a struct column, actual " + columnType.toSql());
            }
            String prunedStruct = buildNamedStructExpr(quoteIdentifier(output.root_column), path, columnType);
            if (prunedStruct == null) {
                throw new StarRocksException("remote scan required output " + outputIndex
                        + " produced empty pruned struct projection");
            }
            return prunedStruct + " AS " + alias;
        }
        throw new StarRocksException("remote scan wire shape is not supported yet: " + wireShape);
    }

    /**
     * Rebuilds the pruned struct from its DECLARED type, keeping only the fields the access path
     * selects. Both the emitted field names and the subfield identifiers come from
     * {@code declaredType}, so a caller-supplied name can never reach the SQL text — it is only
     * matched (case-insensitively) against the declared field names.
     */
    private static String buildNamedStructExpr(String parentExpr, ColumnAccessPath path, Type declaredType) {
        if (declaredType == null || !declaredType.isStructType()) {
            return null;
        }
        List<String> fields = new ArrayList<>();
        for (StructField field : ((StructType) declaredType).getFields()) {
            ColumnAccessPath child = findChildPath(path, field.getName());
            if (child == null || child.getType() != TAccessPathType.FIELD) {
                continue;
            }
            String childExpr = parentExpr + "." + quoteIdentifier(field.getName());
            String nestedExpr = child.hasChildPath() && field.getType().isStructType() ?
                    buildNamedStructExpr(childExpr, child, field.getType()) : null;
            fields.add(quoteStringLiteral(field.getName()));
            fields.add(nestedExpr == null ? childExpr : nestedExpr);
        }
        return fields.isEmpty() ? null : "named_struct(" + String.join(", ", fields) + ")";
    }

    @VisibleForTesting
    static Map<String, ColumnAccessPath> outputStructAccessPaths(List<ColumnAccessPath> columnAccessPaths) {
        if (columnAccessPaths == null || columnAccessPaths.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, ColumnAccessPath> paths = new HashMap<>();
        for (ColumnAccessPath path : columnAccessPaths) {
            if (path.getType() == TAccessPathType.ROOT && path.hasChildPath() &&
                    !path.isFromPredicate() && !path.isExtended()) {
                paths.putIfAbsent(path.getPath().toLowerCase(Locale.ROOT), path);
            }
        }
        return paths;
    }

    @VisibleForTesting
    static StatementBase parseAndValidateRemoteScanStatement(
            String sql, SessionVariable sessionVariable, StarRocksRemoteScanWire.PrepareScanRequest request)
            throws StarRocksException {
        List<StatementBase> statements = SqlParser.parse(sql, sessionVariable);
        if (statements.size() != 1) {
            throw new StarRocksException("remote scan SQL must contain exactly one SELECT statement");
        }
        StatementBase statement = statements.get(0);
        if (!(statement instanceof QueryStatement)) {
            throw new StarRocksException("remote scan SQL must be a SELECT statement");
        }
        if (!(((QueryStatement) statement).getQueryRelation() instanceof SelectRelation)) {
            throw new StarRocksException("remote scan SQL only supports a single-table SELECT");
        }
        SelectRelation select = (SelectRelation) ((QueryStatement) statement).getQueryRelation();
        if (!(select.getRelation() instanceof TableRelation)) {
            throw new StarRocksException("remote scan SQL only supports a single table relation");
        }
        if (select.hasGroupByClause() || select.hasHavingClause() || select.hasOrderByClause() ||
                select.hasWithClause()) {
            throw new StarRocksException("remote scan SQL does not support group by, having, order by, or with");
        }
        TableName tableName = ((TableRelation) select.getRelation()).getName();
        if (!request.db.equalsIgnoreCase(tableName.getDb()) ||
                !request.table.equalsIgnoreCase(tableName.getTbl())) {
            throw new StarRocksException("remote scan SQL table does not match request table");
        }
        if (!Strings.isNullOrEmpty(request.pushdownPredicateSql)) {
            Expr where;
            try {
                where = SqlParser.parseExpression(request.pushdownPredicateSql, sessionVariable);
            } catch (Exception e) {
                throw new StarRocksException(
                        "invalid remote scan pushdown predicate SQL: " + e.getMessage());
            }
            String unsafe = findUnsafeReason(where);
            if (unsafe != null) {
                throw new StarRocksException("remote scan pushdown predicate is unsafe: " + unsafe);
            }
            select.setWhereClause(where);
        }
        return statement;
    }

    /**
     * Deny-list scan of a parsed-but-unanalyzed predicate expression. Mirrors the local-side
     * {@code StarRocksRemotePredicateSerializer} deny list to provide defense in depth: even if
     * the local FE is buggy or replaced, the remote refuses to execute predicates whose
     * subtrees contain unsafe constructs.
     *
     * <p>Returns the first unsafe-reason string found, or {@code null} when every node passes.
     */
    @VisibleForTesting
    static String findUnsafeReason(Expr expr) {
        if (expr instanceof Subquery) {
            return "subquery references another query";
        }
        if (expr instanceof InformationFunction) {
            return "information function refers to local session state";
        }
        if (expr instanceof VariableExpr) {
            return "session variable is not portable across clusters";
        }
        if (expr instanceof DictMappingExpr || expr instanceof DictionaryGetExpr ||
                expr instanceof CloneExpr) {
            return "internal optimizer placeholder: " + expr.getClass().getSimpleName();
        }
        if (expr instanceof FunctionCallExpr fnCall) {
            String name = fnCall.getFunctionName();
            if (name != null && FunctionSet.allNonDeterministicFunctions.contains(name.toLowerCase(Locale.ROOT))) {
                return "non-deterministic function: " + name;
            }
        }
        for (Expr child : expr.getChildren()) {
            String childReason = findUnsafeReason(child);
            if (childReason != null) {
                return childReason;
            }
        }
        return null;
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @VisibleForTesting
    static String quoteStringLiteral(String value) {
        // Backslash MUST be escaped before the quote doubling, and it must be escaped at all:
        // the lexer treats it as an escape character inside single-quoted text
        // (SINGLE_QUOTED_TEXT : '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\''), so a value ending
        // in one would escape its own closing quote and leave the whole synthesized statement
        // unparseable. quoteIdentifier needs no such care: BACKQUOTED_IDENTIFIER does not treat
        // backslash specially.
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    private static ColumnAccessPath findChildPath(ColumnAccessPath path, String childName) {
        for (ColumnAccessPath child : path.getChildren()) {
            if (child.getPath().equalsIgnoreCase(childName)) {
                return child;
            }
        }
        return null;
    }

    @VisibleForTesting
    void registerRemoteScan(RemoteScanContext remoteScanContext) throws StarRocksException {
        RemoteScanSession existingSession = remoteSessions.get(remoteScanContext.sessionId);
        if (existingSession != null && !existingSession.owner.matches(remoteScanContext.owner)) {
            throw new StarRocksException("remote scan session owner mismatch: " + remoteScanContext.sessionId);
        }
        // One session legitimately holds several scans (a query with several StarRocks scan nodes
        // shares one execution id), so registration appends. But a RETRIED prepare_scan arrives
        // with that same session id after an ambiguous failure — the coordinator was built and the
        // response was lost — and startRemoteScanSession starts every context in the session, so
        // the superseded one would scan this cluster a second time with nobody consuming it. Drop
        // the previous not-yet-started context for the same table; anything already started keeps
        // running because the client may be reading its streams.
        List<RemoteScanContext> superseded = new ArrayList<>();
        remoteSessions.compute(remoteScanContext.sessionId, (sessionId, sessionInMap) -> {
            RemoteScanSession session = sessionInMap == null ?
                    new RemoteScanSession(sessionId, remoteScanContext.owner) : sessionInMap;
            session.scans.removeIf(previous -> {
                if (!previous.isSameTable(remoteScanContext) || !previous.trySupersede()) {
                    return false;
                }
                superseded.add(previous);
                return true;
            });
            session.scans.add(remoteScanContext);
            session.expireMs = Math.max(session.expireMs, remoteScanContext.expireMs);
            return session;
        });
        for (RemoteScanContext previous : superseded) {
            LOG.info("superseding an earlier prepared remote scan: session={} db={} table={} query={}",
                    previous.sessionId, previous.db, previous.table, previous.queryIdString);
            cleanupCoordinator(previous.queryId, previous.coordinator, PPlanFragmentCancelReason.INTERNAL_ERROR,
                    "superseded by a newer prepare_scan for the same session and table");
        }
    }

    private void cleanupExpiredSessions() {
        long now = System.currentTimeMillis();
        for (RemoteScanSession session : new ArrayList<>(remoteSessions.values())) {
            if (session.expireMs > 0 && now > session.expireMs) {
                cleanupRemoteScanSession(session.sessionId, PPlanFragmentCancelReason.TIMEOUT,
                        "remote scan session expired");
            }
        }
    }

    private void startRemoteScanSession(String sessionId) throws Exception {
        RemoteScanSession session = remoteSessions.get(sessionId);
        if (session == null || session.scans.isEmpty()) {
            throw new StarRocksException("remote scan session not found: " + sessionId);
        }
        try {
            for (RemoteScanContext remoteScanContext : new ArrayList<>(session.scans)) {
                remoteScanContext.start();
            }
        } catch (Exception e) {
            cleanupRemoteScanSession(sessionId, PPlanFragmentCancelReason.INTERNAL_ERROR,
                    "start remote scan session failed");
            throw e;
        }
    }

    private void cleanupRemoteScanSession(String sessionId, PPlanFragmentCancelReason cancelReason, String reason) {
        RemoteScanSession session = remoteSessions.remove(sessionId);
        if (session == null) {
            return;
        }
        for (RemoteScanContext remoteScanContext : new ArrayList<>(session.scans)) {
            cleanupCoordinator(remoteScanContext.queryId, remoteScanContext.coordinator, cancelReason, reason);
        }
        session.scans.clear();
    }

    private static void cleanupCoordinator(TUniqueId queryId, Coordinator coordinator,
                                           PPlanFragmentCancelReason cancelReason, String reason) {
        if (coordinator == null) {
            // EMPTYSET plans (e.g., empty OlapTable) never reach the coordinator stage; no
            // query was registered with QeProcessorImpl and there is nothing to cancel.
            return;
        }
        try {
            coordinator.cancel(cancelReason, reason);
        } catch (Exception e) {
            LOG.warn("failed to cancel remote scan query {}", DebugUtil.printId(queryId), e);
        } finally {
            QeProcessorImpl.INSTANCE.unMonitorQuery(queryId);
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }

    private static StarRocksRemoteScanWire.Table buildWireTable(String dbName, String tableName,
                                                                Database db, Table table) {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            return toWireTableLocked(dbName, tableName, table);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
    }

    @VisibleForTesting
    static StarRocksRemoteScanWire.Table toWireTableLocked(String dbName, String tableName, Table table) {
        // Expose only the user-visible base schema. getFullSchema() also returns internal
        // columns (schema-change shadow columns and flat-JSON extended columns added by the
        // local optimizer's JsonPathRewriteRule, e.g. "json_col.name"); projecting those to
        // the catalog consumer makes SELECT * request columns the remote cannot produce.
        List<String> partitionColumns = Lists.newArrayList(table.getPartitionColumnNames());
        StarRocksRemoteScanWire.Table wireTable = new StarRocksRemoteScanWire.Table();
        wireTable.db = dbName;
        wireTable.table = tableName;
        wireTable.schemaVersion = getSchemaVersionLocked(table);
        wireTable.columns = toWireColumns(table.getBaseSchema(), partitionColumns);
        wireTable.partitionColumns = partitionColumns;
        wireTable.rowCount = table instanceof OlapTable ? ((OlapTable) table).getRowCount() : 0L;
        // Identity of the table for the consumer's StarRocksExternalTable.getUUID(); unlike the
        // create time it is unique per incarnation, not per second.
        wireTable.tableId = table.getId();
        return wireTable;
    }

    private static long getSchemaVersionLocked(Table table) {
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByMetaId(olapTable.getBaseIndexMetaId());
            if (indexMeta != null) {
                return indexMeta.getSchemaVersion();
            }
        }
        return 0L;
    }

    private static List<StarRocksRemoteScanWire.Column> toOutputSchema(
            StarRocksRemoteScanWire.Table wireTable, List<String> requiredColumns) {
        if (requiredColumns == null || requiredColumns.isEmpty()) {
            return wireTable.columns;
        }
        Map<String, StarRocksRemoteScanWire.Column> columnsByName = new HashMap<>();
        for (StarRocksRemoteScanWire.Column column : wireTable.columns) {
            columnsByName.put(column.name.toLowerCase(Locale.ROOT), column);
        }
        List<StarRocksRemoteScanWire.Column> output = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (String name : requiredColumns) {
            StarRocksRemoteScanWire.Column column = columnsByName.get(name.toLowerCase(Locale.ROOT));
            if (column == null) {
                missing.add(name);
            } else {
                output.add(column);
            }
        }
        if (!missing.isEmpty()) {
            LOG.warn("remote scan output schema dropping columns missing from snapshot " +
                            "(table={}.{}, schema_version={}, missing={})",
                    wireTable.db, wireTable.table, wireTable.schemaVersion, missing);
        }
        return output;
    }

    private static List<StarRocksRemoteScanWire.Column> toWireColumns(List<Column> columns,
                                                                      List<String> partitionColumns) {
        Set<String> partitionColumnNames = partitionColumns.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toCollection(HashSet::new));
        List<StarRocksRemoteScanWire.Column> wireColumns = Lists.newArrayList();
        for (Column column : columns) {
            StarRocksRemoteScanWire.Column wireColumn = new StarRocksRemoteScanWire.Column();
            wireColumn.name = column.getName();
            wireColumn.type = column.getType().toSql().toLowerCase(Locale.ROOT);
            wireColumn.nullable = column.isAllowNull();
            wireColumn.isPartitionColumn =
                    partitionColumnNames.contains(column.getName().toLowerCase(Locale.ROOT));
            wireColumns.add(wireColumn);
        }
        return wireColumns;
    }

    private static class RemoteScanSession {
        private final String sessionId;
        private final RemoteScanOwner owner;
        private final List<RemoteScanContext> scans = new CopyOnWriteArrayList<>();
        private volatile long expireMs;

        private RemoteScanSession(String sessionId, RemoteScanOwner owner) {
            this.sessionId = sessionId;
            this.owner = owner;
        }
    }

    /** Read-only view of a session's prepared scans, for asserting registration behaviour. */
    @VisibleForTesting
    List<RemoteScanContext> preparedScansForTest(String sessionId) {
        RemoteScanSession session = remoteSessions.get(sessionId);
        return session == null ? Collections.emptyList() : ImmutableList.copyOf(session.scans);
    }

    @VisibleForTesting
    static class RemoteScanOwner {
        private final UserIdentity currentUserIdentity;
        private final String qualifiedUser;

        private RemoteScanOwner(UserIdentity currentUserIdentity, String qualifiedUser) {
            this.currentUserIdentity = currentUserIdentity;
            this.qualifiedUser = qualifiedUser;
        }

        static RemoteScanOwner fromContext(ConnectContext context) {
            return new RemoteScanOwner(context.getCurrentUserIdentity(), context.getQualifiedUser());
        }

        private boolean matches(RemoteScanOwner other) {
            return Objects.equals(currentUserIdentity, other.currentUserIdentity) &&
                    Objects.equals(qualifiedUser, other.qualifiedUser);
        }
    }

    private static void runWithRemoteScanContext(ConnectContext context, RemoteScanAction action) throws Exception {
        ConnectContext previousContext = ConnectContext.exchangeThreadLocalInfo(context);
        try {
            action.run();
        } finally {
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @FunctionalInterface
    private interface RemoteScanAction {
        void run() throws Exception;
    }

    @VisibleForTesting
    static class RemoteScanContext {
        private final String sessionId;
        private final String db;
        private final String table;
        private final String scanTokenPrefix;
        private final TUniqueId queryId;
        private final String queryIdString;
        private final Coordinator coordinator;
        private final List<StarRocksRemoteScanWire.ScanStream> streams;
        private final List<TStarRocksRemoteScanOutput> outputs;
        private final long expireMs;
        private final RemoteScanOwner owner;
        private boolean started;
        private boolean superseded;
        private Exception startException;

        RemoteScanContext(String sessionId, String db, String table, String scanTokenPrefix,
                                  TUniqueId queryId, String queryIdString, Coordinator coordinator,
                                  List<StarRocksRemoteScanWire.ScanStream> streams,
                                  List<TStarRocksRemoteScanOutput> outputs, long expireMs,
                                  RemoteScanOwner owner) {
            this.sessionId = sessionId;
            this.db = db;
            this.table = table;
            this.scanTokenPrefix = scanTokenPrefix;
            this.queryId = queryId;
            this.queryIdString = queryIdString;
            this.coordinator = coordinator;
            this.streams = streams;
            this.outputs = outputs;
            this.expireMs = expireMs;
            this.owner = owner;
        }

        boolean isSameTable(RemoteScanContext other) {
            return db.equalsIgnoreCase(other.db) && table.equalsIgnoreCase(other.table);
        }

        /**
         * Atomically claims a not-yet-deployed context so a superseding prepare can drop it.
         * Fails once the context is (or is being) started, in which case the caller must leave
         * it alone — the client is already consuming its streams.
         */
        synchronized boolean trySupersede() {
            if (started || superseded) {
                return false;
            }
            superseded = true;
            return true;
        }

        private synchronized void start() throws Exception {
            if (started) {
                return;
            }
            if (startException != null) {
                // A failed deploy is terminal for this context: never deploy its fragments twice.
                throw startException;
            }
            if (superseded) {
                // Dropped by a later prepare_scan for the same session and table; its
                // coordinator was already cancelled.
                throw new StarRocksException("remote scan context was superseded: " + sessionId);
            }
            if (coordinator == null) {
                // EMPTYSET scan: nothing to deploy, no monitored query. Treat the prepared
                // context as immediately started so subsequent startScanSession calls are
                // no-ops and cleanup proceeds normally.
                started = true;
                return;
            }
            DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;
            try {
                runWithRemoteScanContext(defaultCoordinator.getConnectContext(), () -> {
                    defaultCoordinator.deployPreparedFragments();
                    QeProcessorImpl.INSTANCE.monitorQuery(queryId, expireMs);
                });
            } catch (Exception e) {
                startException = e;
                throw e;
            }
            started = true;
        }
    }

}
