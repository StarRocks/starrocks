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

package com.starrocks.service.arrow.flight.sql;

import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.PFetchArrowSchemaRequest;
import com.starrocks.proto.PFetchArrowSchemaResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpSessionOptionValueVisitor;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlServiceImpl implements FlightSqlProducer, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlServiceImpl.class);
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final ArrowFlightSqlSessionManager sessionManager;
    private final Location feEndpoint;
    private final SqlInfoBuilder sqlInfoBuilder;

    private final ExecutorService executor = ThreadPoolManager
            .newDaemonCacheThreadPool(Config.arrow_max_service_task_threads_num, "arrow-flight-executor", true);

    public ArrowFlightSqlServiceImpl(final ArrowFlightSqlSessionManager sessionManager, final Location feEndpoint) {
        this.sessionManager = sessionManager;
        this.feEndpoint = feEndpoint;
        this.sqlInfoBuilder = new SqlInfoBuilder();
        this.sqlInfoBuilder.withFlightSqlServerName("StarRocks")
                .withFlightSqlServerVersion("1.0.0")
                .withFlightSqlServerArrowVersion("18.0.0")
                .withFlightSqlServerReadOnly(false)
                .withSqlIdentifierQuoteChar("`")
                .withSqlDdlCatalog(true)
                .withSqlDdlSchema(false).withSqlDdlTable(false)
                .withSqlIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
                .withSqlQuotedIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE);
    }

    /**
     * Since Arrow Flight SQL V17.0.0, the client will send a closeSession RPC to the server.
     * - JDBC: The connection URL includes `?catalog=xxx` and call Connection::close().
     * - ADBC: Connection::close().
     * - Native Java FlightSqlClient: FlightSqlClient::closeSession().
     */
    @Override
    public void close() throws Exception {
        AutoCloseables.close(rootAllocator);
    }

    @Override
    public void closeSession(CloseSessionRequest request, CallContext context, StreamListener<CloseSessionResult> listener) {
        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(context.peerIdentity());
        ctx.kill(true, "arrow flight sql close session");
        sessionManager.closeSession(ctx.getArrowFlightSqlToken());
    }

    /**
     * Create a prepared statement.// TODO: Note that, we only support the statement without parameters.
     *
     * <p> JDBC and ODBC always use prepared statements to execute queries, even if the simple Statement::execute() is called.
     * The RPC sequence for Statement::execute() is : createPreparedStatement -> getFlightInfoPreparedStatement
     * -> getStreamStatement.
     * The RPC for Statement::close() is closePreparedStatement.
     */
    @Override
    public void createPreparedStatement(
            FlightSql.ActionCreatePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        executor.submit(() -> {
            try {
                String token = context.peerIdentity();
                ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);

                ctx.reset(request.getQuery());
                // To prevent the client from mistakenly interpreting an empty Schema as an update statement (instead of a query statement),
                // we need to ensure that the Schema returned by createPreparedStatement includes the query metadata.
                // This means we need to correctly set the DatasetSchema and ParameterSchema in ActionCreatePreparedStatementResult.
                // We generate a minimal Schema. This minimal Schema can include an integer column to ensure the Schema is not empty.
                try (VectorSchemaRoot schemaRoot = ArrowUtil.createSingleSchemaRoot("r", "0")) {
                    Schema schema = schemaRoot.getSchema();
                    FlightSql.ActionCreatePreparedStatementResult result =
                            FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                    .setDatasetSchema(ByteString.copyFrom(serializeMetadata(schema)))
                                    .setParameterSchema(ByteString.copyFrom(serializeMetadata(schema))).build();
                    listener.onNext(new Result(Any.pack(result).toByteArray()));
                }
            } catch (Exception e) {
                listener.onError(
                        CallStatus.INTERNAL.withDescription("createPreparedStatement error: " + e.getMessage()).withCause(e)
                                .toRuntimeException());
            } catch (Throwable e) {
                listener.onError(
                        CallStatus.INTERNAL.withDescription("createPreparedStatement unexpected error: " + e.getMessage())
                                .withCause(e).toRuntimeException());

            } finally {
                listener.onCompleted();
            }
        });
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context,
                                       StreamListener<Result> listener) {
        executor.submit(listener::onCompleted);
    }

    /**
     * Execute the prepared statement and return the endpoint of the result stream.
     *
     * <p> Planner and coordinator will be executed in this method.
     */
    @Override
    public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                                     FlightDescriptor descriptor) {
        String token = context.peerIdentity();
        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);
        String database = context.getMiddleware(FlightConstants.HEADER_KEY).headers().get("database");
        if (!StringUtils.isEmpty(database)) {
            ctx.setDatabase(database);
        }
        return getFlightInfoFromQuery(ctx, descriptor);
    }

    /**
     * Execute the normal statement and return the endpoint of the result stream.
     *
     * <p> Planner and coordinator will be executed in this method.
     *
     * <p> This is for the native Arrow Flight Client, such as FlightSqlClient in Java.
     * The RPC sequence is getFlightInfoStatement -> getStreamStatement.
     */
    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command, CallContext context,
                                             FlightDescriptor descriptor) {
        String token = context.peerIdentity();
        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);
        ctx.reset(command.getQuery());
        ctx.setThreadLocalInfo();
        return getFlightInfoFromQuery(ctx, descriptor);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context, ServerStreamListener listener) {
        getStreamResult(ticket.getStatementHandle().toStringUtf8(), listener);
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                           ServerStreamListener listener) {
        getStreamResult(command.getPreparedStatementHandle().toStringUtf8(), listener);
    }

    /**
     * When the JDBC URL contains `?catalog=<catalog_name>`, this method will be called.
     */
    @Override
    public void setSessionOptions(SetSessionOptionsRequest request, CallContext context,
                                  StreamListener<SetSessionOptionsResult> listener) {
        executor.submit(() -> {
            Map<String, SetSessionOptionsResult.Error> errors = Maps.newHashMap();
            request.getSessionOptions().forEach((key, value) -> {
                // Only support set `catalog` for now.
                if (!key.equalsIgnoreCase("catalog")) {
                    errors.put(key, new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_NAME));
                } else {
                    ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(context.peerIdentity());
                    String catalog = value.acceptVisitor(new NoOpSessionOptionValueVisitor<>() {
                        @Override
                        public String visit(String value) {
                            return value;
                        }
                    });
                    if (catalog == null) {
                        errors.put(key, new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE));
                    } else {
                        try {
                            ctx.changeCatalog(catalog);
                        } catch (DdlException e) {
                            LOG.warn("[ARROW] Failed to change catalog to {} [queryID={}]",
                                    catalog, DebugUtil.printId(ctx.getExecutionId()), e);
                            errors.put(key, new SetSessionOptionsResult.Error(SetSessionOptionsResult.ErrorValue.INVALID_VALUE));
                        }
                    }
                }
            });

            listener.onNext(new SetSessionOptionsResult(errors));
            listener.onCompleted();
        });
    }

    /**
     * When creating connection in ADBC, getFlightInfoSqlInfo and getStreamSqlInfo will be called.
     * When creating connection in JDBC, they will not be called.
     */
    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context,
                                           FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
        sqlInfoBuilder.send(command.getInfoList(), listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo command, CallContext context,
                                            FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs command, CallContext context,
                                            FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas command, CallContext context,
                                           FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables command, CallContext context, FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!command.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return buildFlightInfoFromFE(command, descriptor, schemaToUse);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes command, CallContext context,
                                              FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                                FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                                FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                                  FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                               FlightDescriptor descriptor) {
        return buildFlightInfoFromFE(command, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context,
                                           FlightDescriptor descriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getSchemaStatement unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context, FlightStream flightStream,
                                       StreamListener<PutResult> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutStatement unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command, CallContext context,
                                                     FlightStream flightStream, StreamListener<PutResult> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementUpdate unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery,
                                                    CallContext context, FlightStream flightStream,
                                                    StreamListener<PutResult> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementQuery unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo command, CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTypeInfo unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCatalogs unimplemented").toRuntimeException();

    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamSchemas unimplemented").toRuntimeException();

    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTables unimplemented").toRuntimeException();

    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTableTypes unimplemented").toRuntimeException();

    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                     ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamPrimaryKeys unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamExportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamImportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                        ServerStreamListener listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCrossReference unimplemented").toRuntimeException();
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("listFlights unimplemented").toRuntimeException();
    }

    protected static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            // Convert to Arrow IPC format.
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    private void getStreamResult(String ticket, ServerStreamListener listener) {
        String[] ticketParts = ticket.split(":");
        String token = ticketParts[0];
        String queryId = ticketParts[1];

        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);
        VectorSchemaRoot vectorSchemaRoot = ctx.getResult(queryId);
        if (vectorSchemaRoot == null) {
            throw CallStatus.NOT_FOUND.withDescription("cannot find result of the query [" + queryId + "]").toRuntimeException();
        }

        listener.setOnCancelHandler(ctx::cancelQuery);

        listener.start(vectorSchemaRoot);
        listener.putNext();
        listener.completed();

        ctx.removeResult(queryId);
    }

    protected FlightInfo getFlightInfoFromQuery(ArrowFlightSqlConnectContext ctx, FlightDescriptor descriptor) {
        try {
            CompletableFuture<Void> processorFinished = new CompletableFuture<>();
            executor.submit(() -> {
                try {
                    StatementBase parsedStmt = parse(ctx.getQuery(), ctx.getSessionVariable());
                    ctx.setStatement(parsedStmt);
                    ctx.setThreadLocalInfo();

                    ArrowFlightSqlConnectProcessor processor = new ArrowFlightSqlConnectProcessor(ctx);
                    processor.processOnce();

                    ctx.setDeploymentFinished(null);
                    processorFinished.complete(null);
                } catch (Exception e) {
                    processorFinished.completeExceptionally(e);
                } catch (Throwable t) {
                    processorFinished.completeExceptionally(t);
                }
            });

            processorFinished.get();
            // ------------------------------------------------------------------------------------
            // FE task will return FE as endpoint.
            // ------------------------------------------------------------------------------------
            if (ctx.returnFromFE() || ctx.isFromFECoordinator()) {
                if (ctx.getState().isError()) {
                    throw new RuntimeException(String.format("failed to process query [queryID=%s] [error=%s]",
                            DebugUtil.printId(ctx.getExecutionId()),
                            ctx.getState().getErrorMessage()));
                }
                String queryId = DebugUtil.printId(ctx.getExecutionId());
                if (ctx.getResult(queryId) == null) {
                    ctx.setEmptyResultIfNotExist(queryId);
                }
                final ByteString handle = buildFETicket(ctx);

                FlightSql.TicketStatementQuery ticketStatement =
                        FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
                return buildFlightInfoFromFE(ticketStatement, descriptor, ctx.getResult(queryId).getSchema());
            }

            // ------------------------------------------------------------------------------------
            // Query task will wait until deployment to BE is finished and return BE as endpoint.
            // ------------------------------------------------------------------------------------
            SessionVariable sv = ctx.getSessionVariable();
            Coordinator coordinator = ctx.waitForDeploymentFinished(sv.getQueryTimeoutS() * 1000L);
            if (coordinator == null || ctx.getState().isError()) {
                throw new RuntimeException(String.format("failed to process query [queryID=%s] [error=%s]",
                        DebugUtil.printId(ctx.getExecutionId()),
                        ctx.getState().getErrorMessage()));
            }

            if (!(coordinator instanceof DefaultCoordinator)) {
                throw new RuntimeException("Coordinator is not DefaultCoordinator, cannot proceed with BE execution.");
            }
            DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;

            ExecutionFragment rootFragment = defaultCoordinator.getExecutionDAG().getRootFragment();
            FragmentInstance rootFragmentInstance = rootFragment.getInstances().get(0);
            ComputeNode worker = rootFragmentInstance.getWorker();
            TUniqueId rootFragmentInstanceId = rootFragmentInstance.getInstanceId();

            // Fetch arrow schema from BE.
            PUniqueId pInstanceId = new PUniqueId();
            pInstanceId.setHi(rootFragmentInstanceId.getHi());
            pInstanceId.setLo(rootFragmentInstanceId.getLo());
            long timeoutMs = Math.min(sv.getQueryDeliveryTimeoutS(), sv.getQueryTimeoutS()) * 1000L;
            Schema schema = fetchArrowSchema(ctx, worker.getBrpcAddress(), pInstanceId, timeoutMs);

            // Build BE ticket.
            final ByteString handle = buildBETicket(defaultCoordinator.getQueryId(), rootFragmentInstanceId);
            FlightSql.TicketStatementQuery ticketStatement =
                    FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
            Location endpoint = Location.forGrpcInsecure(worker.getHost(), worker.getArrowFlightPort());
            return buildFlightInfo(ticketStatement, descriptor, schema, endpoint);
        } catch (Exception e) {
            LOG.warn("[ARROW] failed to getFlightInfoFromQuery [queryID={}]", DebugUtil.printId(ctx.getExecutionId()), e);
            throw CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
        }
    }

    private static ByteString buildFETicket(ArrowFlightSqlConnectContext ctx) {
        // FETicket: <Token> : <QueryId>
        return ByteString.copyFromUtf8(ctx.getArrowFlightSqlToken() + ":" + DebugUtil.printId(ctx.getExecutionId()));
    }

    private static ByteString buildBETicket(TUniqueId queryId, TUniqueId rootFragmentInstanceId) {
        // BETicket: <QueryId> : <FragmentInstanceId>
        return ByteString.copyFromUtf8(hexStringFromUniqueId(queryId) + ":" + hexStringFromUniqueId(rootFragmentInstanceId));
    }

    private <T extends Message> FlightInfo buildFlightInfoFromFE(T request, FlightDescriptor descriptor,
                                                                 Schema schema) {
        return buildFlightInfo(request, descriptor, schema, feEndpoint);
    }

    protected  <T extends Message> FlightInfo buildFlightInfo(T request, FlightDescriptor descriptor,
                                                           Schema schema, Location endpoint) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, endpoint));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    protected Schema fetchArrowSchema(ConnectContext ctx, TNetworkAddress brpcAddress, PUniqueId rootInstanceId, long timeoutMs) {
        PFetchArrowSchemaRequest request = new PFetchArrowSchemaRequest();
        request.setFinstId(rootInstanceId);

        try {
            Future<PFetchArrowSchemaResult> resFuture = BackendServiceClient.getInstance().fetchArrowSchema(brpcAddress, request);
            PFetchArrowSchemaResult res = resFuture.get(timeoutMs, TimeUnit.MILLISECONDS);

            try (RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);
                    ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(res.getSchema()), rootAllocator)) {
                return reader.getVectorSchemaRoot().getSchema();
            }
        } catch (Exception e) {
            String errorMsg = String.format(
                    "Failed to fetchArrowSchema [queryID=%s] [rootInstanceId=%s] [brpcAddress=%s:%d] [msg=%s]",
                    DebugUtil.printId(ctx.getExecutionId()), DebugUtil.printId(rootInstanceId), brpcAddress.getHostname(),
                    brpcAddress.getPort(), e.getMessage());
            LOG.warn("[ARROW] {}", errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    protected StatementBase parse(String sql, SessionVariable sessionVariables) {
        List<StatementBase> stmts;

        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, sessionVariables);
        // TODO: Support multiple stmts, as long as at most one stmt needs to return the result.
        if (stmts.size() > 1) {
            throw new RuntimeException("arrow flight sql query does not support execute multiple query");
        }

        StatementBase parsedStmt = stmts.get(0);
        parsedStmt.setOrigStmt(new OriginStatement(sql));
        return parsedStmt;
    }

    private static String hexStringFromUniqueId(final TUniqueId id) {
        if (id == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(Long.toHexString(id.hi)).append("-").append(Long.toHexString(id.lo));
        return builder.toString();
    }
}
