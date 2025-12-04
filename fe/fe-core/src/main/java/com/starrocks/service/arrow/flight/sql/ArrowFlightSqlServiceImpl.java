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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
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
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ArrowFlightSqlServiceImpl implements FlightSqlProducer, AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlServiceImpl.class);
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final ArrowFlightSqlSessionManager sessionManager;
    private final Location feEndpoint;
    private final SqlInfoBuilder sqlInfoBuilder;

    private static final ExecutorService EXECUTOR = ThreadPoolManager
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

    public static void submitTask(Runnable task) {
        EXECUTOR.submit(task);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(rootAllocator);
    }

    /**
     * Since Arrow Flight SQL V17.0.0, the client will send a closeSession RPC to the server.
     * - JDBC: The connection URL includes `?catalog=xxx` and call Connection::close().
     * - ADBC: Connection::close().
     * - Native Java FlightSqlClient: FlightSqlClient::closeSession().
     */
    @Override
    public void closeSession(CloseSessionRequest request, CallContext context, StreamListener<CloseSessionResult> listener) {
        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(context.peerIdentity());
        ctx.kill(true, "Close Arrow Flight SQL session via RPC request");
        sessionManager.closeSession(ctx.getArrowFlightSqlToken());
        listener.onCompleted();
    }

    /**
     * Create a prepared statement.// TODO: Note that, we only support the statement without parameters.
     *
     * <p> JDBC and ADBC always use prepared statements to execute queries, even if the simple Statement::execute() is called.
     * The RPC sequence for Statement::execute() is:
     * createPreparedStatement -> getFlightInfoPreparedStatement-> getStreamStatement.
     * The RPC for Statement::close() is closePreparedStatement.
     *
     * <p> For a single connection, multiple prepared statements may be created.
     * When executing a query with a cursor, if the query is identical to the previous one, the existing preparedStmtId is reused
     * instead of creating a new one via createPreparedStatement. Specifically:
     * - If the current query is identical to the previous one, the RPC sequence for Statement::execute() is:
     * getFlightInfoPreparedStatement -> getStreamStatement.
     * - Otherwise, the RPC sequence for Statement::execute() is:
     * closePreparedStatement(prevPreparedStmtId) -> createPreparedStatement->getFlightInfoPreparedStatement->getStreamStatement.
     */
    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, CallContext context,
                                        StreamListener<Result> listener) {
        EXECUTOR.submit(() -> {
            try {
                String token = context.peerIdentity();
                ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);

                String preparedStmtId = ctx.addPreparedStatement(request.getQuery());

                // To prevent the client from mistakenly interpreting an empty Schema as an update statement (instead of a query statement),
                // we need to ensure that the Schema returned by createPreparedStatement includes the query metadata.
                // This means we need to correctly set the DatasetSchema and ParameterSchema in ActionCreatePreparedStatementResult.
                // We generate a minimal Schema. This minimal Schema can include an integer column to ensure the Schema is not empty.
                try (VectorSchemaRoot schemaRoot = ArrowUtil.createSingleSchemaRoot("r", "0")) {
                    Schema schema = schemaRoot.getSchema();
                    FlightSql.ActionCreatePreparedStatementResult result =
                            FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                    .setPreparedStatementHandle(ByteString.copyFromUtf8(preparedStmtId))
                                    .setDatasetSchema(ByteString.copyFrom(serializeMetadata(schema)))
                                    .setParameterSchema(ByteString.copyFrom(serializeMetadata(schema))).build();
                    listener.onNext(new Result(Any.pack(result).toByteArray()));
                }
                listener.onCompleted();
            } catch (Exception e) {
                listener.onError(
                        CallStatus.INTERNAL.withDescription("createPreparedStatement error: " + e.getMessage()).withCause(e)
                                .toRuntimeException());
            } catch (Throwable e) {
                listener.onError(
                        CallStatus.INTERNAL.withDescription("createPreparedStatement unexpected error: " + e.getMessage())
                                .withCause(e).toRuntimeException());

            }
        });
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context,
                                       StreamListener<Result> listener) {
        String token = context.peerIdentity();
        ArrowFlightSqlConnectContext ctx = sessionManager.validateAndGetConnectContext(token);

        String preparedStmtId = request.getPreparedStatementHandle().toStringUtf8();
        ctx.removePreparedStatement(preparedStmtId);

        EXECUTOR.submit(listener::onCompleted);
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

        String preparedStmtId = command.getPreparedStatementHandle().toStringUtf8();
        String query = ctx.getPreparedStatement(preparedStmtId);
        if (query == null) {
            throw CallStatus.INVALID_ARGUMENT.withDescription("Prepared statement not found: " + preparedStmtId)
                    .toRuntimeException();
        }

        return getFlightInfoFromQuery(ctx, descriptor, query);
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
        String query = command.getQuery();
        return getFlightInfoFromQuery(ctx, descriptor, query);
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
        EXECUTOR.submit(() -> {
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

    /**
     * In ADBC, a single connection can execute multiple statements concurrently, but since ConnectContext is not thread-safe,
     * we currently cannot support this behavior.
     * Therefore, use {@link ArrowFlightSqlConnectContext#acquireRunningToken(long)} to ensure that only one statement is
     * executing at any given time on the same connection.
     * TODO: Refactor ConnectContext into ConnectContext + StatementContext to support concurrent execution of multiple
     *  statements on a single connection.
     */
    protected FlightInfo getFlightInfoFromQuery(ArrowFlightSqlConnectContext ctx, FlightDescriptor descriptor, String query) {
        try {
            ArrowFlightSqlConnectProcessor processor = new ArrowFlightSqlConnectProcessor(ctx, query);
            ArrowFlightSqlResultDescriptor result = processor.execute();

            // FE task will return FE as endpoint.
            if (!result.isBackendResultDescriptor()) {
                final ByteString handle = buildFETicket(ctx);
                FlightSql.TicketStatementQuery ticketStatement =
                        FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
                return buildFlightInfoFromFE(ticketStatement, descriptor, result.getSchema());
            }

            // Query task will wait until deployment to BE is finished and return BE as endpoint.
            long workerId = result.getBackendId();
            TUniqueId rootFragmentInstanceId = result.getFragmentInstanceId();
            Schema schema = result.getSchema();

            SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
            ComputeNode worker = clusterInfoService.getBackendOrComputeNode(workerId);

            final ByteString handle = buildBETicket(ctx.getExecutionId(), rootFragmentInstanceId);
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

    protected <T extends Message> FlightInfo buildFlightInfo(T request, FlightDescriptor descriptor,
                                                             Schema schema, Location endpoint) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, endpoint));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    public static Schema buildSchema(ExecPlan execPlan) {
        List<Field> arrowFields = Lists.newArrayList();

        List<String> colNames = execPlan.getColNames();
        List<Expr> outExprs = execPlan.getOutputExprs();
        for (int i = 0; i < colNames.size(); i++) {
            Expr expr = outExprs.get(i);
            Field arrowField = ArrowUtils.convertToArrowType(expr.getOriginType(), colNames.get(i), expr.isNullable());
            arrowFields.add(arrowField);
        }

        return new Schema(arrowFields);
    }

    private static String hexStringFromUniqueId(final TUniqueId id) {
        if (id == null) {
            return "";
        }
        return Long.toHexString(id.hi) + "-" + Long.toHexString(id.lo);
    }
}
