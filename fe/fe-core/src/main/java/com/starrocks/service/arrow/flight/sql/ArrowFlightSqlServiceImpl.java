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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.proto.PUniqueId;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.system.Backend;
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
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ArrowFlightSqlServiceImpl implements FlightSqlProducer, AutoCloseable {
    private final BufferAllocator rootAllocator = new RootAllocator();
    private final ArrowFlightSqlSessionManager arrowFlightSqlSessionManager;
    private final Location location;
    private final SqlInfoBuilder sqlInfoBuilder;
    private final ExecutorService executorService = Executors.newFixedThreadPool(200);

    public ArrowFlightSqlServiceImpl(final ArrowFlightSqlSessionManager arrowFlightSqlSessionManager,
                                     final Location location) {
        this.arrowFlightSqlSessionManager = arrowFlightSqlSessionManager;
        this.location = location;
        sqlInfoBuilder = new SqlInfoBuilder();
        sqlInfoBuilder.withFlightSqlServerName("StarRocks").withFlightSqlServerVersion("1.0")
                .withFlightSqlServerArrowVersion("18.0.0")
                .withFlightSqlServerReadOnly(false)
                .withSqlIdentifierQuoteChar("`")
                .withSqlDdlCatalog(true)
                .withSqlDdlSchema(false)
                .withSqlDdlTable(false)
                .withSqlIdentifierCase(FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE)
                .withSqlQuotedIdentifierCase(
                        FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE);
    }

    @Override
    public void createPreparedStatement(
            FlightSql.ActionCreatePreparedStatementRequest actionCreatePreparedStatementRequest,
            CallContext callContext, StreamListener<Result> streamListener) {
        executorService.submit(() -> {
            try {
                String peerIdentity = callContext.peerIdentity();
                ArrowFlightSqlConnectContext ctx = arrowFlightSqlSessionManager.getConnectContext(peerIdentity);

                String query = actionCreatePreparedStatementRequest.getQuery();
                initConnectContext(ctx, query);
                // To prevent the client from mistakenly interpreting an empty Schema as an update statement (instead of a query statement),
                // we need to ensure that the Schema returned by createPreparedStatement includes the query metadata.
                // This means we need to correctly set the DatasetSchema and ParameterSchema in ActionCreatePreparedStatementResult.
                // We generate a minimal Schema. This minimal Schema can include a integer column to ensure the Schema is not empty.
                Schema schema = ArrowUtil.createSingleSchemaRoot("r", "0").getSchema();
                FlightSql.ActionCreatePreparedStatementResult result =
                        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                .setDatasetSchema(ByteString.copyFrom(serializeMetadata(schema)))
                                .setParameterSchema(ByteString.copyFrom(serializeMetadata(schema))).build();
                streamListener.onNext(new Result(Any.pack(result).toByteArray()));
            } catch (Exception e) {
                streamListener.onError(
                        CallStatus.INTERNAL.withDescription("createPreparedStatement error: " + e.getMessage())
                                .toRuntimeException());
            } catch (Throwable e) {
                streamListener.onError(
                        CallStatus.INTERNAL
                                .withDescription("createPreparedStatement unexpected error: " + e.getMessage())
                                .toRuntimeException());
            } finally {
                streamListener.onCompleted();
            }
        });
    }

    @Override
    public void closePreparedStatement(
            FlightSql.ActionClosePreparedStatementRequest actionClosePreparedStatementRequest, CallContext callContext,
            StreamListener<Result> streamListener) {
        executorService.submit(streamListener::onCompleted);
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery commandStatementQuery,
                                             CallContext callContext, FlightDescriptor flightDescriptor) {
        String peerIdentity = callContext.peerIdentity();
        ArrowFlightSqlConnectContext ctx = arrowFlightSqlSessionManager.getConnectContext(peerIdentity);
        return getFlightInfoFromQuery(peerIdentity, ctx, flightDescriptor);
    }

    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery, CallContext callContext,
            FlightDescriptor flightDescriptor) {
        String peerIdentity = callContext.peerIdentity();
        ArrowFlightSqlConnectContext ctx = arrowFlightSqlSessionManager.getConnectContext(peerIdentity);
        String database = callContext.getMiddleware(FlightConstants.HEADER_KEY).headers().get("database");
        if (database != null && !database.isEmpty()) {
            ctx.setDatabase(database);
        }
        return getFlightInfoFromQuery(peerIdentity, ctx, flightDescriptor);
    }

    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery commandStatementQuery,
                                           CallContext callContext, FlightDescriptor flightDescriptor) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getSchemaStatement unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticketStatementQuery, CallContext callContext,
                                   ServerStreamListener serverStreamListener) {
        getStreamResult(callContext.peerIdentity(), serverStreamListener);
    }

    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery,
                                           CallContext callContext, ServerStreamListener serverStreamListener) {
        getStreamResult(callContext.peerIdentity(), serverStreamListener);
    }

    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate commandStatementUpdate, CallContext callContext,
                                       FlightStream flightStream, StreamListener<PutResult> streamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutStatement unimplemented").toRuntimeException();
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
            FlightSql.CommandPreparedStatementUpdate commandPreparedStatementUpdate, CallContext callContext,
            FlightStream flightStream, StreamListener<PutResult> streamListener) {
        return () -> {
            try {
                while (flightStream.next()) {
                    final int recordCount = -1;
                    final FlightSql.DoPutUpdateResult
                            build = FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();
                    try (final ArrowBuf buffer = rootAllocator.buffer(build.getSerializedSize())) {
                        buffer.writeBytes(build.toByteArray());
                        streamListener.onNext(PutResult.metadata(buffer));
                    }
                }
                streamListener.onCompleted();
            } catch (Exception e) {
                throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
            }
        };
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(
            FlightSql.CommandPreparedStatementQuery commandPreparedStatementQuery, CallContext callContext,
            FlightStream flightStream, StreamListener<PutResult> streamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("acceptPutPreparedStatementQuery unimplemented")
                .toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                                           FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetSqlInfo, flightDescriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                                 ServerStreamListener serverStreamListener) {
        this.sqlInfoBuilder.send(commandGetSqlInfo.getInfoList(), serverStreamListener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo commandGetXdbcTypeInfo,
                                            CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetXdbcTypeInfo, flightDescriptor, Schemas.GET_TYPE_INFO_SCHEMA);
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo commandGetXdbcTypeInfo, CallContext callContext,
                                  ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTypeInfo unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs commandGetCatalogs, CallContext callContext,
                                            FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetCatalogs, flightDescriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(CallContext callContext, ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCatalogs unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas commandGetDbSchemas, CallContext callContext,
                                           FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetDbSchemas, flightDescriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas commandGetDbSchemas, CallContext callContext,
                                 ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamSchemas unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables commandGetTables, CallContext callContext,
                                          FlightDescriptor flightDescriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!commandGetTables.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(commandGetTables, flightDescriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables commandGetTables, CallContext callContext,
                                ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTables unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes commandGetTableTypes,
                                              CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetTableTypes, flightDescriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }

    @Override
    public void getStreamTableTypes(CallContext callContext, ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamTableTypes unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys commandGetPrimaryKeys,
                                               CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetPrimaryKeys, flightDescriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys commandGetPrimaryKeys, CallContext callContext,
                                     ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamPrimaryKeys unimplemented").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys commandGetExportedKeys,
                                                CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetExportedKeys, flightDescriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys commandGetImportedKeys,
                                                CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetImportedKeys, flightDescriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference commandGetCrossReference,
                                                  CallContext callContext, FlightDescriptor flightDescriptor) {
        return getFlightInfoForSchema(commandGetCrossReference, flightDescriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys commandGetExportedKeys, CallContext callContext,
                                      ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamExportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys commandGetImportedKeys, CallContext callContext,
                                      ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamImportedKeys unimplemented").toRuntimeException();
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference commandGetCrossReference,
                                        CallContext callContext, ServerStreamListener serverStreamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("getStreamCrossReference unimplemented").toRuntimeException();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(rootAllocator);
    }

    @Override
    public void closeSession(CloseSessionRequest request, CallContext context,
                             StreamListener<CloseSessionResult> listener) {
        ArrowFlightSqlConnectContext ctx = arrowFlightSqlSessionManager.getConnectContext(context.peerIdentity());
        if (ctx != null) {
            ctx.kill(true, "arrow flight sql close session");
        }
    }

    @Override
    public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
        throw CallStatus.UNIMPLEMENTED.withDescription("listFlights unimplemented").toRuntimeException();
    }

    private void initConnectContext(ArrowFlightSqlConnectContext ctx, String query) {
        ctx.setPreparedQuery(query);
        ctx.setShowResult(false);
        ctx.setReturnFromFE(true);
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    private void getStreamResult(String peerIdentity, ServerStreamListener serverStreamListener) {
        try {
            ArrowFlightSqlConnectContext ctx = arrowFlightSqlSessionManager.getConnectContext(peerIdentity);
            final VectorSchemaRoot vectorSchemaRoot = ctx.getResult();
            serverStreamListener.start(vectorSchemaRoot);
            serverStreamListener.putNext();
        } catch (Exception e) {
            throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
        } finally {
            serverStreamListener.completed();
        }
    }

    private FlightInfo getFlightInfoFromQuery(String peerIdentity, ArrowFlightSqlConnectContext ctx,
                                              FlightDescriptor flightDescriptor) {
        try {
            StatementBase parsedStmt = parse(ctx.getPreparedQuery(), ctx.getSessionVariable());
            ctx.setStatement(parsedStmt);
            ctx.setThreadLocalInfo();

            ArrowFlightSqlConnectProcessor arrowConnectProcessor =
                    new ArrowFlightSqlConnectProcessor(ctx);
            arrowConnectProcessor.processOnce();

            if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                throw new RuntimeException("Process Query Error: " + ctx.getState().getErrorMessage());
            }

            if (ctx.returnFromFE()) {
                final ByteString handle = ByteString.copyFromUtf8(peerIdentity + ":" + ctx.getQueryId());
                FlightSql.TicketStatementQuery ticketStatement =
                        FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
                if (!ctx.isShowResult()) {
                    ctx.addOKResult();
                }
                return getFlightInfoForSchema(ticketStatement, flightDescriptor, ctx.getResult().getSchema());
            }

            DefaultCoordinator coordinator = (DefaultCoordinator) ctx.getCoordinator();
            TNetworkAddress address = coordinator.getReceiver().getAddress();
            long beId = coordinator.getReceiver().getBackendId();
            Backend be = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(beId);
            if (be == null) {
                throw CallStatus.INTERNAL.withDescription("Backend information unavailable for BE ID: " + beId)
                        .toRuntimeException();
            }

            TUniqueId resultFragmentId =
                    coordinator.getExecutionDAG().getFragmentInstanceInfos().get(0).getInstanceId();
            PUniqueId pUniqueId = new PUniqueId();
            pUniqueId.hi = resultFragmentId.hi;
            pUniqueId.lo = resultFragmentId.lo;

            TUniqueId queryId = coordinator.getQueryId();
            String queryIdentifier = hexStringFromUniqueId(queryId) + ":" + hexStringFromUniqueId(resultFragmentId);
            final ByteString handle = ByteString.copyFromUtf8(queryIdentifier);
            FlightSql.TicketStatementQuery ticketStatementQuery =
                    FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(handle).build();

            int beArrowPort = be.getArrowFlightPort();
            if (beArrowPort <= 0) {
                throw CallStatus.INTERNAL.withDescription(String.format("BE [%d] has already disabled Arrow Flight Server. " +
                                "You could set `arrow_flight_port` in `be.conf` to a positive value to enable it.", beId))
                        .toRuntimeException();
            }
            Location grpcLocation = Location.forGrpcInsecure(address.hostname, beArrowPort);
            Ticket ticket = new Ticket(Any.pack(ticketStatementQuery).toByteArray());
            List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, grpcLocation));

            Schema schema = arrowConnectProcessor.fetchArrowSchema(address, pUniqueId, 600);
            return new FlightInfo(schema, flightDescriptor, endpoints, -1, -1);
        } catch (Exception e) {
            throw CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
        } finally {
            ctx.setCommand(MysqlCommand.COM_SLEEP);
        }
    }

    private StatementBase parse(String sql, SessionVariable sessionVariables) {
        List<StatementBase> stmts;

        stmts = com.starrocks.sql.parser.SqlParser.parse(sql, sessionVariables);
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

    private <T extends Message> FlightInfo getFlightInfoForSchema(final T request, final FlightDescriptor descriptor,
                                                                  final Schema schema) {
        final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
        final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket, location));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }
}
