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

import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectProcessor;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlResultDescriptor;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AuditEncryptionChecker;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpSessionOptionValueVisitor;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.ServerHeaderMiddleware;
import org.apache.arrow.flight.SessionOptionValue;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlServiceImplTest {

    private ArrowFlightSqlServiceImpl service;
    private ArrowFlightSqlSessionManager sessionManager;
    private ArrowFlightSqlConnectContext mockContext;
    private FlightProducer.CallContext mockCallContext;
    @Mocked
    AuditEncryptionChecker mockChecker;

    @BeforeEach
    public void setUp() throws IllegalAccessException, NoSuchFieldException, InterruptedException {
        sessionManager = mock(ArrowFlightSqlSessionManager.class);
        mockContext = mock(ArrowFlightSqlConnectContext.class, RETURNS_DEEP_STUBS);
        mockCallContext = mock(FlightProducer.CallContext.class);
        when(mockCallContext.peerIdentity()).thenReturn("token123");
        when(sessionManager.validateAndGetConnectContext("token123")).thenReturn(mockContext);
        when(mockContext.getState()).thenReturn(mock(QueryState.class));
        when(mockContext.getResult(anyString())).thenReturn(mock(VectorSchemaRoot.class));
        when(mockContext.getExecutionId()).thenReturn(new com.starrocks.thrift.TUniqueId(1, 1));
        when(mockContext.getArrowFlightSqlToken()).thenReturn("token123");

        SessionVariable mockSessionVariable = mock(SessionVariable.class);
        when(mockContext.getSessionVariable()).thenReturn(mockSessionVariable);
        when(mockContext.acquireRunningToken(anyLong())).thenReturn(true);
        when(mockSessionVariable.getQueryTimeoutS()).thenReturn(10);
        when(mockSessionVariable.getQueryDeliveryTimeoutS()).thenReturn(10);
        when(mockSessionVariable.getSqlDialect()).thenReturn("mysql");
        when(mockSessionVariable.isArrowFlightProxyEnabled()).thenReturn(false);

        CallHeaders mockHeaders = mock(CallHeaders.class);
        when(mockHeaders.get("database")).thenReturn("test_db");

        ServerHeaderMiddleware mockMiddleware = mock(ServerHeaderMiddleware.class);
        when(mockMiddleware.headers()).thenReturn(mockHeaders);
        when(mockCallContext.getMiddleware(FlightConstants.HEADER_KEY)).thenReturn(mockMiddleware);

        AuditEvent.AuditEventBuilder mockBuilder = mock(AuditEvent.AuditEventBuilder.class, RETURNS_DEEP_STUBS);
        when(mockContext.getAuditEventBuilder()).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(new AuditEvent());

        new Expectations() {
            {
                mockChecker.visit((ParseNode) any);
                result = Boolean.FALSE;
                times = 0;
            }
        };

        LongCounterMetric mockCounter = mock(LongCounterMetric.class);
        doNothing().when(mockCounter).increase(anyLong());

        Field field = MetricRepo.class.getDeclaredField("COUNTER_REQUEST_ALL");
        field.setAccessible(true);
        field.set(null, mockCounter);

        StatementBase mockStmtBase = mock(StatementBase.class);
        when(mockStmtBase.getOrigStmt()).thenReturn(new OriginStatement("SELECT 1", 0));

        service = new ArrowFlightSqlServiceImpl(sessionManager, Location.forGrpcInsecure("localhost", 1234));
    }

    @Test
    public void testClosePreparedStatement() {
        FlightSql.ActionClosePreparedStatementRequest request = FlightSql
                .ActionClosePreparedStatementRequest.newBuilder().build();
        FlightSqlProducer.StreamListener<Result> listener = mock(FlightSqlProducer.StreamListener.class);
        service.closePreparedStatement(request, mockCallContext, listener);
        verify(listener, timeout(1000)).onCompleted();
    }

    @Test
    public void testCloseSession() {
        doNothing().when(mockContext).kill(anyBoolean(), anyString());
        doNothing().when(sessionManager).closeSession(anyString());
        FlightSqlProducer.StreamListener<CloseSessionResult> listener = mock(FlightSqlProducer.StreamListener.class);
        service.closeSession(new CloseSessionRequest(), mockCallContext, listener);
        verify(sessionManager).closeSession("token123");
    }

    @Test
    public void testGetFlightInfoSqlInfo() {
        FlightSql.CommandGetSqlInfo command = FlightSql.CommandGetSqlInfo.newBuilder().build();
        FlightInfo info = service.getFlightInfoSqlInfo(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoTypeInfo() {
        FlightSql.CommandGetXdbcTypeInfo command = FlightSql.CommandGetXdbcTypeInfo.newBuilder().build();
        FlightInfo info = service.getFlightInfoTypeInfo(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoCatalogs() {
        FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
        FlightInfo info = service.getFlightInfoCatalogs(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoCatalogsWithProxy() {
        // Configure proxy enabled with proxy endpoint
        SessionVariable mockSv = mock(SessionVariable.class);
        when(mockContext.getSessionVariable()).thenReturn(mockSv);
        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
        when(mockSv.getArrowFlightProxy()).thenReturn("proxy.example.com:9408");

        FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
        FlightInfo info = service.getFlightInfoCatalogs(command, mockCallContext, FlightDescriptor.command("".getBytes()));

        assertNotNull(info);
        assertEquals(1, info.getEndpoints().size());
        assertEquals(Location.forGrpcInsecure("proxy.example.com", 9408),
                info.getEndpoints().get(0).getLocations().get(0));
    }

    @Test
    public void testGetFlightInfoCatalogsWithProxyEnabledButEmpty() {
        SessionVariable mockSv = mock(SessionVariable.class);
        when(mockContext.getSessionVariable()).thenReturn(mockSv);
        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
        when(mockSv.getArrowFlightProxy()).thenReturn("");

        FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
        FlightInfo info = service.getFlightInfoCatalogs(command, mockCallContext, FlightDescriptor.command("".getBytes()));

        assertNotNull(info);
        assertEquals(1, info.getEndpoints().size());
        assertEquals(Location.forGrpcInsecure("localhost", 1234),
                info.getEndpoints().get(0).getLocations().get(0));
    }

    @Test
    public void testGetFlightInfoCatalogsWithInvalidProxy() {
        SessionVariable mockSv = mock(SessionVariable.class);
        when(mockContext.getSessionVariable()).thenReturn(mockSv);
        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
        when(mockSv.getArrowFlightProxy()).thenReturn("invalid-proxy-format");

        FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
        FlightRuntimeException ex = assertThrows(FlightRuntimeException.class, () ->
                service.getFlightInfoCatalogs(command, mockCallContext, FlightDescriptor.command("".getBytes())));
        assertTrue(ex.getMessage().contains("Expected format 'hostname:port'"));
    }

    @Test
    public void testGetFlightInfoSchemas() {
        FlightSql.CommandGetDbSchemas command = FlightSql.CommandGetDbSchemas.newBuilder().build();
        FlightInfo info = service.getFlightInfoSchemas(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoTables() {
        FlightSql.CommandGetTables command = FlightSql.CommandGetTables.newBuilder().setIncludeSchema(true).build();
        FlightInfo info = service.getFlightInfoTables(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoTableTypes() {
        FlightSql.CommandGetTableTypes command = FlightSql.CommandGetTableTypes.newBuilder().build();
        FlightInfo info = service.getFlightInfoTableTypes(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoExportedKeys() {
        FlightSql.CommandGetExportedKeys command = FlightSql.CommandGetExportedKeys.newBuilder().build();
        FlightInfo info = service.getFlightInfoExportedKeys(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoImportedKeys() {
        FlightSql.CommandGetImportedKeys command = FlightSql.CommandGetImportedKeys.newBuilder().build();
        FlightInfo info = service.getFlightInfoImportedKeys(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoCrossReference() {
        FlightSql.CommandGetCrossReference command = FlightSql.CommandGetCrossReference.newBuilder().build();
        FlightInfo info = service.getFlightInfoCrossReference(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetFlightInfoPrimaryKeys() {
        FlightSql.CommandGetPrimaryKeys command = FlightSql.CommandGetPrimaryKeys.newBuilder().build();
        FlightInfo info = service.getFlightInfoPrimaryKeys(command, mockCallContext, FlightDescriptor.command("".getBytes()));
        assertNotNull(info);
    }

    @Test
    public void testGetStreamStatement() {
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|queryId")).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        when(mockContext.getResult("queryId")).thenReturn(mock(VectorSchemaRoot.class));
        service.getStreamStatement(ticket, mockCallContext, listener);
        verify(listener).start(any());
        verify(listener).putNext();
        verify(listener).completed();
    }

    @Test
    public void testGetStreamResultWithBEProxyTicket() throws Exception {
        FlightClient mockBeClient = mock(FlightClient.class);
        FlightStream mockBeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        service.addToCacheForTesting("127.0.0.1:9400", mockBeClient);

        when(mockBeClient.getStream(any(Ticket.class))).thenReturn(mockBeStream);
        when(mockBeStream.getRoot()).thenReturn(mockRoot);
        when(mockBeStream.next()).thenReturn(true).thenReturn(false); // One batch then done

        String proxyTicket = "19abc7b87307530-9965dc19f22a94a8|19abc7b87307530-9965dc19f22a94a9|127.0.0.1|9400";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(proxyTicket))
                .build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        verify(listener).start(mockRoot);
        verify(listener).putNext();
        verify(listener).completed();
        verify(mockBeStream).close();

        assertEquals(service.getClientFromCacheForTesting("127.0.0.1:9400"), mockBeClient);
    }

    @Test
    public void testGetStreamStatementCancelHandler() {
        FlightClient mockBeClient = mock(FlightClient.class);
        FlightStream mockBeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        service.addToCacheForTesting("127.0.0.1:9400", mockBeClient);

        when(mockBeClient.getStream(any(Ticket.class))).thenReturn(mockBeStream);
        when(mockBeStream.getRoot()).thenReturn(mockRoot);
        when(mockBeStream.next()).thenReturn(true).thenReturn(false);

        String proxyTicket = "19abc7b87307530-9965dc19f22a94a8|19abc7b87307530-9965dc19f22a94a9|127.0.0.1|9400";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(proxyTicket))
                .build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        ArgumentCaptor<Runnable> cancelHandlerCaptor = ArgumentCaptor.forClass(Runnable.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        verify(listener).setOnCancelHandler(cancelHandlerCaptor.capture());
        verify(listener).start(mockRoot);
        verify(listener).completed();

        // Invoke the captured cancel handler to verify it calls beStream.cancel()
        cancelHandlerCaptor.getValue().run();
        verify(mockBeStream).cancel("Client cancelled request", null);

        assertEquals(service.getClientFromCacheForTesting("127.0.0.1:9400"), mockBeClient);
    }

    @Test
    public void testStreamResultFromBEError() throws Exception {
        FlightClient mockBeClient = mock(FlightClient.class);
        FlightStream mockBeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        service.addToCacheForTesting("127.0.0.1:9400", mockBeClient);

        when(mockBeClient.getStream(any(Ticket.class))).thenReturn(mockBeStream);
        when(mockBeStream.getRoot()).thenReturn(mockRoot);
        RuntimeException streamError = new RuntimeException("Connection lost");
        when(mockBeStream.next()).thenThrow(streamError);

        String proxyTicket = "19abc7b87307530-9965dc19f22a94a8|19abc7b87307530-9965dc19f22a94a9|127.0.0.1|9400";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(proxyTicket))
                .build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        verify(mockBeStream).cancel("Error during streaming", streamError);
        ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(errorCaptor.capture());
        assertEquals("Failed to proxy result from BE: Connection lost", errorCaptor.getValue().getMessage());
        verify(mockBeStream).close();

        // client not removed from cache on error
        assertEquals(service.getClientFromCacheForTesting("127.0.0.1:9400"), mockBeClient);
    }

    @Test
    public void testGetStreamResultFromBERetrySuccess() throws Exception {
        FlightClient mockBeClient = mock(FlightClient.class);
        FlightStream mockBeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        // Mock the cache to always return our mock client
        Cache<String, FlightClient> mockCache = mock(Cache.class);
        when(mockCache.get(anyString(), any())).thenReturn(mockBeClient);
        setFinalField(service, "beClientCache", mockCache);

        // First call throws exception, second call succeeds
        when(mockBeClient.getStream(any(Ticket.class)))
                .thenThrow(new RuntimeException("Connection refused"))
                .thenReturn(mockBeStream);
        when(mockBeStream.getRoot()).thenReturn(mockRoot);
        when(mockBeStream.next()).thenReturn(true).thenReturn(false);

        String proxyTicket = "19abc7b87307530-9965dc19f22a94a8|19abc7b87307530-9965dc19f22a94a9|127.0.0.1|9400";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(proxyTicket))
                .build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        // Verify success after retry
        verify(listener).start(mockRoot);
        verify(listener).putNext();
        verify(listener).completed();
        verify(mockBeStream).close();

        // Verify getStream was called twice (initial + retry)
        verify(mockBeClient, org.mockito.Mockito.times(2)).getStream(any(Ticket.class));
    }

    @Test
    public void testGetStreamStatementInvalid() {
        FlightSql.TicketStatementQuery threeSections = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|queryId|invalid_section")).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        assertThrows(FlightRuntimeException.class, () ->
                    service.getStreamStatement(threeSections, mockCallContext, listener
        ));

        FlightSql.TicketStatementQuery nonZeroPort = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|queryId|hostname|non-zero-port")).build();

        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(nonZeroPort, mockCallContext, listener
                ));
    }

    @Test
    public void testGetStreamPreparedStatement() {
        FlightSql.CommandPreparedStatementQuery command = FlightSql.CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8("token123|queryId")).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        when(mockContext.getResult("queryId")).thenReturn(mock(VectorSchemaRoot.class));
        service.getStreamPreparedStatement(command, mockCallContext, listener);
        verify(listener).start(any());
        verify(listener).putNext();
        verify(listener).completed();
    }

    @Test
    public void testGetStreamSqlInfo() {
        FlightSql.CommandGetSqlInfo command = FlightSql.CommandGetSqlInfo.newBuilder().build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);
        service.getStreamSqlInfo(command, mockCallContext, listener);
        verify(listener, timeout(1000)).completed();
    }

    @Test
    public void testCreatePreparedStatement() throws Exception {
        // mock request
        FlightSql.ActionCreatePreparedStatementRequest request = mock(FlightSql.ActionCreatePreparedStatementRequest.class);
        when(request.getQuery()).thenReturn("SELECT 1");

        // mock context
        FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
        when(callContext.peerIdentity()).thenReturn("token123");

        // mock sessionManager
        when(sessionManager.validateAndGetConnectContext("token123")).thenReturn(mockContext);

        // mock listener
        FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);

        // mock ArrowUtil.createSingleSchemaRoot
        try (
                MockedStatic<ArrowUtil> mockArrowUtil = mockStatic(ArrowUtil.class);
                MockedStatic<ArrowFlightSqlServiceImpl> mockStatic =
                        mockStatic(ArrowFlightSqlServiceImpl.class, CALLS_REAL_METHODS)
        ) {
            // mock createSingleSchemaRoot
            VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
            Schema mockSchema = mock(Schema.class);
            when(mockRoot.getSchema()).thenReturn(mockSchema);
            mockArrowUtil.when(() -> ArrowUtil.createSingleSchemaRoot(anyString(), anyString())).thenReturn(mockRoot);

            // mock serializeMetadata
            mockStatic.when(() -> ArrowFlightSqlServiceImpl.serializeMetadata(mockSchema))
                    .thenReturn(ByteBuffer.wrap("mock".getBytes(StandardCharsets.UTF_8)));

            // call method
            service.createPreparedStatement(request, callContext, listener);

            // wait executor
            Thread.sleep(500);
        }
    }

    @Test
    public void testSetSessionOptions() throws Exception {
        // Arrange
        ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(mock(ArrowFlightSqlSessionManager.class), null);
        ArrowFlightSqlSessionManager sessionManager = mock(ArrowFlightSqlSessionManager.class);

        setFinalField(service, "sessionManager", sessionManager);

        // mock context
        FlightProducer.CallContext context = mock(FlightProducer.CallContext.class);
        when(context.peerIdentity()).thenReturn("token");

        // mock listener
        FlightProducer.StreamListener<SetSessionOptionsResult> listener = mock(FlightProducer.StreamListener.class);

        // mock connectContext
        ArrowFlightSqlConnectContext connectContext = mock(ArrowFlightSqlConnectContext.class);
        when(sessionManager.validateAndGetConnectContext("token")).thenReturn(connectContext);
        TUniqueId mockId = new TUniqueId();
        mockId.setHi(123L);
        mockId.setLo(456L);
        when(connectContext.getExecutionId()).thenReturn(mockId);
        doNothing().when(connectContext).changeCatalog("valid");
        doThrow(new DdlException("fail")).when(connectContext).changeCatalog("fail");

        // mock sessionOptionValue
        SessionOptionValue validValue = mock(SessionOptionValue.class);
        when(validValue.acceptVisitor(any(NoOpSessionOptionValueVisitor.class))).thenReturn("valid");

        SessionOptionValue nullValue = mock(SessionOptionValue.class);
        when(nullValue.acceptVisitor(any(NoOpSessionOptionValueVisitor.class))).thenReturn(null);

        SessionOptionValue failValue = mock(SessionOptionValue.class);
        when(failValue.acceptVisitor(any(NoOpSessionOptionValueVisitor.class))).thenReturn("fail");

        Map<String, SessionOptionValue> options = new HashMap<>();
        options.put("catalog", validValue);
        options.put("catalog_null", nullValue);
        options.put("catalog_throw", failValue);
        options.put("invalid_key", validValue);

        SetSessionOptionsRequest request = mock(SetSessionOptionsRequest.class);
        when(request.getSessionOptions()).thenReturn(options);

        service.setSessionOptions(request, context, listener);
    }

    private void setFinalField(Object target, String fieldName, Object value) {
        Deencapsulation.setField(target, fieldName, value);
    }

    @Test
    public void testCacheClose() {
        ArrowFlightSqlServiceImpl service = new ArrowFlightSqlServiceImpl(mock(ArrowFlightSqlSessionManager.class), null);

        FlightClient mockBeClient = mock(FlightClient.class);

        service.addToCacheForTesting("127.0.0.1:9400", mockBeClient);

        try {
            service.close();
            verify(mockBeClient).close();
            assertNull(service.getClientFromCacheForTesting("127.0.0.1:9400"));
        } catch (Exception ignored) {

        }
    }

    @Test
    public void testGetBEEndpointAllPaths() throws Exception {
        // Setup common mocks
        SessionVariable mockSv = mock(SessionVariable.class);
        ComputeNode mockWorker = mock(ComputeNode.class);
        TUniqueId mockFragmentInstanceId = new TUniqueId(1L, 2L);
        TUniqueId mockQueryId = new TUniqueId(3L, 4L);

        when(mockWorker.getHost()).thenReturn("be-host");
        when(mockWorker.getArrowFlightPort()).thenReturn(8815);

        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(false);
        Pair<Location, ByteString> result = service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId);
        assertEquals(Location.forGrpcInsecure("be-host", 8815), result.first);
        String beTicket = result.second.toStringUtf8();
        assertEquals("3-4:1-2", beTicket);

        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
        when(mockSv.getArrowFlightProxy()).thenReturn("");
        result = service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId);
        assertEquals(Location.forGrpcInsecure("localhost", 1234), result.first);
        String feProxyTicket = result.second.toStringUtf8();
        assertEquals("3-4|1-2|be-host|8815", feProxyTicket);

        when(mockSv.getArrowFlightProxy()).thenReturn("proxy-host:9400");
        result = service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId);
        assertEquals(Location.forGrpcInsecure("proxy-host", 9400), result.first);
        // Ticket should still be FE proxy ticket format
        assertEquals("3-4|1-2|be-host|8815", result.second.toStringUtf8());

        when(mockSv.getArrowFlightProxy()).thenReturn("invalidproxy");
        InvalidConfException ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Expected format 'hostname:port', got 'invalidproxy'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn(":9400");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Hostname cannot be empty, got ':9400'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:abc");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Port must be a valid integer, got 'abc'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:99999");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Port must be between 1 and 65535, got '99999'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:0");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Port must be between 1 and 65535, got '0'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("host:port:extra");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId));
        assertEquals("Expected format 'hostname:port', got 'host:port:extra'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:65535");
        result = service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId);
        assertEquals(Location.forGrpcInsecure("hostname", 65535), result.first);

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:1");
        result = service.getBEEndpoint(mockSv, mockQueryId, mockWorker, mockFragmentInstanceId);
        assertEquals(Location.forGrpcInsecure("hostname", 1), result.first);
    }

    @Test
    public void testGetFlightInfoFromQueryFETaskWithProxy() throws Exception {
        // Test that FE-handled queries respect the proxy setting
        SessionVariable mockSv = mock(SessionVariable.class);
        when(mockContext.getSessionVariable()).thenReturn(mockSv);

        ArrowFlightSqlResultDescriptor mockResult = mock(ArrowFlightSqlResultDescriptor.class);
        when(mockResult.isBackendResultDescriptor()).thenReturn(false); // FE task
        when(mockResult.getSchema()).thenReturn(mock(Schema.class));

        try (MockedConstruction<ArrowFlightSqlConnectProcessor> mockedProcessor =
                     org.mockito.Mockito.mockConstruction(ArrowFlightSqlConnectProcessor.class,
                             (mock, context) -> when(mock.execute()).thenReturn(mockResult))) {

            FlightSql.CommandStatementQuery command = FlightSql.CommandStatementQuery.newBuilder()
                    .setQuery("SHOW VARIABLES").build();
            FlightDescriptor descriptor = FlightDescriptor.command(command.toByteArray());

            // Test 1: Proxy disabled - should return internal FE endpoint (localhost:1234)
            when(mockSv.isArrowFlightProxyEnabled()).thenReturn(false);
            FlightInfo info = service.getFlightInfoStatement(command, mockCallContext, descriptor);
            assertNotNull(info);
            assertEquals(1, info.getEndpoints().size());
            assertEquals(Location.forGrpcInsecure("localhost", 1234), info.getEndpoints().get(0).getLocations().get(0));

            // Test 2: Proxy enabled with proxy configured - should return proxy endpoint
            when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
            when(mockSv.getArrowFlightProxy()).thenReturn("proxy.example.com:443");
            info = service.getFlightInfoStatement(command, mockCallContext, descriptor);
            assertNotNull(info);
            assertEquals(1, info.getEndpoints().size());
            assertEquals(Location.forGrpcInsecure("proxy.example.com", 443), info.getEndpoints().get(0).getLocations().get(0));

            // Test 3: Proxy enabled but empty - should return internal FE endpoint
            when(mockSv.getArrowFlightProxy()).thenReturn("");
            info = service.getFlightInfoStatement(command, mockCallContext, descriptor);
            assertNotNull(info);
            assertEquals(1, info.getEndpoints().size());
            assertEquals(Location.forGrpcInsecure("localhost", 1234), info.getEndpoints().get(0).getLocations().get(0));
        }
    }

    @Test
    public void testGetFEEndpointAllPaths() throws Exception {
        SessionVariable mockSv = mock(SessionVariable.class);

        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(false);
        Location result = service.getFEEndpoint(mockSv);
        assertEquals(Location.forGrpcInsecure("localhost", 1234), result);

        when(mockSv.isArrowFlightProxyEnabled()).thenReturn(true);
        when(mockSv.getArrowFlightProxy()).thenReturn("");
        result = service.getFEEndpoint(mockSv);
        assertEquals(Location.forGrpcInsecure("localhost", 1234), result);

        when(mockSv.getArrowFlightProxy()).thenReturn("proxy-host:9408");
        result = service.getFEEndpoint(mockSv);
        assertEquals(Location.forGrpcInsecure("proxy-host", 9408), result);

        when(mockSv.getArrowFlightProxy()).thenReturn("invalidproxy");
        InvalidConfException ex = assertThrows(InvalidConfException.class, () ->
                service.getFEEndpoint(mockSv));
        assertEquals("Expected format 'hostname:port', got 'invalidproxy'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn(":9408");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getFEEndpoint(mockSv));
        assertEquals("Hostname cannot be empty, got ':9408'", ex.getMessage());

        when(mockSv.getArrowFlightProxy()).thenReturn("hostname:abc");
        ex = assertThrows(InvalidConfException.class, () ->
                service.getFEEndpoint(mockSv));
        assertEquals("Port must be a valid integer, got 'abc'", ex.getMessage());
    }

    @Test
    public void testUnimplementedFlightSqlMethods() {
        FlightStream mockStream = mock(FlightStream.class);
        FlightProducer.ServerStreamListener mockServerListener = mock(FlightProducer.ServerStreamListener.class);
        FlightProducer.StreamListener<PutResult> mockPutListener = mock(FlightProducer.StreamListener.class);
        FlightDescriptor mockDescriptor = FlightDescriptor.command("SELECT 1".getBytes());
        Criteria mockCriteria = mock(Criteria.class);

        assertThrows(FlightRuntimeException.class, () -> service.getSchemaStatement(FlightSql.CommandStatementQuery
                .getDefaultInstance(), mockCallContext, mockDescriptor));

        assertThrows(FlightRuntimeException.class, () -> service.acceptPutStatement(FlightSql.CommandStatementUpdate
                .getDefaultInstance(), mockCallContext, mockStream, mockPutListener));

        assertThrows(FlightRuntimeException.class,
                () -> service.acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate
                        .getDefaultInstance(), mockCallContext, mockStream, mockPutListener));

        assertThrows(FlightRuntimeException.class,
                () -> service.acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery
                        .getDefaultInstance(), mockCallContext, mockStream, mockPutListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamCatalogs(mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamSchemas(FlightSql.CommandGetDbSchemas
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class,
                () -> service.getStreamTables(FlightSql.CommandGetTables.getDefaultInstance(), mockCallContext,
                        mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamTableTypes(mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamExportedKeys(FlightSql.CommandGetExportedKeys
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamImportedKeys(FlightSql.CommandGetImportedKeys
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class, () -> service.getStreamCrossReference(FlightSql.CommandGetCrossReference
                .getDefaultInstance(), mockCallContext, mockServerListener));

        assertThrows(FlightRuntimeException.class,
                () -> service.listFlights(mockCallContext, mockCriteria, mock(FlightProducer.StreamListener.class)));
    }
}
