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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.ArrowUtil;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.plugin.AuditEvent;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.AuditEncryptionChecker;
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

    /**
     * Testable subclass that allows controlling isProxyEnabled() without MockedStatic.
     */
    private static class TestableArrowFlightSqlServiceImpl extends ArrowFlightSqlServiceImpl {
        private boolean proxyEnabled = false;

        public TestableArrowFlightSqlServiceImpl(ArrowFlightSqlSessionManager sessionManager, Location feEndpoint) {
            super(sessionManager, feEndpoint);
        }

        public void setProxyEnabled(boolean enabled) {
            this.proxyEnabled = enabled;
        }

        @Override
        protected boolean isProxyEnabled() {
            return proxyEnabled;
        }
    }

    @BeforeEach
    public void setUp() throws IllegalAccessException, NoSuchFieldException, InterruptedException {
        sessionManager = mock(ArrowFlightSqlSessionManager.class);
        mockContext = mock(ArrowFlightSqlConnectContext.class, RETURNS_DEEP_STUBS);
        mockCallContext = mock(FlightProducer.CallContext.class);
        when(mockCallContext.peerIdentity()).thenReturn("token123");
        when(sessionManager.validateAndGetConnectContext("token123")).thenReturn(mockContext);
        when(sessionManager.isLocalToken("token123")).thenReturn(true);
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

        service = new ArrowFlightSqlServiceImpl(sessionManager,
                Location.forGrpcInsecure("localhost", 1234));
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

        ArgumentCaptor<CloseSessionResult> resultCaptor = ArgumentCaptor.forClass(CloseSessionResult.class);
        verify(listener).onNext(resultCaptor.capture());
        verify(listener).onCompleted();

        CloseSessionResult result = resultCaptor.getValue();
        assertEquals(CloseSessionResult.Status.CLOSED, result.getStatus());
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
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mockedGlobal.when(GlobalVariable::getArrowFlightProxy).thenReturn("proxy.example.com:9408");

            ArrowFlightSqlServiceImpl proxyService = new ArrowFlightSqlServiceImpl(sessionManager,
                    Location.forGrpcInsecure("localhost", 1234));

            FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());
            FlightInfo info = proxyService.getFlightInfoCatalogs(command, mockCallContext, descriptor);

            assertNotNull(info);
            assertEquals(1, info.getEndpoints().size());
            assertEquals(Location.forGrpcInsecure("proxy.example.com", 9408),
                    info.getEndpoints().get(0).getLocations().get(0));
        }
    }

    @Test
    public void testGetFlightInfoCatalogsWithProxyEnabledButEmpty() {
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mockedGlobal.when(GlobalVariable::getArrowFlightProxy).thenReturn("");

            ArrowFlightSqlServiceImpl proxyService = new ArrowFlightSqlServiceImpl(sessionManager,
                    Location.forGrpcInsecure("localhost", 1234));

            FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());
            FlightInfo info = proxyService.getFlightInfoCatalogs(command, mockCallContext, descriptor);

            assertNotNull(info);
            assertEquals(1, info.getEndpoints().size());
            assertEquals(Location.forGrpcInsecure("localhost", 1234),
                    info.getEndpoints().get(0).getLocations().get(0));
        }
    }

    @Test
    public void testGetFlightInfoCatalogsWithInvalidProxy() {
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mockedGlobal.when(GlobalVariable::getArrowFlightProxy).thenReturn("invalid-proxy-format");

            ArrowFlightSqlServiceImpl proxyService = new ArrowFlightSqlServiceImpl(sessionManager,
                    Location.forGrpcInsecure("localhost", 1234));

            FlightSql.CommandGetCatalogs command = FlightSql.CommandGetCatalogs.newBuilder().build();
            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());
            FlightRuntimeException ex = assertThrows(FlightRuntimeException.class, () ->
                    proxyService.getFlightInfoCatalogs(command, mockCallContext, descriptor));
            assertTrue(ex.getMessage().contains("Expected format 'hostname:port'"));
        }
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
        setFinalField(service, "clientCache", mockCache);

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
    public void testGetStreamStatementInvalidTicketFormats() {
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        // 3-part ticket with invalid host:port format (missing port)
        FlightSql.TicketStatementQuery invalidFeProxy = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|queryId|invalid_section")).build();
        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(invalidFeProxy, mockCallContext, listener));

        // 3-part ticket with non-numeric port
        FlightSql.TicketStatementQuery invalidFeProxyPort = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|queryId|hostname:abc")).build();
        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(invalidFeProxyPort, mockCallContext, listener));

        // 4-part BE ticket with non-numeric port
        FlightSql.TicketStatementQuery invalidBePort = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("queryId|fragmentId|hostname|non-zero-port")).build();
        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(invalidBePort, mockCallContext, listener));

        // 1-part ticket (invalid)
        FlightSql.TicketStatementQuery onePart = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("just-one-part")).build();
        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(onePart, mockCallContext, listener));

        // 5-part ticket (invalid)
        FlightSql.TicketStatementQuery fiveParts = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("a|b|c|d|e")).build();
        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(fiveParts, mockCallContext, listener));
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

    private void setFinalField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
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

    @Test
    public void testGetStreamResultFromRemoteFE_shortCircuitLocalFE() {
        String localFeTicket = "token123|queryId|localhost:1234";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(localFeTicket)).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        when(mockContext.getResult("queryId")).thenReturn(mock(VectorSchemaRoot.class));

        service.getStreamStatement(ticket, mockCallContext, listener);

        verify(listener).start(any());
        verify(listener).putNext();
        verify(listener).completed();
    }

    @Test
    public void testGetStreamResultFromRemoteFE_forwardToRemote() throws Exception {
        // When the 3-part ticket points to a remote FE, it should forward the request
        FlightClient mockFeClient = mock(FlightClient.class);
        FlightStream mockFeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        Cache<String, FlightClient> mockCache = mock(Cache.class);
        when(mockCache.get(anyString(), any())).thenReturn(mockFeClient);
        setFinalField(service, "clientCache", mockCache);

        when(mockFeClient.getStream(any(Ticket.class), any())).thenReturn(mockFeStream);
        when(mockFeStream.getRoot()).thenReturn(mockRoot);
        when(mockFeStream.next()).thenReturn(true).thenReturn(false);

        // Remote FE ticket - different host than local (localhost:1234)
        String remoteFeTicket = "token123|queryId|remote-fe:9408";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(remoteFeTicket)).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        // Should have forwarded to remote FE
        verify(mockFeClient).getStream(any(Ticket.class), any());
        verify(listener).start(mockRoot);
        verify(listener).putNext();
        verify(listener).completed();
        verify(mockFeStream).close();
    }

    @Test
    public void testProxyForwarding_closeSession_allScenarios() throws Exception {
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class);
                MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                        mockStatic(ArrowFlightSqlSessionManager.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            String remoteToken = "remote-fe|some-uuid-token";
            when(mockCallContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Cache<String, FlightClient> mockCache = mock(Cache.class);
            when(mockCache.get(anyString(), any())).thenReturn(mockFeClient);
            setFinalField(service, "clientCache", mockCache);

            // Test 1: Success case
            CloseSessionResult mockResult = mock(CloseSessionResult.class);
            when(mockFeClient.closeSession(any(CloseSessionRequest.class), any())).thenReturn(mockResult);

            FlightSqlProducer.StreamListener<CloseSessionResult> listener1 = mock(FlightSqlProducer.StreamListener.class);
            service.closeSession(new CloseSessionRequest(), mockCallContext, listener1);
            verify(listener1).onNext(mockResult);
            verify(listener1).onCompleted();

            // Test 2: Invalid token
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost("invalid-token"))
                    .thenReturn(null);
            when(mockCallContext.peerIdentity()).thenReturn("invalid-token");
            when(sessionManager.isLocalToken("invalid-token")).thenReturn(false);

            FlightSqlProducer.StreamListener<CloseSessionResult> listener2 = mock(FlightSqlProducer.StreamListener.class);
            service.closeSession(new CloseSessionRequest(), mockCallContext, listener2);
            verify(listener2).onError(any(FlightRuntimeException.class));

            // Test 3: Remote exception
            when(mockCallContext.peerIdentity()).thenReturn(remoteToken);
            when(mockFeClient.closeSession(any(CloseSessionRequest.class), any()))
                    .thenThrow(new RuntimeException("Connection refused"));

            FlightSqlProducer.StreamListener<CloseSessionResult> listener3 = mock(FlightSqlProducer.StreamListener.class);
            service.closeSession(new CloseSessionRequest(), mockCallContext, listener3);
            verify(listener3).onError(any(FlightRuntimeException.class));
        }
    }

    @Test
    public void testProxyForwarding_getFlightInfoPreparedStatement() throws Exception {
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class);
                MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                        mockStatic(ArrowFlightSqlSessionManager.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Cache<String, FlightClient> mockCache = mock(Cache.class);
            when(mockCache.get(anyString(), any())).thenReturn(mockFeClient);
            setFinalField(service, "clientCache", mockCache);

            FlightInfo mockFlightInfo = mock(FlightInfo.class);
            when(mockFeClient.getInfo(any(FlightDescriptor.class), any())).thenReturn(mockFlightInfo);

            FlightSql.CommandPreparedStatementQuery getInfoCmd = FlightSql.CommandPreparedStatementQuery.newBuilder()
                    .setPreparedStatementHandle(ByteString.copyFromUtf8("stmt-id")).build();
            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());
            FlightInfo result = service.getFlightInfoPreparedStatement(getInfoCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, result);

            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost("invalid-token"))
                    .thenReturn(null);
            when(callContext.peerIdentity()).thenReturn("invalid-token");
            when(sessionManager.isLocalToken("invalid-token")).thenReturn(false);
            assertThrows(FlightRuntimeException.class, () ->
                    service.getFlightInfoPreparedStatement(getInfoCmd, callContext, descriptor));

            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(mockFeClient.getInfo(any(FlightDescriptor.class), any()))
                    .thenThrow(new RuntimeException("Connection timeout"));
            assertThrows(FlightRuntimeException.class, () ->
                    service.getFlightInfoPreparedStatement(getInfoCmd, callContext, descriptor));
        }
    }

    @Test
    public void testProxyForwarding_getFlightInfoStatement() throws Exception {
        try (MockedStatic<GlobalVariable> mockedGlobal = mockStatic(GlobalVariable.class);
                MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                        mockStatic(ArrowFlightSqlSessionManager.class)) {
            mockedGlobal.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Cache<String, FlightClient> mockCache = mock(Cache.class);
            when(mockCache.get(anyString(), any())).thenReturn(mockFeClient);
            setFinalField(service, "clientCache", mockCache);

            FlightInfo mockFlightInfo = mock(FlightInfo.class);
            when(mockFeClient.getInfo(any(FlightDescriptor.class), any())).thenReturn(mockFlightInfo);

            FlightSql.CommandStatementQuery stmtCmd = FlightSql.CommandStatementQuery.newBuilder()
                    .setQuery("SELECT 1").build();
            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());
            FlightInfo stmtResult = service.getFlightInfoStatement(stmtCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, stmtResult);
        }
    }

    @Test
    public void testStreamErrorHandling_cancelAndCloseErrors() throws Exception {
        FlightClient mockBeClient = mock(FlightClient.class);
        FlightStream mockBeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        service.addToCacheForTesting("127.0.0.1:9400", mockBeClient);

        when(mockBeClient.getStream(any(Ticket.class))).thenReturn(mockBeStream);
        when(mockBeStream.getRoot()).thenReturn(mockRoot);

        when(mockBeStream.next()).thenReturn(true).thenReturn(false);
        doThrow(new RuntimeException("Cancel failed")).when(mockBeStream).cancel(anyString(), any());
        doNothing().when(mockBeStream).close();

        String proxyTicket = "19abc7b87307530-9965dc19f22a94a8|19abc7b87307530-9965dc19f22a94a9|127.0.0.1|9400";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(proxyTicket)).build();

        FlightProducer.ServerStreamListener listener1 = mock(FlightProducer.ServerStreamListener.class);
        ArgumentCaptor<Runnable> cancelCaptor = ArgumentCaptor.forClass(Runnable.class);

        service.getStreamStatement(ticket, mockCallContext, listener1);
        verify(listener1).setOnCancelHandler(cancelCaptor.capture());
        cancelCaptor.getValue().run(); // Should handle exception gracefully

        doThrow(new RuntimeException("Close failed")).when(mockBeStream).close();
        when(mockBeStream.next()).thenReturn(true).thenReturn(false);

        FlightProducer.ServerStreamListener listener2 = mock(FlightProducer.ServerStreamListener.class);
        service.getStreamStatement(ticket, mockCallContext, listener2);
        verify(listener2).completed();

        RuntimeException streamError = new RuntimeException("Connection lost");
        when(mockBeStream.next()).thenThrow(streamError);
        doThrow(new RuntimeException("Cancel also failed")).when(mockBeStream).cancel(anyString(), any(Throwable.class));

        FlightProducer.ServerStreamListener listener3 = mock(FlightProducer.ServerStreamListener.class);
        service.getStreamStatement(ticket, mockCallContext, listener3);
        verify(listener3).error(any(FlightRuntimeException.class));
    }

    @Test
    public void testGetStreamWithRetry_withBearerToken() throws Exception {
        FlightClient mockFeClient = mock(FlightClient.class);
        FlightStream mockFeStream = mock(FlightStream.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);

        Cache<String, FlightClient> mockCache = mock(Cache.class);
        when(mockCache.get(anyString(), any())).thenReturn(mockFeClient);
        setFinalField(service, "clientCache", mockCache);

        // First call fails, retry with bearer token succeeds
        when(mockFeClient.getStream(any(Ticket.class), any()))
                .thenThrow(new RuntimeException("First attempt failed"))
                .thenReturn(mockFeStream);
        when(mockFeStream.getRoot()).thenReturn(mockRoot);
        when(mockFeStream.next()).thenReturn(true).thenReturn(false);

        String remoteFeTicket = "token-with-bearer|queryId|remote-fe:9408";
        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(remoteFeTicket)).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticket, mockCallContext, listener);

        verify(mockFeClient, org.mockito.Mockito.times(2)).getStream(any(Ticket.class), any());
        verify(mockCache).invalidate(anyString());
        verify(listener).completed();
    }

    @Test
    public void testGetStreamResultNotFound() {
        when(mockContext.getResult("nonexistent-query")).thenReturn(null);

        FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8("token123|nonexistent-query")).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        assertThrows(FlightRuntimeException.class, () ->
                service.getStreamStatement(ticket, mockCallContext, listener));
    }

    @Test
    public void testProxyForwarding_createPreparedStatement_withTestableService() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Config.arrow_flight_port = 9408;
            testService.addToCacheForTesting("remote-fe:9408", mockFeClient);

            Result mockResult = mock(Result.class);
            java.util.Iterator<Result> mockIter = mock(java.util.Iterator.class);
            when(mockIter.hasNext()).thenReturn(true).thenReturn(false);
            when(mockIter.next()).thenReturn(mockResult);
            when(mockFeClient.doAction(any(org.apache.arrow.flight.Action.class), any()))
                    .thenReturn(mockIter);

            FlightSql.ActionCreatePreparedStatementRequest createReq =
                    FlightSql.ActionCreatePreparedStatementRequest.newBuilder()
                            .setQuery("SELECT 1").build();
            FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);

            testService.createPreparedStatement(createReq, callContext, listener);
            Thread.sleep(500);

            verify(mockFeClient).doAction(any(org.apache.arrow.flight.Action.class), any());
            verify(listener).onNext(mockResult);
            verify(listener).onCompleted();
        }
    }

    @Test
    public void testProxyForwarding_closePreparedStatement_withTestableService() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Config.arrow_flight_port = 9408;
            testService.addToCacheForTesting("remote-fe:9408", mockFeClient);

            java.util.Iterator<Result> emptyIter = mock(java.util.Iterator.class);
            when(emptyIter.hasNext()).thenReturn(false);
            when(mockFeClient.doAction(any(org.apache.arrow.flight.Action.class), any()))
                    .thenReturn(emptyIter);

            FlightSql.ActionClosePreparedStatementRequest closeReq =
                    FlightSql.ActionClosePreparedStatementRequest.newBuilder()
                            .setPreparedStatementHandle(ByteString.copyFromUtf8("stmt-id")).build();
            FlightSqlProducer.StreamListener<Result> listener = mock(FlightSqlProducer.StreamListener.class);

            testService.closePreparedStatement(closeReq, callContext, listener);
            Thread.sleep(500);

            verify(mockFeClient).doAction(any(org.apache.arrow.flight.Action.class), any());
            verify(listener).onCompleted();
        }
    }

    @Test
    public void testProxyForwarding_setSessionOptions_withTestableService() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Config.arrow_flight_port = 9408;
            testService.addToCacheForTesting("remote-fe:9408", mockFeClient);

            SetSessionOptionsResult mockResult = new SetSessionOptionsResult(new HashMap<>());
            when(mockFeClient.setSessionOptions(any(SetSessionOptionsRequest.class), any()))
                    .thenReturn(mockResult);

            SetSessionOptionsRequest setReq = mock(SetSessionOptionsRequest.class);
            FlightProducer.StreamListener<SetSessionOptionsResult> listener =
                    mock(FlightProducer.StreamListener.class);

            testService.setSessionOptions(setReq, callContext, listener);
            Thread.sleep(500);

            verify(mockFeClient).setSessionOptions(any(SetSessionOptionsRequest.class), any());
            verify(listener).onNext(any(SetSessionOptionsResult.class));
            verify(listener).onCompleted();
        }
    }

    @Test
    public void testProxyForwarding_invalidToken_withTestableService() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String invalidToken = "invalid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(invalidToken);
            when(sessionManager.isLocalToken(invalidToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(invalidToken))
                    .thenReturn(null);

            FlightSql.ActionCreatePreparedStatementRequest createReq =
                    FlightSql.ActionCreatePreparedStatementRequest.newBuilder()
                            .setQuery("SELECT 1").build();
            FlightProducer.StreamListener<Result> createListener = mock(FlightProducer.StreamListener.class);
            testService.createPreparedStatement(createReq, callContext, createListener);
            Thread.sleep(500);
            verify(createListener).onError(any(FlightRuntimeException.class));

            SetSessionOptionsRequest setReq = mock(SetSessionOptionsRequest.class);
            FlightProducer.StreamListener<SetSessionOptionsResult> setListener =
                    mock(FlightProducer.StreamListener.class);
            testService.setSessionOptions(setReq, callContext, setListener);
            Thread.sleep(500);
            verify(setListener).onError(any(FlightRuntimeException.class));
        }
    }

    @Test
    public void testProxyForwarding_remoteException_withTestableService() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String remoteToken = "remote-fe|some-uuid-token";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            Config.arrow_flight_port = 9408;
            testService.addToCacheForTesting("remote-fe:9408", mockFeClient);

            when(mockFeClient.doAction(any(org.apache.arrow.flight.Action.class), any()))
                    .thenThrow(new RuntimeException("Network error"));

            FlightSql.ActionCreatePreparedStatementRequest createReq =
                    FlightSql.ActionCreatePreparedStatementRequest.newBuilder()
                            .setQuery("SELECT 1").build();
            FlightProducer.StreamListener<Result> listener = mock(FlightProducer.StreamListener.class);
            testService.createPreparedStatement(createReq, callContext, listener);
            Thread.sleep(500);
            verify(listener).onError(any(FlightRuntimeException.class));
        }
    }

    @Test
    public void testProxyForwarding_metadataRequests_forwardToRemoteFE() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String remoteToken = "remote-fe|550e8400-e29b-41d4-a716-446655440000";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(remoteToken);
            when(sessionManager.isLocalToken(remoteToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(remoteToken))
                    .thenReturn("remote-fe");

            FlightClient mockFeClient = mock(FlightClient.class);
            FlightInfo mockFlightInfo = mock(FlightInfo.class);
            when(mockFeClient.getInfo(any(FlightDescriptor.class), any())).thenReturn(mockFlightInfo);

            Config.arrow_flight_port = 9408;
            testService.addToCacheForTesting("remote-fe:9408", mockFeClient);

            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());

            FlightSql.CommandGetCatalogs catalogsCmd = FlightSql.CommandGetCatalogs.newBuilder().build();
            FlightInfo catalogsResult = testService.getFlightInfoCatalogs(catalogsCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, catalogsResult);

            FlightSql.CommandGetDbSchemas schemasCmd = FlightSql.CommandGetDbSchemas.newBuilder().build();
            FlightInfo schemasResult = testService.getFlightInfoSchemas(schemasCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, schemasResult);

            FlightSql.CommandGetTables tablesCmd = FlightSql.CommandGetTables.newBuilder()
                    .setIncludeSchema(true).build();
            FlightInfo tablesResult = testService.getFlightInfoTables(tablesCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, tablesResult);

            FlightSql.CommandGetSqlInfo sqlInfoCmd = FlightSql.CommandGetSqlInfo.newBuilder().build();
            FlightInfo sqlInfoResult = testService.getFlightInfoSqlInfo(sqlInfoCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, sqlInfoResult);

            FlightSql.CommandGetXdbcTypeInfo typeInfoCmd = FlightSql.CommandGetXdbcTypeInfo.newBuilder().build();
            FlightInfo typeInfoResult = testService.getFlightInfoTypeInfo(typeInfoCmd, callContext, descriptor);
            assertEquals(mockFlightInfo, typeInfoResult);

            verify(mockFeClient, org.mockito.Mockito.atLeast(5)).getInfo(any(FlightDescriptor.class), any());
        }
    }

    @Test
    public void testProxyForwarding_metadataRequests_invalidToken() throws Exception {
        try (MockedStatic<ArrowFlightSqlSessionManager> mockedSessionMgr =
                     mockStatic(ArrowFlightSqlSessionManager.class)) {
            TestableArrowFlightSqlServiceImpl testService =
                    new TestableArrowFlightSqlServiceImpl(sessionManager,
                            Location.forGrpcInsecure("localhost", 1234));
            testService.setProxyEnabled(true);

            String invalidToken = "invalid-token-format";
            FlightProducer.CallContext callContext = mock(FlightProducer.CallContext.class);
            when(callContext.peerIdentity()).thenReturn(invalidToken);
            when(sessionManager.isLocalToken(invalidToken)).thenReturn(false);
            mockedSessionMgr.when(() -> ArrowFlightSqlSessionManager.extractFeHost(invalidToken))
                    .thenReturn(null);

            FlightDescriptor descriptor = FlightDescriptor.command("".getBytes());

            FlightSql.CommandGetCatalogs catalogsCmd = FlightSql.CommandGetCatalogs.newBuilder().build();
            FlightRuntimeException ex = assertThrows(FlightRuntimeException.class, () ->
                    testService.getFlightInfoCatalogs(catalogsCmd, callContext, descriptor));
            assertTrue(ex.getMessage().contains("Invalid token format"));
        }
    }

}
