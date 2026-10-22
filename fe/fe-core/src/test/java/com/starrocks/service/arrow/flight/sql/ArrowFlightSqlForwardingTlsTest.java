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
import com.starrocks.common.Config;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SessionOptionValueFactory;
import org.apache.arrow.flight.SetSessionOptionsRequest;
import org.apache.arrow.flight.SetSessionOptionsResult;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * When a request arrives at an FE for a session of another FE, the FE forwards it to that FE, over TLS if TLS is enabled.
 * These tests send each kind of request to a stand-in FE that only speaks TLS, so a forwarding path that opened a
 * plaintext connection fails.
 */
public class ArrowFlightSqlForwardingTlsTest {

    // "<FE host>|<session id>": a session that belongs to the stand-in FE
    private static final String TOKEN = "localhost|forwarding-test-session";
    private static final String AUTHORIZATION = "Bearer " + TOKEN;

    /** Forwards requests of sessions it does not own, as an FE with the Arrow Flight proxy enabled does. */
    private static class ProxyingService extends ArrowFlightSqlServiceImpl {
        ProxyingService(ArrowFlightSqlSessionManager sessionManager, Location feEndpoint, ArrowFlightSqlTlsConfig tls) {
            super(sessionManager, feEndpoint, tls);
        }

        @Override
        protected boolean isProxyEnabled() {
            return true;
        }
    }

    @TempDir
    Path tempDir;

    private int savedFlightPort;
    private BufferAllocator allocator;
    private ArrowFlightTestSupport.FakeFe fakeFe;
    private ProxyingService service;
    private FlightProducer.CallContext callContext;

    @BeforeEach
    public void setUp() throws Exception {
        allocator = new RootAllocator();
        ArrowFlightTestSupport.PemFiles pem = ArrowFlightTestSupport.generateSelfSignedCertificate(tempDir, "fe");
        ArrowFlightSqlTlsConfig tls = new ArrowFlightSqlTlsConfig(
                pem.cert.toString(), pem.key.toString(), pem.cert.toString(), false);
        fakeFe = new ArrowFlightTestSupport.FakeFe(allocator, tls);

        // the forwarding code connects to the FE named in the token, on the Arrow Flight port of this FE
        savedFlightPort = Config.arrow_flight_port;
        Config.arrow_flight_port = fakeFe.getPort();

        // the session manager is a mock, so no session is local
        service = new ProxyingService(mock(ArrowFlightSqlSessionManager.class),
                Location.forGrpcTls("localhost", 1234), tls);
        callContext = mock(FlightProducer.CallContext.class);
        when(callContext.peerIdentity()).thenReturn(TOKEN);
    }

    @AfterEach
    public void tearDown() throws Exception {
        Config.arrow_flight_port = savedFlightPort;
        service.close();
        fakeFe.close();
        allocator.close();
    }

    @Test
    public void testCloseSession() {
        FlightSqlProducer.StreamListener<CloseSessionResult> listener = mock(FlightSqlProducer.StreamListener.class);

        service.closeSession(new CloseSessionRequest(), callContext, listener);

        ArgumentCaptor<CloseSessionResult> result = ArgumentCaptor.forClass(CloseSessionResult.class);
        verify(listener).onNext(result.capture());
        assertEquals(CloseSessionResult.Status.CLOSED, result.getValue().getStatus());
        verify(listener).onCompleted();
        assertEquals(List.of("DO_ACTION " + AUTHORIZATION), fakeFe.calls);
        assertEquals(List.of(FlightConstants.CLOSE_SESSION.getType()), fakeFe.actions);
    }

    @Test
    public void testSetSessionOptions() {
        SetSessionOptionsRequest request = new SetSessionOptionsRequest(
                Map.of("catalog", SessionOptionValueFactory.makeSessionOptionValue("default_catalog")));
        FlightSqlProducer.StreamListener<SetSessionOptionsResult> listener = mock(FlightSqlProducer.StreamListener.class);

        service.setSessionOptions(request, callContext, listener);

        verify(listener, timeout(10_000)).onCompleted();
        verify(listener, never()).onError(any());
        assertEquals(List.of("DO_ACTION " + AUTHORIZATION), fakeFe.calls);
        assertEquals(List.of(FlightConstants.SET_SESSION_OPTIONS.getType()), fakeFe.actions);
    }

    @Test
    public void testCreatePreparedStatement() {
        FlightSql.ActionCreatePreparedStatementRequest request =
                FlightSql.ActionCreatePreparedStatementRequest.newBuilder().setQuery("SELECT 1").build();
        FlightSqlProducer.StreamListener<Result> listener = mock(FlightSqlProducer.StreamListener.class);

        service.createPreparedStatement(request, callContext, listener);

        verify(listener, timeout(10_000)).onCompleted();
        verify(listener, never()).onError(any());
        // the type of the forwarded action is not checked, this test is about the connection
        assertEquals(List.of("DO_ACTION " + AUTHORIZATION), fakeFe.calls);
        assertEquals(1, fakeFe.actions.size());
    }

    @Test
    public void testGetFlightInfo() {
        FlightSql.CommandPreparedStatementQuery command = FlightSql.CommandPreparedStatementQuery.newBuilder()
                .setPreparedStatementHandle(ByteString.copyFromUtf8("handle")).build();
        FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());

        FlightInfo info = service.getFlightInfoPreparedStatement(command, callContext, descriptor);

        assertNotNull(info);
        assertEquals(List.of("GET_FLIGHT_INFO " + AUTHORIZATION), fakeFe.calls);
    }

    @Test
    public void testResultStreamOfAnotherFe() {
        // "<session id>|<query id>|<FE host>:<FE port>": the result is on the FE that owns the session
        String ticket = "forwarding-test-session|query-1|localhost:" + fakeFe.getPort();
        FlightSql.TicketStatementQuery ticketStatement = FlightSql.TicketStatementQuery.newBuilder()
                .setStatementHandle(ByteString.copyFromUtf8(ticket)).build();
        FlightProducer.ServerStreamListener listener = mock(FlightProducer.ServerStreamListener.class);

        service.getStreamStatement(ticketStatement, callContext, listener);

        // the stand-in has no result to send, and says so: the error shows that the request got through
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        assertTrue(error.getValue().getMessage().contains(ArrowFlightTestSupport.FakeFe.NO_SUCH_RESULT),
                error.getValue().getMessage());
        assertEquals(List.of("DO_GET " + AUTHORIZATION), fakeFe.calls);
    }
}
