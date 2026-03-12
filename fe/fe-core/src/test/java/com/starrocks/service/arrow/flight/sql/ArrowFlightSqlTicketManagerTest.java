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

import com.google.protobuf.ByteString;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.Pair;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ArrowFlightSqlTicketManagerTest {

    private ArrowFlightSqlTicketManager ticketManager;
    private Location feEndpoint;

    @BeforeEach
    public void setUp() {
        feEndpoint = Location.forGrpcInsecure("localhost", 1234);
        ticketManager = new ArrowFlightSqlTicketManager(feEndpoint);
    }

    @Test
    public void testParseTicketValidFormats() {
        ArrowFlightSqlTicketManager.ParsedTicket feLocal = ticketManager.parseTicket("token123|queryId456");
        assertEquals(ArrowFlightSqlTicketManager.TicketType.FE_LOCAL, feLocal.getType());
        assertEquals("token123", feLocal.getToken());
        assertEquals("queryId456", feLocal.getQueryId());
        assertNull(feLocal.getFragmentInstanceId());
        assertNull(feLocal.getHost());
        assertEquals(0, feLocal.getPort());

        ArrowFlightSqlTicketManager.ParsedTicket feProxy = ticketManager.parseTicket("token123|queryId456|fe-host:9408");
        assertEquals(ArrowFlightSqlTicketManager.TicketType.FE_PROXY, feProxy.getType());
        assertEquals("token123", feProxy.getToken());
        assertEquals("queryId456", feProxy.getQueryId());
        assertNull(feProxy.getFragmentInstanceId());
        assertEquals("fe-host", feProxy.getHost());
        assertEquals(9408, feProxy.getPort());

        ArrowFlightSqlTicketManager.ParsedTicket beProxy = ticketManager.parseTicket("queryId|fragmentId|be-host|8815");
        assertEquals(ArrowFlightSqlTicketManager.TicketType.BE_PROXY, beProxy.getType());
        assertNull(beProxy.getToken());
        assertEquals("queryId", beProxy.getQueryId());
        assertEquals("fragmentId", beProxy.getFragmentInstanceId());
        assertEquals("be-host", beProxy.getHost());
        assertEquals(8815, beProxy.getPort());
    }

    @Test
    public void testParseTicketInvalidFormats() {
        FlightRuntimeException ex1 = assertThrows(FlightRuntimeException.class,
                () -> ticketManager.parseTicket("just-one-part"));
        assertTrue(ex1.getMessage().contains("expected 2, 3, or 4 parts, got [1]"));

        FlightRuntimeException ex5 = assertThrows(FlightRuntimeException.class,
                () -> ticketManager.parseTicket("a|b|c|d|e"));
        assertTrue(ex5.getMessage().contains("expected 2, 3, or 4 parts, got [5]"));

        FlightRuntimeException exFeHost = assertThrows(FlightRuntimeException.class,
                () -> ticketManager.parseTicket("token123|queryId|invalid_host_port"));
        assertTrue(exFeHost.getMessage().contains("Invalid FE host:port format"));

        FlightRuntimeException exFePort = assertThrows(FlightRuntimeException.class,
                () -> ticketManager.parseTicket("token123|queryId|hostname:abc"));
        assertTrue(exFePort.getMessage().contains("Invalid FE port"));

        FlightRuntimeException exBePort = assertThrows(FlightRuntimeException.class,
                () -> ticketManager.parseTicket("queryId|fragmentId|hostname|non-numeric"));
        assertTrue(exBePort.getMessage().contains("expected numerical port"));
    }

    @Test
    public void testIsLocalFE() {
        ArrowFlightSqlTicketManager.ParsedTicket localMatch = ticketManager.parseTicket("token|query|localhost:1234");
        assertTrue(ticketManager.isLocalFE(localMatch));

        ArrowFlightSqlTicketManager.ParsedTicket diffHost = ticketManager.parseTicket("token|query|remote-host:1234");
        assertFalse(ticketManager.isLocalFE(diffHost));

        ArrowFlightSqlTicketManager.ParsedTicket diffPort = ticketManager.parseTicket("token|query|localhost:9999");
        assertFalse(ticketManager.isLocalFE(diffPort));

        ArrowFlightSqlTicketManager.ParsedTicket notProxy = ticketManager.parseTicket("token|query");
        assertFalse(ticketManager.isLocalFE(notProxy));
    }

    @Test
    public void testBuildTickets() {
        ByteString beTicketStr = ticketManager.buildBETicket("queryId", "fragmentId");
        assertEquals("queryId:fragmentId", beTicketStr.toStringUtf8());

        ByteString beTicketId = ticketManager.buildBETicket(new TUniqueId(0x1234L, 0x5678L), new TUniqueId(0xABCDL, 0xEF01L));
        assertEquals("1234-5678:abcd-ef01", beTicketId.toStringUtf8());

        ByteString feLocal = ticketManager.buildFELocalTicket("test-token", new TUniqueId(5L, 6L));
        assertEquals("test-token|00000000-0000-0005-0000-000000000006", feLocal.toStringUtf8());

        ByteString feProxy = ticketManager.buildFEProxyTicket("test-token", new TUniqueId(5L, 6L));
        assertEquals("test-token|00000000-0000-0005-0000-000000000006|localhost:1234", feProxy.toStringUtf8());

        ComputeNode worker = mock(ComputeNode.class);
        when(worker.getHost()).thenReturn("be-host");
        when(worker.getArrowFlightPort()).thenReturn(8815);
        ByteString feProxyBe = ticketManager.buildFEProxyTicketForBE(new TUniqueId(3L, 4L), new TUniqueId(1L, 2L), worker);
        assertEquals("3-4|1-2|be-host|8815", feProxyBe.toStringUtf8());
    }

    @Test
    public void testValidateProxyFormatValid() throws InvalidConfException {
        // All valid formats should not throw
        ticketManager.validateProxyFormat("hostname:9408");
        ticketManager.validateProxyFormat("grpc://hostname:9408");
        ticketManager.validateProxyFormat("grpcs://hostname:443");
        ticketManager.validateProxyFormat("hostname:1");
        ticketManager.validateProxyFormat("hostname:65535");
        ticketManager.validateProxyFormat("");
    }

    @Test
    public void testValidateProxyFormatInvalid() {
        InvalidConfException noPort = assertThrows(InvalidConfException.class,
                () -> ticketManager.validateProxyFormat("hostname-only"));
        assertTrue(noPort.getMessage().contains("Expected format 'hostname:port'"));

        InvalidConfException emptyHost = assertThrows(InvalidConfException.class,
                () -> ticketManager.validateProxyFormat(":9408"));
        assertTrue(emptyHost.getMessage().contains("Hostname cannot be empty"));

        InvalidConfException nonNumeric = assertThrows(InvalidConfException.class,
                () -> ticketManager.validateProxyFormat("hostname:abc"));
        assertTrue(nonNumeric.getMessage().contains("Port must be a valid integer"));

        InvalidConfException portLow = assertThrows(InvalidConfException.class,
                () -> ticketManager.validateProxyFormat("hostname:0"));
        assertTrue(portLow.getMessage().contains("Port must be between 1 and 65535"));

        InvalidConfException portHigh = assertThrows(InvalidConfException.class,
                () -> ticketManager.validateProxyFormat("hostname:99999"));
        assertTrue(portHigh.getMessage().contains("Port must be between 1 and 65535"));
    }

    @Test
    public void testGetProxyLocation() throws InvalidConfException {
        assertEquals(Location.forGrpcInsecure("proxy-host", 9408),
                ticketManager.getProxyLocation("proxy-host:9408"));

        assertEquals(Location.forGrpcInsecure("proxy-host", 9408),
                ticketManager.getProxyLocation("grpc://proxy-host:9408"));

        assertEquals(Location.forGrpcTls("proxy-host", 443),
                ticketManager.getProxyLocation("grpcs://proxy-host:443"));

        assertEquals(Location.forGrpcInsecure("::1", 9408),
                ticketManager.getProxyLocation("[::1]:9408"));

        assertEquals(Location.forGrpcTls("2001:db8::1", 443),
                ticketManager.getProxyLocation("grpcs://[2001:db8::1]:443"));
    }

    @Test
    public void testParseTicketWithIPv6() {
        ArrowFlightSqlTicketManager.ParsedTicket ipv6Ticket =
                ticketManager.parseTicket("token123|queryId456|[::1]:9408");
        assertEquals(ArrowFlightSqlTicketManager.TicketType.FE_PROXY, ipv6Ticket.getType());
        assertEquals("token123", ipv6Ticket.getToken());
        assertEquals("queryId456", ipv6Ticket.getQueryId());
        assertEquals("::1", ipv6Ticket.getHost());
        assertEquals(9408, ipv6Ticket.getPort());
    }

    @Test
    public void testBuildTicketsWithProxyToken() {
        // When proxy is enabled, tokens have format "FE_HOST|UUID"
        String proxyToken = "fe-host-1|550e8400-e29b-41d4-a716-446655440000";

        ByteString feLocal = ticketManager.buildFELocalTicket(proxyToken, new TUniqueId(5L, 6L));
        String feLocalStr = feLocal.toStringUtf8();
        assertEquals("550e8400-e29b-41d4-a716-446655440000|00000000-0000-0005-0000-000000000006", feLocalStr);

        ArrowFlightSqlTicketManager.ParsedTicket parsedFeLocal = ticketManager.parseTicket(feLocalStr);
        assertEquals(ArrowFlightSqlTicketManager.TicketType.FE_LOCAL, parsedFeLocal.getType());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", parsedFeLocal.getToken());
        assertEquals("00000000-0000-0005-0000-000000000006", parsedFeLocal.getQueryId());

        ByteString feProxy = ticketManager.buildFEProxyTicket(proxyToken, new TUniqueId(7L, 8L));
        String feProxyStr = feProxy.toStringUtf8();
        assertEquals("550e8400-e29b-41d4-a716-446655440000|00000000-0000-0007-0000-000000000008|localhost:1234",
                     feProxyStr);

        ArrowFlightSqlTicketManager.ParsedTicket parsedFeProxy = ticketManager.parseTicket(feProxyStr);
        assertEquals(ArrowFlightSqlTicketManager.TicketType.FE_PROXY, parsedFeProxy.getType());
        assertEquals("550e8400-e29b-41d4-a716-446655440000", parsedFeProxy.getToken());
        assertEquals("00000000-0000-0007-0000-000000000008", parsedFeProxy.getQueryId());
        assertEquals("localhost", parsedFeProxy.getHost());
        assertEquals(1234, parsedFeProxy.getPort());
    }

    @Test
    public void testGetFEEndpoint() throws Exception {
        ArrowFlightSqlConnectContext mockCtx = mock(ArrowFlightSqlConnectContext.class);
        when(mockCtx.getArrowFlightSqlToken()).thenReturn("test-token");
        when(mockCtx.getExecutionId()).thenReturn(new TUniqueId(5L, 6L));

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            Pair<Location, ByteString> result = ticketManager.getFEEndpoint(mockCtx);
            assertEquals(Location.forGrpcInsecure("localhost", 1234), result.first);
            assertTrue(result.second.toStringUtf8().startsWith("test-token|"));
            assertFalse(result.second.toStringUtf8().contains("localhost:1234"));
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("");

            Pair<Location, ByteString> emptyProxy = ticketManager.getFEEndpoint(mockCtx);
            assertEquals(Location.forGrpcInsecure("localhost", 1234), emptyProxy.first);
            assertTrue(emptyProxy.second.toStringUtf8().contains("localhost:1234"));
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("proxy-host:9408");

            Pair<Location, ByteString> withProxy = ticketManager.getFEEndpoint(mockCtx);
            assertEquals(Location.forGrpcInsecure("proxy-host", 9408), withProxy.first);
            assertTrue(withProxy.second.toStringUtf8().contains("localhost:1234"));
        }
    }

    @Test
    public void testGetBEEndpoint() throws Exception {
        ComputeNode mockWorker = mock(ComputeNode.class);
        when(mockWorker.getHost()).thenReturn("be-host");
        when(mockWorker.getArrowFlightPort()).thenReturn(8815);
        TUniqueId queryId = new TUniqueId(3L, 4L);
        TUniqueId fragmentId = new TUniqueId(1L, 2L);

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);

            Pair<Location, ByteString> result = ticketManager.getBEEndpoint(queryId, mockWorker, fragmentId);
            assertEquals(Location.forGrpcInsecure("be-host", 8815), result.first);
            assertEquals("3-4:1-2", result.second.toStringUtf8());
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("");

            Pair<Location, ByteString> emptyProxy = ticketManager.getBEEndpoint(queryId, mockWorker, fragmentId);
            assertEquals(Location.forGrpcInsecure("localhost", 1234), emptyProxy.first);
            assertEquals("3-4|1-2|be-host|8815", emptyProxy.second.toStringUtf8());
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("proxy-host:9400");

            Pair<Location, ByteString> withProxy = ticketManager.getBEEndpoint(queryId, mockWorker, fragmentId);
            assertEquals(Location.forGrpcInsecure("proxy-host", 9400), withProxy.first);
            assertEquals("3-4|1-2|be-host|8815", withProxy.second.toStringUtf8());
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("grpcs://proxy-host:443");

            Pair<Location, ByteString> grpcs = ticketManager.getBEEndpoint(queryId, mockWorker, fragmentId);
            assertEquals(Location.forGrpcTls("proxy-host", 443), grpcs.first);
        }
    }

    @Test
    public void testGetFELocation() throws Exception {
        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(false);
            assertEquals(Location.forGrpcInsecure("localhost", 1234), ticketManager.getFELocation());
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("");
            assertEquals(Location.forGrpcInsecure("localhost", 1234), ticketManager.getFELocation());
        }

        try (MockedStatic<GlobalVariable> mocked = mockStatic(GlobalVariable.class)) {
            mocked.when(GlobalVariable::isArrowFlightProxyEnabled).thenReturn(true);
            mocked.when(GlobalVariable::getArrowFlightProxy).thenReturn("proxy-host:9408");
            assertEquals(Location.forGrpcInsecure("proxy-host", 9408), ticketManager.getFELocation());
        }
    }
}
