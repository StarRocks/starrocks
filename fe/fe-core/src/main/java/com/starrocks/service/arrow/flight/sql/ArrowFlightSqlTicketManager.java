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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.NetUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TUniqueId;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Location;
import org.apache.arrow.util.VisibleForTesting;

import java.net.URI;


/**
 * Manages Arrow Flight SQL ticket creation, parsing, and endpoint resolution.
 *
 * <p>Ticket formats:
 * <ul>
 *   <li>FE Local: {@code <Token>|<QueryId>} - 2 parts, handled by this FE</li>
 *   <li>FE Proxy: {@code <Token>|<QueryId>|<FEHost>:<FEPort>} - 3 parts, proxied through FE</li>
 *   <li>BE Proxy: {@code <QueryId>|<FragmentInstanceId>|<BEHost>|<BEPort>} - 4 parts, proxied to BE</li>
 *   <li>BE Direct: {@code <QueryId>:<FragmentInstanceId>} - used for direct BE connection</li>
 * </ul>
 */
public class ArrowFlightSqlTicketManager {

    private static final String TICKET_DELIMITER = "\\|"; // regexp
    private static final String BE_TICKET_DELIMITER = ":";
    private static final String FE_TICKET_DELIMITER = "|";
    private static final String GRPCS_SCHEME = "grpcs://";
    private static final String GRPC_SCHEME = "grpc://";

    private final Location feEndpoint;

    public enum TicketType {
        // FE local ticket: Token|QueryId
        FE_LOCAL,
        // FE proxy ticket: Token|QueryId|FEHost:FEPort
        FE_PROXY,
        //BE proxy ticket: QueryId|FragmentInstanceId|BEHost|BEPort
        BE_PROXY
    }

    public static class ParsedTicket {
        private final TicketType type;
        private final String token;
        private final String queryId;
        private final String fragmentInstanceId;
        private final String host;
        private final int port;

        private ParsedTicket(TicketType type, String token, String queryId,
                             String fragmentInstanceId, String host, int port) {
            this.type = type;
            this.token = token;
            this.queryId = queryId;
            this.fragmentInstanceId = fragmentInstanceId;
            this.host = host;
            this.port = port;
        }

        public TicketType getType() {
            return type;
        }

        public String getToken() {
            return token;
        }

        public String getQueryId() {
            return queryId;
        }

        public String getFragmentInstanceId() {
            return fragmentInstanceId;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        static ParsedTicket feLocal(String token, String queryId) {
            return new ParsedTicket(TicketType.FE_LOCAL, token, queryId, null, null, 0);
        }

        static ParsedTicket feProxy(String token, String queryId, String host, int port) {
            return new ParsedTicket(TicketType.FE_PROXY, token, queryId, null, host, port);
        }

        static ParsedTicket beProxy(String queryId, String fragmentInstanceId, String host, int port) {
            return new ParsedTicket(TicketType.BE_PROXY, null, queryId, fragmentInstanceId, host, port);
        }
    }

    public ArrowFlightSqlTicketManager(Location feEndpoint) {
        this.feEndpoint = feEndpoint;
    }

    /**
     * Parses a ticket string and returns the parsed ticket data.
     *
     * Ticket formats:
     * - FE_LOCAL (2 parts):  token|queryId
     * - FE_PROXY (3 parts):  token|queryId|feHost:fePort
     * - BE_PROXY (4 parts):  queryId|fragmentInstanceId|beHost|bePort
     *
     * @param ticket the ticket string to parse
     * @return parsed ticket data
     * @throws org.apache.arrow.flight.FlightRuntimeException if the ticket format is invalid
     */
    public ParsedTicket parseTicket(String ticket) {
        String[] ticketParts = ticket.split(TICKET_DELIMITER);

        switch (ticketParts.length) {
            case 2:
                // FE_LOCAL: token|queryId
                return ParsedTicket.feLocal(ticketParts[0], ticketParts[1]);
            case 3:
                return parseFEProxyTicket(ticketParts);
            case 4:
                return parseBEProxyTicket(ticketParts);
            default:
                throw CallStatus.INVALID_ARGUMENT.withDescription(
                        String.format("Invalid ticket format: expected 2, 3, or 4 parts, got [%d]",
                                ticketParts.length)).toRuntimeException();
        }
    }

    /**
     * Parses FE proxy ticket format: token|queryId|feHost:fePort
     * ticketParts[0] = token
     * ticketParts[1] = queryId
     * ticketParts[2] = feHost:fePort (or [ipv6]:fePort)
     */
    private ParsedTicket parseFEProxyTicket(String[] ticketParts) {
        String[] hostPort;
        try {
            hostPort = NetUtils.resolveHostInfoFromHostPort(ticketParts[2]);
        } catch (AnalysisException e) { // deprecated, but NetUtils uses
            throw CallStatus.INVALID_ARGUMENT.withDescription(
                    "Invalid FE host:port format: " + ticketParts[2]).toRuntimeException();
        }

        int port;
        try {
            port = Integer.parseInt(hostPort[1]);
        } catch (NumberFormatException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(
                    "Invalid FE port: " + hostPort[1]).toRuntimeException();
        }

        return ParsedTicket.feProxy(ticketParts[0], ticketParts[1], hostPort[0], port);
    }

    /**
     * Parses BE proxy ticket format: queryId|fragmentInstanceId|beHost|bePort
     * ticketParts[0] = queryId
     * ticketParts[1] = fragmentInstanceId
     * ticketParts[2] = beHost
     * ticketParts[3] = bePort
     */
    private ParsedTicket parseBEProxyTicket(String[] ticketParts) {
        int port;
        try {
            port = Integer.parseInt(ticketParts[3]);
        } catch (NumberFormatException e) {
            throw CallStatus.INVALID_ARGUMENT.withDescription(
                    String.format("Invalid ticket format: expected numerical port, received [%s]",
                            ticketParts[3])).toRuntimeException();
        }

        return ParsedTicket.beProxy(ticketParts[0], ticketParts[1], ticketParts[2], port);
    }

    public boolean isLocalFE(ParsedTicket ticket) {
        if (ticket.getType() != TicketType.FE_PROXY) {
            return false;
        }

        URI feUri = feEndpoint.getUri(); 
        return ticket.getHost().equals(feUri.getHost()) && ticket.getPort() == feUri.getPort();
    }

    public ByteString buildBETicket(TUniqueId queryId, TUniqueId fragmentInstanceId) {
        return ByteString.copyFromUtf8(
                hexStringFromUniqueId(queryId) + BE_TICKET_DELIMITER + hexStringFromUniqueId(fragmentInstanceId));
    }

    public ByteString buildBETicket(String queryId, String fragmentInstanceId) {
        return ByteString.copyFromUtf8(queryId + BE_TICKET_DELIMITER + fragmentInstanceId);
    }

    public ByteString buildFEProxyTicketForBE(TUniqueId queryId, TUniqueId fragmentInstanceId,
                                               ComputeNode worker) {
        return ByteString.copyFromUtf8(
                hexStringFromUniqueId(queryId) + "|"
                        + hexStringFromUniqueId(fragmentInstanceId) + "|"
                        + worker.getHost() + "|"
                        + worker.getArrowFlightPort());
    }

    public ByteString buildFELocalTicket(String token, TUniqueId queryId) {
        // Extract UUID from token (token may be "host|uuid" or just "uuid")
        String uuid = extractUuidFromToken(token);
        return ByteString.copyFromUtf8(uuid + FE_TICKET_DELIMITER + DebugUtil.printId(queryId));
    }

    public ByteString buildFEProxyTicket(String token, TUniqueId queryId) {
        // Extract UUID from token (token may be "host|uuid" or just "uuid")
        String uuid = extractUuidFromToken(token);
        String feHost = feEndpoint.getUri().getHost();
        int fePort = feEndpoint.getUri().getPort();
        String hostPort = NetUtils.getHostPortInAccessibleFormat(feHost, fePort);
        return ByteString.copyFromUtf8(uuid + FE_TICKET_DELIMITER + DebugUtil.printId(queryId) + FE_TICKET_DELIMITER + hostPort);
    }

    /**
     * Gets the FE endpoint and ticket handle for FE tasks.
     *
     * @return a pair of (endpoint location, ticket handle)
     */
    public Pair<Location, ByteString> getFEEndpoint(ArrowFlightSqlConnectContext ctx)
            throws InvalidConfException {
        ByteString handle;
        Location endpoint;

        if (isProxyEnabled()) {
            String arrowFlightProxy = getProxyAddress();
            handle = buildFEProxyTicket(ctx.getArrowFlightSqlToken(), ctx.getExecutionId());

            if (!arrowFlightProxy.isEmpty()) {
                endpoint = getProxyLocation(arrowFlightProxy);
            } else {
                endpoint = feEndpoint;
            }
        } else {
            handle = buildFELocalTicket(ctx.getArrowFlightSqlToken(), ctx.getExecutionId());
            endpoint = feEndpoint;
        }

        return new Pair<>(endpoint, handle);
    }

    /**
     * Gets the BE endpoint and ticket handle for BE tasks.
     *
     * @return a pair of (endpoint location, ticket handle)
     */
    public Pair<Location, ByteString> getBEEndpoint(TUniqueId queryId, ComputeNode worker,
                                                     TUniqueId rootFragmentInstanceId)
            throws InvalidConfException {
        ByteString handle;
        Location endpoint;

        if (isProxyEnabled()) {
            String arrowFlightProxy = getProxyAddress();
            handle = buildFEProxyTicketForBE(queryId, rootFragmentInstanceId, worker);
            if (arrowFlightProxy.isEmpty()) {
                endpoint = feEndpoint;
            } else {
                endpoint = getProxyLocation(arrowFlightProxy);
            }
        } else {
            handle = buildBETicket(queryId, rootFragmentInstanceId);
            endpoint = Location.forGrpcInsecure(worker.getHost(), worker.getArrowFlightPort());
        }

        return new Pair<>(endpoint, handle);
    }

    public Location getFELocation() throws InvalidConfException {
        if (isProxyEnabled()) {
            String arrowFlightProxy = getProxyAddress();
            if (!arrowFlightProxy.isEmpty()) {
                return getProxyLocation(arrowFlightProxy);
            }
        }
        return feEndpoint;
    }

    public Location getProxyLocation(String arrowFlightProxy) throws InvalidConfException {
        validateProxyFormat(arrowFlightProxy);

        boolean useTls = arrowFlightProxy.startsWith(GRPCS_SCHEME);
        String hostPortStr = arrowFlightProxy;

        // Strip scheme if present
        if (arrowFlightProxy.startsWith(GRPCS_SCHEME)) {
            hostPortStr = arrowFlightProxy.substring(GRPCS_SCHEME.length());
        } else if (arrowFlightProxy.startsWith(GRPC_SCHEME)) {
            hostPortStr = arrowFlightProxy.substring(GRPC_SCHEME.length());
        }

        String[] hostPort;
        try {
            hostPort = NetUtils.resolveHostInfoFromHostPort(hostPortStr);
        } catch (AnalysisException e) { // deprecated, but NetUtils uses
            throw new InvalidConfException(e.getMessage());
        }
        int port = Integer.parseInt(hostPort[1]);

        if (useTls) {
            return Location.forGrpcTls(hostPort[0], port);
        }
        return Location.forGrpcInsecure(hostPort[0], port);
    }

    public void validateProxyFormat(String arrowFlightProxy) throws InvalidConfException {
        if (arrowFlightProxy.isEmpty()) {
            return;
        }

        String hostPortStr = arrowFlightProxy;
        // Strip scheme if present (grpcs:// or grpc://)
        if (arrowFlightProxy.startsWith(GRPCS_SCHEME)) {
            hostPortStr = arrowFlightProxy.substring(GRPCS_SCHEME.length());
        } else if (arrowFlightProxy.startsWith(GRPC_SCHEME)) {
            hostPortStr = arrowFlightProxy.substring(GRPC_SCHEME.length());
        }

        String[] hostPort;
        try {
            hostPort = NetUtils.resolveHostInfoFromHostPort(hostPortStr);
        } catch (AnalysisException e) { // deprecated, but NetUtils uses
            throw new InvalidConfException(
                    String.format("Expected format 'hostname:port' or 'grpcs://hostname:port', got '%s'",
                            arrowFlightProxy));
        }

        if (hostPort[0].isEmpty()) {
            throw new InvalidConfException(
                    String.format("Hostname cannot be empty, got '%s'", arrowFlightProxy));
        }

        try {
            int port = Integer.parseInt(hostPort[1]);
            if (port < 1 || port > 65535) {
                throw new InvalidConfException(
                        String.format("Port must be between 1 and 65535, got '%d'", port));
            }
        } catch (NumberFormatException e) {
            throw new InvalidConfException(
                    String.format("Port must be a valid integer, got '%s'", hostPort[1]));
        }
    }

    public Location getInternalFEEndpoint() {
        return feEndpoint;
    }

    @VisibleForTesting
    protected boolean isProxyEnabled() {
        return GlobalVariable.isArrowFlightProxyEnabled();
    }

    @VisibleForTesting
    protected String getProxyAddress() {
        return GlobalVariable.getArrowFlightProxy();
    }

    /**
     * Extracts the UUID portion from a token.
     * When proxy is enabled, tokens have format "FE_HOST|UUID", otherwise just "UUID".
     * This method returns just the UUID part to avoid delimiter conflicts in tickets.
     */
    private static String extractUuidFromToken(String token) {
        if (token == null || token.isEmpty()) {
            return token;
        }
        int lastDelimiter = token.lastIndexOf('|');
        if (lastDelimiter > 0) {
            // Token contains delimiter, extract UUID after last '|'
            return token.substring(lastDelimiter + 1);
        }
        // Token doesn't contain delimiter, return as-is
        return token;
    }

    private static String hexStringFromUniqueId(TUniqueId id) {
        if (id == null) {
            return "";
        }
        return Long.toHexString(id.hi) + "-" + Long.toHexString(id.lo);
    }
}
