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

package com.starrocks.mysql;

import com.google.common.annotations.VisibleForTesting;
import inet.ipaddr.IPAddressString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Parses HAProxy PROXY protocol v1 (text) headers from an incoming MySQL TCP connection.
 * The header is written by the load balancer before any application data and carries the
 * real client IP and port.
 *
 * Spec: https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
 *
 * Returns null for UNKNOWN-family connections (short form "PROXY UNKNOWN\r\n" or long form),
 * in which case callers should fall back to the downstream peer address.
 */
public class ProxyProtocolParser {

    // v1 max line length per spec (including trailing \r\n)
    private static final int V1_MAX_LEN = 108;
    private static final byte[] V1_PREFIX = "PROXY ".getBytes(StandardCharsets.US_ASCII);

    public static class Result {
        public final String ip;
        public final int port;

        Result(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }
    }

    /**
     * Reads and parses a PROXY protocol header from the given MySQL channel.
     * Must be called before the MySQL handshake begins.
     *
     * @return the real client address, or null for UNKNOWN-family connections (health checks)
     * @throws IOException if the header is missing, malformed, or the connection closes early
     */
    public static Result parse(MysqlChannel channel, long timeoutMs) throws IOException {
        return parse(n -> {
            ByteBuffer buf = ByteBuffer.allocate(n);
            int read = channel.readAllPlainWithTimeout(buf, timeoutMs);
            if (read < n) {
                throw new IOException(
                        "Connection closed while reading PROXY protocol header (read " + read + " of " + n + ")");
            }
            return buf.array();
        });
    }

    @VisibleForTesting
    static Result parse(ByteReader reader) throws IOException {
        byte[] first6 = reader.read(6);
        if (Arrays.equals(first6, V1_PREFIX)) {
            return parseV1(reader, first6);
        }
        throw new IOException("Not a valid PROXY protocol v1 header");
    }

    // Parses: PROXY <proto> <src-ip> <dst-ip> <src-port> <dst-port>\r\n
    // dst-ip and dst-port are the proxy's listening address; only src-ip and src-port are used.
    private static Result parseV1(ByteReader reader, byte[] prefix) throws IOException {
        byte[] lineBuf = new byte[V1_MAX_LEN];
        System.arraycopy(prefix, 0, lineBuf, 0, prefix.length);
        int pos = prefix.length;
        byte prev = prefix[prefix.length - 1];

        while (pos < V1_MAX_LEN) {
            byte b = reader.read(1)[0];
            lineBuf[pos++] = b;
            if (prev == '\r' && b == '\n') {
                break;
            }
            if (pos == V1_MAX_LEN) {
                throw new IOException("PROXY protocol v1 header exceeds maximum length of " + V1_MAX_LEN);
            }
            prev = b;
        }

        String line = new String(lineBuf, 0, pos, StandardCharsets.US_ASCII).trim();
        String[] parts = line.split(" ");
        if (parts.length < 2) {
            throw new IOException("Invalid PROXY protocol v1 header: " + line);
        }

        // UNKNOWN may be short form ("PROXY UNKNOWN") or long form with optional address fields.
        // Either way, fall back to the socket peer address.
        if ("UNKNOWN".equals(parts[1])) {
            return null;
        }

        if (parts.length < 6) {
            throw new IOException("Invalid PROXY protocol v1 header: " + line);
        }

        boolean isTcp4 = "TCP4".equals(parts[1]);
        boolean isTcp6 = "TCP6".equals(parts[1]);
        if (!isTcp4 && !isTcp6) {
            throw new IOException("Unsupported protocol family in PROXY protocol v1 header: " + parts[1]);
        }

        String srcIp = parts[2];
        IPAddressString srcAddr = new IPAddressString(srcIp);
        if (!srcAddr.isValid()) {
            throw new IOException("Invalid source IP address in PROXY protocol v1 header: " + srcIp);
        }
        if (isTcp4 && !srcAddr.isIPv4()) {
            throw new IOException("PROXY protocol v1 TCP4 requires an IPv4 source address, got: " + srcIp);
        }
        if (isTcp6 && !srcAddr.isIPv6()) {
            throw new IOException("PROXY protocol v1 TCP6 requires an IPv6 source address, got: " + srcIp);
        }

        int srcPort;
        try {
            srcPort = Integer.parseInt(parts[4]);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid source port in PROXY protocol v1 header: " + parts[4]);
        }
        if (srcPort < 0 || srcPort > 65535) {
            throw new IOException("Source port out of range in PROXY protocol v1 header: " + srcPort);
        }

        return new Result(srcIp, srcPort);
    }

    @FunctionalInterface
    interface ByteReader {
        byte[] read(int n) throws IOException;
    }
}
