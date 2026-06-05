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

package com.starrocks.mysql.nio;

import com.starrocks.mysql.MysqlChannel;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AcceptListenerTest {

    @Test
    public void testWildcard_allPeersTrusted() {
        assertTrue(AcceptListener.isTrustedProxyPeer("1.2.3.4", "*"));
        assertTrue(AcceptListener.isTrustedProxyPeer("192.168.1.100", "*"));
        assertTrue(AcceptListener.isTrustedProxyPeer("::1", "*"));
    }

    @Test
    public void testSingleIpv4Cidr() {
        assertTrue(AcceptListener.isTrustedProxyPeer("10.0.0.1", "10.0.0.0/8"));
        assertTrue(AcceptListener.isTrustedProxyPeer("10.255.255.255", "10.0.0.0/8"));
        assertFalse(AcceptListener.isTrustedProxyPeer("11.0.0.1", "10.0.0.0/8"));
        assertFalse(AcceptListener.isTrustedProxyPeer("192.168.1.1", "10.0.0.0/8"));
    }

    @Test
    public void testMultipleCidrRanges() {
        String networks = "10.0.0.0/8;172.16.0.0/12";
        assertTrue(AcceptListener.isTrustedProxyPeer("10.1.2.3", networks));
        assertTrue(AcceptListener.isTrustedProxyPeer("172.16.0.1", networks));
        assertTrue(AcceptListener.isTrustedProxyPeer("172.31.255.255", networks));
        assertFalse(AcceptListener.isTrustedProxyPeer("172.32.0.1", networks));
        assertFalse(AcceptListener.isTrustedProxyPeer("192.168.1.1", networks));
    }

    @Test
    public void testIpv6Cidr() {
        assertTrue(AcceptListener.isTrustedProxyPeer("::1", "::1/128"));
        assertFalse(AcceptListener.isTrustedProxyPeer("::2", "::1/128"));
    }

    @Test
    public void testMixedIpv4AndIpv6() {
        String networks = "10.0.0.0/8;fd00::/8";
        assertTrue(AcceptListener.isTrustedProxyPeer("10.5.5.5", networks));
        assertTrue(AcceptListener.isTrustedProxyPeer("fd00::1", networks));
        assertFalse(AcceptListener.isTrustedProxyPeer("192.168.1.1", networks));
        assertFalse(AcceptListener.isTrustedProxyPeer("fe80::1", networks));
    }

    @Test
    public void testWhitespaceAroundEntries() {
        assertTrue(AcceptListener.isTrustedProxyPeer("10.1.2.3", " 10.0.0.0/8 ; 172.16.0.0/12 "));
        assertTrue(AcceptListener.isTrustedProxyPeer("172.20.0.1", " 10.0.0.0/8 ; 172.16.0.0/12 "));
    }

    @Test
    public void testWildcardWithWhitespace() {
        assertTrue(AcceptListener.isTrustedProxyPeer("1.2.3.4", " * "));
    }

    // Subclass that feeds a fixed byte array through readAllPlain/readAllPlainWithTimeout,
    // avoiding a real NIO connection.
    private static class FakeMysqlChannel extends MysqlChannel {
        private final byte[] data;
        private int pos = 0;

        FakeMysqlChannel(String remoteIp, String proxyHeader) {
            super((StreamConnection) null);
            this.remoteIp = remoteIp;
            this.remoteHostPortString = remoteIp + ":12345";
            this.data = proxyHeader.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        protected int readAllPlain(ByteBuffer dst) {
            int n = dst.remaining();
            dst.put(data, pos, n);
            pos += n;
            return n;
        }

        @Override
        protected int realNetReadWithTimeout(ByteBuffer dst, long timeoutMs) throws IOException {
            return readAllPlain(dst);
        }
    }

    // Variant that simulates a timeout: realNetReadWithTimeout returns 0 (timeout signal),
    // causing readAllPlainWithTimeout to throw IOException.
    private static class TimeoutMysqlChannel extends FakeMysqlChannel {
        TimeoutMysqlChannel(String remoteIp) {
            super(remoteIp, "");
        }

        @Override
        protected int realNetReadWithTimeout(ByteBuffer dst, long timeoutMs) {
            return 0; // signals timeout to readAllPlainWithTimeout
        }
    }

    @Test
    public void testApplyProxyProtocol_noUpdate() throws IOException {
        record Case(String peerIp, String header, String networks) {}
        Case[] cases = {
                new Case("10.0.0.1",  "", ""),           // disabled
                new Case("192.168.1.1", "", "10.0.0.0/8"), // untrusted peer
                new Case("10.0.0.1", "PROXY UNKNOWN 1.2.3.4 5.6.7.8 1000 9030\r\n", "10.0.0.0/8"), // UNKNOWN family
        };
        for (Case c : cases) {
            FakeMysqlChannel channel = new FakeMysqlChannel(c.peerIp(), c.header());
            ConnectContext context = mock(ConnectContext.class);
            AcceptListener.applyProxyProtocol(channel, context, c.networks());
            assertEquals(c.peerIp(), channel.getRemoteIp());
            verify(context, never()).setRemoteIP(any());
        }
    }

    @Test
    public void testApplyProxyProtocol_trustedPeer_updatesAddress() throws IOException {
        FakeMysqlChannel channel = new FakeMysqlChannel("10.0.0.1",
                "PROXY TCP4 1.2.3.4 5.6.7.8 1000 9030\r\n");
        ConnectContext context = mock(ConnectContext.class);
        AcceptListener.applyProxyProtocol(channel, context, "10.0.0.0/8");
        assertEquals("1.2.3.4", channel.getRemoteIp());
        verify(context).setRemoteIP("1.2.3.4");
    }

    @Test
    public void testApplyProxyProtocol_timeout_throwsIoException() {
        TimeoutMysqlChannel channel = new TimeoutMysqlChannel("10.0.0.1");
        ConnectContext context = mock(ConnectContext.class);
        assertThrows(IOException.class,
                () -> AcceptListener.applyProxyProtocol(channel, context, "10.0.0.0/8"));
    }
}
