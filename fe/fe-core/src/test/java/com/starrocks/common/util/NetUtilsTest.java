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

package com.starrocks.common.util;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NetUtilsTest {
    @Test
    public void testGetCidrPrefixLength() {
        for (int i = 0; i <= 32; i++) {
            String addr = "192.168.0.1/" + i;
            assertThat(NetUtils.getCidrPrefixLength(addr)).isEqualTo(i);
        }
    }

    @Test
    public void testIsIPInSubnet() {
        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "192.168.0.1/32")).isTrue();
        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "192.168.1.1/32")).isFalse();

        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "192.168.0.1/24")).isTrue();
        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "192.168.0.2/24")).isTrue();
        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "192.168.1.0/24")).isFalse();

        assertThat(NetUtils.isIPInSubnet("192.168.0.1", "10.0.0.0/8")).isFalse();
    }

    @Test
    public void testIsPortUsingWithBoundPort() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(loopback, 0));
            int port = serverSocket.getLocalPort();
            assertThat(NetUtils.isPortUsing(loopback.getHostAddress(), port)).isTrue();
        }
    }

    @Test
    public void testIsPortUsingWithFreePort() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        int port;
        try (ServerSocket serverSocket = new ServerSocket(0, 0, loopback)) {
            port = serverSocket.getLocalPort();
        }
        assertThat(NetUtils.isPortUsing(loopback.getHostAddress(), port)).isFalse();
    }

    @Test
    public void testIsPortUsingWithUnknownHost() {
        assertThatThrownBy(() -> NetUtils.isPortUsing("nonexistent.invalid", 1))
                .isInstanceOf(UnknownHostException.class);
    }

    @Test
    public void testIsPortUsingWithNonLocalAddressFallsBackToConnect() throws Exception {
        AtomicBoolean connectCalled = new AtomicBoolean(false);
        new MockUp<java.net.NetworkInterface>() {
            @Mock
            public java.net.NetworkInterface getByInetAddress(InetAddress address) throws SocketException {
                return null;
            }
        };
        new MockUp<Socket>() {
            @Mock
            public void connect(SocketAddress endpoint, int timeout) throws java.io.IOException {
                connectCalled.set(true);
                throw new java.io.IOException("connect failed");
            }
        };

        assertThat(NetUtils.isPortUsing("203.0.113.1", 65535)).isFalse();
        assertThat(connectCalled.get()).isTrue();
    }
}
