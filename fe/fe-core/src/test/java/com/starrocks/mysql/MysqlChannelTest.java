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

import com.starrocks.common.Config;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;
import org.xnio.conduits.ConduitStreamSinkChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class MysqlChannelTest {

    @Mocked
    private StreamConnection conn;

    @Mocked
    private ConduitStreamSinkChannel sinkChannel;

    // @Mocked on Channels makes JMockit intercept the static writeBlocking / flushBlocking
    // calls; without it the real XNIO code would block on the mocked sink channel.
    @Mocked
    private Channels channels;

    private long originalTimeout;

    @BeforeEach
    public void setUp() {
        originalTimeout = Config.mysql_send_packet_timeout_ms;
    }

    @AfterEach
    public void tearDown() {
        Config.mysql_send_packet_timeout_ms = originalTimeout;
    }

    private MysqlChannel newChannelWithConn() {
        // Constructor reads getPeerAddress() for diagnostics; return a real InetSocketAddress.
        InetSocketAddress peer = new InetSocketAddress("127.0.0.1", 12345);
        new Expectations() {
            {
                conn.getPeerAddress();
                result = peer;
                minTimes = 0;
            }
        };
        return new MysqlChannel(conn);
    }

    @Test
    public void testRealNetSendUsesTimedWriteAndFlushWhenTimeoutConfigured() throws Exception {
        Config.mysql_send_packet_timeout_ms = 30_000L;
        MysqlChannel channel = newChannelWithConn();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.position(buffer.limit());
        buffer.flip();

        new Expectations() {
            {
                conn.getSinkChannel();
                result = sinkChannel;
                minTimes = 1;

                Channels.writeBlocking(sinkChannel, buffer, 30_000L, TimeUnit.MILLISECONDS);
                result = 16;
                times = 1;

                Channels.flushBlocking(sinkChannel, 30_000L, TimeUnit.MILLISECONDS);
                result = true;
                times = 1;
            }
        };

        channel.realNetSend(buffer);
    }

    @Test
    public void testRealNetSendUsesUntimedWriteAndFlushWhenTimeoutDisabled() throws Exception {
        Config.mysql_send_packet_timeout_ms = 0L;
        MysqlChannel channel = newChannelWithConn();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.position(buffer.limit());
        buffer.flip();

        new Expectations() {
            {
                conn.getSinkChannel();
                result = sinkChannel;
                minTimes = 1;

                Channels.writeBlocking(sinkChannel, buffer);
                result = 16;
                times = 1;

                Channels.flushBlocking(sinkChannel);
                times = 1;
            }
        };

        channel.realNetSend(buffer);
    }

    @Test
    public void testRealNetSendThrowsOnPartialWriteAfterTimeout() throws Exception {
        Config.mysql_send_packet_timeout_ms = 5_000L;
        MysqlChannel channel = newChannelWithConn();
        ByteBuffer buffer = ByteBuffer.allocate(100);
        buffer.position(buffer.limit());
        buffer.flip();

        new Expectations() {
            {
                conn.getSinkChannel();
                result = sinkChannel;
                minTimes = 1;

                Channels.writeBlocking(sinkChannel, buffer, 5_000L, TimeUnit.MILLISECONDS);
                result = 40; // simulate timeout after partial write
                times = 1;
            }
        };

        IOException ex = Assertions.assertThrows(IOException.class, () -> channel.realNetSend(buffer));
        Assertions.assertTrue(ex.getMessage().contains("write=40"),
                "Exception message should report bytes actually written; got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("needToWrite=100"),
                "Exception message should report bytes requested; got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("timeoutMs=5000"),
                "Exception message should include the configured timeout; got: " + ex.getMessage());
    }

    @Test
    public void testRealNetSendThrowsOnFlushTimeout() throws Exception {
        Config.mysql_send_packet_timeout_ms = 5_000L;
        MysqlChannel channel = newChannelWithConn();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.position(buffer.limit());
        buffer.flip();

        new Expectations() {
            {
                conn.getSinkChannel();
                result = sinkChannel;
                minTimes = 1;

                Channels.writeBlocking(sinkChannel, buffer, 5_000L, TimeUnit.MILLISECONDS);
                result = 16;
                times = 1;

                Channels.flushBlocking(sinkChannel, 5_000L, TimeUnit.MILLISECONDS);
                result = false; // flush timed out
                times = 1;
            }
        };

        IOException ex = Assertions.assertThrows(IOException.class, () -> channel.realNetSend(buffer));
        Assertions.assertTrue(ex.getMessage().contains("Flush"),
                "Exception message should indicate flush failure; got: " + ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("timeoutMs=5000"),
                "Exception message should include the configured timeout; got: " + ex.getMessage());
    }
}
